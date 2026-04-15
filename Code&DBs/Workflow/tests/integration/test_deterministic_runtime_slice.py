from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from receipts import AppendOnlyWorkflowEvidenceWriter
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RunState, RuntimeOrchestrator, WorkflowIntakePlanner


def _request(
    *,
    first_node_force_failure: bool = False,
    payload_mapping: dict[str, str] | None = None,
    node_execution_boundary_ref: str | None = None,
) -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    execution_boundary_ref = node_execution_boundary_ref or workspace_ref
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={
                    "task_name": "prepare",
                    "input_payload": {
                        "step": 0,
                        "force_failure": first_node_force_failure,
                    },
                },
                expected_outputs={"result": "prepared"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": execution_boundary_ref},
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="node_1",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="admit",
                inputs={
                    "task_name": "admit",
                    "input_payload": {"step": 1},
                },
                expected_outputs={"result": "admitted"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": execution_boundary_ref},
                position_index=1,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_1",
                release_condition={"upstream_result": "success"},
                payload_mapping=payload_mapping or {"prepared_result": "result"},
                position_index=0,
            ),
        ),
    )


def _resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _event_types(writer: AppendOnlyWorkflowEvidenceWriter, run_id: str) -> list[str]:
    return [
        row.record.event_type
        for row in writer.evidence_timeline(run_id)
        if row.kind == "workflow_event"
    ]


def _node_event_rows(
    writer: AppendOnlyWorkflowEvidenceWriter,
    run_id: str,
    *,
    event_type: str,
) -> Sequence:
    return [
        row
        for row in writer.evidence_timeline(run_id)
        if row.kind == "workflow_event" and row.record.event_type == event_type
    ]


def _receipt_by_type(
    writer: AppendOnlyWorkflowEvidenceWriter,
    run_id: str,
    *,
    receipt_type: str,
):
    return next(
        receipt
        for receipt in writer.receipts(run_id)
        if receipt.receipt_type == receipt_type
    )


def test_runtime_executes_the_minimal_path_in_after_success_order() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.SUCCEEDED
    assert result.terminal_reason_code == "runtime.workflow_succeeded"
    assert result.node_order == ("node_0", "node_1")
    assert [node.node_id for node in result.node_results] == ["node_0", "node_1"]

    events = _event_types(writer, result.run_id)
    assert events == [
        "claim_received",
        "claim_validated",
        "workflow_queued",
        "workflow_started",
        "node_started",
        "node_succeeded",
        "node_started",
        "node_succeeded",
        "workflow_succeeded",
    ]

    node_started_rows = _node_event_rows(writer, result.run_id, event_type="node_started")
    upstream_completion_receipt_id = result.node_results[0].completion_receipt_id
    assert (
        node_started_rows[1].record.payload["dependency_receipts"][0]["upstream_receipt_id"]
        == upstream_completion_receipt_id
    )
    upstream_completion_row = next(
        row
        for row in writer.evidence_timeline(result.run_id)
        if row.row_id == upstream_completion_receipt_id
    )
    assert node_started_rows[1].record.evidence_seq > upstream_completion_row.evidence_seq

    node_receipts = writer.receipts(result.run_id)
    assert node_receipts[-1].receipt_type == "workflow_completion_receipt"
    assert node_receipts[-1].status == "succeeded"
    downstream_execution_receipt = next(
        receipt
        for receipt in node_receipts
        if receipt.node_id == "node_1"
        and receipt.receipt_type == "node_execution_receipt"
    )
    assert downstream_execution_receipt.inputs["dependency_inputs"] == {
        "prepared_result": "prepared"
    }


def test_runtime_uses_default_context_budget_when_model_slug_is_missing(
    monkeypatch,
) -> None:
    import runtime.execution.orchestrator as execution_orchestrator

    monkeypatch.setattr(
        execution_orchestrator,
        "safe_context_budget",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("model-specific context budget should not be resolved without model_slug")
        ),
    )

    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()

    result = RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.SUCCEEDED
    assert result.node_order == ("node_0", "node_1")


def test_runtime_uses_admitted_authority_boundary_instead_of_request_boundary() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    spoofed_request = replace(
        outcome.workflow_request,
        nodes=tuple(
            replace(
                node,
                execution_boundary={"workspace_ref": "workspace.spoofed"},
            )
            for node in outcome.workflow_request.nodes
        ),
    )
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=replace(outcome, workflow_request=spoofed_request),
        evidence_writer=writer,
    )

    assert result.current_state is RunState.SUCCEEDED
    node_started_rows = _node_event_rows(writer, result.run_id, event_type="node_started")
    assert node_started_rows[0].record.payload["node_id"] == "node_0"
    assert _receipt_by_type(
        writer,
        result.run_id,
        receipt_type="node_start_receipt",
    ).inputs["execution_boundary_ref"] == "workspace.alpha"
    downstream_execution_receipt = next(
        receipt
        for receipt in writer.receipts(result.run_id)
        if receipt.node_id == "node_1"
        and receipt.receipt_type == "node_execution_receipt"
    )
    assert downstream_execution_receipt.inputs["execution_boundary_ref"] == "workspace.alpha"


def test_runtime_fails_closed_and_does_not_release_downstream_after_failed_dependency() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request(first_node_force_failure=True))
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.FAILED
    assert "failed" in result.terminal_reason_code or "not_satisfied" in result.terminal_reason_code
    assert "node_0" in result.node_order
    assert result.node_results[0].failure_code == "adapter.command_failed"
    assert any(node.node_id == "node_1" and node.status == "skipped" for node in result.node_results)

    events = _event_types(writer, result.run_id)
    assert events == [
        "claim_received",
        "claim_validated",
        "workflow_queued",
        "workflow_started",
        "node_started",
        "node_failed",
        "node_skipped",
        "workflow_failed",
    ]
    assert [row.record.node_id for row in _node_event_rows(writer, result.run_id, event_type="node_started")] == [
        "node_0"
    ]
    skipped_receipt = next(
        receipt
        for receipt in writer.receipts(result.run_id)
        if receipt.node_id == "node_1"
        and receipt.receipt_type == "node_execution_receipt"
    )
    assert skipped_receipt.status == "skipped"
    last_failure = writer.receipts(result.run_id)[-1].failure_code
    assert last_failure in ("adapter.command_failed", "runtime.dependency_edge_not_satisfied")


def test_runtime_fails_closed_when_dependency_mapping_cannot_be_satisfied() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(
        request=_request(payload_mapping={"prepared_result": "missing_output"})
    )
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.FAILED
    assert result.terminal_reason_code == "runtime.dependency_missing_output"
    assert result.node_order == ("node_0",)
    assert [node.node_id for node in result.node_results] == ["node_0"]
    assert all(receipt.node_id != "node_1" for receipt in writer.receipts(result.run_id))
    failure_receipt = writer.receipts(result.run_id)[-1]
    failure_reason = failure_receipt.inputs["failure_reason"]
    assert failure_receipt.failure_code == "runtime.dependency_missing_output"
    assert failure_reason["reason_code"] == "runtime.dependency_missing_output"
    assert failure_reason["source_key"] == "missing_output"
    assert failure_reason["node_id"] == "node_1"


def test_runtime_fails_closed_when_admitted_graph_uses_unsupported_edge_semantics() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    unsupported_request = replace(
        outcome.workflow_request,
        edges=tuple(
            replace(edge, edge_type="invented_edge_type")
            for edge in outcome.workflow_request.edges
        ),
    )
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=replace(outcome, workflow_request=unsupported_request),
        evidence_writer=writer,
    )

    assert result.current_state is RunState.FAILED
    assert "unsupported_edge" in result.terminal_reason_code or "not_satisfied" in result.terminal_reason_code


def test_runtime_fails_closed_when_frontier_makes_no_progress() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    stalled_request = replace(
        outcome.workflow_request,
        edges=tuple(
            replace(edge, from_node_id="node_missing")
            for edge in outcome.workflow_request.edges
        ),
    )
    writer = AppendOnlyWorkflowEvidenceWriter()
    orchestrator = RuntimeOrchestrator()

    result = orchestrator.execute_deterministic_path(
        intake_outcome=replace(outcome, workflow_request=stalled_request),
        evidence_writer=writer,
    )

    assert result.current_state is RunState.FAILED
    assert result.terminal_reason_code == "runtime.frontier_no_progress"
    failure_receipt = writer.receipts(result.run_id)[-1]
    failure_reason = failure_receipt.inputs["failure_reason"]
    assert failure_reason["reason_code"] == "runtime.frontier_no_progress"
    assert failure_reason["pending_node_ids"] == ("node_1",)
    assert failure_reason["completed_node_ids"] == ("node_0",)
