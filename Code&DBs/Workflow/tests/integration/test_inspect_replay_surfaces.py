from __future__ import annotations

from dataclasses import replace
from io import StringIO

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
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from surfaces.cli import main


def _request(*, first_node_force_failure: bool = False) -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
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
                execution_boundary={"workspace_ref": workspace_ref},
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
                execution_boundary={"workspace_ref": workspace_ref},
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
                payload_mapping={"prepared_result": "result"},
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
                ),
            ),
        },
    )


def _execute_successful_run() -> tuple[AppendOnlyWorkflowEvidenceWriter, RuntimeOrchestrator, str]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    inspecting_orchestrator = RuntimeOrchestrator(evidence_reader=writer)
    return writer, inspecting_orchestrator, outcome.run_id


def test_runtime_inspect_run_derives_state_timeline_and_metadata_from_canonical_evidence() -> None:
    writer, orchestrator, run_id = _execute_successful_run()

    view = orchestrator.inspect_run(run_id=run_id)

    assert view.request_id == "request.alpha"
    assert view.completeness.is_complete is True
    assert view.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert view.current_state == "succeeded"
    assert view.node_timeline == (
        "node_0:running",
        "node_0:succeeded",
        "node_1:running",
        "node_1:succeeded",
    )
    assert view.terminal_reason == "runtime.workflow_succeeded"
    assert view.evidence_refs == tuple(row.row_id for row in writer.evidence_timeline(run_id))


def test_runtime_replay_run_derives_dependency_order_and_definition_binding_from_canonical_evidence() -> None:
    writer, orchestrator, run_id = _execute_successful_run()

    view = orchestrator.replay_run(run_id=run_id)

    assert view.request_id == "request.alpha"
    assert view.completeness.is_complete is True
    assert view.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert view.admitted_definition_ref == "workflow_definition.alpha.v1"
    assert view.dependency_order == ("node_0", "node_1")
    assert view.node_outcomes == ("node_0:succeeded", "node_1:succeeded")
    assert view.terminal_reason == "runtime.workflow_succeeded"


def test_cli_frontdoor_renders_derived_inspect_and_replay_views() -> None:
    writer, orchestrator, run_id = _execute_successful_run()

    inspect_stdout = StringIO()
    inspect_exit = main(
        ["inspect", run_id],
        runtime_orchestrator=orchestrator,
        stdout=inspect_stdout,
    )

    replay_stdout = StringIO()
    replay_exit = main(
        ["replay", run_id],
        runtime_orchestrator=orchestrator,
        stdout=replay_stdout,
    )

    assert inspect_exit == 0
    assert replay_exit == 0
    assert "kind: inspection" in inspect_stdout.getvalue()
    assert "current_state: succeeded" in inspect_stdout.getvalue()
    assert f"watermark_seq: {writer.last_evidence_seq(run_id)}" in inspect_stdout.getvalue()
    assert "kind: replay" in replay_stdout.getvalue()
    assert "dependency_order: node_0, node_1" in replay_stdout.getvalue()
    assert "terminal_reason: runtime.workflow_succeeded" in replay_stdout.getvalue()


def test_runtime_inspect_and_replay_surface_typed_frontier_failure_reason() -> None:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    missing_output_request = replace(
        outcome.workflow_request,
        edges=tuple(
            replace(edge, payload_mapping={"prepared_result": "missing_output"})
            for edge in outcome.workflow_request.edges
        ),
    )
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=replace(outcome, workflow_request=missing_output_request),
        evidence_writer=writer,
    )
    orchestrator = RuntimeOrchestrator(evidence_reader=writer)

    inspect_view = orchestrator.inspect_run(run_id=outcome.run_id)
    replay_view = orchestrator.replay_run(run_id=outcome.run_id)
    failure_event = next(
        row
        for row in writer.evidence_timeline(outcome.run_id)
        if row.kind == "workflow_event" and row.record.event_type == "workflow_failed"
    )

    assert inspect_view.current_state == "failed"
    assert inspect_view.terminal_reason == "runtime.dependency_missing_output"
    assert replay_view.terminal_reason == "runtime.dependency_missing_output"
    assert failure_event.record.payload["failure_reason"]["reason_code"] == (
        "runtime.dependency_missing_output"
    )
    assert failure_event.record.payload["failure_reason"]["source_key"] == "missing_output"
