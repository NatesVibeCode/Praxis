from __future__ import annotations

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability import inspect_run, replay_run
from receipts import AppendOnlyWorkflowEvidenceWriter
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner


def _deterministic_fixture_outputs(input_payload: dict[str, object]) -> dict[str, object]:
    step = input_payload.get("step")
    if step == 0:
        return {"result": "prepared"}
    if step == 1:
        return {"result": "admitted"}
    raise AssertionError(f"unexpected deterministic fixture step: {step!r}")


def _request() -> WorkflowRequest:
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
                        "deterministic_builder": (
                            "tests.integration.test_inspect_and_replay_slice."
                            "_deterministic_fixture_outputs"
                        ),
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
                    "input_payload": {
                        "step": 1,
                        "deterministic_builder": (
                            "tests.integration.test_inspect_and_replay_slice."
                            "_deterministic_fixture_outputs"
                        ),
                    },
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
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _canonical_success_slice() -> tuple[str, AppendOnlyWorkflowEvidenceWriter]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return outcome.run_id, writer


def test_inspect_and_replay_readers_derive_complete_views_from_canonical_evidence() -> None:
    run_id, writer = _canonical_success_slice()
    canonical_evidence = tuple(writer.evidence_timeline(run_id))

    inspection = inspect_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    replay = replay_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    assert inspection.request_id == "request.alpha"
    assert inspection.completeness.is_complete is True
    assert inspection.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert inspection.current_state == "succeeded"
    assert inspection.node_timeline == (
        "node_0:running",
        "node_0:succeeded",
        "node_1:running",
        "node_1:succeeded",
    )
    assert inspection.terminal_reason == "runtime.workflow_succeeded"

    assert replay.request_id == "request.alpha"
    assert replay.completeness.is_complete is True
    assert replay.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert replay.admitted_definition_ref == "workflow_definition.alpha.v1"
    assert replay.dependency_order == ("node_0", "node_1")
    assert replay.node_outcomes == ("node_0:succeeded", "node_1:succeeded")
    assert replay.terminal_reason == "runtime.workflow_succeeded"
    assert replay.path_break is None


def test_inspect_and_replay_readers_surface_incomplete_evidence_with_watermark_and_missing_refs() -> None:
    run_id, writer = _canonical_success_slice()
    canonical_evidence = tuple(
        row
        for row in writer.evidence_timeline(run_id)
        if not (
            row.kind == "receipt"
            and getattr(row.record, "receipt_type", None) == "node_execution_receipt"
            and getattr(row.record, "node_id", None) == "node_0"
        )
    )

    inspection = inspect_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    replay = replay_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    assert inspection.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert inspection.current_state == "succeeded"
    assert inspection.completeness.is_complete is False
    assert "transition:6:receipt" in inspection.completeness.missing_evidence_refs
    assert "transition:6:bundle_size" in inspection.completeness.missing_evidence_refs

    assert replay.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert replay.completeness.is_complete is False
    assert "transition:6:receipt" in replay.completeness.missing_evidence_refs
    assert "transition:6:bundle_size" in replay.completeness.missing_evidence_refs
    assert any(ref.startswith("receipt:receipt:run:workflow.alpha:") for ref in replay.completeness.missing_evidence_refs)
    assert "node:node_0:outcome" in replay.completeness.missing_evidence_refs
    assert "node:node_1:dependency_receipts" in replay.completeness.missing_evidence_refs
    assert replay.path_break is not None
    assert replay.path_break.missing_ref == "evidence_seq:12"
    assert replay.path_break.reason_code == "evidence.sequence_gap"
    assert replay.path_break.evidence_seq == 12
    assert replay.path_break.expected == "contiguous canonical evidence sequence"
