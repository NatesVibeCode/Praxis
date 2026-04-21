from __future__ import annotations

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability import (
    NativeOperatorSupportSnapshot,
    cutover_scoreboard_run,
    render_cutover_scoreboard,
)
from receipts import AppendOnlyWorkflowEvidenceWriter
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from runtime.execution import orchestrator as execution_orchestrator


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
                    "input_payload": {"step": 0, "allow_passthrough_echo": True},
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
                    "input_payload": {"step": 1, "allow_passthrough_echo": True},
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


def _successful_run(monkeypatch: pytest.MonkeyPatch) -> tuple[str, tuple[object, ...]]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    monkeypatch.setattr(execution_orchestrator, "safe_context_budget", lambda *_args, **_kwargs: 4096)
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return outcome.run_id, tuple(writer.evidence_timeline(outcome.run_id))


def _status_snapshot(*, run_id: str) -> dict[str, object]:
    return {
        "native_instance": {
            "mode": "repo_local",
            "profile": "praxis",
        },
        "run": {
            "run_id": run_id,
            "workflow_id": "workflow.alpha",
            "request_id": "request.alpha",
            "workflow_definition_id": "workflow_definition.alpha.v1",
            "current_state": "succeeded",
            "terminal_reason_code": "runtime.workflow_succeeded",
            "run_idempotency_key": "run.alpha",
            "context_bundle_id": "context.alpha",
            "authority_context_digest": "digest.alpha",
            "admission_decision_id": "decision.alpha",
            "requested_at": "2026-04-02T00:00:00+00:00",
            "admitted_at": "2026-04-02T00:00:01+00:00",
            "started_at": "2026-04-02T00:00:02+00:00",
            "finished_at": "2026-04-02T00:00:03+00:00",
            "last_event_id": "event.alpha",
        },
    }


def test_native_cutover_scoreboard_is_deterministic_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id, canonical_evidence = _successful_run(monkeypatch)
    last_evidence_seq = max(row.evidence_seq for row in canonical_evidence)
    support = NativeOperatorSupportSnapshot(
        outbox_depth=len(canonical_evidence),
        outbox_latest_evidence_seq=last_evidence_seq,
        checkpoint_id=f"checkpoint:dispatch:worker:cutover:{run_id}",
        subscription_id="dispatch:worker:cutover",
        subscription_last_evidence_seq=last_evidence_seq - 2,
        checkpoint_status="committed",
    )

    scoreboard = cutover_scoreboard_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
        status_snapshot=_status_snapshot(run_id=run_id),
        support=support,
    )
    reordered_scoreboard = cutover_scoreboard_run(
        run_id=run_id,
        canonical_evidence=tuple(reversed(canonical_evidence)),
        status_snapshot=dict(_status_snapshot(run_id=run_id)),
        support=support,
    )

    rendered = render_cutover_scoreboard(scoreboard)
    reordered_rendered = render_cutover_scoreboard(reordered_scoreboard)

    assert rendered == reordered_rendered
    assert scoreboard.readiness_state == "ready"
    assert scoreboard.completeness.is_complete is True
    assert "readiness.state: ready" in rendered
    assert f"receipts.receipt_count: {len(canonical_evidence) // 2}" in rendered
    assert f"operator_proofs.graph_lineage.completeness.is_complete: true" in rendered

    drifted_status = _status_snapshot(run_id=run_id)
    drifted_status["run"] = dict(drifted_status["run"])
    drifted_status["run"]["run_id"] = "run.other"

    blocked_scoreboard = cutover_scoreboard_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
        status_snapshot=drifted_status,
        support=support,
    )
    blocked_rendered = render_cutover_scoreboard(blocked_scoreboard)

    assert blocked_scoreboard.readiness_state == "blocked"
    assert blocked_scoreboard.completeness.is_complete is False
    assert "status:run_id_mismatch" in blocked_scoreboard.completeness.missing_evidence_refs
    assert "readiness.state: blocked" in blocked_rendered
