from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import json
from pathlib import Path

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability.operator_topology import cutover_scoreboard_run, render_cutover_scoreboard
from observability.operator_dashboard import NativeOperatorSupportSnapshot
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RunState, RuntimeOrchestrator, WorkflowIntakePlanner
from runtime._helpers import _format_bool

_QUEUE_FILENAME = "DAGW23C_bounded_burnin_proof.queue.json"
_RUN_COUNT = 3


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _queue_path() -> Path:
    return _repo_root() / "artifacts" / "dispatch" / _QUEUE_FILENAME


def _load_queue() -> dict[str, object]:
    return json.loads(_queue_path().read_text(encoding="utf-8"))


def _repo_local_resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=str(_repo_root()),
                    workdir=str(_repo_root()),
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


def _request(run_index: int) -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    suffix = f"burnin.{run_index}"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=f"workflow.alpha.{suffix}",
        request_id=f"request.alpha.{suffix}",
        workflow_definition_id=f"workflow_definition.alpha.v1.{suffix}",
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


def _last_workflow_event_id(canonical_evidence: tuple[EvidenceRow, ...]) -> str:
    for row in reversed(canonical_evidence):
        if row.kind == "workflow_event":
            return row.record.event_id
    raise AssertionError("workflow event row not found")


def _shared_text(values: tuple[str, ...]) -> str | None:
    if not values:
        return None
    first = values[0]
    if all(value == first for value in values):
        return first
    return None


def _shared_int(values: tuple[int, ...]) -> int | None:
    if not values:
        return None
    first = values[0]
    if all(value == first for value in values):
        return first
    return None


def _receipt_rows(canonical_evidence: tuple[EvidenceRow, ...]) -> tuple[EvidenceRow, ...]:
    return tuple(
        row
        for row in sorted(canonical_evidence, key=lambda item: (item.evidence_seq, item.row_id))
        if row.kind == "receipt"
    )


@dataclass(frozen=True, slots=True)
class _BurnInRunSurface:
    run_index: int
    run_id: str
    request_id: str
    workflow_id: str
    terminal_reason_code: str
    node_order: tuple[str, ...]
    evidence_count: int
    receipt_count: int
    scoreboard: object
    scoreboard_render: str


def _assert_receipt_to_scoreboard_reconciliation(
    *,
    canonical_evidence: tuple[EvidenceRow, ...],
    scoreboard,
) -> None:
    ordered_rows = tuple(sorted(canonical_evidence, key=lambda item: (item.evidence_seq, item.row_id)))
    receipt_rows = _receipt_rows(canonical_evidence)

    assert scoreboard.receipts.row_count == len(ordered_rows)
    assert scoreboard.receipts.receipt_count == len(receipt_rows)
    assert scoreboard.receipts.latest_evidence_seq == ordered_rows[-1].evidence_seq
    assert scoreboard.receipts.evidence_refs == tuple(row.row_id for row in ordered_rows)
    assert scoreboard.receipts.receipt_ids == tuple(row.record.receipt_id for row in receipt_rows)
    assert scoreboard.receipts.receipt_types == tuple(row.record.receipt_type for row in receipt_rows)
    assert scoreboard.receipts.workflow_id == receipt_rows[0].record.workflow_id
    assert scoreboard.receipts.request_id == receipt_rows[0].record.request_id
    assert scoreboard.receipts.completeness.is_complete is True
    assert scoreboard.receipts.completeness.missing_evidence_refs == ()


def _bounded_run(run_index: int) -> _BurnInRunSurface:
    planner = WorkflowIntakePlanner(registry=_repo_local_resolver())
    outcome = planner.plan(request=_request(run_index))
    writer = AppendOnlyWorkflowEvidenceWriter()
    result = RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )

    assert result.current_state is RunState.SUCCEEDED
    assert result.terminal_reason_code == "runtime.workflow_succeeded"
    assert result.node_order == ("node_0", "node_1")

    canonical_evidence = tuple(writer.evidence_timeline(result.run_id))
    last_evidence_seq = max(row.evidence_seq for row in canonical_evidence)
    last_event_id = _last_workflow_event_id(canonical_evidence)

    support = NativeOperatorSupportSnapshot(
        outbox_depth=len(canonical_evidence),
        outbox_latest_evidence_seq=last_evidence_seq,
        checkpoint_id=f"checkpoint:dispatch:worker:burnin:{result.run_id}",
        subscription_id="dispatch:worker:burnin",
        subscription_last_evidence_seq=last_evidence_seq - 2,
        checkpoint_status="committed",
    )
    status_snapshot = {
        "run": {
            "run_id": result.run_id,
            "workflow_id": outcome.workflow_request.workflow_id,
            "workflow_definition_id": outcome.workflow_request.workflow_definition_id,
            "request_id": outcome.workflow_request.request_id,
            "current_state": result.current_state.value,
            "terminal_reason_code": result.terminal_reason_code,
            "last_event_id": last_event_id,
        }
    }

    scoreboard = cutover_scoreboard_run(
        run_id=result.run_id,
        canonical_evidence=canonical_evidence,
        status_snapshot=status_snapshot,
        support=support,
    )
    scoreboard_render = render_cutover_scoreboard(scoreboard)

    _assert_receipt_to_scoreboard_reconciliation(
        canonical_evidence=canonical_evidence,
        scoreboard=scoreboard,
    )
    assert scoreboard.completeness.is_complete is True
    assert scoreboard.readiness_state == "ready"
    assert scoreboard.receipts.completeness.is_complete is True
    assert scoreboard.operator_proofs.completeness.is_complete is True

    return _BurnInRunSurface(
        run_index=run_index,
        run_id=result.run_id,
        request_id=outcome.workflow_request.request_id,
        workflow_id=outcome.workflow_request.workflow_id,
        terminal_reason_code=result.terminal_reason_code,
        node_order=result.node_order,
        evidence_count=len(canonical_evidence),
        receipt_count=len(writer.receipts(result.run_id)),
        scoreboard=scoreboard,
        scoreboard_render=scoreboard_render,
    )


def _render_burn_in_surface(runs: tuple[_BurnInRunSurface, ...]) -> str:
    ready_run_count = sum(run.scoreboard.readiness_state == "ready" for run in runs)
    complete_run_count = sum(run.scoreboard.completeness.is_complete for run in runs)
    shared_terminal_reason_code = _shared_text(tuple(run.terminal_reason_code for run in runs))
    shared_request_id = _shared_text(tuple(run.request_id for run in runs))
    shared_workflow_id = _shared_text(tuple(run.workflow_id for run in runs))
    shared_node_order = _shared_text(tuple(",".join(run.node_order) for run in runs))
    shared_evidence_count = _shared_int(tuple(run.evidence_count for run in runs))
    shared_receipt_count = _shared_int(tuple(run.receipt_count for run in runs))
    lines = [
        "kind: bounded_burnin_proof",
        f"burn_in_run_count: {len(runs)}",
        f"burn_in_ready_run_count: {ready_run_count}",
        f"burn_in_complete_run_count: {complete_run_count}",
        f"burn_in_all_ready: {_format_bool(ready_run_count == len(runs))}",
        f"burn_in_all_complete: {_format_bool(complete_run_count == len(runs))}",
        f"burn_in_shared_workflow_id: {shared_workflow_id or '-'}",
        f"burn_in_shared_request_id: {shared_request_id or '-'}",
        f"burn_in_shared_terminal_reason_code: {shared_terminal_reason_code or '-'}",
        f"burn_in_shared_node_order: {shared_node_order or '-'}",
        f"burn_in_shared_evidence_count: {shared_evidence_count if shared_evidence_count is not None else '-'}",
        f"burn_in_shared_receipt_count: {shared_receipt_count if shared_receipt_count is not None else '-'}",
    ]
    for run in runs:
        lines.extend(
            [
                f"burn_in_run[{run.run_index}].run_id: {run.run_id}",
                f"burn_in_run[{run.run_index}].watermark.evidence_seq: {run.scoreboard.watermark.evidence_seq}",
                f"burn_in_run[{run.run_index}].current_state: {run.scoreboard.status.current_state}",
                f"burn_in_run[{run.run_index}].readiness_state: {run.scoreboard.readiness_state}",
                f"burn_in_run[{run.run_index}].receipt_count: {run.receipt_count}",
            ]
        )
    return "\n".join(lines)


def test_bounded_burnin_proof_repeats_bounded_native_default_runs_truthfully() -> None:
    queue_payload = _load_queue()
    assert queue_payload["phase"] == "DAGW23C"
    assert queue_payload["workflow_id"] == "dag_wave23c_bounded_burnin_proof"
    assert queue_payload["anti_requirements"] == [
        "no broad cutover",
        "no hosted dashboard",
        "no synthetic fake metrics",
    ]
    _root = str(_repo_root())
    assert queue_payload["verify"] == [
        {
            "command": (
                f"PYTHONPATH='{_root}/Code&DBs/Workflow' "
                "python3 -m pytest -q "
                f"'{_root}/Code&DBs/Workflow/tests/integration/"
                "test_bounded_burnin_proof.py'"
            )
        },
        {
            "command": (
                f"cd '{_root}' && ./scripts/validate-queue.sh"
                f"'{_root}/artifacts/workflow/"
                "DAGW23C_bounded_burnin_proof.queue.json'"
            )
        },
    ]
    assert len(queue_payload["jobs"]) == 1
    assert queue_payload["jobs"][0]["label"] == "DAGW23C.1_build_bounded_burnin_proof"
    assert queue_payload["jobs"][0]["prompt"].startswith("OBJECTIVE:\nBuild one bounded burn-in proof.")

    runs = tuple(_bounded_run(run_index) for run_index in range(_RUN_COUNT))
    surface = _render_burn_in_surface(runs)

    assert len({run.run_id for run in runs}) == _RUN_COUNT
    assert "kind: bounded_burnin_proof" in surface
    assert "burn_in_run_count: 3" in surface
    assert "burn_in_ready_run_count: 3" in surface
    assert "burn_in_complete_run_count: 3" in surface
    assert "burn_in_all_ready: true" in surface
    assert "burn_in_all_complete: true" in surface
    assert "burn_in_shared_evidence_count: 18" in surface
    assert "burn_in_shared_receipt_count: 9" in surface
    assert "burn_in_shared_terminal_reason_code: runtime.workflow_succeeded" in surface
    assert "burn_in_shared_node_order: node_0,node_1" in surface
    assert queue_payload["verify"][1]["command"] == (
        f"cd '{_root}' && ./scripts/validate-queue.sh"
        f"'{_root}/artifacts/workflow/"
        "DAGW23C_bounded_burnin_proof.queue.json'"
    )

    for run in runs:
        assert "kind: cutover_scoreboard" in run.scoreboard_render
        assert "readiness.state: ready" in run.scoreboard_render
        assert "completeness.is_complete: true" in run.scoreboard_render
        assert "receipts.receipt_count: 9" in run.scoreboard_render
        assert "status.current_state: succeeded" in run.scoreboard_render
        assert "operator_proofs.graph_lineage.completeness.is_complete: true" in run.scoreboard_render
        assert f"burn_in_run[{run.run_index}].run_id: {run.run_id}" in surface
        assert f"burn_in_run[{run.run_index}].watermark.evidence_seq: 18" in surface
        assert f"burn_in_run[{run.run_index}].current_state: succeeded" in surface
        assert f"burn_in_run[{run.run_index}].readiness_state: ready" in surface
        assert f"burn_in_run[{run.run_index}].receipt_count: 9" in surface
