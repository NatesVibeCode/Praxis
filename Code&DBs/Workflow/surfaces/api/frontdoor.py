"""Native DAG frontdoor over repo-local authority.

This surface is intentionally narrow:

- native instance authority resolves first and fails closed
- submit delegates to runtime intake truth plus explicit Postgres persistence
- status reads the durable workflow run row and optionally adds derived evidence views
- health delegates to repo-local Postgres health/bootstrap helpers
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
import json
import os
from typing import Any, Protocol

from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from observability.read_models import InspectionReadModel
from observability.status_observability import build_frontdoor_observability
from policy.domain import AdmissionDecisionRecord
from registry.domain import RegistryResolver
from runtime.execution import RuntimeOrchestrator
from runtime.instance import NativeWorkflowInstance, resolve_native_instance
from runtime.intake import WorkflowIntakeOutcome, WorkflowIntakePlanner
from storage.dev_postgres import local_postgres_bootstrap, local_postgres_health
from storage.postgres import (
    PostgresEvidenceReader,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowAdmissionWriteResult,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    persist_workflow_admission,
)

from ._operator_helpers import _json_compatible, _now, _run_async as _shared_run_async


class NativeFrontdoorError(RuntimeError):
    """Raised when the native frontdoor cannot complete safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


class _JsonStatus(Protocol):
    def to_json(self) -> dict[str, Any]:
        """Return a JSON-safe status payload."""


class _Connection(Protocol):
    async def fetch(self, query: str, *args: object) -> Sequence[Any]:
        """Return many rows."""

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Return one row."""

    async def close(self) -> None:
        """Close the connection."""


_RunQuery = """
SELECT
    run_id,
    workflow_id,
    request_id,
    request_digest,
    workflow_definition_id,
    admitted_definition_hash,
    current_state,
    terminal_reason_code,
    run_idempotency_key,
    context_bundle_id,
    authority_context_digest,
    admission_decision_id,
    packet_inspection,
    request_envelope,
    requested_at,
    admitted_at,
    started_at,
    finished_at,
    last_event_id
FROM workflow_runs
WHERE run_id = $1
"""

_LegacyRunQuery = """
SELECT
    run_id,
    workflow_id,
    request_id,
    request_digest,
    workflow_definition_id,
    admitted_definition_hash,
    current_state,
    terminal_reason_code,
    run_idempotency_key,
    context_bundle_id,
    authority_context_digest,
    admission_decision_id,
    NULL::jsonb AS packet_inspection,
    request_envelope,
    requested_at,
    admitted_at,
    started_at,
    finished_at,
    last_event_id
FROM workflow_runs
WHERE run_id = $1
"""

_ExecutionPacketsQuery = """
SELECT COALESCE(
    json_agg(payload ORDER BY created_at, execution_packet_id),
    '[]'::jsonb
) AS packets
FROM execution_packets
WHERE run_id = $1
"""


@dataclass(frozen=True, slots=True)
class _RunRowLoadMetadata:
    contract_drift_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _RunStatusObservabilityHints:
    packet_inspection_source: str
    contract_drift_refs: tuple[str, ...] = ()


def _missing_packet_inspection_column_error(exc: Exception) -> bool:
    sqlstate = str(getattr(exc, "sqlstate", "") or "").strip()
    if sqlstate and sqlstate != "42703":
        return False
    message = str(exc).lower()
    if "packet_inspection" not in message:
        return False
    return "does not exist" in message or "undefined column" in message


async def _fetch_run_row(
    conn: _Connection,
    *,
    run_id: str,
) -> tuple[Any, _RunRowLoadMetadata]:
    try:
        return await conn.fetchrow(_RunQuery, run_id), _RunRowLoadMetadata()
    except Exception as exc:
        if not _missing_packet_inspection_column_error(exc):
            raise
    return await conn.fetchrow(_LegacyRunQuery, run_id), _RunRowLoadMetadata(
        contract_drift_refs=("workflow_runs.packet_inspection_column_missing",),
    )

_RunJobsWithSubmissionQuery = """
SELECT
    j.label,
    j.status,
    j.attempt,
    j.agent_slug,
    j.resolved_agent,
    j.last_error_code,
    j.created_at,
    j.ready_at,
    j.claimed_at,
    j.started_at,
    j.finished_at,
    s.submission_id,
    s.result_kind AS submission_result_kind,
    s.summary AS submission_summary,
    s.comparison_status AS submission_comparison_status,
    s.acceptance_status AS submission_acceptance_status,
    s.operation_set AS submission_operation_set,
    r.decision AS latest_submission_review_decision,
    r.summary AS latest_submission_review_summary
FROM workflow_jobs AS j
LEFT JOIN LATERAL (
    SELECT submission_id, result_kind, summary, comparison_status, acceptance_status, operation_set
    FROM workflow_job_submissions
    WHERE run_id = j.run_id AND job_label = j.label
    ORDER BY attempt_no DESC, sealed_at DESC, submission_id DESC
    LIMIT 1
) AS s ON TRUE
LEFT JOIN LATERAL (
    SELECT decision, summary
    FROM workflow_job_submission_reviews
    WHERE submission_id = s.submission_id
    ORDER BY reviewed_at DESC, review_id DESC
    LIMIT 1
) AS r ON TRUE
WHERE j.run_id = $1
ORDER BY j.created_at, j.id
"""


def _run_async(awaitable: Awaitable[Any]) -> Any:
    return _shared_run_async(
        awaitable,
        error_type=NativeFrontdoorError,
        reason_code="frontdoor.async_boundary_required",
        message="native frontdoor sync entrypoints require a non-async call boundary",
    )


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise NativeFrontdoorError(
            "frontdoor.invalid_request",
            f"{field_name} must be an object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _json_loads_maybe(value: object, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return default
    return value


def _measured_summary(operation_set: object) -> dict[str, int]:
    operations = _json_loads_maybe(operation_set, [])
    summary = {"create": 0, "update": 0, "delete": 0, "rename": 0}
    if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes, bytearray)):
        summary["total"] = 0
        return summary
    for item in operations:
        if not isinstance(item, Mapping):
            continue
        action = str(item.get("action") or "").strip().lower()
        if action in summary:
            summary[action] += 1
    summary["total"] = sum(summary.values())
    return summary


def _submission_summary_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    submission_id = str(row.get("submission_id") or "").strip()
    if not submission_id:
        return None
    payload: dict[str, Any] = {
        "submission_id": submission_id,
        "result_kind": str(row.get("submission_result_kind") or "").strip() or None,
        "summary": str(row.get("submission_summary") or "").strip() or None,
        "comparison_status": str(row.get("submission_comparison_status") or "").strip() or None,
        "integrity_status": str(row.get("submission_comparison_status") or "").strip() or None,
        "acceptance_status": str(row.get("submission_acceptance_status") or "").strip() or None,
        "measured_summary": _measured_summary(row.get("submission_operation_set")),
        "latest_review_decision": str(row.get("latest_submission_review_decision") or "").strip() or None,
        "latest_review_summary": str(row.get("latest_submission_review_summary") or "").strip() or None,
    }
    return payload


async def _load_run_jobs_with_submission_summary(
    conn: _Connection,
    *,
    run_id: str,
) -> list[dict[str, Any]]:
    try:
        rows = await conn.fetch(_RunJobsWithSubmissionQuery, run_id)
    except Exception:
        return []
    jobs: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        job_summary: dict[str, Any] = {
            "label": str(payload.get("label") or "").strip(),
            "status": str(payload.get("status") or "").strip(),
            "attempt": int(payload.get("attempt") or 0),
            "agent_slug": str(payload.get("resolved_agent") or payload.get("agent_slug") or "").strip(),
            "last_error_code": str(payload.get("last_error_code") or "").strip() or None,
            "created_at": _json_compatible(payload.get("created_at")),
            "ready_at": _json_compatible(payload.get("ready_at")),
            "claimed_at": _json_compatible(payload.get("claimed_at")),
            "started_at": _json_compatible(payload.get("started_at")),
            "finished_at": _json_compatible(payload.get("finished_at")),
        }
        submission = _submission_summary_from_row(payload)
        if submission is not None:
            job_summary["submission"] = submission
        jobs.append(job_summary)
    return jobs


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NativeFrontdoorError(
            "frontdoor.invalid_request",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise NativeFrontdoorError(
            "frontdoor.invalid_request",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _node_from_mapping(index: int, payload: Mapping[str, Any]) -> WorkflowNodeContract:
    return WorkflowNodeContract(
        node_id=_require_text(payload.get("node_id"), field_name=f"nodes[{index}].node_id"),
        node_type=_require_text(payload.get("node_type"), field_name=f"nodes[{index}].node_type"),
        adapter_type=_require_text(
            payload.get("adapter_type"),
            field_name=f"nodes[{index}].adapter_type",
        ),
        display_name=_require_text(
            payload.get("display_name"),
            field_name=f"nodes[{index}].display_name",
        ),
        inputs=_require_mapping(payload.get("inputs"), field_name=f"nodes[{index}].inputs"),
        expected_outputs=_require_mapping(
            payload.get("expected_outputs"),
            field_name=f"nodes[{index}].expected_outputs",
        ),
        success_condition=_require_mapping(
            payload.get("success_condition"),
            field_name=f"nodes[{index}].success_condition",
        ),
        failure_behavior=_require_mapping(
            payload.get("failure_behavior"),
            field_name=f"nodes[{index}].failure_behavior",
        ),
        authority_requirements=_require_mapping(
            payload.get("authority_requirements"),
            field_name=f"nodes[{index}].authority_requirements",
        ),
        execution_boundary=_require_mapping(
            payload.get("execution_boundary"),
            field_name=f"nodes[{index}].execution_boundary",
        ),
        position_index=_require_int(
            payload.get("position_index"),
            field_name=f"nodes[{index}].position_index",
        ),
    )


def _edge_from_mapping(index: int, payload: Mapping[str, Any]) -> WorkflowEdgeContract:
    return WorkflowEdgeContract(
        edge_id=_require_text(payload.get("edge_id"), field_name=f"edges[{index}].edge_id"),
        edge_type=_require_text(payload.get("edge_type"), field_name=f"edges[{index}].edge_type"),
        from_node_id=_require_text(
            payload.get("from_node_id"),
            field_name=f"edges[{index}].from_node_id",
        ),
        to_node_id=_require_text(
            payload.get("to_node_id"),
            field_name=f"edges[{index}].to_node_id",
        ),
        release_condition=_require_mapping(
            payload.get("release_condition"),
            field_name=f"edges[{index}].release_condition",
        ),
        payload_mapping=_require_mapping(
            payload.get("payload_mapping"),
            field_name=f"edges[{index}].payload_mapping",
        ),
        position_index=_require_int(
            payload.get("position_index"),
            field_name=f"edges[{index}].position_index",
        ),
    )


def _request_from_mapping(payload: Mapping[str, Any]) -> WorkflowRequest:
    raw_nodes = payload.get("nodes")
    raw_edges = payload.get("edges")
    if not isinstance(raw_nodes, Sequence) or isinstance(raw_nodes, (str, bytes, bytearray)):
        raise NativeFrontdoorError(
            "frontdoor.invalid_request",
            "nodes must be an array",
            details={"field": "nodes", "value_type": type(raw_nodes).__name__},
        )
    if not isinstance(raw_edges, Sequence) or isinstance(raw_edges, (str, bytes, bytearray)):
        raise NativeFrontdoorError(
            "frontdoor.invalid_request",
            "edges must be an array",
            details={"field": "edges", "value_type": type(raw_edges).__name__},
        )

    nodes = tuple(
        _node_from_mapping(index, _require_mapping(item, field_name=f"nodes[{index}]"))
        for index, item in enumerate(raw_nodes)
    )
    edges = tuple(
        _edge_from_mapping(index, _require_mapping(item, field_name=f"edges[{index}]"))
        for index, item in enumerate(raw_edges)
    )
    return WorkflowRequest(
        schema_version=_require_int(payload.get("schema_version"), field_name="schema_version"),
        workflow_id=_require_text(payload.get("workflow_id"), field_name="workflow_id"),
        request_id=_require_text(payload.get("request_id"), field_name="request_id"),
        workflow_definition_id=_require_text(
            payload.get("workflow_definition_id"),
            field_name="workflow_definition_id",
        ),
        definition_hash=_require_text(payload.get("definition_hash"), field_name="definition_hash"),
        workspace_ref=_require_text(payload.get("workspace_ref"), field_name="workspace_ref"),
        runtime_profile_ref=_require_text(
            payload.get("runtime_profile_ref"),
            field_name="runtime_profile_ref",
        ),
        nodes=nodes,
        edges=edges,
    )


def _request_envelope(request: WorkflowRequest) -> dict[str, Any]:
    workflow_definition_id = request.workflow_definition_id
    return {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": 1,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
        "nodes": [
            {
                "workflow_definition_node_id": f"{workflow_definition_id}:{node.node_id}",
                "workflow_definition_id": workflow_definition_id,
                "node_id": node.node_id,
                "node_type": node.node_type,
                "schema_version": request.schema_version,
                "adapter_type": node.adapter_type,
                "display_name": node.display_name,
                "inputs": dict(node.inputs),
                "expected_outputs": dict(node.expected_outputs),
                "success_condition": dict(node.success_condition),
                "failure_behavior": dict(node.failure_behavior),
                "authority_requirements": dict(node.authority_requirements),
                "execution_boundary": dict(node.execution_boundary),
                "position_index": node.position_index,
            }
            for node in request.nodes
        ],
        "edges": [
            {
                "workflow_definition_edge_id": f"{workflow_definition_id}:{edge.edge_id}",
                "workflow_definition_id": workflow_definition_id,
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "schema_version": request.schema_version,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": dict(edge.release_condition),
                "payload_mapping": dict(edge.payload_mapping),
                "position_index": edge.position_index,
            }
            for edge in request.edges
        ],
    }


def _submission_from_outcome(
    *,
    outcome: WorkflowIntakeOutcome,
    requested_at: datetime,
) -> WorkflowAdmissionSubmission:
    request = outcome.workflow_request
    decision = outcome.admission_decision
    decision_write = WorkflowAdmissionDecisionWrite(
        admission_decision_id=decision.admission_decision_id,
        workflow_id=request.workflow_id,
        request_id=request.request_id,
        decision=decision.decision.value,
        reason_code=decision.reason_code,
        decided_at=decision.decided_at,
        decided_by=decision.decided_by,
        policy_snapshot_ref=decision.policy_snapshot_ref,
        validation_result_ref=decision.validation_result_ref,
        authority_context_ref=decision.authority_context_ref,
    )
    run_write = WorkflowRunWrite(
        run_id=outcome.run_id,
        workflow_id=request.workflow_id,
        request_id=request.request_id,
        request_digest=outcome.request_digest,
        authority_context_digest=outcome.route_identity.authority_context_digest,
        workflow_definition_id=outcome.admitted_definition_ref or request.workflow_definition_id,
        admitted_definition_hash=outcome.admitted_definition_hash or request.definition_hash,
        run_idempotency_key=outcome.run_idempotency_key,
        schema_version=request.schema_version,
        request_envelope=_request_envelope(request),
        context_bundle_id=decision.authority_context_ref,
        admission_decision_id=decision.admission_decision_id,
        current_state=outcome.current_state.value,
        requested_at=requested_at,
        admitted_at=decision.decided_at,
        terminal_reason_code=None,
        started_at=None,
        finished_at=None,
        last_event_id=None,
    )
    return WorkflowAdmissionSubmission(decision=decision_write, run=run_write)


def _serialize_decision(decision: AdmissionDecisionRecord) -> dict[str, Any]:
    return {
        "admission_decision_id": decision.admission_decision_id,
        "workflow_id": decision.workflow_id,
        "request_id": decision.request_id,
        "decision": decision.decision.value,
        "reason_code": decision.reason_code,
        "decided_at": decision.decided_at.isoformat(),
        "decided_by": decision.decided_by,
        "policy_snapshot_ref": decision.policy_snapshot_ref,
        "validation_result_ref": decision.validation_result_ref,
        "authority_context_ref": decision.authority_context_ref,
    }


def _serialize_inspection(model: InspectionReadModel) -> dict[str, Any]:
    return {
        "run_id": model.run_id,
        "request_id": model.request_id,
        "current_state": model.current_state,
        "node_timeline": list(model.node_timeline),
        "terminal_reason": model.terminal_reason,
        "operator_frame_source": model.operator_frame_source,
        "operator_frames": [
            {
                "operator_frame_id": frame.operator_frame_id,
                "node_id": frame.node_id,
                "operator_kind": frame.operator_kind,
                "frame_state": frame.frame_state,
                "item_index": frame.item_index,
                "iteration_index": frame.iteration_index,
                "source_snapshot": dict(frame.source_snapshot or {}),
                "aggregate_outputs": dict(frame.aggregate_outputs or {}),
                "active_count": frame.active_count,
                "stop_reason": frame.stop_reason,
                "started_at": (
                    frame.started_at.isoformat() if frame.started_at is not None else None
                ),
                "finished_at": (
                    frame.finished_at.isoformat() if frame.finished_at is not None else None
                ),
            }
            for frame in model.operator_frames
        ],
        "completeness": {
            "is_complete": model.completeness.is_complete,
            "missing_evidence_refs": list(model.completeness.missing_evidence_refs),
        },
        "watermark": {
            "evidence_seq": model.watermark.evidence_seq,
            "source": model.watermark.source,
        },
        "evidence_refs": list(model.evidence_refs),
    }


def _load_sync_status(
    run_id: str,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    try:
        from runtime.post_workflow_sync import get_workflow_run_sync_status
        from storage.postgres import ensure_postgres_available

        status = get_workflow_run_sync_status(
            run_id,
            conn=ensure_postgres_available(env=env),
        )
        return status.to_json()
    except Exception:
        return {
            "run_id": run_id,
            "sync_status": "skipped",
            "sync_cycle_id": None,
            "sync_error_count": 0,
        }


def _default_evidence_reader_factory(env: Mapping[str, str] | None) -> PostgresEvidenceReader:
    return PostgresEvidenceReader(env=env)


@dataclass(slots=True)
class NativeDagFrontdoor:
    """Thin repo-local frontdoor for submit, status, and health."""

    registry: RegistryResolver | None = None
    postgres_health_service: Callable[[Mapping[str, str] | None], _JsonStatus] = local_postgres_health
    postgres_bootstrap_service: Callable[[Mapping[str, str] | None], _JsonStatus] = (
        local_postgres_bootstrap
    )
    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    bootstrap_schema: Callable[[_Connection], Awaitable[Any]] = bootstrap_control_plane_schema
    persist_submission: Callable[
        [_Connection],
        Awaitable[WorkflowAdmissionWriteResult],
    ] | None = None
    evidence_reader_factory: Callable[[Mapping[str, str] | None], PostgresEvidenceReader] = (
        _default_evidence_reader_factory
    )

    def __post_init__(self) -> None:
        if self.persist_submission is None:
            self.persist_submission = self._persist_submission

    async def _persist_submission(
        self,
        conn: _Connection,
        *,
        submission: WorkflowAdmissionSubmission,
    ) -> WorkflowAdmissionWriteResult:
        return await persist_workflow_admission(conn, submission=submission)

    def _require_registry(self) -> RegistryResolver:
        if self.registry is None:
            raise NativeFrontdoorError(
                "frontdoor.registry_missing",
                "submit requires an explicit registry authority",
            )
        return self.registry

    def _resolve_instance(self, *, env: Mapping[str, str] | None) -> tuple[Mapping[str, str], NativeWorkflowInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    def health(
        self,
        *,
        env: Mapping[str, str] | None = None,
        bootstrap: bool = False,
    ) -> dict[str, Any]:
        source, instance = self._resolve_instance(env=env)
        postgres_status = (
            self.postgres_bootstrap_service(source)
            if bootstrap
            else self.postgres_health_service(source)
        )
        return {
            "native_instance": instance.to_contract(),
            "database": postgres_status.to_json(),
        }

    def submit(
        self,
        *,
        request_payload: Mapping[str, Any],
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        source, instance = self._resolve_instance(env=env)
        request = _request_from_mapping(request_payload)
        requested_at = request.requested_at or _now()
        request = replace(request, requested_at=requested_at)
        registry = self._require_registry()
        planner = WorkflowIntakePlanner(registry=registry)
        outcome = planner.plan(request=request)
        submission = _submission_from_outcome(
            outcome=outcome,
            requested_at=requested_at,
        )
        write_result = _run_async(self._submit_submission(source, submission=submission))
        sync_payload = _load_sync_status(write_result.run_id, env=source)
        return {
            "native_instance": instance.to_contract(),
            "run": {
                "run_id": write_result.run_id,
                "workflow_id": outcome.workflow_request.workflow_id,
                "request_id": outcome.workflow_request.request_id,
                "current_state": outcome.current_state.value,
                "workflow_definition_id": submission.run.workflow_definition_id,
                "admitted_definition_hash": submission.run.admitted_definition_hash,
                "persisted": True,
                "sync_status": sync_payload["sync_status"],
                "sync_cycle_id": sync_payload["sync_cycle_id"],
                "sync_error_count": sync_payload["sync_error_count"],
            },
            "admission_decision": _serialize_decision(outcome.admission_decision),
        }

    async def _submit_submission(
        self,
        env: Mapping[str, str] | None,
        *,
        submission: WorkflowAdmissionSubmission,
    ) -> WorkflowAdmissionWriteResult:
        conn = await self.connect_database(env)
        try:
            await self.bootstrap_schema(conn)
            assert self.persist_submission is not None
            return await self.persist_submission(conn, submission=submission)
        finally:
            await conn.close()

    def status(
        self,
        *,
        run_id: str,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        source, instance = self._resolve_instance(env=env)
        row, packet_inspection, jobs, observability_hints = _run_async(
            self._load_run_status_row(source, run_id=run_id),
        )
        inspection = None
        inspection_payload = None
        last_event_id = row.get("last_event_id")
        if isinstance(last_event_id, str) and last_event_id:
            inspection = RuntimeOrchestrator(
                evidence_reader=self.evidence_reader_factory(source),
            ).inspect_run(run_id=run_id)
            inspection_payload = _serialize_inspection(inspection)
        observability_payload = build_frontdoor_observability(
            run_id=run_id,
            run_row=row,
            inspection=inspection,
            jobs=jobs,
            packet_inspection=packet_inspection,
            packet_inspection_source=observability_hints.packet_inspection_source,
            contract_drift_refs=observability_hints.contract_drift_refs,
        ).to_json()
        sync_payload = _load_sync_status(run_id, env=source)
        payload = {
            "native_instance": instance.to_contract(),
            "run": {
                "run_id": row["run_id"],
                "workflow_id": row["workflow_id"],
                "request_id": row["request_id"],
                "request_digest": row["request_digest"],
                "workflow_definition_id": row["workflow_definition_id"],
                "admitted_definition_hash": row["admitted_definition_hash"],
                "current_state": row["current_state"],
                "terminal_reason_code": row["terminal_reason_code"],
                "run_idempotency_key": row["run_idempotency_key"],
                "context_bundle_id": row["context_bundle_id"],
                "authority_context_digest": row["authority_context_digest"],
                "admission_decision_id": row["admission_decision_id"],
                "requested_at": _json_compatible(row["requested_at"]),
                "admitted_at": _json_compatible(row["admitted_at"]),
                "started_at": _json_compatible(row["started_at"]),
                "finished_at": _json_compatible(row["finished_at"]),
                "last_event_id": last_event_id,
                "persisted": True,
                "sync_status": sync_payload["sync_status"],
                "sync_cycle_id": sync_payload["sync_cycle_id"],
                "sync_error_count": sync_payload["sync_error_count"],
                "jobs": jobs,
            },
            "inspection": inspection_payload,
            "observability": observability_payload,
        }
        if packet_inspection is not None:
            payload["packet_inspection"] = packet_inspection
        return payload

    async def _load_run_row(
        self,
        env: Mapping[str, str] | None,
        *,
        run_id: str,
    ) -> Mapping[str, Any]:
        conn = await self.connect_database(env)
        try:
            row, _metadata = await _fetch_run_row(conn, run_id=run_id)
        finally:
            await conn.close()
        if row is None:
            raise NativeFrontdoorError(
                "frontdoor.run_missing",
                "run_id is not present in the native control plane",
                details={"run_id": run_id},
            )
        return dict(row)

    async def _load_run_status_row(
        self,
        env: Mapping[str, str] | None,
        *,
        run_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any] | None, list[dict[str, Any]], _RunStatusObservabilityHints]:
        conn = await self.connect_database(env)
        try:
            row, metadata = await _fetch_run_row(conn, run_id=run_id)
            if row is None:
                raise NativeFrontdoorError(
                    "frontdoor.run_missing",
                    "run_id is not present in the native control plane",
                    details={"run_id": run_id},
                )
            try:
                packet_row = await conn.fetchrow(_ExecutionPacketsQuery, run_id)
            except Exception:
                packet_row = None
            jobs = await _load_run_jobs_with_submission_summary(conn, run_id=run_id)
        finally:
            await conn.close()

        packet_inspection = None
        packet_inspection_source = "missing"
        try:
            from runtime.execution_packet_authority import (
                inspect_execution_packets,
                packet_inspection_from_row,
            )
        except Exception:
            inspect_execution_packets = None
            packet_inspection_from_row = None

        if packet_inspection_from_row is not None:
            packet_inspection = packet_inspection_from_row(row)
            if packet_inspection is not None:
                packet_inspection_source = "materialized"
        if packet_row is not None:
            packets = packet_row.get("packets")
            if isinstance(packets, str):
                try:
                    packets = json.loads(packets)
                except (json.JSONDecodeError, ValueError, TypeError):
                    packets = []
            if (
                packet_inspection is None
                and inspect_execution_packets is not None
                and isinstance(packets, Sequence)
                and not isinstance(packets, (str, bytes, bytearray))
                and packets
            ):
                try:
                    packet_inspection = inspect_execution_packets(
                        packets,
                        run_row=dict(row),
                    )
                    if packet_inspection is not None:
                        packet_inspection_source = "derived"
                except Exception:
                    packet_inspection = None

        return (
            dict(row),
            packet_inspection,
            jobs,
            _RunStatusObservabilityHints(
                packet_inspection_source=packet_inspection_source,
                contract_drift_refs=metadata.contract_drift_refs,
            ),
        )


def submit(
    *,
    request_payload: Mapping[str, Any],
    registry: RegistryResolver,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Submit one workflow request through repo-local native authority."""

    return NativeDagFrontdoor(registry=registry).submit(
        request_payload=request_payload,
        env=env,
    )


_DEFAULT_NATIVE_DAG_FRONTDOOR = NativeDagFrontdoor()

# Publish the repo-local control-plane methods directly so callers and tests
# hit the actual authority path instead of a compatibility wrapper.
status = _DEFAULT_NATIVE_DAG_FRONTDOOR.status
health = _DEFAULT_NATIVE_DAG_FRONTDOOR.health


def _emit(payload: Mapping[str, Any]) -> int:
    json.dump(_json_compatible(payload), fp=os.fdopen(os.dup(1), "w"), indent=2, sort_keys=True)
    return 0


__all__ = [
    "NativeDagFrontdoor",
    "NativeFrontdoorError",
    "health",
    "status",
    "submit",
]
