"""Repository functions: SQL query assembly, row-to-record parsing, snapshot queries."""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import asyncpg

from runtime.instance import (
    NativeWorkflowInstance,
    resolve_native_instance,
)
from runtime.work_item_assessment import WorkItemAssessmentRecord, assess_work_items
from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord
from storage.postgres import connect_workflow_database
from ._payload_contract import optional_text, require_text
from ._operator_helpers import _json_compatible, _normalize_as_of, _now, _run_async


class NativeOperatorQueryError(RuntimeError):
    """Raised when the native operator query surface cannot complete safely."""

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


class _Connection(Protocol):
    async def fetch(self, query: str, *args: object) -> Sequence[Mapping[str, Any]]:
        """Return query rows."""

    async def close(self) -> None:
        """Close the connection."""


def _require_text(value: object, *, field_name: str) -> str:
    try:
        return require_text(value, field_name=field_name)
    except ValueError as exc:
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            str(exc),
            details={"field": field_name, "value_type": type(value).__name__},
        ) from exc


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        return optional_text(value, field_name=field_name)
    except ValueError as exc:
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            str(exc),
            details={"field": field_name, "value_type": type(value).__name__},
        ) from exc


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _optional_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_datetime(value, field_name=field_name)


def _coerce_datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise NativeOperatorQueryError(
                "operator_query.invalid_row",
                f"{field_name} must be a datetime",
                details={"field": field_name, "value_type": type(value).__name__},
            ) from exc
    return _require_datetime(value, field_name=field_name)


def _normalize_string_sequence(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            parsed = value
        value = parsed
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            f"{field_name} must be an array of strings",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(
            _require_text(item, field_name=f"{field_name}[{index}]")
        )
    return tuple(dict.fromkeys(normalized))


def _coerce_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    """Best-effort coercion to a mapping for JSONB columns that may be NULL or string-encoded."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, Mapping):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, str):
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if not isinstance(value, Mapping):
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_ids(
    values: Sequence[str] | None,
    *,
    field_name: str,
) -> tuple[str, ...] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes, bytearray)):
        raise NativeOperatorQueryError(
            "operator_query.invalid_request",
            f"{field_name} must be an array of strings",
            details={"field": field_name, "value_type": type(values).__name__},
        )
    normalized = tuple(
        _require_text(value, field_name=f"{field_name}[{index}]")
        for index, value in enumerate(values)
    )
    if not normalized:
        raise NativeOperatorQueryError(
            "operator_query.invalid_request",
            f"{field_name} must not be empty",
            details={"field": field_name},
        )
    return normalized


def _json_list(value: tuple[str, ...] | None) -> list[str] | None:
    if value is None:
        return None
    return list(value)


def _row_clause(
    *,
    column_name: str,
    values: tuple[str, ...] | None,
    args: list[object],
    clauses: list[str],
) -> None:
    if values is None:
        return
    placeholder = f"${len(args) + 1}"
    clauses.append(f"{column_name} = ANY({placeholder}::text[])")
    args.append(list(values))


_WORKFLOW_RUN_PACKET_INSPECTIONS_QUERY = """
    SELECT
        wr.run_id,
        wr.workflow_id,
        wr.request_id,
        wr.request_digest,
        wr.workflow_definition_id,
        wr.admitted_definition_hash,
        wr.current_state,
        wr.terminal_reason_code,
        wr.run_idempotency_key,
        wr.packet_inspection,
        wr.request_envelope,
        wr.requested_at,
        wr.admitted_at,
        wr.started_at,
        wr.finished_at,
        wr.last_event_id,
        COALESCE(ep.packets, '[]'::jsonb) AS packets,
        COALESCE(rof.operator_frames, '[]'::jsonb) AS operator_frames
    FROM workflow_runs wr
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(payload ORDER BY created_at, execution_packet_id),
                '[]'::jsonb
            ) AS packets
        FROM execution_packets
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) ep
        ON ep.run_id = wr.run_id
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(
                    json_build_object(
                        'operator_frame_id', operator_frame_id,
                        'node_id', node_id,
                        'operator_kind', operator_kind,
                        'frame_state', frame_state,
                        'item_index', item_index,
                        'iteration_index', iteration_index,
                        'source_snapshot', COALESCE(source_snapshot, '{}'::jsonb),
                        'aggregate_outputs', COALESCE(aggregate_outputs, '{}'::jsonb),
                        'active_count', COALESCE(active_count, 0),
                        'stop_reason', stop_reason,
                        'started_at', to_json(started_at),
                        'finished_at', to_json(finished_at)
                    )
                    ORDER BY
                        node_id,
                        COALESCE(item_index, -1),
                        COALESCE(iteration_index, -1),
                        operator_frame_id
                ),
                '[]'::jsonb
            ) AS operator_frames
        FROM run_operator_frames
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) rof
        ON rof.run_id = wr.run_id
    WHERE wr.run_id = ANY($1::text[])
    ORDER BY wr.requested_at DESC NULLS LAST, wr.run_id
"""

_LEGACY_WORKFLOW_RUN_PACKET_INSPECTIONS_QUERY = """
    SELECT
        wr.run_id,
        wr.workflow_id,
        wr.request_id,
        wr.request_digest,
        wr.workflow_definition_id,
        wr.admitted_definition_hash,
        wr.current_state,
        wr.terminal_reason_code,
        wr.run_idempotency_key,
        NULL::jsonb AS packet_inspection,
        wr.request_envelope,
        wr.requested_at,
        wr.admitted_at,
        wr.started_at,
        wr.finished_at,
        wr.last_event_id,
        COALESCE(ep.packets, '[]'::jsonb) AS packets,
        COALESCE(rof.operator_frames, '[]'::jsonb) AS operator_frames
    FROM workflow_runs wr
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(payload ORDER BY created_at, execution_packet_id),
                '[]'::jsonb
            ) AS packets
        FROM execution_packets
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) ep
        ON ep.run_id = wr.run_id
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(
                    json_build_object(
                        'operator_frame_id', operator_frame_id,
                        'node_id', node_id,
                        'operator_kind', operator_kind,
                        'frame_state', frame_state,
                        'item_index', item_index,
                        'iteration_index', iteration_index,
                        'source_snapshot', COALESCE(source_snapshot, '{}'::jsonb),
                        'aggregate_outputs', COALESCE(aggregate_outputs, '{}'::jsonb),
                        'active_count', COALESCE(active_count, 0),
                        'stop_reason', stop_reason,
                        'started_at', to_json(started_at),
                        'finished_at', to_json(finished_at)
                    )
                    ORDER BY
                        node_id,
                        COALESCE(item_index, -1),
                        COALESCE(iteration_index, -1),
                        operator_frame_id
                ),
                '[]'::jsonb
            ) AS operator_frames
        FROM run_operator_frames
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) rof
        ON rof.run_id = wr.run_id
    WHERE wr.run_id = ANY($1::text[])
    ORDER BY wr.requested_at DESC NULLS LAST, wr.run_id
"""


_WORKFLOW_RUN_PACKET_INSPECTIONS_WITHOUT_OPERATOR_FRAMES_QUERY = """
    SELECT
        wr.run_id,
        wr.workflow_id,
        wr.request_id,
        wr.request_digest,
        wr.workflow_definition_id,
        wr.admitted_definition_hash,
        wr.current_state,
        wr.terminal_reason_code,
        wr.run_idempotency_key,
        wr.packet_inspection,
        wr.request_envelope,
        wr.requested_at,
        wr.admitted_at,
        wr.started_at,
        wr.finished_at,
        wr.last_event_id,
        COALESCE(ep.packets, '[]'::jsonb) AS packets,
        '[]'::jsonb AS operator_frames
    FROM workflow_runs wr
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(payload ORDER BY created_at, execution_packet_id),
                '[]'::jsonb
            ) AS packets
        FROM execution_packets
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) ep
        ON ep.run_id = wr.run_id
    WHERE wr.run_id = ANY($1::text[])
    ORDER BY wr.requested_at DESC NULLS LAST, wr.run_id
"""

_LEGACY_WORKFLOW_RUN_PACKET_INSPECTIONS_WITHOUT_OPERATOR_FRAMES_QUERY = """
    SELECT
        wr.run_id,
        wr.workflow_id,
        wr.request_id,
        wr.request_digest,
        wr.workflow_definition_id,
        wr.admitted_definition_hash,
        wr.current_state,
        wr.terminal_reason_code,
        wr.run_idempotency_key,
        NULL::jsonb AS packet_inspection,
        wr.request_envelope,
        wr.requested_at,
        wr.admitted_at,
        wr.started_at,
        wr.finished_at,
        wr.last_event_id,
        COALESCE(ep.packets, '[]'::jsonb) AS packets,
        '[]'::jsonb AS operator_frames
    FROM workflow_runs wr
    LEFT JOIN (
        SELECT
            run_id,
            COALESCE(
                json_agg(payload ORDER BY created_at, execution_packet_id),
                '[]'::jsonb
            ) AS packets
        FROM execution_packets
        WHERE run_id = ANY($1::text[])
        GROUP BY run_id
    ) ep
        ON ep.run_id = wr.run_id
    WHERE wr.run_id = ANY($1::text[])
    ORDER BY wr.requested_at DESC NULLS LAST, wr.run_id
"""


def _missing_packet_inspection_column_error(error: BaseException) -> bool:
    sqlstate = str(getattr(error, "sqlstate", "") or "").strip()
    if sqlstate and sqlstate != "42703":
        return False
    message = str(error).lower()
    if "packet_inspection" not in message:
        return False
    return "does not exist" in message or "undefined column" in message


def _missing_run_operator_frames_table_error(error: BaseException) -> bool:
    sqlstate = str(getattr(error, "sqlstate", "") or "").strip()
    if sqlstate and sqlstate != "42P01":
        return False
    message = str(error).lower()
    return "run_operator_frames" in message and (
        "does not exist" in message or "undefined table" in message
    )


def _missing_roadmap_embedding_column_error(error: BaseException) -> bool:
    sqlstate = str(getattr(error, "sqlstate", "") or "").strip()
    if sqlstate and sqlstate != "42703":
        return False
    message = str(error).lower()
    if "embedding" not in message or "roadmap_items" not in message:
        return False
    return "does not exist" in message or "undefined column" in message


def _workflow_run_failure_category(row: Mapping[str, Any]) -> str:
    pieces = [
        str(row.get("terminal_reason_code") or "").strip().lower(),
        str(row.get("current_state") or "").strip().lower(),
    ]
    normalized = " ".join(piece for piece in pieces if piece)
    if not normalized:
        return "unknown"
    if any(token in normalized for token in ("succeeded", "success", "promoted", "completed")):
        return "success"
    if any(token in normalized for token in ("schema", "migration", "column", "undefinedcolumn")):
        return "schema_drift"
    if any(token in normalized for token in ("timeout", "timed_out", "deadline", "latency")):
        return "provider_timeout"
    if any(token in normalized for token in ("sandbox", "seatbelt", "permission denied", "denied")):
        return "sandbox_denied"
    if any(token in normalized for token in ("idempotency", "duplicate", "dedupe", "conflict")):
        return "idempotency_conflict"
    if any(token in normalized for token in ("database", "postgres", "sqlstate", "unreachable", "connection")):
        return "db_unreachable"
    if any(token in normalized for token in ("packet", "drift", "compile_index")):
        return "packet_drift"
    if "cancel" in normalized:
        return "cancelled"
    if any(token in normalized for token in ("blocked", "rejected", "admission", "gate")):
        return "policy_blocked"
    if any(token in normalized for token in ("running", "claim_", "lease_", "proposal_", "gate_", "accepted", "requested")):
        return "in_progress"
    if any(token in normalized for token in ("failed", "dead_letter", "error", "invalid")):
        return "execution_failed"
    return "unknown"


def _workflow_run_isolation_suffix(row: Mapping[str, Any]) -> str | None:
    values = [
        str(row.get("workflow_id") or "").strip(),
        str(row.get("request_id") or "").strip(),
        str(row.get("workflow_definition_id") or "").strip(),
        str(_coerce_mapping(row.get("request_envelope"), field_name="request_envelope").get("definition_hash") or "").strip(),
    ]
    suffixes: list[str] = []
    for value in values:
        if not value:
            continue
        if "." not in value:
            return None
        suffix = value.rsplit(".", 1)[-1].strip()
        if len(suffix) < 6:
            return None
        if not suffix.replace("-", "").replace("_", "").isalnum():
            return None
        suffixes.append(suffix)
    if len(suffixes) < 2:
        return None
    first = suffixes[0]
    return first if all(item == first for item in suffixes[1:]) else None


def _workflow_run_is_synthetic(row: Mapping[str, Any]) -> bool:
    parts = [
        str(row.get("workflow_id") or "").strip().lower(),
        str(row.get("request_id") or "").strip().lower(),
        str(_coerce_mapping(row.get("request_envelope"), field_name="request_envelope").get("name") or "").strip().lower(),
    ]
    normalized = " ".join(part for part in parts if part)
    return bool(normalized) and any(token in normalized for token in ("smoke", "synthetic", "probe", "canary"))


@dataclass(frozen=True, slots=True)
class OperatorIssueRecord:
    """Canonical issue row exposed by the native operator query surface."""

    issue_id: str
    issue_key: str
    title: str
    status: str
    severity: str
    priority: str
    summary: str
    source_kind: str
    discovered_in_run_id: str | None
    discovered_in_receipt_id: str | None
    owner_ref: str | None
    decision_ref: str | None
    resolution_summary: str | None
    opened_at: datetime
    resolved_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "issue_key": self.issue_key,
            "title": self.title,
            "status": self.status,
            "severity": self.severity,
            "priority": self.priority,
            "summary": self.summary,
            "source_kind": self.source_kind,
            "discovered_in_run_id": self.discovered_in_run_id,
            "discovered_in_receipt_id": self.discovered_in_receipt_id,
            "owner_ref": self.owner_ref,
            "decision_ref": self.decision_ref,
            "resolution_summary": self.resolution_summary,
            "opened_at": self.opened_at.isoformat(),
            "resolved_at": None if self.resolved_at is None else self.resolved_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorBugRecord:
    """Canonical bug row exposed by the native operator query surface."""

    bug_id: str
    bug_key: str
    title: str
    status: str
    severity: str
    priority: str
    summary: str
    source_kind: str
    discovered_in_run_id: str | None
    discovered_in_receipt_id: str | None
    owner_ref: str | None
    source_issue_id: str | None
    decision_ref: str | None
    resolution_summary: str | None
    opened_at: datetime
    resolved_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "bug_id": self.bug_id,
            "bug_key": self.bug_key,
            "title": self.title,
            "status": self.status,
            "severity": self.severity,
            "priority": self.priority,
            "summary": self.summary,
            "source_kind": self.source_kind,
            "discovered_in_run_id": self.discovered_in_run_id,
            "discovered_in_receipt_id": self.discovered_in_receipt_id,
            "owner_ref": self.owner_ref,
            "source_issue_id": self.source_issue_id,
            "decision_ref": self.decision_ref,
            "resolution_summary": self.resolution_summary,
            "opened_at": self.opened_at.isoformat(),
            "resolved_at": None if self.resolved_at is None else self.resolved_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorRoadmapItemRecord:
    """Canonical roadmap row exposed by the native operator query surface."""

    roadmap_item_id: str
    roadmap_key: str
    title: str
    item_kind: str
    status: str
    priority: str
    parent_roadmap_item_id: str | None
    source_bug_id: str | None
    registry_paths: tuple[str, ...]
    summary: str
    acceptance_criteria: Mapping[str, Any]
    decision_ref: str | None
    target_start_at: datetime | None
    target_end_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "roadmap_item_id": self.roadmap_item_id,
            "roadmap_key": self.roadmap_key,
            "title": self.title,
            "item_kind": self.item_kind,
            "status": self.status,
            "priority": self.priority,
            "parent_roadmap_item_id": self.parent_roadmap_item_id,
            "source_bug_id": self.source_bug_id,
            "registry_paths": list(self.registry_paths),
            "summary": self.summary,
            "acceptance_criteria": _json_compatible(self.acceptance_criteria),
            "decision_ref": self.decision_ref,
            "target_start_at": (
                None if self.target_start_at is None else self.target_start_at.isoformat()
            ),
            "target_end_at": None if self.target_end_at is None else self.target_end_at.isoformat(),
            "completed_at": None if self.completed_at is None else self.completed_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorCutoverGateRecord:
    """Canonical cutover-gate row exposed by the native operator query surface."""

    cutover_gate_id: str
    gate_key: str
    gate_name: str
    gate_kind: str
    gate_status: str
    target_kind: str
    target_ref: str
    gate_policy: Mapping[str, Any]
    required_evidence: Mapping[str, Any]
    opened_by_decision_id: str
    closed_by_decision_id: str | None
    opened_at: datetime
    closed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "cutover_gate_id": self.cutover_gate_id,
            "gate_key": self.gate_key,
            "gate_name": self.gate_name,
            "gate_kind": self.gate_kind,
            "gate_status": self.gate_status,
            "target_kind": self.target_kind,
            "target_ref": self.target_ref,
            "gate_policy": _json_compatible(self.gate_policy),
            "required_evidence": _json_compatible(self.required_evidence),
            "opened_by_decision_id": self.opened_by_decision_id,
            "closed_by_decision_id": self.closed_by_decision_id,
            "opened_at": self.opened_at.isoformat(),
            "closed_at": None if self.closed_at is None else self.closed_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorRoadmapDependencyRecord:
    """Canonical roadmap dependency row exposed by roadmap tree reads."""

    roadmap_item_dependency_id: str
    roadmap_item_id: str
    depends_on_roadmap_item_id: str
    dependency_kind: str
    decision_ref: str | None
    created_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "roadmap_item_dependency_id": self.roadmap_item_dependency_id,
            "roadmap_item_id": self.roadmap_item_id,
            "depends_on_roadmap_item_id": self.depends_on_roadmap_item_id,
            "dependency_kind": self.dependency_kind,
            "decision_ref": self.decision_ref,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorRoadmapSemanticNeighborRecord:
    """Semantically close roadmap item discovered from embedding similarity."""

    roadmap_item_id: str
    title: str
    status: str
    priority: str
    similarity: float
    match_kind: str = "embedding"

    def to_json(self) -> dict[str, Any]:
        return {
            "roadmap_item_id": self.roadmap_item_id,
            "title": self.title,
            "status": self.status,
            "priority": self.priority,
            "similarity": round(self.similarity, 4),
            "match_kind": self.match_kind,
        }


@dataclass(frozen=True, slots=True)
class OperatorWorkflowRunPacketInspectionRecord:
    """Shadow packet inspection surfaced for one workflow run."""

    workflow_run_id: str
    workflow_id: str
    request_id: str
    workflow_definition_id: str
    current_state: str
    terminal_reason_code: str | None
    request_digest: str | None
    admitted_definition_hash: str | None
    run_idempotency_key: str | None
    packet_inspection_source: str
    failure_category: str
    synthetic_run: bool
    isolation_suffix: str | None
    operator_frame_source: str = "missing"
    operator_frame_count: int = 0
    operator_frame_state_counts: tuple[tuple[str, int], ...] = ()
    contract_drift_refs: tuple[str, ...] = ()
    packet_inspection: Mapping[str, Any] | None = None
    operator_frames: tuple[Mapping[str, Any], ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "workflow_id": self.workflow_id,
            "request_id": self.request_id,
            "workflow_definition_id": self.workflow_definition_id,
            "current_state": self.current_state,
            "terminal_reason_code": self.terminal_reason_code,
            "request_digest": self.request_digest,
            "admitted_definition_hash": self.admitted_definition_hash,
            "run_idempotency_key": self.run_idempotency_key,
            "packet_inspection_source": self.packet_inspection_source,
            "failure_category": self.failure_category,
            "synthetic_run": self.synthetic_run,
            "isolation_suffix": self.isolation_suffix,
            "operator_frame_source": self.operator_frame_source,
            "operator_frame_count": self.operator_frame_count,
            "operator_frame_state_counts": {
                name: count for name, count in self.operator_frame_state_counts
            },
            "contract_drift_refs": list(self.contract_drift_refs),
            "packet_inspection": _json_compatible(self.packet_inspection),
            "operator_frames": _json_compatible(self.operator_frames),
        }


@dataclass(frozen=True, slots=True)
class OperatorWorkflowRunObservabilitySummary:
    """Coverage and failure rollup for queried workflow runs."""

    workflow_run_count: int
    packet_inspection_source_counts: tuple[tuple[str, int], ...]
    packet_inspection_coverage_rate: float
    operator_frame_source_counts: tuple[tuple[str, int], ...]
    operator_frame_coverage_rate: float
    active_operator_frame_run_count: int
    failure_category_counts: tuple[tuple[str, int], ...]
    dominant_failure_category: str | None
    synthetic_run_count: int
    isolated_run_count: int
    missing_workflow_run_ids: tuple[str, ...] = ()
    contract_drift_refs: tuple[str, ...] = ()

    def observability_digest(self) -> str:
        coverage_pct = round(self.packet_inspection_coverage_rate * 100, 1)
        dominant = self.dominant_failure_category or "none"
        parts = [
            f"{self.workflow_run_count} runs",
            f"{coverage_pct}% packet coverage",
            f"{round(self.operator_frame_coverage_rate * 100, 1)}% operator-frame coverage",
            f"dominant failure {dominant}",
            f"{self.synthetic_run_count} synthetic",
            f"{self.isolated_run_count} isolated",
        ]
        if self.active_operator_frame_run_count:
            parts.append(f"{self.active_operator_frame_run_count} active frame run(s)")
        if self.missing_workflow_run_ids:
            parts.append(f"{len(self.missing_workflow_run_ids)} missing")
        if self.contract_drift_refs:
            parts.append(f"{len(self.contract_drift_refs)} drift refs")
        return " | ".join(parts)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "workflow_run_observability",
            "observability_digest": self.observability_digest(),
            "workflow_run_count": self.workflow_run_count,
            "packet_inspection_source_counts": {
                name: count for name, count in self.packet_inspection_source_counts
            },
            "packet_inspection_coverage_rate": self.packet_inspection_coverage_rate,
            "operator_frame_source_counts": {
                name: count for name, count in self.operator_frame_source_counts
            },
            "operator_frame_coverage_rate": self.operator_frame_coverage_rate,
            "active_operator_frame_run_count": self.active_operator_frame_run_count,
            "failure_category_counts": {
                name: count for name, count in self.failure_category_counts
            },
            "dominant_failure_category": self.dominant_failure_category,
            "synthetic_run_count": self.synthetic_run_count,
            "isolated_run_count": self.isolated_run_count,
            "missing_workflow_run_ids": list(self.missing_workflow_run_ids),
            "contract_drift_refs": list(self.contract_drift_refs),
        }


@dataclass(frozen=True, slots=True)
class OperatorWorkItemCloseoutRecommendationRecord:
    """Derived closeout recommendation assembled from work-item assessments."""

    anchor_kind: str
    anchor_id: str
    closeout_state: str
    closeout_action: str
    confidence: float
    reason_codes: tuple[str, ...]
    bug_ids: tuple[str, ...]
    roadmap_item_ids: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "anchor_kind": self.anchor_kind,
            "anchor_id": self.anchor_id,
            "closeout_state": self.closeout_state,
            "closeout_action": self.closeout_action,
            "confidence": round(self.confidence, 4),
            "reason_codes": list(self.reason_codes),
            "bug_ids": list(self.bug_ids),
            "roadmap_item_ids": list(self.roadmap_item_ids),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorQuerySnapshot:
    """Inspectable snapshot of canonical operator rows."""

    issues: tuple[OperatorIssueRecord, ...]
    bugs: tuple[OperatorBugRecord, ...]
    roadmap_items: tuple[OperatorRoadmapItemRecord, ...]
    cutover_gates: tuple[OperatorCutoverGateRecord, ...]
    work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...]
    work_item_assessments: tuple[WorkItemAssessmentRecord, ...]
    work_item_closeout_recommendations: tuple[
        OperatorWorkItemCloseoutRecommendationRecord, ...
    ]
    as_of: datetime
    issue_ids: tuple[str, ...] | None = None
    bug_ids: tuple[str, ...] | None = None
    roadmap_item_ids: tuple[str, ...] | None = None
    cutover_gate_ids: tuple[str, ...] | None = None
    work_item_workflow_binding_ids: tuple[str, ...] | None = None
    workflow_run_ids: tuple[str, ...] | None = None
    workflow_run_packet_inspections: tuple[OperatorWorkflowRunPacketInspectionRecord, ...] = ()
    workflow_run_observability: OperatorWorkflowRunObservabilitySummary | None = None

    def to_json(self) -> dict[str, Any]:
        assessments_by_key = {
            (record.item_kind, record.item_id): record.to_json()
            for record in self.work_item_assessments
        }
        payload = {
            "kind": "operator_query",
            "instruction_authority": _operator_query_instruction_authority(self),
            "as_of": self.as_of.isoformat(),
            "query": {
                "roadmap_item_ids": _json_list(self.roadmap_item_ids),
                "cutover_gate_ids": _json_list(self.cutover_gate_ids),
                "work_item_workflow_binding_ids": _json_list(self.work_item_workflow_binding_ids),
                "workflow_run_ids": _json_list(self.workflow_run_ids),
            },
            "counts": {
                "roadmap_items": len(self.roadmap_items),
                "cutover_gates": len(self.cutover_gates),
                "work_item_workflow_bindings": len(self.work_item_workflow_bindings),
                "work_item_assessments": len(self.work_item_assessments),
                "work_item_closeout_recommendations": len(
                    self.work_item_closeout_recommendations
                ),
            },
            "roadmap_items": [],
            "cutover_gates": [record.to_json() for record in self.cutover_gates],
            "work_item_workflow_bindings": [record.to_json() for record in self.work_item_workflow_bindings],
            "work_item_assessments": [record.to_json() for record in self.work_item_assessments],
            "work_item_closeout_recommendations": [
                record.to_json()
                for record in self.work_item_closeout_recommendations
            ],
        }
        if self.issue_ids is not None or self.issues:
            payload["query"]["issue_ids"] = _json_list(self.issue_ids)
            payload["counts"]["issues"] = len(self.issues)
            payload["issues"] = []
        if self.bug_ids is not None or self.bugs:
            payload["query"]["bug_ids"] = _json_list(self.bug_ids)
            payload["counts"]["bugs"] = len(self.bugs)
            payload["bugs"] = []
        for record in self.issues:
            row = record.to_json()
            assessment = assessments_by_key.get(("issue", record.issue_id))
            if assessment is not None:
                row["assessment"] = assessment
            payload["issues"].append(row)
        for record in self.bugs:
            row = record.to_json()
            assessment = assessments_by_key.get(("bug", record.bug_id))
            if assessment is not None:
                row["assessment"] = assessment
            payload["bugs"].append(row)
        for record in self.roadmap_items:
            row = record.to_json()
            assessment = assessments_by_key.get(("roadmap_item", record.roadmap_item_id))
            if assessment is not None:
                row["assessment"] = assessment
            payload["roadmap_items"].append(row)
        if self.workflow_run_packet_inspections:
            payload["counts"]["workflow_run_packet_inspections"] = len(self.workflow_run_packet_inspections)
            payload["workflow_run_packet_inspections"] = [
                record.to_json() for record in self.workflow_run_packet_inspections
            ]
        if self.workflow_run_observability is not None:
            payload["workflow_run_observability"] = self.workflow_run_observability.to_json()
        return payload


@dataclass(frozen=True, slots=True)
class OperatorRoadmapTreeSnapshot:
    """Roadmap subtree snapshot rooted at one canonical roadmap item."""

    root_roadmap_item_id: str
    root_item: OperatorRoadmapItemRecord
    roadmap_items: tuple[OperatorRoadmapItemRecord, ...]
    roadmap_item_dependencies: tuple[OperatorRoadmapDependencyRecord, ...]
    as_of: datetime
    work_item_assessments: tuple[WorkItemAssessmentRecord, ...] = ()
    semantic_neighbors: tuple[OperatorRoadmapSemanticNeighborRecord, ...] = ()
    semantic_neighbors_reason_code: str = "roadmap.semantic_neighbors.none"

    def to_json(self) -> dict[str, Any]:
        assessments_by_key = {
            (record.item_kind, record.item_id): record.to_json()
            for record in self.work_item_assessments
        }
        return {
            "kind": "roadmap_tree",
            "instruction_authority": _roadmap_tree_instruction_authority(self),
            "as_of": self.as_of.isoformat(),
            "root_roadmap_item_id": self.root_roadmap_item_id,
            "counts": {
                "roadmap_items": len(self.roadmap_items),
                "roadmap_item_dependencies": len(self.roadmap_item_dependencies),
                "work_item_assessments": len(self.work_item_assessments),
                "semantic_neighbors": len(self.semantic_neighbors),
            },
            "root_item": {
                **self.root_item.to_json(),
                **(
                    {"assessment": assessments_by_key[("roadmap_item", self.root_item.roadmap_item_id)]}
                    if ("roadmap_item", self.root_item.roadmap_item_id) in assessments_by_key
                    else {}
                ),
            },
            "roadmap_items": [
                {
                    **record.to_json(),
                    **(
                        {"assessment": assessments_by_key[("roadmap_item", record.roadmap_item_id)]}
                        if ("roadmap_item", record.roadmap_item_id) in assessments_by_key
                        else {}
                    ),
                }
                for record in self.roadmap_items
            ],
            "roadmap_item_dependencies": [
                record.to_json() for record in self.roadmap_item_dependencies
            ],
            "work_item_assessments": [record.to_json() for record in self.work_item_assessments],
            "semantic_neighbors": [record.to_json() for record in self.semantic_neighbors],
            "semantic_neighbors_reason_code": self.semantic_neighbors_reason_code,
            "rendered_markdown": _render_roadmap_tree_markdown(
                root_item=self.root_item,
                roadmap_items=self.roadmap_items,
                roadmap_item_dependencies=self.roadmap_item_dependencies,
            ),
        }


def _bug_record_from_row(row: Mapping[str, Any]) -> OperatorBugRecord:
    return OperatorBugRecord(
        bug_id=_require_text(row.get("bug_id"), field_name="bug_id"),
        bug_key=_require_text(row.get("bug_key"), field_name="bug_key"),
        title=_require_text(row.get("title"), field_name="title"),
        status=_require_text(row.get("status"), field_name="status"),
        severity=_require_text(row.get("severity"), field_name="severity"),
        priority=_require_text(row.get("priority"), field_name="priority"),
        summary=_require_text(row.get("summary"), field_name="summary"),
        source_kind=_require_text(row.get("source_kind"), field_name="source_kind"),
        discovered_in_run_id=_optional_text(
            row.get("discovered_in_run_id"),
            field_name="discovered_in_run_id",
        ),
        discovered_in_receipt_id=_optional_text(
            row.get("discovered_in_receipt_id"),
            field_name="discovered_in_receipt_id",
        ),
        owner_ref=_optional_text(row.get("owner_ref"), field_name="owner_ref"),
        source_issue_id=_optional_text(
            row.get("source_issue_id"),
            field_name="source_issue_id",
        ),
        decision_ref=_optional_text(row.get("decision_ref"), field_name="decision_ref"),
        resolution_summary=_optional_text(
            row.get("resolution_summary"),
            field_name="resolution_summary",
        ),
        opened_at=_require_datetime(row.get("opened_at"), field_name="opened_at"),
        resolved_at=_optional_datetime(row.get("resolved_at"), field_name="resolved_at"),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _issue_record_from_row(row: Mapping[str, Any]) -> OperatorIssueRecord:
    return OperatorIssueRecord(
        issue_id=_require_text(row.get("issue_id"), field_name="issue_id"),
        issue_key=_require_text(row.get("issue_key"), field_name="issue_key"),
        title=_require_text(row.get("title"), field_name="title"),
        status=_require_text(row.get("status"), field_name="status"),
        severity=_require_text(row.get("severity"), field_name="severity"),
        priority=_require_text(row.get("priority"), field_name="priority"),
        summary=_require_text(row.get("summary"), field_name="summary"),
        source_kind=_require_text(row.get("source_kind"), field_name="source_kind"),
        discovered_in_run_id=_optional_text(
            row.get("discovered_in_run_id"),
            field_name="discovered_in_run_id",
        ),
        discovered_in_receipt_id=_optional_text(
            row.get("discovered_in_receipt_id"),
            field_name="discovered_in_receipt_id",
        ),
        owner_ref=_optional_text(row.get("owner_ref"), field_name="owner_ref"),
        decision_ref=_optional_text(row.get("decision_ref"), field_name="decision_ref"),
        resolution_summary=_optional_text(
            row.get("resolution_summary"),
            field_name="resolution_summary",
        ),
        opened_at=_require_datetime(row.get("opened_at"), field_name="opened_at"),
        resolved_at=_optional_datetime(row.get("resolved_at"), field_name="resolved_at"),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _roadmap_record_from_row(row: Mapping[str, Any]) -> OperatorRoadmapItemRecord:
    return OperatorRoadmapItemRecord(
        roadmap_item_id=_require_text(row.get("roadmap_item_id"), field_name="roadmap_item_id"),
        roadmap_key=_require_text(row.get("roadmap_key"), field_name="roadmap_key"),
        title=_require_text(row.get("title"), field_name="title"),
        item_kind=_require_text(row.get("item_kind"), field_name="item_kind"),
        status=_require_text(row.get("status"), field_name="status"),
        priority=_require_text(row.get("priority"), field_name="priority"),
        parent_roadmap_item_id=_optional_text(
            row.get("parent_roadmap_item_id"),
            field_name="parent_roadmap_item_id",
        ),
        source_bug_id=_optional_text(row.get("source_bug_id"), field_name="source_bug_id"),
        registry_paths=_normalize_string_sequence(
            row.get("registry_paths"),
            field_name="registry_paths",
        ),
        summary=_require_text(row.get("summary"), field_name="summary"),
        acceptance_criteria=_coerce_mapping(
            row.get("acceptance_criteria"),
            field_name="acceptance_criteria",
        ),
        decision_ref=_optional_text(row.get("decision_ref"), field_name="decision_ref"),
        target_start_at=_optional_datetime(
            row.get("target_start_at"),
            field_name="target_start_at",
        ),
        target_end_at=_optional_datetime(row.get("target_end_at"), field_name="target_end_at"),
        completed_at=_optional_datetime(row.get("completed_at"), field_name="completed_at"),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _roadmap_dependency_record_from_row(
    row: Mapping[str, Any],
) -> OperatorRoadmapDependencyRecord:
    return OperatorRoadmapDependencyRecord(
        roadmap_item_dependency_id=_require_text(
            row.get("roadmap_item_dependency_id"),
            field_name="roadmap_item_dependency_id",
        ),
        roadmap_item_id=_require_text(
            row.get("roadmap_item_id"),
            field_name="roadmap_item_id",
        ),
        depends_on_roadmap_item_id=_require_text(
            row.get("depends_on_roadmap_item_id"),
            field_name="depends_on_roadmap_item_id",
        ),
        dependency_kind=_require_text(
            row.get("dependency_kind"),
            field_name="dependency_kind",
        ),
        decision_ref=_optional_text(
            row.get("decision_ref"),
            field_name="decision_ref",
        ),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
    )


def _parse_phase_order(value: object) -> tuple[int, ...] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    tokens = value.strip().split(".")
    parsed: list[int] = []
    for token in tokens:
        if not token.isdigit():
            return None
        parsed.append(int(token))
    return tuple(parsed) if parsed else None


def _roadmap_phase_sort_key(
    record: OperatorRoadmapItemRecord,
) -> tuple[bool, tuple[int, ...], datetime, str]:
    phase_order = _parse_phase_order(record.acceptance_criteria.get("phase_order"))
    return (
        phase_order is None,
        phase_order or (),
        record.created_at,
        record.roadmap_item_id,
    )


def _roadmap_tree_sort_key(
    *,
    root_roadmap_item_id: str,
    record: OperatorRoadmapItemRecord,
) -> tuple[int, bool, tuple[int, ...], datetime, str]:
    phase_order = _parse_phase_order(record.acceptance_criteria.get("phase_order"))
    return (
        0 if record.roadmap_item_id == root_roadmap_item_id else 1,
        phase_order is None,
        phase_order or (),
        record.created_at,
        record.roadmap_item_id,
    )


def _phase_order_sort_key(record: OperatorRoadmapItemRecord) -> tuple[bool, tuple[int, ...], datetime, str]:
    return _roadmap_phase_sort_key(record)


def _roadmap_truth_summary(
    roadmap_items: Sequence[OperatorRoadmapItemRecord],
) -> list[dict[str, Any]]:
    return [
        {
            "roadmap_item_id": record.roadmap_item_id,
            "roadmap_key": record.roadmap_key,
            "title": record.title,
            "status": record.status,
            "priority": record.priority,
            "parent_roadmap_item_id": record.parent_roadmap_item_id,
            "decision_ref": record.decision_ref,
        }
        for record in roadmap_items
    ]


def _derive_closeout_recommendations(
    assessments: Sequence[WorkItemAssessmentRecord],
) -> tuple[OperatorWorkItemCloseoutRecommendationRecord, ...]:
    recommendations: list[OperatorWorkItemCloseoutRecommendationRecord] = []
    seen_scopes: set[tuple[str, str, tuple[str, ...], tuple[str, ...]]] = set()
    for record in assessments:
        if record.closeout_state == "none":
            continue
        scope_key = (
            record.closeout_state,
            record.closeout_action,
            tuple(record.closeout_bug_ids),
            tuple(record.closeout_roadmap_item_ids),
        )
        if scope_key in seen_scopes:
            continue
        seen_scopes.add(scope_key)
        recommendations.append(
            OperatorWorkItemCloseoutRecommendationRecord(
                anchor_kind=record.item_kind,
                anchor_id=record.item_id,
                closeout_state=record.closeout_state,
                closeout_action=record.closeout_action,
                confidence=record.confidence,
                reason_codes=record.reason_codes,
                bug_ids=record.closeout_bug_ids,
                roadmap_item_ids=record.closeout_roadmap_item_ids,
            )
        )
    return tuple(
        sorted(
            recommendations,
            key=lambda record: (
                record.closeout_action,
                record.anchor_kind,
                record.anchor_id,
            ),
        )
    )


def _operator_query_instruction_authority(
    snapshot: NativeOperatorQuerySnapshot,
) -> dict[str, Any]:
    roadmap_item_ids = tuple(record.roadmap_item_id for record in snapshot.roadmap_items)
    binding_ids = tuple(
        record.work_item_workflow_binding_id for record in snapshot.work_item_workflow_bindings
    )
    has_run_observability = bool(snapshot.workflow_run_packet_inspections)
    packet_read_order = ["roadmap_truth", "queue_refs"]
    if snapshot.issues:
        packet_read_order.append("issues")
    if has_run_observability:
        packet_read_order.append("workflow_run_packet_inspections")
    packet_read_order.extend(
        [
            "work_item_assessments",
            "work_item_closeout_recommendations",
            "bugs",
            "cutover_gates",
            "work_item_workflow_bindings",
        ]
    )
    return {
        "kind": "operator_query_instruction_authority",
        "authority": "surfaces.api.operator_read.query_operator_surface",
        "packet_read_order": packet_read_order,
        "roadmap_truth": {
            "authority": "roadmap_items",
            "roadmap_item_ids": list(roadmap_item_ids),
            "items": _roadmap_truth_summary(snapshot.roadmap_items),
        },
        "queue_refs": {
            "workflow_run_ids": [] if snapshot.workflow_run_ids is None else list(snapshot.workflow_run_ids),
            "work_item_workflow_binding_ids": list(binding_ids),
            "cutover_gate_ids": [] if snapshot.cutover_gate_ids is None else list(snapshot.cutover_gate_ids),
        },
        "work_item_assessments": {
            "authority": "runtime.work_item_assessment.assess_work_items",
            "count": len(snapshot.work_item_assessments),
            "kinds": sorted({record.item_kind for record in snapshot.work_item_assessments}),
        },
        "work_item_closeout_recommendations": {
            "authority": "runtime.work_item_assessment.assess_work_items",
            "count": len(snapshot.work_item_closeout_recommendations),
            "actions": sorted(
                {
                    record.closeout_action
                    for record in snapshot.work_item_closeout_recommendations
                }
            ),
        },
        "directive": (
            "Read roadmap-backed rows, queue refs, work-item assessments, closeout recommendations, packet inspections, and operator-frame summaries here before using repo files or prior chat state."
            if has_run_observability
            else "Read roadmap-backed rows, queue refs, work-item assessments, and closeout recommendations here before using repo files or prior chat state."
        ),
    }


def _roadmap_tree_instruction_authority(
    snapshot: OperatorRoadmapTreeSnapshot,
) -> dict[str, Any]:
    return {
        "kind": "roadmap_tree_instruction_authority",
        "authority": "surfaces.api.operator_read.query_roadmap_tree",
        "packet_read_order": [
            "root_item",
            "roadmap_items",
            "roadmap_item_dependencies",
            "rendered_markdown",
        ],
        "roadmap_truth": {
            "root_roadmap_item_id": snapshot.root_roadmap_item_id,
            "items": _roadmap_truth_summary(snapshot.roadmap_items),
        },
        "queue_refs": {
            "roadmap_item_ids": [record.roadmap_item_id for record in snapshot.roadmap_items],
        },
        "directive": "Read the roadmap subtree here before assuming scope from files or chat.",
    }


def _render_roadmap_tree_markdown(
    *,
    root_item: OperatorRoadmapItemRecord,
    roadmap_items: tuple[OperatorRoadmapItemRecord, ...],
    roadmap_item_dependencies: tuple[OperatorRoadmapDependencyRecord, ...],
) -> str:
    item_by_id = {record.roadmap_item_id: record for record in roadmap_items}
    children_by_parent: dict[str, list[OperatorRoadmapItemRecord]] = {}
    external_dependencies: dict[str, list[str]] = {}
    for dependency in roadmap_item_dependencies:
        if dependency.depends_on_roadmap_item_id not in item_by_id:
            external_dependencies.setdefault(
                dependency.roadmap_item_id,
                [],
            ).append(dependency.depends_on_roadmap_item_id)
    for record in roadmap_items:
        parent_id = record.parent_roadmap_item_id
        if parent_id is None:
            continue
        children_by_parent.setdefault(parent_id, []).append(record)
    for children in children_by_parent.values():
        children.sort(key=_roadmap_phase_sort_key)

    def _render_item(record: OperatorRoadmapItemRecord, depth: int) -> list[str]:
        indent = "  " * depth
        phase_order = record.acceptance_criteria.get("phase_order")
        phase_label = f" [{phase_order}]" if isinstance(phase_order, str) and phase_order else ""
        lines = [
            f"{indent}- {record.title}{phase_label}",
            f"{indent}  id: {record.roadmap_item_id}",
            f"{indent}  priority: {record.priority}",
            f"{indent}  outcome_gate: {record.acceptance_criteria.get('outcome_gate', record.summary)}",
        ]
        external = external_dependencies.get(record.roadmap_item_id)
        if external:
            lines.append(f"{indent}  external_depends_on: {', '.join(sorted(external))}")
        for child in children_by_parent.get(record.roadmap_item_id, ()):
            lines.extend(_render_item(child, depth + 1))
        return lines

    lines = [f"# {root_item.title}", ""]
    lines.extend(_render_item(root_item, 0))
    return "\n".join(lines)


def _gate_target_from_row(row: Mapping[str, Any]) -> tuple[str, str]:
    target_columns = (
        ("roadmap_item_id", "roadmap_item", row.get("roadmap_item_id")),
        ("workflow_class_id", "workflow_class", row.get("workflow_class_id")),
        ("schedule_definition_id", "schedule_definition", row.get("schedule_definition_id")),
    )
    populated_targets: list[tuple[str, str]] = []
    for field_name, target_kind, value in target_columns:
        if value is None:
            continue
        populated_targets.append(
            (
                target_kind,
                _require_text(value, field_name=field_name),
            )
        )
    if len(populated_targets) != 1:
        raise NativeOperatorQueryError(
            "operator_query.invalid_row",
            "cutover gate must target exactly one authority row",
            details={
                "cutover_gate_id": _require_text(
                    row.get("cutover_gate_id"),
                    field_name="cutover_gate_id",
                ),
                "target_columns": ",".join(
                    field_name for field_name, _, value in target_columns if value is not None
                ),
            },
        )
    return populated_targets[0]


def _gate_record_from_row(row: Mapping[str, Any]) -> OperatorCutoverGateRecord:
    target_kind, target_ref = _gate_target_from_row(row)
    return OperatorCutoverGateRecord(
        cutover_gate_id=_require_text(row.get("cutover_gate_id"), field_name="cutover_gate_id"),
        gate_key=_require_text(row.get("gate_key"), field_name="gate_key"),
        gate_name=_require_text(row.get("gate_name"), field_name="gate_name"),
        gate_kind=_require_text(row.get("gate_kind"), field_name="gate_kind"),
        gate_status=_require_text(row.get("gate_status"), field_name="gate_status"),
        target_kind=target_kind,
        target_ref=target_ref,
        gate_policy=_coerce_mapping(
            row.get("gate_policy"),
            field_name="gate_policy",
        ),
        required_evidence=_coerce_mapping(
            row.get("required_evidence"),
            field_name="required_evidence",
        ),
        opened_by_decision_id=_require_text(
            row.get("opened_by_decision_id"),
            field_name="opened_by_decision_id",
        ),
        closed_by_decision_id=_optional_text(
            row.get("closed_by_decision_id"),
            field_name="closed_by_decision_id",
        ),
        opened_at=_require_datetime(row.get("opened_at"), field_name="opened_at"),
        closed_at=_optional_datetime(row.get("closed_at"), field_name="closed_at"),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _binding_record_from_row(row: Mapping[str, Any]) -> WorkItemWorkflowBindingRecord:
    return WorkItemWorkflowBindingRecord(
        work_item_workflow_binding_id=_require_text(
            row.get("work_item_workflow_binding_id"),
            field_name="work_item_workflow_binding_id",
        ),
        binding_kind=_require_text(row.get("binding_kind"), field_name="binding_kind"),
        binding_status=_require_text(row.get("binding_status"), field_name="binding_status"),
        issue_id=_optional_text(row.get("issue_id"), field_name="issue_id"),
        roadmap_item_id=_optional_text(row.get("roadmap_item_id"), field_name="roadmap_item_id"),
        bug_id=_optional_text(row.get("bug_id"), field_name="bug_id"),
        cutover_gate_id=_optional_text(row.get("cutover_gate_id"), field_name="cutover_gate_id"),
        workflow_class_id=_optional_text(
            row.get("workflow_class_id"),
            field_name="workflow_class_id",
        ),
        schedule_definition_id=_optional_text(
            row.get("schedule_definition_id"),
            field_name="schedule_definition_id",
        ),
        workflow_run_id=_optional_text(row.get("workflow_run_id"), field_name="workflow_run_id"),
        bound_by_decision_id=_optional_text(
            row.get("bound_by_decision_id"),
            field_name="bound_by_decision_id",
        ),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _operator_frame_payload_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "operator_frame_id": _require_text(
            row.get("operator_frame_id"),
            field_name="operator_frame_id",
        ),
        "node_id": _require_text(row.get("node_id"), field_name="node_id"),
        "operator_kind": _require_text(row.get("operator_kind"), field_name="operator_kind"),
        "frame_state": _require_text(row.get("frame_state"), field_name="frame_state"),
        "item_index": row.get("item_index"),
        "iteration_index": row.get("iteration_index"),
        "source_snapshot": dict(
            _coerce_mapping(row.get("source_snapshot"), field_name="source_snapshot")
        ),
        "aggregate_outputs": dict(
            _coerce_mapping(row.get("aggregate_outputs"), field_name="aggregate_outputs")
        ),
        "active_count": int(row.get("active_count") or 0),
        "stop_reason": _optional_text(row.get("stop_reason"), field_name="stop_reason"),
        "started_at": _require_datetime(row.get("started_at"), field_name="started_at").isoformat(),
        "finished_at": (
            None
            if row.get("finished_at") is None
            else _require_datetime(row.get("finished_at"), field_name="finished_at").isoformat()
        ),
    }


def _normalize_operator_frame_payloads(value: object) -> tuple[dict[str, Any], ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError, TypeError):
            value = []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        started_at = _coerce_datetime_value(item.get("started_at"), field_name="started_at")
        finished_at = item.get("finished_at")
        normalized.append(
            {
                "operator_frame_id": _require_text(
                    item.get("operator_frame_id"),
                    field_name="operator_frame_id",
                ),
                "node_id": _require_text(item.get("node_id"), field_name="node_id"),
                "operator_kind": _require_text(
                    item.get("operator_kind"),
                    field_name="operator_kind",
                ),
                "frame_state": _require_text(
                    item.get("frame_state"),
                    field_name="frame_state",
                ),
                "item_index": item.get("item_index"),
                "iteration_index": item.get("iteration_index"),
                "source_snapshot": dict(
                    _coerce_mapping(item.get("source_snapshot"), field_name="source_snapshot")
                ),
                "aggregate_outputs": dict(
                    _coerce_mapping(item.get("aggregate_outputs"), field_name="aggregate_outputs")
                ),
                "active_count": int(item.get("active_count") or 0),
                "stop_reason": _optional_text(item.get("stop_reason"), field_name="stop_reason"),
                "started_at": started_at.isoformat(),
                "finished_at": (
                    None
                    if finished_at is None
                    else _coerce_datetime_value(
                        finished_at,
                        field_name="finished_at",
                    ).isoformat()
                ),
            }
        )
    return tuple(normalized)


def _workflow_run_packet_inspection_record_from_row(
    row: Mapping[str, Any],
    packet_inspection: Mapping[str, Any] | None,
    *,
    packet_inspection_source: str,
    operator_frame_source: str,
    operator_frames: Sequence[Mapping[str, Any]] = (),
    contract_drift_refs: tuple[str, ...] = (),
) -> OperatorWorkflowRunPacketInspectionRecord:
    frame_state_counts = Counter(
        _require_text(frame.get("frame_state"), field_name="operator_frames.frame_state")
        for frame in operator_frames
    )
    return OperatorWorkflowRunPacketInspectionRecord(
        workflow_run_id=_require_text(row.get("run_id"), field_name="run_id"),
        workflow_id=_require_text(row.get("workflow_id"), field_name="workflow_id"),
        request_id=_require_text(row.get("request_id"), field_name="request_id"),
        workflow_definition_id=_require_text(
            row.get("workflow_definition_id"),
            field_name="workflow_definition_id",
        ),
        current_state=_require_text(row.get("current_state"), field_name="current_state"),
        terminal_reason_code=_optional_text(
            row.get("terminal_reason_code"),
            field_name="terminal_reason_code",
        ),
        request_digest=_optional_text(row.get("request_digest"), field_name="request_digest"),
        admitted_definition_hash=_optional_text(
            row.get("admitted_definition_hash"),
            field_name="admitted_definition_hash",
        ),
        run_idempotency_key=_optional_text(
            row.get("run_idempotency_key"),
            field_name="run_idempotency_key",
        ),
        packet_inspection_source=packet_inspection_source,
        failure_category=_workflow_run_failure_category(row),
        synthetic_run=_workflow_run_is_synthetic(row),
        isolation_suffix=_workflow_run_isolation_suffix(row),
        operator_frame_source=operator_frame_source,
        operator_frame_count=len(operator_frames),
        operator_frame_state_counts=tuple(sorted(frame_state_counts.items())),
        contract_drift_refs=contract_drift_refs,
        packet_inspection=packet_inspection,
        operator_frames=tuple(dict(frame) for frame in operator_frames),
    )


def _workflow_run_observability_summary(
    *,
    workflow_run_ids: tuple[str, ...],
    records: Sequence[OperatorWorkflowRunPacketInspectionRecord],
    contract_drift_refs: tuple[str, ...] = (),
) -> OperatorWorkflowRunObservabilitySummary:
    observed_run_ids = {record.workflow_run_id for record in records}
    missing_workflow_run_ids = tuple(
        run_id for run_id in workflow_run_ids if run_id not in observed_run_ids
    )
    source_counts = Counter(record.packet_inspection_source for record in records)
    operator_frame_source_counts = Counter(record.operator_frame_source for record in records)
    if missing_workflow_run_ids:
        source_counts["missing"] += len(missing_workflow_run_ids)
        operator_frame_source_counts["missing"] += len(missing_workflow_run_ids)
    failure_counts = Counter(
        record.failure_category for record in records if record.failure_category != "unknown"
    )
    covered = source_counts.get("materialized", 0) + source_counts.get("derived", 0)
    workflow_run_count = len(workflow_run_ids)
    coverage_rate = 0.0 if workflow_run_count == 0 else round(covered / workflow_run_count, 4)
    operator_frame_coverage_rate = (
        0.0
        if workflow_run_count == 0
        else round(
            (
                operator_frame_source_counts.get("canonical_operator_frames", 0)
                + operator_frame_source_counts.get("explicit_operator_frames", 0)
            )
            / workflow_run_count,
            4,
        )
    )
    ranked_failures = [
        item
        for item in failure_counts.most_common()
        if item[0] not in {"success", "in_progress"}
    ]
    dominant_failure_category = (
        ranked_failures[0][0]
        if ranked_failures
        else failure_counts.most_common(1)[0][0]
        if failure_counts
        else None
    )
    return OperatorWorkflowRunObservabilitySummary(
        workflow_run_count=workflow_run_count,
        packet_inspection_source_counts=tuple(sorted(source_counts.items())),
        packet_inspection_coverage_rate=coverage_rate,
        operator_frame_source_counts=tuple(sorted(operator_frame_source_counts.items())),
        operator_frame_coverage_rate=operator_frame_coverage_rate,
        active_operator_frame_run_count=sum(
            1
            for record in records
            if any(state in {"created", "running"} for state, _count in record.operator_frame_state_counts)
        ),
        failure_category_counts=tuple(sorted(failure_counts.items())),
        dominant_failure_category=dominant_failure_category,
        synthetic_run_count=sum(1 for record in records if record.synthetic_run),
        isolated_run_count=sum(1 for record in records if record.isolation_suffix is not None),
        missing_workflow_run_ids=missing_workflow_run_ids,
        contract_drift_refs=contract_drift_refs,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


@dataclass(slots=True)
class NativeOperatorQueryFrontdoor:
    """Repo-local frontdoor for canonical operator reads."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )

    def _resolve_instance(
        self,
        *,
        env: Mapping[str, str] | None,
    ) -> tuple[Mapping[str, str], NativeWorkflowInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    async def _fetch_issue_records(
        self,
        *,
        conn: _Connection,
        issue_ids: tuple[str, ...] | None,
    ) -> tuple[OperatorIssueRecord, ...]:
        clauses: list[str] = []
        args: list[object] = []
        _row_clause(
            column_name="issue_id",
            values=issue_ids,
            args=args,
            clauses=clauses,
        )
        query = """
            SELECT
                issue_id,
                issue_key,
                title,
                status,
                severity,
                priority,
                summary,
                source_kind,
                discovered_in_run_id,
                discovered_in_receipt_id,
                owner_ref,
                decision_ref,
                resolution_summary,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM issues
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY opened_at DESC, created_at DESC, issue_id"
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read issue rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_issue_record_from_row(row) for row in rows)

    async def _fetch_bug_records(
        self,
        *,
        conn: _Connection,
        bug_ids: tuple[str, ...] | None,
        source_issue_ids: tuple[str, ...] | None = None,
    ) -> tuple[OperatorBugRecord, ...]:
        clauses: list[str] = []
        args: list[object] = []
        source_clauses: list[str] = []
        if bug_ids is not None:
            args.append(list(bug_ids))
            source_clauses.append(f"bug_id = ANY(${len(args)}::text[])")
        if source_issue_ids:
            args.append(list(source_issue_ids))
            source_clauses.append(f"source_issue_id = ANY(${len(args)}::text[])")
        if source_clauses:
            clauses.append("(" + " OR ".join(source_clauses) + ")")
        query = """
            SELECT
                bug_id,
                bug_key,
                title,
                status,
                severity,
                priority,
                summary,
                source_kind,
                discovered_in_run_id,
                discovered_in_receipt_id,
                owner_ref,
                source_issue_id,
                decision_ref,
                resolution_summary,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM bugs
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY opened_at DESC, created_at DESC, bug_id"
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read bug rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_bug_record_from_row(row) for row in rows)

    async def _fetch_bug_evidence_links(
        self,
        *,
        conn: _Connection,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[Mapping[str, Any], ...]]:
        if not bug_ids:
            return {}
        try:
            rows = await conn.fetch(
                """
                SELECT
                    bug_id,
                    evidence_kind,
                    evidence_ref,
                    evidence_role
                FROM bug_evidence_links
                WHERE bug_id = ANY($1::text[])
                ORDER BY bug_id, created_at, bug_evidence_link_id
                """,
                list(bug_ids),
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read bug evidence rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            bug_id = _require_text(row.get("bug_id"), field_name="bug_id")
            grouped.setdefault(bug_id, []).append(dict(row))
        return {
            bug_id: tuple(group)
            for bug_id, group in grouped.items()
        }

    async def _fetch_roadmap_item_records(
        self,
        *,
        conn: _Connection,
        roadmap_item_ids: tuple[str, ...] | None,
    ) -> tuple[OperatorRoadmapItemRecord, ...]:
        clauses: list[str] = []
        args: list[object] = []
        _row_clause(
            column_name="roadmap_item_id",
            values=roadmap_item_ids,
            args=args,
            clauses=clauses,
        )
        query = """
            SELECT
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                registry_paths,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_items
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY target_end_at DESC NULLS LAST, created_at DESC, roadmap_item_id"
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read roadmap item rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_roadmap_record_from_row(row) for row in rows)

    async def _fetch_roadmap_tree_records(
        self,
        *,
        conn: _Connection,
        root_roadmap_item_id: str,
        include_completed_nodes: bool = True,
    ) -> tuple[OperatorRoadmapItemRecord, ...]:
        # DECISION: completed-node visibility is controlled explicitly by caller contract.
        # SEE: runtime.operations.queries.roadmap_tree for the public surface that binds this
        #      behavior to a query parameter instead of implicit filtering.
        completed_filter = ""
        args = [root_roadmap_item_id, f"{root_roadmap_item_id}.%"]
        if not include_completed_nodes:
            completed_filter = " AND (roadmap_item_id = $1 OR status != 'completed')"
        try:
            rows = await conn.fetch(
                """
                SELECT
                    roadmap_item_id,
                    roadmap_key,
                    title,
                    item_kind,
                    status,
                    priority,
                    parent_roadmap_item_id,
                    source_bug_id,
                    registry_paths,
                    summary,
                    acceptance_criteria,
                    decision_ref,
                    target_start_at,
                    target_end_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM roadmap_items
                WHERE (roadmap_item_id = $1 OR roadmap_item_id LIKE $2)
                  {completed_filter}
                """.format(completed_filter=completed_filter),
                *args,
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read roadmap tree rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        if not rows:
            raise NativeOperatorQueryError(
                "operator_query.roadmap_root_missing",
                "roadmap tree root was not found",
                details={"root_roadmap_item_id": root_roadmap_item_id},
            )
        records = tuple(_roadmap_record_from_row(row) for row in rows)
        return tuple(
            sorted(
                records,
                key=lambda record: _roadmap_tree_sort_key(
                    root_roadmap_item_id=root_roadmap_item_id,
                    record=record,
                ),
            )
        )

    async def _fetch_roadmap_tree_dependencies(
        self,
        *,
        conn: _Connection,
        roadmap_item_ids: tuple[str, ...],
    ) -> tuple[OperatorRoadmapDependencyRecord, ...]:
        if not roadmap_item_ids:
            return ()
        try:
            rows = await conn.fetch(
                """
                SELECT
                    roadmap_item_dependency_id,
                    roadmap_item_id,
                    depends_on_roadmap_item_id,
                    dependency_kind,
                    decision_ref,
                    created_at
                FROM roadmap_item_dependencies
                WHERE roadmap_item_id = ANY($1::text[])
                ORDER BY roadmap_item_id, created_at, roadmap_item_dependency_id
                """,
                list(roadmap_item_ids),
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read roadmap dependency rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_roadmap_dependency_record_from_row(row) for row in rows)

    async def _fetch_roadmap_semantic_neighbors(
        self,
        *,
        conn: _Connection,
        root_roadmap_item_id: str,
        subtree_roadmap_item_ids: tuple[str, ...],
        limit: int,
    ) -> tuple[tuple[OperatorRoadmapSemanticNeighborRecord, ...], str]:
        if limit <= 0:
            return (), "roadmap.semantic_neighbors.disabled"
        try:
            rows = await conn.fetch(
                """
                WITH anchor AS (
                    SELECT embedding
                    FROM roadmap_items
                    WHERE roadmap_item_id = $1
                      AND embedding IS NOT NULL
                    LIMIT 1
                )
                SELECT
                    ri.roadmap_item_id,
                    ri.title,
                    ri.status,
                    ri.priority,
                    1 - (ri.embedding <=> anchor.embedding) AS similarity
                FROM roadmap_items ri
                CROSS JOIN anchor
                WHERE ri.roadmap_item_id <> $1
                  AND ri.embedding IS NOT NULL
                  AND (ri.status IS NULL OR lower(ri.status) NOT IN ('completed', 'done', 'closed'))
                  AND NOT (ri.roadmap_item_id = ANY($2::text[]))
                ORDER BY ri.embedding <=> anchor.embedding ASC, ri.updated_at DESC, ri.roadmap_item_id
                LIMIT $3
                """,
                root_roadmap_item_id,
                list(subtree_roadmap_item_ids),
                limit,
            )
        except asyncpg.PostgresError as exc:
            if _missing_roadmap_embedding_column_error(exc):
                return (), "roadmap.semantic_neighbors.schema_unavailable"
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read roadmap semantic neighbors",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        neighbors = tuple(
            OperatorRoadmapSemanticNeighborRecord(
                roadmap_item_id=_require_text(row.get("roadmap_item_id"), field_name="roadmap_item_id"),
                title=_require_text(row.get("title"), field_name="title"),
                status=_require_text(row.get("status"), field_name="status"),
                priority=_require_text(row.get("priority"), field_name="priority"),
                similarity=float(row.get("similarity") or 0.0),
            )
            for row in rows
        )
        if neighbors:
            return neighbors, "roadmap.semantic_neighbors.found"
        return (), "roadmap.semantic_neighbors.none"

    async def _fetch_cutover_gate_records(
        self,
        *,
        conn: _Connection,
        cutover_gate_ids: tuple[str, ...] | None,
        as_of: datetime,
    ) -> tuple[OperatorCutoverGateRecord, ...]:
        clauses = [
            "opened_at <= $1",
            "(closed_at IS NULL OR closed_at > $1)",
        ]
        args: list[object] = [as_of]
        _row_clause(
            column_name="cutover_gate_id",
            values=cutover_gate_ids,
            args=args,
            clauses=clauses,
        )
        query = """
            SELECT
                cutover_gate_id,
                gate_key,
                gate_name,
                gate_kind,
                gate_status,
                roadmap_item_id,
                workflow_class_id,
                schedule_definition_id,
                gate_policy,
                required_evidence,
                opened_by_decision_id,
                closed_by_decision_id,
                opened_at,
                closed_at,
                created_at,
                updated_at
            FROM cutover_gates
        """
        query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY gate_key, opened_at DESC, created_at DESC, cutover_gate_id"
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read cutover gate rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_gate_record_from_row(row) for row in rows)

    async def _fetch_binding_records(
        self,
        *,
        conn: _Connection,
        work_item_workflow_binding_ids: tuple[str, ...] | None,
        workflow_run_ids: tuple[str, ...] | None,
        issue_ids: tuple[str, ...] | None = None,
        bug_ids: tuple[str, ...] | None = None,
        roadmap_item_ids: tuple[str, ...] | None = None,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...]:
        clauses: list[str] = []
        args: list[object] = []
        _row_clause(
            column_name="work_item_workflow_binding_id",
            values=work_item_workflow_binding_ids,
            args=args,
            clauses=clauses,
        )
        _row_clause(
            column_name="workflow_run_id",
            values=workflow_run_ids,
            args=args,
            clauses=clauses,
        )
        source_clauses: list[str] = []
        if issue_ids:
            args.append(list(issue_ids))
            source_clauses.append(f"issue_id = ANY(${len(args)}::text[])")
        if bug_ids:
            args.append(list(bug_ids))
            source_clauses.append(f"bug_id = ANY(${len(args)}::text[])")
        if roadmap_item_ids:
            args.append(list(roadmap_item_ids))
            source_clauses.append(f"roadmap_item_id = ANY(${len(args)}::text[])")
        if source_clauses:
            clauses.append("(" + " OR ".join(source_clauses) + ")")
        query = """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                issue_id,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC, work_item_workflow_binding_id"
        try:
            rows = await conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read work-item binding rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_binding_record_from_row(row) for row in rows)

    async def _fetch_workflow_run_activity(
        self,
        *,
        conn: _Connection,
        workflow_run_ids: tuple[str, ...] | None,
    ) -> dict[str, dict[str, Any]]:
        if not workflow_run_ids:
            return {}
        try:
            rows = await conn.fetch(
                """
                SELECT
                    wr.run_id AS workflow_run_id,
                    wr.current_state,
                    wr.started_at,
                    wr.finished_at,
                    GREATEST(
                        wr.requested_at,
                        COALESCE(wr.admitted_at, wr.requested_at),
                        COALESCE(wr.started_at, wr.requested_at),
                        COALESCE(wr.finished_at, wr.requested_at),
                        COALESCE(MAX(wj.created_at), wr.requested_at),
                        COALESCE(MAX(wj.claimed_at), wr.requested_at),
                        COALESCE(MAX(wj.started_at), wr.requested_at),
                        COALESCE(MAX(wj.finished_at), wr.requested_at),
                        COALESCE(MAX(wj.heartbeat_at), wr.requested_at)
                    ) AS last_touched_at
                FROM workflow_runs AS wr
                LEFT JOIN workflow_jobs AS wj
                    ON wj.run_id = wr.run_id
                WHERE wr.run_id = ANY($1::text[])
                GROUP BY
                    wr.run_id,
                    wr.current_state,
                    wr.requested_at,
                    wr.admitted_at,
                    wr.started_at,
                    wr.finished_at
                """,
                list(workflow_run_ids),
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorQueryError(
                "operator_query.read_failed",
                "failed to read workflow run activity",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return {
            str(row["workflow_run_id"]): {
                "workflow_run_id": str(row["workflow_run_id"]),
                "current_state": str(row["current_state"]),
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "last_touched_at": row["last_touched_at"],
            }
            for row in rows
        }

    @staticmethod
    def _prefetched_binding_records(
        *,
        prefetched_work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
        work_item_workflow_binding_ids: tuple[str, ...] | None,
        workflow_run_ids: tuple[str, ...] | None,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...] | None:
        if not prefetched_work_item_workflow_bindings:
            return None
        bindings_by_id = {
            binding.work_item_workflow_binding_id: binding
            for binding in prefetched_work_item_workflow_bindings
        }
        if (
            work_item_workflow_binding_ids is not None
            and any(binding_id not in bindings_by_id for binding_id in work_item_workflow_binding_ids)
        ):
            return None
        selected = (
            tuple(bindings_by_id[binding_id] for binding_id in work_item_workflow_binding_ids)
            if work_item_workflow_binding_ids is not None
            else tuple(prefetched_work_item_workflow_bindings)
        )
        if workflow_run_ids is None:
            return selected
        workflow_run_id_set = set(workflow_run_ids)
        return tuple(
            binding
            for binding in selected
            if binding.workflow_run_id is not None
            and binding.workflow_run_id in workflow_run_id_set
        )

    async def _fetch_workflow_run_packet_inspections(
        self,
        *,
        conn: _Connection,
        workflow_run_ids: tuple[str, ...] | None,
    ) -> tuple[
        tuple[OperatorWorkflowRunPacketInspectionRecord, ...],
        OperatorWorkflowRunObservabilitySummary | None,
    ]:
        if workflow_run_ids is None:
            return (), None
        contract_drift_refs: tuple[str, ...] = ()
        operator_frame_source = "canonical_operator_frames"
        run_id_list = list(workflow_run_ids)
        try:
            rows = await conn.fetch(_WORKFLOW_RUN_PACKET_INSPECTIONS_QUERY, run_id_list)
        except Exception as exc:
            if _missing_packet_inspection_column_error(exc):
                contract_drift_refs = ("workflow_runs.packet_inspection_column_missing",)
                try:
                    rows = await conn.fetch(
                        _LEGACY_WORKFLOW_RUN_PACKET_INSPECTIONS_QUERY,
                        run_id_list,
                    )
                except Exception as legacy_exc:
                    if not _missing_run_operator_frames_table_error(legacy_exc):
                        raise
                    operator_frame_source = "missing"
                    rows = await conn.fetch(
                        _LEGACY_WORKFLOW_RUN_PACKET_INSPECTIONS_WITHOUT_OPERATOR_FRAMES_QUERY,
                        run_id_list,
                    )
            elif _missing_run_operator_frames_table_error(exc):
                operator_frame_source = "missing"
                try:
                    rows = await conn.fetch(
                        _WORKFLOW_RUN_PACKET_INSPECTIONS_WITHOUT_OPERATOR_FRAMES_QUERY,
                        run_id_list,
                    )
                except Exception as no_frames_exc:
                    if not _missing_packet_inspection_column_error(no_frames_exc):
                        raise
                    contract_drift_refs = ("workflow_runs.packet_inspection_column_missing",)
                    rows = await conn.fetch(
                        _LEGACY_WORKFLOW_RUN_PACKET_INSPECTIONS_WITHOUT_OPERATOR_FRAMES_QUERY,
                        run_id_list,
                    )
            else:
                return (), None

        try:
            from runtime.execution_packet_authority import (
                inspect_execution_packets,
                packet_inspection_from_row,
            )
        except Exception:
            inspect_execution_packets = None
            packet_inspection_from_row = None

        packet_inspections: list[OperatorWorkflowRunPacketInspectionRecord] = []
        for row in rows:
            packet_inspection_source = "missing"
            packet_inspection = None
            operator_frames = _normalize_operator_frame_payloads(row.get("operator_frames"))
            if packet_inspection_from_row is not None:
                packet_inspection = packet_inspection_from_row(row)
                if packet_inspection is not None:
                    packet_inspection_source = "materialized"
            if packet_inspection is None:
                packets = row.get("packets")
                if isinstance(packets, str):
                    try:
                        packets = json.loads(packets)
                    except (json.JSONDecodeError, ValueError, TypeError):
                        packets = []
                if not isinstance(packets, Sequence) or isinstance(packets, (str, bytes, bytearray)):
                    packets = []
                if (
                    packets
                    and inspect_execution_packets is not None
                ):
                    try:
                        packet_inspection = inspect_execution_packets(packets, run_row=dict(row))
                        if packet_inspection is not None:
                            packet_inspection_source = "derived"
                    except Exception:
                        packet_inspection = None
            packet_inspections.append(
                _workflow_run_packet_inspection_record_from_row(
                    row,
                    packet_inspection,
                    packet_inspection_source=packet_inspection_source,
                    operator_frame_source=operator_frame_source,
                    operator_frames=operator_frames,
                    contract_drift_refs=contract_drift_refs,
                )
            )
        return (
            tuple(packet_inspections),
            _workflow_run_observability_summary(
                workflow_run_ids=workflow_run_ids,
                records=packet_inspections,
                contract_drift_refs=contract_drift_refs,
            ),
        )

    async def _query_operator_surface(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
        issue_ids: tuple[str, ...] | None,
        bug_ids: tuple[str, ...] | None,
        roadmap_item_ids: tuple[str, ...] | None,
        cutover_gate_ids: tuple[str, ...] | None,
        work_item_workflow_binding_ids: tuple[str, ...] | None,
        workflow_run_ids: tuple[str, ...] | None,
        conn: _Connection | None = None,
        prefetched_work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...] | None = None,
    ) -> NativeOperatorQuerySnapshot:
        managed_conn = conn is None
        if conn is None:
            conn = await self.connect_database(env)
        try:
            issues: tuple[OperatorIssueRecord, ...] = ()
            assessment_issues: tuple[OperatorIssueRecord, ...] = ()
            if issue_ids is None:
                assessment_issue_ids = ()
            else:
                assessment_issue_ids = tuple(dict.fromkeys(issue_ids))
                assessment_issues = await self._fetch_issue_records(
                    conn=conn,
                    issue_ids=assessment_issue_ids,
                )
                requested_issue_ids = set(issue_ids)
                issues = tuple(
                    record for record in assessment_issues if record.issue_id in requested_issue_ids
                )
            roadmap_items = await self._fetch_roadmap_item_records(
                conn=conn,
                roadmap_item_ids=roadmap_item_ids,
            )
            if bug_ids is None:
                bugs = await self._fetch_bug_records(
                    conn=conn,
                    bug_ids=tuple(
                        record.source_bug_id
                        for record in roadmap_items
                        if record.source_bug_id is not None
                    )
                    or None,
                    source_issue_ids=assessment_issue_ids or None,
                )
                if not bugs and not roadmap_items and not assessment_issue_ids:
                    bugs = await self._fetch_bug_records(conn=conn, bug_ids=None)
                assessment_bug_ids = tuple(
                    dict.fromkeys(
                        (
                            *(record.bug_id for record in bugs),
                            *(
                                record.source_bug_id
                                for record in roadmap_items
                                if record.source_bug_id is not None
                            ),
                        )
                    )
                )
                assessment_bugs = bugs
            else:
                assessment_bug_ids = tuple(
                    dict.fromkeys(
                        (
                            *bug_ids,
                            *(
                                record.source_bug_id
                                for record in roadmap_items
                                if record.source_bug_id is not None
                            ),
                        )
                    )
                )
                assessment_bugs = await self._fetch_bug_records(
                    conn=conn,
                    bug_ids=assessment_bug_ids,
                    source_issue_ids=assessment_issue_ids or None,
                )
                requested_bug_ids = set(bug_ids)
                bugs = tuple(
                    record for record in assessment_bugs if record.bug_id in requested_bug_ids
                )
            if issue_ids is not None:
                assessment_issue_ids = tuple(
                    dict.fromkeys(
                        (
                            *issue_ids,
                            *(
                                record.source_issue_id
                                for record in assessment_bugs
                                if record.source_issue_id is not None
                            ),
                        )
                    )
                )
                assessment_issues = await self._fetch_issue_records(
                    conn=conn,
                    issue_ids=assessment_issue_ids,
                )
                requested_issue_ids = set(issue_ids)
                issues = tuple(
                    record for record in assessment_issues if record.issue_id in requested_issue_ids
                )
            cutover_gates = await self._fetch_cutover_gate_records(
                conn=conn,
                cutover_gate_ids=cutover_gate_ids,
                as_of=as_of,
            )
            work_item_workflow_bindings = self._prefetched_binding_records(
                prefetched_work_item_workflow_bindings=(
                    ()
                    if prefetched_work_item_workflow_bindings is None
                    else prefetched_work_item_workflow_bindings
                ),
                work_item_workflow_binding_ids=work_item_workflow_binding_ids,
                workflow_run_ids=workflow_run_ids,
            )
            if work_item_workflow_bindings is None:
                work_item_workflow_bindings = await self._fetch_binding_records(
                    conn=conn,
                    work_item_workflow_binding_ids=work_item_workflow_binding_ids,
                    workflow_run_ids=workflow_run_ids,
                    issue_ids=assessment_issue_ids,
                    bug_ids=assessment_bug_ids,
                    roadmap_item_ids=tuple(
                        record.roadmap_item_id for record in roadmap_items
                    ),
                )
            assessment_workflow_run_ids = tuple(
                dict.fromkeys(
                    binding.workflow_run_id
                    for binding in work_item_workflow_bindings
                    if binding.workflow_run_id is not None
                )
            )
            workflow_run_activity = await self._fetch_workflow_run_activity(
                conn=conn,
                workflow_run_ids=assessment_workflow_run_ids,
            )
            (
                workflow_run_packet_inspections,
                workflow_run_observability,
            ) = await self._fetch_workflow_run_packet_inspections(
                conn=conn,
                workflow_run_ids=workflow_run_ids,
            )
            bug_evidence_links = await self._fetch_bug_evidence_links(
                conn=conn,
                bug_ids=assessment_bug_ids,
            )
            work_item_assessments = assess_work_items(
                issues=[
                    {
                        "issue_id": record.issue_id,
                        "updated_at": record.updated_at,
                        "resolved_at": record.resolved_at,
                    }
                    for record in (
                        issues
                        if issue_ids is None
                        else assessment_issues
                    )
                ],
                bugs=[
                    {
                        "bug_id": record.bug_id,
                        "updated_at": record.updated_at,
                        "resolved_at": record.resolved_at,
                        "source_issue_id": record.source_issue_id,
                    }
                    for record in assessment_bugs
                ],
                roadmap_items=[
                    {
                        "roadmap_item_id": record.roadmap_item_id,
                        "source_bug_id": record.source_bug_id,
                        "registry_paths": record.registry_paths,
                        "updated_at": record.updated_at,
                        "completed_at": record.completed_at,
                        "target_end_at": record.target_end_at,
                    }
                    for record in roadmap_items
                ],
                bug_evidence_links=bug_evidence_links,
                work_item_workflow_bindings=tuple(
                    {
                        "work_item_workflow_binding_id": binding.work_item_workflow_binding_id,
                        "binding_kind": binding.binding_kind,
                        "binding_status": binding.binding_status,
                        "issue_id": binding.issue_id,
                        "roadmap_item_id": binding.roadmap_item_id,
                        "bug_id": binding.bug_id,
                        "workflow_run_id": binding.workflow_run_id,
                        "created_at": binding.created_at,
                        "updated_at": binding.updated_at,
                    }
                    for binding in work_item_workflow_bindings
                ),
                workflow_run_activity=workflow_run_activity,
                as_of=as_of,
                repo_root=_repo_root(),
            )
            issue_id_filter = {record.issue_id for record in issues}
            bug_id_filter = {record.bug_id for record in bugs}
            roadmap_item_id_filter = {
                record.roadmap_item_id for record in roadmap_items
            }
            work_item_assessments = tuple(
                record
                for record in work_item_assessments
                if (
                    record.item_kind == "issue"
                    and record.item_id in issue_id_filter
                )
                or (
                    record.item_kind == "bug"
                    and record.item_id in bug_id_filter
                )
                or (
                    record.item_kind == "roadmap_item"
                    and record.item_id in roadmap_item_id_filter
                )
            )
            work_item_closeout_recommendations = _derive_closeout_recommendations(
                work_item_assessments
            )
            return NativeOperatorQuerySnapshot(
                issues=issues,
                bugs=bugs,
                roadmap_items=roadmap_items,
                cutover_gates=cutover_gates,
                work_item_workflow_bindings=work_item_workflow_bindings,
                work_item_assessments=work_item_assessments,
                work_item_closeout_recommendations=work_item_closeout_recommendations,
                as_of=as_of,
                issue_ids=issue_ids,
                bug_ids=bug_ids,
                roadmap_item_ids=roadmap_item_ids,
                cutover_gate_ids=cutover_gate_ids,
                work_item_workflow_binding_ids=work_item_workflow_binding_ids,
                workflow_run_ids=workflow_run_ids,
                workflow_run_packet_inspections=workflow_run_packet_inspections,
                workflow_run_observability=workflow_run_observability,
            )
        finally:
            if managed_conn:
                await conn.close()

    async def query_operator_surface_async(
        self,
        *,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
        issue_ids: Sequence[str] | None = None,
        bug_ids: Sequence[str] | None = None,
        roadmap_item_ids: Sequence[str] | None = None,
        cutover_gate_ids: Sequence[str] | None = None,
        work_item_workflow_binding_ids: Sequence[str] | None = None,
        workflow_run_ids: Sequence[str] | None = None,
        conn: _Connection | None = None,
        prefetched_work_item_workflow_bindings: Sequence[WorkItemWorkflowBindingRecord] | None = None,
    ) -> dict[str, Any]:
        """Async operator query entrypoint that can reuse an existing connection."""

        source, instance = self._resolve_instance(env=env)
        snapshot = await self._query_operator_surface(
            env=source,
            as_of=(
                _now()
                if as_of is None
                else _normalize_as_of(
                    as_of,
                    error_type=NativeOperatorQueryError,
                    reason_code="operator_query.invalid_as_of",
                )
            ),
            issue_ids=_normalize_ids(issue_ids, field_name="issue_ids"),
            bug_ids=_normalize_ids(bug_ids, field_name="bug_ids"),
            roadmap_item_ids=_normalize_ids(
                roadmap_item_ids,
                field_name="roadmap_item_ids",
            ),
            cutover_gate_ids=_normalize_ids(
                cutover_gate_ids,
                field_name="cutover_gate_ids",
            ),
            work_item_workflow_binding_ids=_normalize_ids(
                work_item_workflow_binding_ids,
                field_name="work_item_workflow_binding_ids",
            ),
            workflow_run_ids=_normalize_ids(
                workflow_run_ids,
                field_name="workflow_run_ids",
            ),
            conn=conn,
            prefetched_work_item_workflow_bindings=(
                None
                if prefetched_work_item_workflow_bindings is None
                else tuple(prefetched_work_item_workflow_bindings)
            ),
        )
        return {
            "native_instance": instance.to_contract(),
            **snapshot.to_json(),
        }

    async def _query_roadmap_tree(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
        root_roadmap_item_id: str,
        semantic_neighbor_limit: int,
        include_completed_nodes: bool = True,
    ) -> OperatorRoadmapTreeSnapshot:
        conn = await self.connect_database(env)
        try:
            roadmap_items = await self._fetch_roadmap_tree_records(
                conn=conn,
                root_roadmap_item_id=root_roadmap_item_id,
                include_completed_nodes=include_completed_nodes,
            )
            root_item = next(
                (
                    record for record in roadmap_items
                    if record.roadmap_item_id == root_roadmap_item_id
                ),
                None,
            )
            if root_item is None:
                raise NativeOperatorQueryError(
                    "operator_query.roadmap_root_missing",
                    "roadmap tree root was not found in subtree results",
                    details={"root_roadmap_item_id": root_roadmap_item_id},
                )
            roadmap_item_dependencies = await self._fetch_roadmap_tree_dependencies(
                conn=conn,
                roadmap_item_ids=tuple(
                    record.roadmap_item_id for record in roadmap_items
                ),
            )
            source_bug_ids = tuple(
                dict.fromkeys(
                    record.source_bug_id
                    for record in roadmap_items
                    if record.source_bug_id is not None
                )
            )
            bugs = await self._fetch_bug_records(
                conn=conn,
                bug_ids=source_bug_ids or None,
            )
            bug_evidence_links = await self._fetch_bug_evidence_links(
                conn=conn,
                bug_ids=source_bug_ids,
            )
            work_item_workflow_bindings = await self._fetch_binding_records(
                conn=conn,
                work_item_workflow_binding_ids=None,
                workflow_run_ids=None,
                bug_ids=source_bug_ids or None,
                roadmap_item_ids=tuple(
                    record.roadmap_item_id for record in roadmap_items
                ),
            )
            workflow_run_activity = await self._fetch_workflow_run_activity(
                conn=conn,
                workflow_run_ids=tuple(
                    dict.fromkeys(
                        binding.workflow_run_id
                        for binding in work_item_workflow_bindings
                        if binding.workflow_run_id is not None
                    )
                ),
            )
            work_item_assessments = assess_work_items(
                bugs=[
                    {
                        "bug_id": record.bug_id,
                        "updated_at": record.updated_at,
                        "resolved_at": record.resolved_at,
                    }
                    for record in bugs
                ],
                roadmap_items=[
                    {
                        "roadmap_item_id": record.roadmap_item_id,
                        "source_bug_id": record.source_bug_id,
                        "registry_paths": record.registry_paths,
                        "updated_at": record.updated_at,
                        "completed_at": record.completed_at,
                        "target_end_at": record.target_end_at,
                    }
                    for record in roadmap_items
                ],
                bug_evidence_links=bug_evidence_links,
                work_item_workflow_bindings=tuple(
                    {
                        "work_item_workflow_binding_id": binding.work_item_workflow_binding_id,
                        "binding_kind": binding.binding_kind,
                        "binding_status": binding.binding_status,
                        "roadmap_item_id": binding.roadmap_item_id,
                        "bug_id": binding.bug_id,
                        "workflow_run_id": binding.workflow_run_id,
                        "created_at": binding.created_at,
                        "updated_at": binding.updated_at,
                    }
                    for binding in work_item_workflow_bindings
                ),
                workflow_run_activity=workflow_run_activity,
                as_of=as_of,
                repo_root=_repo_root(),
            )
            semantic_neighbors, semantic_neighbors_reason_code = (
                await self._fetch_roadmap_semantic_neighbors(
                    conn=conn,
                    root_roadmap_item_id=root_roadmap_item_id,
                    subtree_roadmap_item_ids=tuple(
                        record.roadmap_item_id for record in roadmap_items
                    ),
                    limit=semantic_neighbor_limit,
                )
            )
            return OperatorRoadmapTreeSnapshot(
                root_roadmap_item_id=root_roadmap_item_id,
                root_item=root_item,
                roadmap_items=roadmap_items,
                roadmap_item_dependencies=roadmap_item_dependencies,
                work_item_assessments=tuple(
                    record
                    for record in work_item_assessments
                    if record.item_kind == "roadmap_item"
                ),
                semantic_neighbors=semantic_neighbors,
                semantic_neighbors_reason_code=semantic_neighbors_reason_code,
                as_of=as_of,
            )
        finally:
            await conn.close()

    def query_operator_surface(
        self,
        *,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
        issue_ids: Sequence[str] | None = None,
        bug_ids: Sequence[str] | None = None,
        roadmap_item_ids: Sequence[str] | None = None,
        cutover_gate_ids: Sequence[str] | None = None,
        work_item_workflow_binding_ids: Sequence[str] | None = None,
        workflow_run_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Read canonical bug, roadmap, cutover, and binding rows through Postgres."""

        source, instance = self._resolve_instance(env=env)
        snapshot = _run_async(
            self._query_operator_surface(
                env=source,
                as_of=(
                _now()
                if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeOperatorQueryError,
                        reason_code="operator_query.invalid_as_of",
                )
            ),
                issue_ids=_normalize_ids(issue_ids, field_name="issue_ids"),
                bug_ids=_normalize_ids(bug_ids, field_name="bug_ids"),
                roadmap_item_ids=_normalize_ids(
                    roadmap_item_ids,
                    field_name="roadmap_item_ids",
                ),
                cutover_gate_ids=_normalize_ids(
                    cutover_gate_ids,
                    field_name="cutover_gate_ids",
                ),
                work_item_workflow_binding_ids=_normalize_ids(
                    work_item_workflow_binding_ids,
                    field_name="work_item_workflow_binding_ids",
                ),
                workflow_run_ids=_normalize_ids(
                    workflow_run_ids,
                    field_name="workflow_run_ids",
                ),
            ),
            error_type=NativeOperatorQueryError,
            reason_code="operator_query.async_boundary_required",
            message="native operator query sync entrypoints require a non-async call boundary",
        )
        return {
            "native_instance": instance.to_contract(),
            **snapshot.to_json(),
        }

    async def _query_issue_backlog(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
        issue_ids: tuple[str, ...] | None,
        status: str | None,
        open_only: bool,
        limit: int,
    ) -> dict[str, Any]:
        conn = await self.connect_database(env)
        try:
            issues = await self._fetch_issue_records(
                conn=conn,
                issue_ids=issue_ids,
            )
            filtered: list[OperatorIssueRecord] = []
            for record in issues:
                normalized_status = record.status.strip().lower()
                if open_only and normalized_status == "resolved":
                    continue
                if status is not None and normalized_status != status:
                    continue
                filtered.append(record)

            filtered.sort(
                key=lambda record: (
                    record.opened_at,
                    record.created_at,
                    record.issue_id,
                ),
                reverse=True,
            )
            limited = filtered[:limit]
            counts = Counter(record.status for record in limited)
            severity_counts = Counter(record.severity for record in limited)
            return {
                "kind": "issue_backlog",
                "as_of": as_of.isoformat(),
                "query": {
                    "issue_ids": _json_list(issue_ids),
                    "status": status,
                    "open_only": open_only,
                    "limit": limit,
                },
                "count": len(limited),
                "total_issues": len(issues),
                "issues": [record.to_json() for record in limited],
                "counts": {
                    "by_status": dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
                    "by_severity": dict(sorted(severity_counts.items(), key=lambda item: (-item[1], item[0]))),
                },
            }
        finally:
            await conn.close()

    def query_issue_backlog(
        self,
        *,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
        issue_ids: Sequence[str] | None = None,
        status: str | None = None,
        open_only: bool = True,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Read canonical issue backlog rows through Postgres."""

        source, instance = self._resolve_instance(env=env)
        try:
            issue_limit = int(limit)
        except (TypeError, ValueError) as exc:
            raise NativeOperatorQueryError(
                "operator_query.invalid_request",
                "limit must be an integer",
                details={"field": "limit"},
            ) from exc
        if issue_limit < 1:
            raise NativeOperatorQueryError(
                "operator_query.invalid_request",
                "limit must be >= 1",
                details={"field": "limit"},
            )

        normalized_status = _optional_text(status, field_name="status")
        if normalized_status is not None:
            normalized_status = normalized_status.strip().lower()
        snapshot = _run_async(
            self._query_issue_backlog(
                env=source,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeOperatorQueryError,
                        reason_code="operator_query.invalid_as_of",
                    )
                ),
                issue_ids=_normalize_ids(issue_ids, field_name="issue_ids"),
                status=normalized_status,
                open_only=bool(open_only) and normalized_status is None,
                limit=issue_limit,
            ),
            error_type=NativeOperatorQueryError,
            reason_code="operator_query.async_boundary_required",
            message="native operator query sync entrypoints require a non-async call boundary",
        )
        return {
            "native_instance": instance.to_contract(),
            **snapshot,
        }

    def query_roadmap_tree(
        self,
        *,
        root_roadmap_item_id: str,
        semantic_neighbor_limit: int = 5,
        include_completed_nodes: bool = True,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Read one roadmap subtree and its dependency edges through Postgres."""

        source, instance = self._resolve_instance(env=env)
        root_id = _require_text(
            root_roadmap_item_id,
            field_name="root_roadmap_item_id",
        )
        try:
            semantic_limit = int(semantic_neighbor_limit)
        except (TypeError, ValueError) as exc:
            raise NativeOperatorQueryError(
                "operator_query.invalid_request",
                "semantic_neighbor_limit must be an integer",
                details={"field": "semantic_neighbor_limit"},
            ) from exc
        if semantic_limit < 0:
            raise NativeOperatorQueryError(
                "operator_query.invalid_request",
                "semantic_neighbor_limit must be >= 0",
                details={"field": "semantic_neighbor_limit"},
            )
        snapshot = _run_async(
            self._query_roadmap_tree(
                env=source,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeOperatorQueryError,
                        reason_code="operator_query.invalid_as_of",
                    )
                ),
                root_roadmap_item_id=root_id,
                semantic_neighbor_limit=semantic_limit,
                include_completed_nodes=include_completed_nodes,
            ),
            error_type=NativeOperatorQueryError,
            reason_code="operator_query.async_boundary_required",
            message="native operator query sync entrypoints require a non-async call boundary",
        )
        return {
            "native_instance": instance.to_contract(),
            **snapshot.to_json(),
        }


_DEFAULT_NATIVE_OPERATOR_QUERY_FRONTDOOR = NativeOperatorQueryFrontdoor()

# Publish the repo-local operator read methods directly so the public surface
# is the control-plane object, not the legacy wrapper function.
query_issue_backlog = _DEFAULT_NATIVE_OPERATOR_QUERY_FRONTDOOR.query_issue_backlog
query_operator_surface = _DEFAULT_NATIVE_OPERATOR_QUERY_FRONTDOOR.query_operator_surface
query_roadmap_tree = _DEFAULT_NATIVE_OPERATOR_QUERY_FRONTDOOR.query_roadmap_tree


__all__ = [
    "NativeOperatorQueryError",
    "NativeOperatorQueryFrontdoor",
    "NativeOperatorQuerySnapshot",
    "OperatorBugRecord",
    "OperatorCutoverGateRecord",
    "OperatorIssueRecord",
    "OperatorRoadmapDependencyRecord",
    "OperatorRoadmapSemanticNeighborRecord",
    "OperatorRoadmapItemRecord",
    "OperatorRoadmapTreeSnapshot",
    "OperatorWorkflowRunPacketInspectionRecord",
    "OperatorWorkflowRunObservabilitySummary",
    "OperatorWorkItemCloseoutRecommendationRecord",
    "_repo_root",
    "query_issue_backlog",
    "query_operator_surface",
    "query_roadmap_tree",
]
