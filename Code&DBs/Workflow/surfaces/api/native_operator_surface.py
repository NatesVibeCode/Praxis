"""Consolidated native operator surface over query and cockpit reads.

This seam is intentionally read-only:

- it resolves the native instance contract once
- it relays the existing operator query surface unchanged
- it folds the W24 operator cockpit into the default native operator read
  surface
- it publishes one shared provenance envelope for the stitched read
- it does not introduce hosted dashboard truth or any mutation path
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg

from observability.operator_dashboard import (
    NativeOperatorCockpitError,
    NativeOperatorCockpitReadModel,
    operator_cockpit_run,
)
from observability.operator_topology import (
    NativeCutoverGraphStatusReadModel,
    cutover_graph_status_run,
)
from authority.workflow_class_resolution import (
    WorkflowClassResolutionDecision,
    load_workflow_class_resolution_runtime,
)
from receipts import EvidenceRow
from authority.operator_control import (
    OperatorControlAuthority,
    load_operator_control_authority,
)
from registry.provider_routing import (
    ProviderRouteAuthority as ProviderRouteControlTower,
    load_provider_route_authority_snapshot as load_provider_route_control_tower_snapshot,
)
from runtime._helpers import _dedupe
from runtime.instance import NativeDagInstance, resolve_native_instance
from registry.persona_authority import (
    ForkOwnershipSelector,
    PersonaActivationSelector,
    PersonaAndForkAuthorityRepositoryError,
    PostgresPersonaAndForkAuthorityRepository,
)
from runtime.work_item_workflow_bindings import (
    WorkItemWorkflowBindingError,
    WorkItemWorkflowBindingRecord,
    load_work_item_workflow_bindings_for_workflow_run,
    project_work_item_workflow_binding,
)
from storage.postgres import PostgresEvidenceReader, connect_workflow_database
from storage.postgres import PostgresConfigurationError

from ._operator_helpers import (
    _json_compatible,
    _normalize_as_of as _shared_normalize_as_of,
    _now,
    _run_async as _shared_run_async,
)
from .frontdoor import status as frontdoor_status
from .operator_read import query_operator_surface

__all__ = [
    "NativeOperatorSurfaceError",
    "NativeOperatorSurfaceFrontdoor",
    "NativeOperatorSurfaceProvenance",
    "NativeOperatorSurfaceReadModel",
    "query_native_operator_surface",
]


class NativeOperatorSurfaceError(RuntimeError):
    """Raised when the consolidated native operator surface cannot complete safely."""

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


_NATIVE_OPERATOR_PERSONA_BINDING_SCOPE = "native_runtime"
_NATIVE_OPERATOR_PERSONA_OPERATOR_PATH = "native_operator_surface"
_BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE = "packet.authoritative_fork"
_BOUNDED_FORK_OWNERSHIP_SELECTION_PATH = "bounded_authoritative_fork"
_NATIVE_SMOKE_WORKFLOW_ID_PREFIX = "workflow.native-self-hosted-smoke"
_SMOKE_FRESHNESS_SLO_SECONDS = 24 * 60 * 60


def _run_async(awaitable: Awaitable[Any]) -> Any:
    return _shared_run_async(
        awaitable,
        error_type=NativeOperatorSurfaceError,
        reason_code="native_operator_surface.async_boundary_required",
        message="native operator surface sync entrypoints require a non-async call boundary",
    )


def _normalize_as_of(value: datetime) -> datetime:
    return _shared_normalize_as_of(
        value,
        error_type=NativeOperatorSurfaceError,
        reason_code="native_operator_surface.invalid_as_of",
    )


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_ids(
    values: Sequence[str] | None,
    *,
    field_name: str,
) -> tuple[str, ...] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes, bytearray)):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_request",
            f"{field_name} must be an array of strings",
            details={"field": field_name, "value_type": type(values).__name__},
        )
    normalized = tuple(
        _require_text(value, field_name=f"{field_name}[{index}]")
        for index, value in enumerate(values)
    )
    if not normalized:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_request",
            f"{field_name} must not be empty",
            details={"field": field_name},
        )
    return normalized


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _parse_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be an ISO datetime string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    try:
        parsed_value = datetime.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - defensive
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be an ISO datetime string",
            details={"field": field_name, "value": value},
        ) from exc
    return _require_datetime(parsed_value, field_name=field_name)


def _is_undefined_table_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) == "42P01"


def _smoke_failure_category(row: Mapping[str, Any]) -> str:
    normalized = " ".join(
        str(row.get(field_name) or "").strip().lower()
        for field_name in ("terminal_reason_code", "current_state")
    )
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


def _datetime_from_row(row: Mapping[str, Any], field_name: str) -> datetime | None:
    value = row.get(field_name)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _duration_seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds()), 3)


def _smoke_freshness_payload(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    if not rows:
        return {
            "kind": "native_smoke_freshness",
            "authority": "workflow_runs",
            "workflow_id_prefix": _NATIVE_SMOKE_WORKFLOW_ID_PREFIX,
            "freshness_slo_seconds": _SMOKE_FRESHNESS_SLO_SECONDS,
            "state": "missing",
            "last_run_id": None,
            "latest_run_current_state": None,
            "latest_terminal_reason_code": None,
            "latest_failure_category": None,
            "last_requested_at": None,
            "last_success_at": None,
            "age_seconds": None,
            "fail_streak": 0,
            "current_failure_signature": None,
            "latest_step_durations_s": {},
            "recent_run_ids": [],
        }

    latest_row = dict(rows[0])
    latest_requested_at = (
        _datetime_from_row(latest_row, "requested_at")
        or _datetime_from_row(latest_row, "admitted_at")
        or _datetime_from_row(latest_row, "started_at")
        or _datetime_from_row(latest_row, "finished_at")
    )
    latest_failure_category = _smoke_failure_category(latest_row)
    last_success_at: datetime | None = None
    fail_streak = 0
    current_failure_signature: str | None = None
    for row in rows:
        category = _smoke_failure_category(row)
        row_requested_at = (
            _datetime_from_row(row, "requested_at")
            or _datetime_from_row(row, "admitted_at")
            or _datetime_from_row(row, "started_at")
            or _datetime_from_row(row, "finished_at")
        )
        if category in {"success", "in_progress"}:
            if last_success_at is None and row_requested_at is not None:
                last_success_at = row_requested_at
            break
        fail_streak += 1
        if current_failure_signature is None:
            current_failure_signature = _text(latest_row.get("terminal_reason_code")) or _text(
                latest_row.get("current_state")
            )
    if latest_requested_at is None:
        state = "missing"
        age_seconds = None
    else:
        age_seconds = round(max(0.0, (as_of - latest_requested_at).total_seconds()), 3)
        state = "fresh" if age_seconds <= _SMOKE_FRESHNESS_SLO_SECONDS else "stale"
    latest_admitted_at = _datetime_from_row(latest_row, "admitted_at")
    latest_started_at = _datetime_from_row(latest_row, "started_at")
    latest_finished_at = _datetime_from_row(latest_row, "finished_at")
    return {
        "kind": "native_smoke_freshness",
        "authority": "workflow_runs",
        "workflow_id_prefix": _NATIVE_SMOKE_WORKFLOW_ID_PREFIX,
        "freshness_slo_seconds": _SMOKE_FRESHNESS_SLO_SECONDS,
        "state": state,
        "last_run_id": _text(latest_row.get("run_id")),
        "latest_run_current_state": _text(latest_row.get("current_state")),
        "latest_terminal_reason_code": _text(latest_row.get("terminal_reason_code")),
        "latest_failure_category": latest_failure_category,
        "last_requested_at": None if latest_requested_at is None else latest_requested_at.isoformat(),
        "last_success_at": None if last_success_at is None else last_success_at.isoformat(),
        "age_seconds": age_seconds,
        "fail_streak": fail_streak,
        "current_failure_signature": current_failure_signature,
        "latest_step_durations_s": {
            "requested_to_admitted": _duration_seconds(latest_requested_at, latest_admitted_at),
            "admitted_to_started": _duration_seconds(latest_admitted_at, latest_started_at),
            "started_to_finished": _duration_seconds(latest_started_at, latest_finished_at),
            "requested_to_finished": _duration_seconds(latest_requested_at, latest_finished_at),
        },
        "recent_run_ids": [
            _require_text(row.get("run_id"), field_name=f"smoke_rows[{index}].run_id")
            for index, row in enumerate(rows[:5])
        ],
    }


def _default_evidence_reader_factory(env: Mapping[str, str] | None) -> PostgresEvidenceReader:
    return PostgresEvidenceReader(env=env)


def _normalize_query_payload(
    query_payload: Mapping[str, Any],
    *,
    expected_native_instance: Mapping[str, Any],
    expected_as_of: datetime,
) -> dict[str, Any]:
    normalized_payload = dict(_require_mapping(query_payload, field_name="query_payload"))

    nested_native_instance = normalized_payload.pop("native_instance", None)
    if nested_native_instance is not None and nested_native_instance != expected_native_instance:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.query_native_instance_mismatch",
            "nested query payload published a different native_instance than the outer surface",
            details={
                "expected_native_instance": dict(expected_native_instance),
                "query_native_instance": _json_compatible(nested_native_instance),
            },
        )

    nested_as_of = normalized_payload.pop("as_of", None)
    if nested_as_of is not None and _parse_datetime(
        nested_as_of,
        field_name="query.as_of",
    ) != expected_as_of:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.query_as_of_mismatch",
            "nested query payload published a different as_of than the outer surface",
            details={
                "expected_as_of": expected_as_of.isoformat(),
                "query_as_of": nested_as_of,
            },
        )

    return normalized_payload


def _binding_from_json(row: Mapping[str, Any]) -> WorkItemWorkflowBindingRecord:
    try:
        return project_work_item_workflow_binding(row)
    except WorkItemWorkflowBindingError as exc:
        raise NativeOperatorSurfaceError(
            f"native_operator_surface.{exc.reason_code.rsplit('.', 1)[-1]}",
            "work binding JSON payload could not be projected into the canonical record",
            details={
                "reason_code": exc.reason_code,
                **exc.details,
            },
        ) from exc
    except Exception as exc:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            "work binding JSON payload could not be projected into the canonical record",
            details={"error_type": type(exc).__name__},
        ) from exc


def _bindings_from_query_payload(
    query_payload: Mapping[str, Any],
) -> tuple[WorkItemWorkflowBindingRecord, ...]:
    raw_bindings = query_payload.get("work_item_workflow_bindings")
    if raw_bindings is None:
        return ()
    if not isinstance(raw_bindings, Sequence) or isinstance(
        raw_bindings,
        (str, bytes, bytearray),
    ):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            "work_item_workflow_bindings must be an array",
            details={"value_type": type(raw_bindings).__name__},
        )
    return tuple(
        _binding_from_json(_require_mapping(row, field_name=f"work_item_workflow_bindings[{index}]"))
        for index, row in enumerate(raw_bindings)
    )


def _binding_ids(
    bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> tuple[str, ...]:
    return tuple(binding.work_item_workflow_binding_id for binding in bindings)


def _duplicate_binding_ids(
    bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: list[str] = []
    recorded: set[str] = set()
    for binding in bindings:
        binding_id = binding.work_item_workflow_binding_id
        if binding_id in seen and binding_id not in recorded:
            duplicates.append(binding_id)
            recorded.add(binding_id)
            continue
        seen.add(binding_id)
    return tuple(duplicates)


def _select_authoritative_bindings(
    *,
    run_scoped_bindings: Sequence[WorkItemWorkflowBindingRecord],
    selected_binding_ids: tuple[str, ...] | None,
) -> tuple[WorkItemWorkflowBindingRecord, ...]:
    if selected_binding_ids is None:
        return tuple(run_scoped_bindings)
    bindings_by_id = {
        binding.work_item_workflow_binding_id: binding for binding in run_scoped_bindings
    }
    return tuple(bindings_by_id[binding_id] for binding_id in selected_binding_ids)


def _resolve_query_binding_ids(
    *,
    run_id: str,
    run_scoped_bindings: Sequence[WorkItemWorkflowBindingRecord],
    requested_binding_ids: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    run_scoped_binding_ids = _binding_ids(run_scoped_bindings)
    if requested_binding_ids is None:
        return run_scoped_binding_ids or None

    run_scoped_binding_id_set = set(run_scoped_binding_ids)
    mismatched_binding_ids = tuple(
        binding_id
        for binding_id in requested_binding_ids
        if binding_id not in run_scoped_binding_id_set
    )
    if mismatched_binding_ids:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.binding_run_scope_mismatch",
            "requested work_item_workflow_binding_ids are not governed by the requested run",
            details={
                "run_id": run_id,
                "requested_binding_ids": requested_binding_ids,
                "run_scoped_binding_ids": run_scoped_binding_ids,
                "mismatched_binding_ids": mismatched_binding_ids,
            },
        )
    return requested_binding_ids


def _validate_query_binding_echo(
    *,
    run_id: str,
    authoritative_bindings: Sequence[WorkItemWorkflowBindingRecord],
    query_payload: Mapping[str, Any],
) -> None:
    echoed_bindings = _bindings_from_query_payload(query_payload)
    authoritative_duplicate_binding_ids = _duplicate_binding_ids(authoritative_bindings)
    echoed_duplicate_binding_ids = _duplicate_binding_ids(echoed_bindings)
    authoritative_by_id = {
        binding.work_item_workflow_binding_id: binding for binding in authoritative_bindings
    }
    echoed_by_id = {
        binding.work_item_workflow_binding_id: binding for binding in echoed_bindings
    }

    missing_binding_ids = tuple(
        binding_id for binding_id in authoritative_by_id if binding_id not in echoed_by_id
    )
    unexpected_binding_ids = tuple(
        binding_id for binding_id in echoed_by_id if binding_id not in authoritative_by_id
    )
    mismatched_bindings = tuple(
        {
            "binding_id": binding_id,
            "expected": _json_compatible(authoritative_by_id[binding_id]),
            "echoed": _json_compatible(echoed_by_id[binding_id]),
        }
        for binding_id in authoritative_by_id
        if binding_id in echoed_by_id
        and authoritative_by_id[binding_id].authority_tuple
        != echoed_by_id[binding_id].authority_tuple
    )
    if (
        authoritative_duplicate_binding_ids
        or echoed_duplicate_binding_ids
        or missing_binding_ids
        or unexpected_binding_ids
        or mismatched_bindings
    ):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.query_binding_echo_mismatch",
            "query work-item binding rows did not round-trip the authoritative run-scoped preload",
            details={
                "run_id": run_id,
                "authoritative_binding_ids": tuple(authoritative_by_id),
                "echoed_binding_ids": tuple(echoed_by_id),
                "authoritative_duplicate_binding_ids": authoritative_duplicate_binding_ids,
                "echoed_duplicate_binding_ids": echoed_duplicate_binding_ids,
                "missing_binding_ids": missing_binding_ids,
                "unexpected_binding_ids": unexpected_binding_ids,
                "mismatched_bindings": mismatched_bindings,
            },
        )


@dataclass(frozen=True, slots=True)
class NativeOperatorSurfaceProvenance:
    """Shared provenance envelope for the stitched native operator surface."""

    as_of: datetime
    native_instance_authority: str = "runtime.instance.resolve_native_instance"
    instruction_authority: str = "surfaces.api.native_operator_surface.query_native_operator_surface"
    query_authority: str = "surfaces.api.operator_read.query_operator_surface"
    cockpit_authority: str = "observability.operator_dashboard.operator_cockpit_run"
    observability_authority: str = "surfaces.api.native_operator_surface.query_native_operator_surface"
    stitched_sections: tuple[str, ...] = (
        "instruction_authority",
        "native_instance",
        "query",
        "cockpit",
        "observability",
    )

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "native_operator_surface_provenance",
            "as_of": self.as_of.isoformat(),
            "native_instance_authority": self.native_instance_authority,
            "section_authorities": {
                "instruction_authority": self.instruction_authority,
                "native_instance": self.native_instance_authority,
                "query": self.query_authority,
                "cockpit": self.cockpit_authority,
                "observability": self.observability_authority,
            },
            "stitched_sections": list(self.stitched_sections),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorSurfaceReadModel:
    """One consolidated native operator surface."""

    provenance: NativeOperatorSurfaceProvenance
    native_instance: NativeDagInstance
    run_id: str
    as_of: datetime
    instruction_authority: Mapping[str, Any]
    persona: Mapping[str, Any]
    status: Mapping[str, Any]
    receipts: Mapping[str, Any]
    query: Mapping[str, Any]
    cockpit: NativeOperatorCockpitReadModel
    observability: Mapping[str, Any] | None = None
    fork_ownership: Mapping[str, Any] | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "native_operator_surface",
            "instruction_authority": _json_compatible(self.instruction_authority),
            "provenance": self.provenance.to_json(),
            "native_instance": self.native_instance.to_contract(),
            "run_id": self.run_id,
            "as_of": self.as_of.isoformat(),
            "persona": _json_compatible(self.persona),
            "fork_ownership": _json_compatible(self.fork_ownership),
            "status": _json_compatible(self.status),
            "receipts": _json_compatible(self.receipts),
            "query": _json_compatible(self.query),
            "cockpit": _json_compatible(self.cockpit),
            "observability": _json_compatible(self.observability),
        }


@dataclass(frozen=True, slots=True)
class _BoundedForkOwnershipSelection:
    """Frontdoor-owned fork selector and route provenance for the bounded receipt."""

    run_id: str
    selection_status: str
    workspace_ref: str | None
    runtime_profile_ref: str | None
    share_mode: str | None = None
    reuse_reason_code: str | None = None
    sandbox_session_id: str | None = None
    selector_binding_id: str | None = None
    selector_binding_scope: str | None = None
    fork_ref: str | None = None
    worktree_ref: str | None = None

    def to_selector_payload(self) -> dict[str, Any] | None:
        if self.selection_status != "resolved":
            return None
        return {
            "run_id": self.run_id,
            "workspace_ref": _require_text(
                self.workspace_ref,
                field_name="fork_ownership.selector.workspace_ref",
            ),
            "runtime_profile_ref": _require_text(
                self.runtime_profile_ref,
                field_name="fork_ownership.selector.runtime_profile_ref",
            ),
            "fork_ref": _require_text(
                self.fork_ref,
                field_name="fork_ownership.selector.fork_ref",
            ),
            "worktree_ref": _require_text(
                self.worktree_ref,
                field_name="fork_ownership.selector.worktree_ref",
            ),
        }

    def to_provenance_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "selection_path": _BOUNDED_FORK_OWNERSHIP_SELECTION_PATH,
            "workspace_ref": self.workspace_ref,
            "runtime_profile_ref": self.runtime_profile_ref,
            "share_mode": self.share_mode,
            "reuse_reason_code": self.reuse_reason_code,
            "sandbox_session_id": self.sandbox_session_id,
            "selector_binding_id": self.selector_binding_id,
            "selector_binding_scope": self.selector_binding_scope,
        }

    def to_repository_selector(self) -> ForkOwnershipSelector:
        if self.selection_status != "resolved":
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_missing",
                "fork/worktree repository selector is only available for resolved ownership selections",
                details={
                    "run_id": self.run_id,
                    "selection_status": self.selection_status,
                },
            )
        return ForkOwnershipSelector(
            workspace_ref=_require_text(
                self.workspace_ref,
                field_name="fork_ownership.selector.workspace_ref",
            ),
            runtime_profile_ref=_require_text(
                self.runtime_profile_ref,
                field_name="fork_ownership.selector.runtime_profile_ref",
            ),
            fork_ref=_require_text(
                self.fork_ref,
                field_name="fork_ownership.selector.fork_ref",
            ),
            worktree_ref=_require_text(
                self.worktree_ref,
                field_name="fork_ownership.selector.worktree_ref",
            ),
        )


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _normalize_json_field(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.invalid_row",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_status_payload(
    status_payload: Mapping[str, Any],
    *,
    expected_native_instance: Mapping[str, Any],
    expected_run_id: str,
) -> dict[str, Any]:
    normalized_payload = dict(_require_mapping(status_payload, field_name="status_payload"))

    nested_native_instance = normalized_payload.pop("native_instance", None)
    if not isinstance(nested_native_instance, Mapping):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.status_native_instance_missing",
            "status payload must include the resolved native_instance contract",
            details={"run_id": expected_run_id},
        )
    if dict(nested_native_instance) != dict(expected_native_instance):
        raise NativeOperatorSurfaceError(
            "native_operator_surface.status_native_instance_mismatch",
            "status payload published a different native_instance than the outer surface",
            details={
                "expected_native_instance": dict(expected_native_instance),
                "status_native_instance": _json_compatible(nested_native_instance),
            },
        )

    run_payload = dict(_require_mapping(normalized_payload.get("run"), field_name="status_payload.run"))
    status_run_id = _require_text(run_payload.get("run_id"), field_name="status_payload.run.run_id")
    if status_run_id != expected_run_id:
        raise NativeOperatorSurfaceError(
            "native_operator_surface.status_run_id_mismatch",
            "status payload did not round-trip the requested run_id",
            details={"expected_run_id": expected_run_id, "status_run_id": status_run_id},
        )

    inspection_payload = normalized_payload.get("inspection")
    payload = {
        "kind": "native_operator_status_truth",
        "authority": "surfaces.api.frontdoor.status",
        "run": run_payload,
        "inspection": (
            None
            if inspection_payload is None
            else dict(_require_mapping(inspection_payload, field_name="status_payload.inspection"))
        ),
    }
    if "packet_inspection" in normalized_payload and normalized_payload["packet_inspection"] is not None:
        payload["packet_inspection"] = dict(
            _require_mapping(
                normalized_payload["packet_inspection"],
                field_name="status_payload.packet_inspection",
            )
        )
    if "observability" in normalized_payload and normalized_payload["observability"] is not None:
        payload["observability"] = dict(
            _require_mapping(
                normalized_payload["observability"],
                field_name="status_payload.observability",
            )
        )
    return payload


def _surface_observability_payload(
    *,
    status_payload: Mapping[str, Any],
    query_payload: Mapping[str, Any],
    smoke_freshness: Mapping[str, Any],
) -> dict[str, Any]:
    payload = {
        "kind": "native_operator_observability",
        "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
        "smoke_freshness": _json_compatible(smoke_freshness),
    }
    run_observability = status_payload.get("observability")
    if isinstance(run_observability, Mapping):
        payload["run"] = _json_compatible(run_observability)
    workflow_run_observability = query_payload.get("workflow_run_observability")
    if isinstance(workflow_run_observability, Mapping):
        payload["workflow_runs"] = _json_compatible(workflow_run_observability)
    return payload


def _persona_profile_payload(persona_profile: object) -> dict[str, Any]:
    profile = _require_mapping(_json_compatible(persona_profile), field_name="persona_profile")
    return {
        "persona_profile_id": _require_text(
            profile.get("persona_profile_id"),
            field_name="persona_profile.persona_profile_id",
        ),
        "persona_name": _require_text(
            profile.get("persona_name"),
            field_name="persona_profile.persona_name",
        ),
        "persona_kind": _require_text(
            profile.get("persona_kind"),
            field_name="persona_profile.persona_kind",
        ),
        "instruction_contract": _require_text(
            profile.get("instruction_contract"),
            field_name="persona_profile.instruction_contract",
        ),
        "response_contract": _normalize_json_field(profile.get("response_contract")),
        "tool_policy": _normalize_json_field(profile.get("tool_policy")),
        "runtime_hints": _normalize_json_field(profile.get("runtime_hints")),
        "effective_from": _parse_datetime(
            profile.get("effective_from"),
            field_name="persona_profile.effective_from",
        ).isoformat(),
        "effective_to": (
            None
            if profile.get("effective_to") is None
            else _parse_datetime(
                profile.get("effective_to"),
                field_name="persona_profile.effective_to",
            ).isoformat()
        ),
        "decision_ref": _require_text(
            profile.get("decision_ref"),
            field_name="persona_profile.decision_ref",
        ),
        "created_at": _parse_datetime(
            profile.get("created_at"),
            field_name="persona_profile.created_at",
        ).isoformat(),
    }


def _persona_context_binding_payload(binding: object) -> dict[str, Any]:
    row = _require_mapping(_json_compatible(binding), field_name="persona_context_binding")
    return {
        "persona_context_binding_id": _require_text(
            row.get("persona_context_binding_id"),
            field_name="persona_context_binding.persona_context_binding_id",
        ),
        "persona_profile_id": _require_text(
            row.get("persona_profile_id"),
            field_name="persona_context_binding.persona_profile_id",
        ),
        "binding_scope": _require_text(
            row.get("binding_scope"),
            field_name="persona_context_binding.binding_scope",
        ),
        "workspace_ref": _optional_text(
            row.get("workspace_ref"),
            field_name="persona_context_binding.workspace_ref",
        ),
        "runtime_profile_ref": _optional_text(
            row.get("runtime_profile_ref"),
            field_name="persona_context_binding.runtime_profile_ref",
        ),
        "model_profile_id": _optional_text(
            row.get("model_profile_id"),
            field_name="persona_context_binding.model_profile_id",
        ),
        "provider_policy_id": _optional_text(
            row.get("provider_policy_id"),
            field_name="persona_context_binding.provider_policy_id",
        ),
        "context_selector": _normalize_json_field(row.get("context_selector")),
        "binding_status": _require_text(
            row.get("binding_status"),
            field_name="persona_context_binding.binding_status",
        ),
        "position_index": _require_int(
            row.get("position_index"),
            field_name="persona_context_binding.position_index",
        ),
        "effective_from": _parse_datetime(
            row.get("effective_from"),
            field_name="persona_context_binding.effective_from",
        ).isoformat(),
        "effective_to": (
            None
            if row.get("effective_to") is None
            else _parse_datetime(
                row.get("effective_to"),
                field_name="persona_context_binding.effective_to",
            ).isoformat()
        ),
        "decision_ref": _require_text(
            row.get("decision_ref"),
            field_name="persona_context_binding.decision_ref",
        ),
        "created_at": _parse_datetime(
            row.get("created_at"),
            field_name="persona_context_binding.created_at",
        ).isoformat(),
    }


def _native_operator_persona_payload(
    *,
    run_id: str,
    as_of: datetime,
    workspace_ref: str,
    runtime_profile_ref: str,
    operator_path: str,
    persona_profile: object,
    persona_context_bindings: Sequence[object],
) -> dict[str, Any]:
    return {
        "kind": "native_operator_persona_activation",
        "authority": (
            "registry.persona_authority."
            "PostgresPersonaAndForkAuthorityRepository.load_persona_activation"
        ),
        "selector_authority": (
            "surfaces.api.native_operator_surface."
            "NativeOperatorSurfaceFrontdoor._load_persona_activation"
        ),
        "selector": {
            "run_id": run_id,
            "binding_scope": _NATIVE_OPERATOR_PERSONA_BINDING_SCOPE,
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
            "operator_path": operator_path,
            "as_of": as_of.isoformat(),
        },
        "persona_profile": _persona_profile_payload(persona_profile),
        "persona_context_bindings": [
            _persona_context_binding_payload(binding)
            for binding in persona_context_bindings
        ],
    }


def _fork_worktree_binding_payload(binding: object) -> dict[str, Any]:
    row = _require_mapping(_json_compatible(binding), field_name="fork_worktree_binding")
    return {
        "fork_worktree_binding_id": _require_text(
            row.get("fork_worktree_binding_id"),
            field_name="fork_worktree_binding.fork_worktree_binding_id",
        ),
        "fork_profile_id": _require_text(
            row.get("fork_profile_id"),
            field_name="fork_worktree_binding.fork_profile_id",
        ),
        "sandbox_session_id": _require_text(
            row.get("sandbox_session_id"),
            field_name="fork_worktree_binding.sandbox_session_id",
        ),
        "workflow_run_id": _require_text(
            row.get("workflow_run_id"),
            field_name="fork_worktree_binding.workflow_run_id",
        ),
        "binding_scope": _require_text(
            row.get("binding_scope"),
            field_name="fork_worktree_binding.binding_scope",
        ),
        "binding_status": _require_text(
            row.get("binding_status"),
            field_name="fork_worktree_binding.binding_status",
        ),
        "workspace_ref": _require_text(
            row.get("workspace_ref"),
            field_name="fork_worktree_binding.workspace_ref",
        ),
        "runtime_profile_ref": _require_text(
            row.get("runtime_profile_ref"),
            field_name="fork_worktree_binding.runtime_profile_ref",
        ),
        "base_ref": _require_text(
            row.get("base_ref"),
            field_name="fork_worktree_binding.base_ref",
        ),
        "fork_ref": _require_text(
            row.get("fork_ref"),
            field_name="fork_worktree_binding.fork_ref",
        ),
        "worktree_ref": _require_text(
            row.get("worktree_ref"),
            field_name="fork_worktree_binding.worktree_ref",
        ),
        "created_at": _parse_datetime(
            row.get("created_at"),
            field_name="fork_worktree_binding.created_at",
        ).isoformat(),
        "retired_at": (
            None
            if row.get("retired_at") is None
            else _parse_datetime(
                row.get("retired_at"),
                field_name="fork_worktree_binding.retired_at",
            ).isoformat()
        ),
        "decision_ref": _optional_text(
            row.get("decision_ref"),
            field_name="fork_worktree_binding.decision_ref",
        ),
    }


def _native_operator_fork_ownership_payload(
    *,
    selection: _BoundedForkOwnershipSelection,
    fork_worktree_binding: object | None,
) -> dict[str, Any]:
    return {
        "kind": "native_operator_fork_worktree_ownership",
        "authority": (
            "registry.persona_authority."
            "PostgresPersonaAndForkAuthorityRepository.load_fork_worktree_binding"
        ),
        "selector_authority": (
            "surfaces.api.native_operator_surface."
            "NativeOperatorSurfaceFrontdoor._load_fork_worktree_ownership_selector"
        ),
        "selection_status": selection.selection_status,
        "selector": selection.to_selector_payload(),
        "provenance": selection.to_provenance_payload(),
        "fork_worktree_binding": (
            None
            if fork_worktree_binding is None
            else _fork_worktree_binding_payload(fork_worktree_binding)
        ),
    }


def _receipt_summary_from_evidence(
    *,
    run_id: str,
    canonical_evidence: tuple[EvidenceRow, ...],
) -> dict[str, Any]:
    receipt_rows = tuple(row for row in canonical_evidence if row.kind == "receipt")
    receipt_summaries: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}

    for index, row in enumerate(receipt_rows):
        receipt_payload = _require_mapping(
            _json_compatible(row.record),
            field_name=f"canonical_evidence[{index}].receipt",
        )
        receipt_run_id = _require_text(
            receipt_payload.get("run_id"),
            field_name=f"canonical_evidence[{index}].receipt.run_id",
        )
        if receipt_run_id != run_id:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.receipt_run_id_mismatch",
                "canonical receipt evidence published a different run_id than the outer surface",
                details={
                    "expected_run_id": run_id,
                    "receipt_run_id": receipt_run_id,
                    "receipt_id": receipt_payload.get("receipt_id"),
                },
            )

        status = _require_text(
            receipt_payload.get("status"),
            field_name=f"canonical_evidence[{index}].receipt.status",
        )
        status_counts[status] = status_counts.get(status, 0) + 1
        receipt_summaries.append(
            {
                "receipt_id": _require_text(
                    receipt_payload.get("receipt_id"),
                    field_name=f"canonical_evidence[{index}].receipt.receipt_id",
                ),
                "receipt_type": _require_text(
                    receipt_payload.get("receipt_type"),
                    field_name=f"canonical_evidence[{index}].receipt.receipt_type",
                ),
                "status": status,
                "node_id": _optional_text(
                    receipt_payload.get("node_id"),
                    field_name=f"canonical_evidence[{index}].receipt.node_id",
                ),
                "failure_code": _optional_text(
                    receipt_payload.get("failure_code"),
                    field_name=f"canonical_evidence[{index}].receipt.failure_code",
                ),
                "transition_seq": _require_int(
                    receipt_payload.get("transition_seq"),
                    field_name=f"canonical_evidence[{index}].receipt.transition_seq",
                ),
                "evidence_seq": _require_int(
                    receipt_payload.get("evidence_seq"),
                    field_name=f"canonical_evidence[{index}].receipt.evidence_seq",
                ),
                "started_at": _parse_datetime(
                    receipt_payload.get("started_at"),
                    field_name=f"canonical_evidence[{index}].receipt.started_at",
                ).isoformat(),
                "finished_at": _parse_datetime(
                    receipt_payload.get("finished_at"),
                    field_name=f"canonical_evidence[{index}].receipt.finished_at",
                ).isoformat(),
            }
        )

    latest_receipt = receipt_summaries[-1] if receipt_summaries else None
    return {
        "kind": "native_operator_receipt_truth",
        "authority": "storage.postgres.PostgresEvidenceReader.load_evidence_timeline",
        "run_id": run_id,
        "evidence_row_count": len(canonical_evidence),
        "receipt_count": len(receipt_summaries),
        "latest_evidence_seq": None if latest_receipt is None else latest_receipt["evidence_seq"],
        "latest_receipt_id": None if latest_receipt is None else latest_receipt["receipt_id"],
        "latest_receipt_type": None if latest_receipt is None else latest_receipt["receipt_type"],
        "terminal_status": None if latest_receipt is None else latest_receipt["status"],
        "status_counts": dict(sorted(status_counts.items())),
        "receipts": receipt_summaries,
    }


def _status_current_state_notes(
    *,
    run_id: str,
    status_payload: Mapping[str, Any],
    receipts_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    run_payload = _require_mapping(status_payload.get("run"), field_name="status.run")
    notes: list[dict[str, Any]] = []
    current_state = _require_text(
        run_payload.get("current_state"),
        field_name="status.run.current_state",
    )
    notes.append(
        {
            "note_code": "workflow_run_state",
            "authority": "surfaces.api.frontdoor.status",
            "run_id": run_id,
            "message": f"workflow_runs.current_state={current_state}",
        }
    )

    terminal_status = receipts_payload.get("terminal_status")
    if isinstance(terminal_status, str) and terminal_status.strip():
        notes.append(
            {
                "note_code": "receipt_terminal_status",
                "authority": "storage.postgres.PostgresEvidenceReader.load_evidence_timeline",
                "run_id": run_id,
                "message": f"latest_receipt.status={terminal_status}",
            }
        )
        if terminal_status != current_state:
            notes.append(
                {
                    "note_code": "status_receipt_mismatch",
                    "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
                    "run_id": run_id,
                    "message": (
                        "workflow_runs.current_state and latest receipt status disagree; "
                        "read both before inferring execution state"
                    ),
                }
            )

    inspection_payload = status_payload.get("inspection")
    if inspection_payload is None:
        return notes
    inspection = _require_mapping(inspection_payload, field_name="status.inspection")
    completeness_payload = inspection.get("completeness")
    if completeness_payload is None:
        return notes
    completeness = _require_mapping(
        completeness_payload,
        field_name="status.inspection.completeness",
    )
    if completeness.get("is_complete") is False:
        missing_refs = completeness.get("missing_evidence_refs")
        if isinstance(missing_refs, Sequence) and not isinstance(
            missing_refs,
            (str, bytes, bytearray),
        ):
            notes.append(
                {
                    "note_code": "inspection_incomplete",
                    "authority": "surfaces.api.frontdoor.status",
                    "run_id": run_id,
                    "missing_evidence_refs": list(missing_refs),
                    "message": "inspection reported missing evidence refs",
                }
            )
    return notes


def _instruction_authority_payload(
    *,
    run_id: str,
    query_payload: Mapping[str, Any],
    status_payload: Mapping[str, Any],
    receipts_payload: Mapping[str, Any],
    authoritative_work_bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> dict[str, Any]:
    run_payload = _require_mapping(status_payload.get("run"), field_name="status.run")
    query_roadmap_items = query_payload.get("roadmap_items")
    if not isinstance(query_roadmap_items, Sequence) or isinstance(
        query_roadmap_items,
        (str, bytes, bytearray),
    ):
        query_roadmap_items = ()
    binding_roadmap_item_ids = _dedupe(
        tuple(
            binding.roadmap_item_id
            for binding in authoritative_work_bindings
            if binding.roadmap_item_id is not None
        )
    )
    query_roadmap_by_id = {
        _require_text(
            row.get("roadmap_item_id"),
            field_name=f"query.roadmap_items[{index}].roadmap_item_id",
        ): row
        for index, row in enumerate(query_roadmap_items)
        if isinstance(row, Mapping)
    }
    selected_roadmap_item_ids = (
        binding_roadmap_item_ids
        if binding_roadmap_item_ids
        else tuple(query_roadmap_by_id)
    )
    selected_roadmap_items = [
        {
            "roadmap_item_id": roadmap_item_id,
            "title": row.get("title"),
            "status": row.get("status"),
            "priority": row.get("priority"),
            "decision_ref": row.get("decision_ref"),
        }
        for roadmap_item_id, row in query_roadmap_by_id.items()
        if roadmap_item_id in selected_roadmap_item_ids
    ]
    return {
        "kind": "native_operator_instruction_authority",
        "authority": "surfaces.api.native_operator_surface.query_native_operator_surface",
        "orient_authority": "surfaces.api.handlers.workflow_admin._handle_orient",
        "packet_read_order": [
            "roadmap_truth",
            "queue_refs",
            "current_state_notes",
            "status",
            "observability",
            "receipts",
            "query",
            "cockpit",
        ],
        "roadmap_truth": {
            "authority": "surfaces.api.operator_read.query_operator_surface",
            "roadmap_item_ids": list(selected_roadmap_item_ids),
            "items": selected_roadmap_items,
        },
        "queue_refs": {
            "run_id": run_id,
            "workflow_id": _require_text(
                run_payload.get("workflow_id"),
                field_name="status.run.workflow_id",
            ),
            "request_id": _require_text(
                run_payload.get("request_id"),
                field_name="status.run.request_id",
            ),
            "workflow_definition_id": _require_text(
                run_payload.get("workflow_definition_id"),
                field_name="status.run.workflow_definition_id",
            ),
            "context_bundle_id": _optional_text(
                run_payload.get("context_bundle_id"),
                field_name="status.run.context_bundle_id",
            ),
            "work_item_workflow_binding_ids": [
                binding.work_item_workflow_binding_id for binding in authoritative_work_bindings
            ],
            "roadmap_item_ids": list(selected_roadmap_item_ids),
        },
        "current_state_notes": _status_current_state_notes(
            run_id=run_id,
            status_payload=status_payload,
            receipts_payload=receipts_payload,
        ),
        "directive": (
            "Read roadmap-backed truth, queue refs, current-state notes, and observability here "
            "before using repo files or prior chat state."
        ),
    }


@dataclass(slots=True)
class NativeOperatorSurfaceFrontdoor:
    """Repo-local frontdoor for the consolidated native operator surface."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[asyncpg.Connection]] = (
        connect_workflow_database
    )
    evidence_reader_factory: Callable[[Mapping[str, str] | None], PostgresEvidenceReader] = (
        _default_evidence_reader_factory
    )

    def _resolve_instance(
        self,
        *,
        env: Mapping[str, str] | None,
    ) -> tuple[Mapping[str, str], NativeDagInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    async def _load_route_authority(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
    ) -> ProviderRouteControlTower:
        conn = await self.connect_database(env)
        try:
            return await load_provider_route_control_tower_snapshot(conn, as_of=as_of)
        finally:
            await conn.close()

    async def _load_operator_control(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
    ) -> OperatorControlAuthority:
        conn = await self.connect_database(env)
        try:
            return await load_operator_control_authority(conn, as_of=as_of)
        finally:
            await conn.close()

    async def _load_canonical_evidence(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
    ) -> tuple[EvidenceRow, ...]:
        evidence_reader = self.evidence_reader_factory(env)
        return await evidence_reader.load_evidence_timeline(run_id=run_id)

    async def _load_status(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
    ) -> Mapping[str, Any]:
        return await asyncio.to_thread(frontdoor_status, run_id=run_id, env=env)

    async def _load_run_scoped_work_bindings(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...]:
        conn = await self.connect_database(env)
        try:
            return await load_work_item_workflow_bindings_for_workflow_run(
                conn,
                workflow_run_id=run_id,
            )
        finally:
            await conn.close()

    async def _load_persona_activation(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
        as_of: datetime,
    ) -> Mapping[str, Any]:
        conn = await self.connect_database(env)
        try:
            try:
                run_row = await conn.fetchrow(
                    """
                    SELECT
                        request_envelope->>'workspace_ref' AS workspace_ref,
                        request_envelope->>'runtime_profile_ref' AS runtime_profile_ref
                    FROM workflow_runs
                    WHERE run_id = $1
                    """,
                    run_id,
                )
            except asyncpg.PostgresError as exc:
                raise NativeOperatorSurfaceError(
                    "native_operator_surface.persona_run_context_read_failed",
                    "failed to read the native operator run context for persona activation",
                    details={
                        "run_id": run_id,
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "table": "workflow_runs",
                    },
                ) from exc

            if run_row is None:
                raise NativeOperatorSurfaceError(
                    "native_operator_surface.persona_run_context_missing",
                    "workflow run was not found for persona activation",
                    details={"run_id": run_id},
                )

            workspace_ref = _require_text(
                run_row["workspace_ref"],
                field_name="workflow_runs.request_envelope.workspace_ref",
            )
            runtime_profile_ref = _require_text(
                run_row["runtime_profile_ref"],
                field_name="workflow_runs.request_envelope.runtime_profile_ref",
            )

            repository = PostgresPersonaAndForkAuthorityRepository(conn)
            try:
                persona_profile, persona_context_bindings = await repository.load_persona_activation(
                    selector=PersonaActivationSelector(
                        binding_scope=_NATIVE_OPERATOR_PERSONA_BINDING_SCOPE,
                        as_of=as_of,
                        workspace_ref=workspace_ref,
                        runtime_profile_ref=runtime_profile_ref,
                        operator_path=_NATIVE_OPERATOR_PERSONA_OPERATOR_PATH,
                    ),
                )
            except PersonaAndForkAuthorityRepositoryError as exc:
                raise NativeOperatorSurfaceError(
                    f"native_operator_surface.{exc.reason_code.rsplit('.', 1)[-1]}",
                    "persona authority repository failed to resolve the selected native operator persona",
                    details={
                        "run_id": run_id,
                        "workspace_ref": workspace_ref,
                        "runtime_profile_ref": runtime_profile_ref,
                        "binding_scope": _NATIVE_OPERATOR_PERSONA_BINDING_SCOPE,
                        "operator_path": _NATIVE_OPERATOR_PERSONA_OPERATOR_PATH,
                        "repository_reason_code": exc.reason_code,
                        **exc.details,
                    },
                ) from exc

            return _native_operator_persona_payload(
                run_id=run_id,
                as_of=as_of,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
                operator_path=_NATIVE_OPERATOR_PERSONA_OPERATOR_PATH,
                persona_profile=persona_profile,
                persona_context_bindings=persona_context_bindings,
            )
        finally:
            await conn.close()

    async def _load_fork_worktree_ownership_selector(
        self,
        *,
        conn: asyncpg.Connection,
        run_id: str,
    ) -> _BoundedForkOwnershipSelection:
        try:
            run_row = await conn.fetchrow(
                """
                SELECT
                    request_envelope->>'workspace_ref' AS workspace_ref,
                    request_envelope->>'runtime_profile_ref' AS runtime_profile_ref
                FROM workflow_runs
                WHERE run_id = $1
                """,
                run_id,
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_run_context_read_failed",
                "failed to read the native operator run context for fork/worktree ownership",
                details={
                    "run_id": run_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "workflow_runs",
                },
            ) from exc

        if run_row is None:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_run_context_missing",
                "workflow run was not found for fork/worktree ownership",
                details={"run_id": run_id},
            )

        workspace_ref = _require_text(
            run_row["workspace_ref"],
            field_name="workflow_runs.request_envelope.workspace_ref",
        )
        runtime_profile_ref = _require_text(
            run_row["runtime_profile_ref"],
            field_name="workflow_runs.request_envelope.runtime_profile_ref",
        )

        try:
            route_row = await conn.fetchrow(
                """
                SELECT
                    share_mode,
                    reuse_reason_code,
                    sandbox_session_id
                FROM workflow_claim_lease_proposal_runtime
                WHERE run_id = $1
                """,
                run_id,
            )
        except asyncpg.PostgresError as exc:
            if _is_undefined_table_error(exc):
                return _BoundedForkOwnershipSelection(
                    run_id=run_id,
                    selection_status="route_runtime_unavailable",
                    workspace_ref=workspace_ref,
                    runtime_profile_ref=runtime_profile_ref,
                )
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_route_context_read_failed",
                "failed to read the native operator route context for fork/worktree ownership",
                details={
                    "run_id": run_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "workflow_claim_lease_proposal_runtime",
                },
            ) from exc

        if route_row is None:
            return _BoundedForkOwnershipSelection(
                run_id=run_id,
                selection_status="not_selected",
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
            )

        share_mode = _require_text(
            route_row["share_mode"],
            field_name="workflow_claim_lease_proposal_runtime.share_mode",
        )
        reuse_reason_code = _optional_text(
            route_row["reuse_reason_code"],
            field_name="workflow_claim_lease_proposal_runtime.reuse_reason_code",
        )
        sandbox_session_id = _optional_text(
            route_row["sandbox_session_id"],
            field_name="workflow_claim_lease_proposal_runtime.sandbox_session_id",
        )

        if (
            share_mode != "shared"
            or reuse_reason_code != _BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE
        ):
            return _BoundedForkOwnershipSelection(
                run_id=run_id,
                selection_status="not_selected",
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
                share_mode=share_mode,
                reuse_reason_code=reuse_reason_code,
                sandbox_session_id=sandbox_session_id,
            )

        if sandbox_session_id is None:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_missing",
                "bounded fork ownership route must carry one sandbox_session_id before operator receipts can resolve ownership",
                details={
                    "run_id": run_id,
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                    "share_mode": share_mode,
                    "reuse_reason_code": reuse_reason_code,
                },
            )

        try:
            selector_rows = await conn.fetch(
                """
                SELECT
                    fork_worktree_binding_id,
                    binding_scope,
                    workspace_ref,
                    runtime_profile_ref,
                    fork_ref,
                    worktree_ref
                FROM fork_worktree_bindings
                WHERE workflow_run_id = $1
                  AND sandbox_session_id = $2
                  AND binding_status = 'active'
                  AND retired_at IS NULL
                ORDER BY created_at DESC, fork_worktree_binding_id
                """,
                run_id,
                sandbox_session_id,
            )
        except asyncpg.PostgresError as exc:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_read_failed",
                "failed to read the effective fork/worktree ownership selector",
                details={
                    "run_id": run_id,
                    "sandbox_session_id": sandbox_session_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "fork_worktree_bindings",
                },
            ) from exc

        if not selector_rows:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_missing",
                "bounded fork ownership route did not resolve one active fork/worktree selector",
                details={
                    "run_id": run_id,
                    "sandbox_session_id": sandbox_session_id,
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
            )
        if len(selector_rows) > 1:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_ambiguous",
                "bounded fork ownership route resolved more than one active fork/worktree selector",
                details={
                    "run_id": run_id,
                    "sandbox_session_id": sandbox_session_id,
                    "fork_worktree_binding_ids": tuple(
                        _require_text(
                            row["fork_worktree_binding_id"],
                            field_name="fork_worktree_bindings.fork_worktree_binding_id",
                        )
                        for row in selector_rows
                    ),
                },
            )

        selector_row = selector_rows[0]
        selector_binding_id = _require_text(
            selector_row["fork_worktree_binding_id"],
            field_name="fork_worktree_bindings.fork_worktree_binding_id",
        )
        selector_binding_scope = _require_text(
            selector_row["binding_scope"],
            field_name="fork_worktree_bindings.binding_scope",
        )
        selector_workspace_ref = _require_text(
            selector_row["workspace_ref"],
            field_name="fork_worktree_bindings.workspace_ref",
        )
        selector_runtime_profile_ref = _require_text(
            selector_row["runtime_profile_ref"],
            field_name="fork_worktree_bindings.runtime_profile_ref",
        )
        selector_fork_ref = _require_text(
            selector_row["fork_ref"],
            field_name="fork_worktree_bindings.fork_ref",
        )
        selector_worktree_ref = _require_text(
            selector_row["worktree_ref"],
            field_name="fork_worktree_bindings.worktree_ref",
        )

        if selector_workspace_ref != workspace_ref or selector_runtime_profile_ref != runtime_profile_ref:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.fork_ownership_selector_run_context_mismatch",
                "fork/worktree selector did not round-trip the workflow run context",
                details={
                    "run_id": run_id,
                    "selector_binding_id": selector_binding_id,
                    "expected_workspace_ref": workspace_ref,
                    "expected_runtime_profile_ref": runtime_profile_ref,
                    "selector_workspace_ref": selector_workspace_ref,
                    "selector_runtime_profile_ref": selector_runtime_profile_ref,
                },
            )

        return _BoundedForkOwnershipSelection(
            run_id=run_id,
            selection_status="resolved",
            workspace_ref=selector_workspace_ref,
            runtime_profile_ref=selector_runtime_profile_ref,
            share_mode=share_mode,
            reuse_reason_code=reuse_reason_code,
            sandbox_session_id=sandbox_session_id,
            selector_binding_id=selector_binding_id,
            selector_binding_scope=selector_binding_scope,
            fork_ref=selector_fork_ref,
            worktree_ref=selector_worktree_ref,
        )

    async def _load_fork_worktree_ownership(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
    ) -> Mapping[str, Any]:
        try:
            conn = await self.connect_database(env)
        except PostgresConfigurationError:
            return _native_operator_fork_ownership_payload(
                selection=_BoundedForkOwnershipSelection(
                    run_id=run_id,
                    selection_status="database_unavailable",
                    workspace_ref=None,
                    runtime_profile_ref=None,
                ),
                fork_worktree_binding=None,
            )
        try:
            selection = await self._load_fork_worktree_ownership_selector(
                conn=conn,
                run_id=run_id,
            )
            if selection.selection_status != "resolved":
                return _native_operator_fork_ownership_payload(
                    selection=selection,
                    fork_worktree_binding=None,
                )

            repository = PostgresPersonaAndForkAuthorityRepository(conn)
            try:
                fork_worktree_binding = await repository.load_fork_worktree_binding(
                    selector=selection.to_repository_selector(),
                )
            except PersonaAndForkAuthorityRepositoryError as exc:
                raise NativeOperatorSurfaceError(
                    f"native_operator_surface.{exc.reason_code.rsplit('.', 1)[-1]}",
                    "fork/worktree authority repository failed to resolve the selected native operator ownership",
                    details={
                        "run_id": run_id,
                        "workspace_ref": selection.workspace_ref,
                        "runtime_profile_ref": selection.runtime_profile_ref,
                        "fork_ref": selection.fork_ref,
                        "worktree_ref": selection.worktree_ref,
                        "selector_binding_id": selection.selector_binding_id,
                        "sandbox_session_id": selection.sandbox_session_id,
                        "repository_reason_code": exc.reason_code,
                        **exc.details,
                    },
                ) from exc

            return _native_operator_fork_ownership_payload(
                selection=selection,
                fork_worktree_binding=fork_worktree_binding,
            )
        finally:
            await conn.close()

    async def _load_dispatch_resolution(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
        work_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    ) -> WorkflowClassResolutionDecision:
        workflow_class_ids = _dedupe(
            tuple(
                binding.workflow_class_id
                for binding in work_bindings
                if binding.workflow_class_id is not None
            )
        )
        if not workflow_class_ids:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.workflow_class_missing",
                "work_item_workflow_bindings must cite one workflow_class_id for cockpit adoption",
                details={"binding_count": len(work_bindings)},
            )
        if len(workflow_class_ids) > 1:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.workflow_class_ambiguous",
                "work_item_workflow_bindings must resolve to one workflow_class_id for cockpit adoption",
                details={"workflow_class_ids": workflow_class_ids},
            )

        conn = await self.connect_database(env)
        try:
            runtime = await load_workflow_class_resolution_runtime(conn, as_of=as_of)
        finally:
            await conn.close()

        dispatch_resolution = runtime.workflow_class_catalog.resolve_by_id(
            workflow_class_id=workflow_class_ids[0],
        )
        matching_lane_policies = tuple(
            record
            for record in runtime.lane_catalog.lane_policy_records
            if record.workflow_lane_id == dispatch_resolution.workflow_lane_id
        )
        if not matching_lane_policies:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.workflow_lane_policy_missing",
                "resolved workflow class did not map to an active lane policy",
                details={
                    "workflow_class_id": dispatch_resolution.workflow_class_id,
                    "workflow_lane_id": dispatch_resolution.workflow_lane_id,
                },
            )
        if len(matching_lane_policies) > 1:
            raise NativeOperatorSurfaceError(
                "native_operator_surface.workflow_lane_policy_ambiguous",
                "resolved workflow lane mapped to multiple active lane policies",
                details={
                    "workflow_class_id": dispatch_resolution.workflow_class_id,
                    "workflow_lane_id": dispatch_resolution.workflow_lane_id,
                    "workflow_lane_policy_ids": tuple(
                        record.workflow_lane_policy_id for record in matching_lane_policies
                    ),
                },
            )
        return WorkflowClassResolutionDecision(
            workflow_class=dispatch_resolution.workflow_class,
            lane_policy=matching_lane_policies[0],
            as_of=runtime.as_of,
        )

    async def _load_operator_cockpit(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
        as_of: datetime,
        authoritative_work_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    ) -> NativeOperatorCockpitReadModel:
        route_authority, dispatch_resolution, cutover_status = await asyncio.gather(
            self._load_route_authority(env=env, as_of=as_of),
            self._load_dispatch_resolution(
                env=env,
                as_of=as_of,
                work_bindings=authoritative_work_bindings,
            ),
            self._load_cutover_status(
                env=env,
                run_id=run_id,
                as_of=as_of,
                work_bindings=authoritative_work_bindings,
            ),
        )
        try:
            return operator_cockpit_run(
                run_id=run_id,
                as_of=as_of,
                route_authority=route_authority,
                dispatch_resolution=dispatch_resolution,
                cutover_status=cutover_status,
            )
        except NativeOperatorCockpitError as exc:
            raise NativeOperatorSurfaceError(
                f"native_operator_surface.{exc.reason_code.rsplit('.', 1)[-1]}",
                "native operator cockpit could not be built from bounded authority",
                details={
                    "run_id": run_id,
                    "as_of": as_of.isoformat(),
                    "cockpit_reason_code": exc.reason_code,
                    **exc.details,
                },
            ) from exc

    async def _load_cutover_status(
        self,
        *,
        env: Mapping[str, str] | None,
        run_id: str,
        as_of: datetime,
        work_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    ) -> NativeCutoverGraphStatusReadModel:
        operator_control, canonical_evidence = await asyncio.gather(
            self._load_operator_control(env=env, as_of=as_of),
            self._load_canonical_evidence(env=env, run_id=run_id),
        )
        return cutover_graph_status_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
            operator_control=operator_control,
            work_bindings=work_bindings,
        )

    async def _load_smoke_freshness(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
    ) -> Mapping[str, Any]:
        conn = await self.connect_database(env)
        try:
            rows = await conn.fetch(
                """
                SELECT
                    run_id,
                    workflow_id,
                    request_id,
                    current_state,
                    terminal_reason_code,
                    requested_at,
                    admitted_at,
                    started_at,
                    finished_at
                FROM workflow_runs
                WHERE workflow_id = $1
                   OR workflow_id LIKE $2
                ORDER BY requested_at DESC NULLS LAST, run_id DESC
                LIMIT 10
                """,
                _NATIVE_SMOKE_WORKFLOW_ID_PREFIX,
                f"{_NATIVE_SMOKE_WORKFLOW_ID_PREFIX}.%",
            )
        except asyncpg.PostgresError as exc:
            return {
                "kind": "native_smoke_freshness",
                "authority": "workflow_runs",
                "workflow_id_prefix": _NATIVE_SMOKE_WORKFLOW_ID_PREFIX,
                "freshness_slo_seconds": _SMOKE_FRESHNESS_SLO_SECONDS,
                "state": "unavailable",
                "reason_code": getattr(exc, "sqlstate", None) or type(exc).__name__,
            }
        finally:
            await conn.close()
        return _smoke_freshness_payload(rows, as_of=as_of)

    async def _load_surface(
        self,
        *,
        env: Mapping[str, str] | None,
        instance: NativeDagInstance,
        run_id: str,
        as_of: datetime,
        persona_payload: Mapping[str, Any],
        query_payload: Mapping[str, Any],
        authoritative_work_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    ) -> NativeOperatorSurfaceReadModel:
        cockpit = await self._load_operator_cockpit(
            env=env,
            run_id=run_id,
            as_of=as_of,
            authoritative_work_bindings=authoritative_work_bindings,
        )
        (
            status_payload,
            canonical_evidence,
            fork_ownership_payload,
            smoke_freshness,
        ) = await asyncio.gather(
            self._load_status(env=env, run_id=run_id),
            self._load_canonical_evidence(env=env, run_id=run_id),
            self._load_fork_worktree_ownership(env=env, run_id=run_id),
            self._load_smoke_freshness(env=env, as_of=as_of),
        )
        normalized_status = _normalize_status_payload(
            status_payload,
            expected_native_instance=instance.to_contract(),
            expected_run_id=run_id,
        )
        receipts = _receipt_summary_from_evidence(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
        )
        return NativeOperatorSurfaceReadModel(
            native_instance=instance,
            run_id=run_id,
            as_of=as_of,
            instruction_authority=_instruction_authority_payload(
                run_id=run_id,
                query_payload=query_payload,
                status_payload=normalized_status,
                receipts_payload=receipts,
                authoritative_work_bindings=authoritative_work_bindings,
            ),
            persona=persona_payload,
            fork_ownership=fork_ownership_payload,
            status=normalized_status,
            receipts=receipts,
            query=query_payload,
            cockpit=cockpit,
            observability=_surface_observability_payload(
                status_payload=normalized_status,
                query_payload=query_payload,
                smoke_freshness=smoke_freshness,
            ),
            provenance=NativeOperatorSurfaceProvenance(as_of=as_of),
        )

    def query_native_operator_surface(
        self,
        *,
        run_id: str,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
        bug_ids: Sequence[str] | None = None,
        roadmap_item_ids: Sequence[str] | None = None,
        cutover_gate_ids: Sequence[str] | None = None,
        work_item_workflow_binding_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Read the native operator query and cockpit surfaces together."""

        source, instance = self._resolve_instance(env=env)
        normalized_run_id = _require_text(run_id, field_name="run_id")
        resolved_as_of = _now() if as_of is None else _normalize_as_of(as_of)
        requested_binding_ids = _normalize_ids(
            work_item_workflow_binding_ids,
            field_name="work_item_workflow_binding_ids",
        )
        run_scoped_bindings = _run_async(
            self._load_run_scoped_work_bindings(
                env=source,
                run_id=normalized_run_id,
            )
        )
        query_binding_ids = _resolve_query_binding_ids(
            run_id=normalized_run_id,
            run_scoped_bindings=run_scoped_bindings,
            requested_binding_ids=requested_binding_ids,
        )
        authoritative_work_bindings = _select_authoritative_bindings(
            run_scoped_bindings=run_scoped_bindings,
            selected_binding_ids=query_binding_ids,
        )
        persona_payload = _run_async(
            self._load_persona_activation(
                env=source,
                run_id=normalized_run_id,
                as_of=resolved_as_of,
            )
        )
        raw_query_payload = query_operator_surface(
            env=source,
            as_of=resolved_as_of,
            bug_ids=bug_ids,
            roadmap_item_ids=roadmap_item_ids,
            cutover_gate_ids=cutover_gate_ids,
            work_item_workflow_binding_ids=query_binding_ids,
            workflow_run_ids=[normalized_run_id],
        )
        query_payload = _normalize_query_payload(
            raw_query_payload,
            expected_native_instance=instance.to_contract(),
            expected_as_of=resolved_as_of,
        )
        _validate_query_binding_echo(
            run_id=normalized_run_id,
            authoritative_bindings=authoritative_work_bindings,
            query_payload=query_payload,
        )
        surface = _run_async(
            self._load_surface(
                env=source,
                instance=instance,
                run_id=normalized_run_id,
                as_of=resolved_as_of,
                persona_payload=persona_payload,
                query_payload=query_payload,
                authoritative_work_bindings=authoritative_work_bindings,
            )
        )
        return surface.to_json()


_DEFAULT_NATIVE_OPERATOR_SURFACE_FRONTDOOR = NativeOperatorSurfaceFrontdoor()

# Publish the stitched read model directly so the module-level entrypoint is
# the actual control-plane frontdoor, not a compatibility wrapper.
query_native_operator_surface = _DEFAULT_NATIVE_OPERATOR_SURFACE_FRONTDOOR.query_native_operator_surface
