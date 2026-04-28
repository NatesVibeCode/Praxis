"""Durable command authority for the bus-brain cutover.

This module owns explicit, Postgres-backed command rows and the deterministic
execution boundary for the boring control paths:

- workflow.submit
- workflow.spawn
- workflow.retry
- workflow.cancel
- sync.repair

It does not call any LLMs. It only writes durable state, emits system_events,
and delegates to existing dispatch / post-dispatch sync authorities.
"""

from __future__ import annotations

import importlib
import json
import logging
import sys
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

from runtime._helpers import _json_compatible
from runtime.command_signatures import (
    _intent_payload_signature,
    _json_dumps,
    _record_payload_signature,
)
from runtime.idempotency import canonical_hash
from runtime.command_handlers import (
    _emit_system_event,
    _event_type_for_status,
    _run_command_handler,
    _workflow_chain_id_from_result_ref,
    _workflow_run_id_from_result_ref,
    render_control_command_failure,
    render_control_command_response,
    render_workflow_chain_submit_response,
    render_workflow_spawn_response,
    render_workflow_submit_response,
    request_workflow_chain_submit_command,
    request_workflow_spawn_command,
    request_workflow_submit_command,
    workflow_cancel_proof,
)
from runtime.capability.plan_envelope import build_plan_envelope
from runtime.capability.resolver import GrantResolution, resolve_capability_grant
from storage.postgres import PostgresCommandRepository
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


class _LazyUnifiedDispatchProxy:
    """Resolve unified dispatch only when a handler actually needs it.

    Startup wiring imports ``bootstrap_control_commands_schema`` during API and
    MCP subsystem initialization. Importing the full workflow dispatch module at
    module import time creates a circular edge back into the control-command
    surface. This proxy keeps the bootstrap path cold while preserving the
    existing ``control_commands.unified_dispatch`` patch seam used by tests.
    """

    def __init__(self) -> None:
        object.__setattr__(self, "_overrides", {})

    class _MissingDispatchAttribute:
        def __init__(self, name: str) -> None:
            self.name = name

    def _loaded_module(self):
        module = sys.modules.get("runtime.workflow.unified")
        return module

    def __getattr__(self, name: str):
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        module = self._loaded_module()
        if module is not None:
            return getattr(module, name)
        return self._MissingDispatchAttribute(name)

    def _resolve_attr(self, name: str):
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        module = self._loaded_module()
        if module is None:
            module = importlib.import_module("runtime.workflow.unified")
        return getattr(module, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        overrides = object.__getattribute__(self, "_overrides")
        overrides[name] = value

    def __delattr__(self, name: str) -> None:
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        overrides = object.__getattribute__(self, "_overrides")
        overrides[name] = self._MissingDispatchAttribute(name)


unified_dispatch = _LazyUnifiedDispatchProxy()


def _resolve_unified_dispatch_attr(name: str):
    return unified_dispatch._resolve_attr(name)


def _workflow_submit_run_snapshot(
    conn: "SyncPostgresConnection",
    run_id: str,
) -> dict[str, Any] | None:
    def _load_snapshot() -> dict[str, Any] | None:
        try:
            from runtime.workflow._status import (
                get_run_status,
                summarize_run_health,
                summarize_run_recovery,
            )

            status = get_run_status(conn, run_id)
        except Exception:
            logger.debug("workflow submit snapshot lookup failed for run %s", run_id, exc_info=True)
            return None
        if not isinstance(status, Mapping):
            return None
        snapshot = dict(status)
        now = datetime.now(timezone.utc)
        try:
            health = snapshot.get("health")
            if not isinstance(health, Mapping):
                health = summarize_run_health(snapshot, now)
                snapshot["health"] = health
            recovery = snapshot.get("recovery")
            if not isinstance(recovery, Mapping):
                snapshot["recovery"] = summarize_run_recovery(snapshot, dict(health), now)
        except Exception:
            logger.debug("workflow submit snapshot enrichment failed for run %s", run_id, exc_info=True)
        return snapshot

    last_snapshot: dict[str, Any] | None = None
    for sleep_seconds in (0.0, 0.05, 0.1, 0.2, 0.3):
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        snapshot = _load_snapshot()
        if snapshot is None:
            continue
        last_snapshot = snapshot
        if _workflow_submit_snapshot_is_informative(snapshot):
            return snapshot
    return last_snapshot


def _workflow_submit_status_counts(jobs: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not isinstance(jobs, list):
        return counts
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        label = str(job.get("status") or "unknown").strip() or "unknown"
        counts[label] = counts.get(label, 0) + 1
    return counts


def _workflow_submit_health_state(snapshot: Mapping[str, Any]) -> str:
    health = snapshot.get("health")
    if not isinstance(health, Mapping):
        return "unknown"
    return str(health.get("state") or "").strip() or "unknown"


def _workflow_submit_terminal_reason(snapshot: Mapping[str, Any]) -> str:
    return str(
        snapshot.get("terminal_reason")
        or snapshot.get("terminal_reason_code")
        or ""
    ).strip()


def _workflow_submit_snapshot_is_informative(snapshot: Mapping[str, Any]) -> bool:
    run_status = str(snapshot.get("status") or "").strip()
    completed_jobs = int(
        snapshot.get("completed_jobs")
        or snapshot.get("completed")
        or 0
    )
    total_jobs = int(snapshot.get("total_jobs") or 0)
    health_state = _workflow_submit_health_state(snapshot)
    terminal_reason = _workflow_submit_terminal_reason(snapshot)
    job_status_counts = _workflow_submit_status_counts(snapshot.get("jobs"))
    if run_status in {"succeeded", "failed", "cancelled"}:
        return (
            (total_jobs == 0 or completed_jobs >= total_jobs)
            and health_state != "unknown"
            and bool(terminal_reason)
        )
    if health_state != "unknown":
        return True
    if completed_jobs > 0:
        return True
    return any(status != "pending" for status in job_status_counts)


def _workflow_submit_metrics(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "completed_jobs": int(
            snapshot.get("completed_jobs")
            or snapshot.get("completed")
            or 0
        ),
        "total_jobs": int(snapshot.get("total_jobs") or 0),
        "elapsed_seconds": float(snapshot.get("elapsed_seconds") or 0.0),
        "health_state": _workflow_submit_health_state(snapshot),
        "job_status_counts": _workflow_submit_status_counts(snapshot.get("jobs")),
        "total_cost_usd": float(snapshot.get("total_cost_usd") or 0.0),
        "total_duration_ms": int(snapshot.get("total_duration_ms") or 0),
        "total_tokens_in": int(snapshot.get("total_tokens_in") or 0),
        "total_tokens_out": int(snapshot.get("total_tokens_out") or 0),
    }
    terminal_reason = _workflow_submit_terminal_reason(snapshot)
    if terminal_reason:
        metrics["terminal_reason"] = terminal_reason
    return metrics


def _merge_workflow_submit_metrics(
    payload: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(payload)
    run_status = str(snapshot.get("status") or "").strip()
    if run_status:
        merged["status"] = run_status
        merged["run_status"] = run_status
    merged["status_source"] = "live_snapshot"
    merged["run_metrics"] = _workflow_submit_metrics(snapshot)
    terminal_reason = _workflow_submit_terminal_reason(snapshot)
    if terminal_reason:
        merged["terminal_reason"] = terminal_reason
    recovery = snapshot.get("recovery")
    if isinstance(recovery, Mapping):
        merged["recovery"] = _json_compatible(dict(recovery))
    return merged


def submit_workflow_command(
    conn: "SyncPostgresConnection",
    *,
    requested_by_kind: str,
    requested_by_ref: str,
    spec_path: str | None = None,
    inline_spec: Mapping[str, Any] | None = None,
    repo_root: str | None = None,
    run_id: str | None = None,
    parent_run_id: str | None = None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    trigger_depth: int | None = None,
    lineage_depth: int | None = None,
    packet_provenance: Mapping[str, Any] | None = None,
    force_fresh_run: bool = False,
    spec_name: str | None = None,
    total_jobs: int | None = None,
    idempotency_key: str | None = None,
    command_id: str | None = None,
    requested_at: Any = None,
) -> dict[str, Any]:
    """Request and render one workflow.submit command in a single step."""

    command = request_workflow_submit_command(
        conn,
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref,
        spec_path=spec_path,
        inline_spec=inline_spec,
        repo_root=repo_root,
        run_id=run_id,
        parent_run_id=parent_run_id,
        parent_job_label=parent_job_label,
        dispatch_reason=dispatch_reason,
        trigger_depth=trigger_depth,
        lineage_depth=lineage_depth,
        packet_provenance=packet_provenance,
        force_fresh_run=force_fresh_run,
        idempotency_key=idempotency_key,
        command_id=command_id,
        requested_at=requested_at,
    )
    payload = render_workflow_submit_response(
        command,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )
    run_id_value = payload.get("run_id")
    if (
        payload.get("status") in {"failed", "approval_required"}
        or not isinstance(run_id_value, str)
        or not run_id_value.strip()
    ):
        return payload
    snapshot = _workflow_submit_run_snapshot(conn, run_id_value)
    if snapshot is None:
        return payload
    return _merge_workflow_submit_metrics(payload, snapshot)


def submit_workflow_chain_command(
    conn: "SyncPostgresConnection",
    *,
    requested_by_kind: str,
    requested_by_ref: str,
    coordination_path: str,
    repo_root: str,
    adopt_active: bool = True,
    idempotency_key: str | None = None,
    command_id: str | None = None,
    requested_at: Any = None,
) -> dict[str, Any]:
    """Request and render one workflow.chain.submit command in a single step."""
    command = request_workflow_chain_submit_command(
        conn,
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref,
        coordination_path=coordination_path,
        repo_root=repo_root,
        adopt_active=adopt_active,
        idempotency_key=idempotency_key,
        command_id=command_id,
        requested_at=requested_at,
    )
    return render_workflow_chain_submit_response(
        conn,
        command,
        coordination_path=coordination_path,
    )

# 040 creates the table; 042 performs the explicit workflow.* type cutover.
_SCHEMA_FILENAMES = (
    "040_control_commands.sql",
    "042_workflow_control_command_types.sql",
)
_SYSTEM_EVENT_SOURCE_TYPE = "control_command"
_AUTO_APPROVAL_REF = "control.policy.auto"


class ControlCommandError(RuntimeError):
    """Base class for durable command authority failures."""

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


class ControlCommandIdempotencyConflict(ControlCommandError):
    """Raised when an idempotency key reappears with a different payload."""

    def __init__(
        self,
        idempotency_key: str,
        existing_command_id: str | None,
        first_seen_at: datetime | None,
    ) -> None:
        super().__init__(
            "control.command.idempotency_conflict",
            f"Idempotency conflict: key={idempotency_key} exists with different payload",
            details={
                "idempotency_key": idempotency_key,
                "existing_command_id": existing_command_id,
                "first_seen_at": (
                    None if first_seen_at is None else first_seen_at.isoformat()
                ),
            },
        )
        self.idempotency_key = idempotency_key
        self.existing_command_id = existing_command_id
        self.first_seen_at = first_seen_at


class ControlCommandPolicyError(ControlCommandError):
    """Raised when a command requires approval or is otherwise unsupported."""


class ControlCommandTransitionError(ControlCommandError):
    """Raised when a lifecycle transition would violate the command contract."""


class ControlCommandExecutionError(ControlCommandError):
    """Raised when a deterministic command handler fails."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        result_ref: str | None = None,
    ) -> None:
        merged = dict(details or {})
        if result_ref is not None:
            merged["result_ref"] = result_ref
        super().__init__(reason_code, message, details=merged)
        self.result_ref = result_ref


class ControlCommandType(str, Enum):
    WORKFLOW_SUBMIT = "workflow.submit"
    WORKFLOW_SPAWN = "workflow.spawn"
    WORKFLOW_CHAIN_SUBMIT = "workflow.chain.submit"
    WORKFLOW_RETRY = "workflow.retry"
    WORKFLOW_CANCEL = "workflow.cancel"
    SYNC_REPAIR = "sync.repair"


class ControlCommandStatus(str, Enum):
    REQUESTED = "requested"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ControlRiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ControlExecutionMode(str, Enum):
    AUTO_EXECUTE = "auto_execute"
    CONFIRM_REQUIRED = "confirm_required"


_COMMAND_TYPES = frozenset(item.value for item in ControlCommandType)
_COMMAND_STATUSES = frozenset(item.value for item in ControlCommandStatus)
_RISK_LEVELS = frozenset(item.value for item in ControlRiskLevel)
_LOW_RISK_LOCAL_COMMAND_TYPES = frozenset(
    {
        ControlCommandType.WORKFLOW_SUBMIT.value,
        ControlCommandType.WORKFLOW_SPAWN.value,
        ControlCommandType.WORKFLOW_CHAIN_SUBMIT.value,
        ControlCommandType.SYNC_REPAIR.value,
    }
)
_LOCAL_AUTO_EXECUTE_REQUESTER_KINDS = frozenset(
    {
        "operator",
        "system",
        "cli",
        "workflow",
        "chat",
        "api",
        "http",
        "mcp",
    }
)
_UNSET = object()
_DEFAULT_RISK_LEVELS = {
    ControlCommandType.WORKFLOW_SUBMIT.value: ControlRiskLevel.LOW.value,
    ControlCommandType.WORKFLOW_SPAWN.value: ControlRiskLevel.LOW.value,
    ControlCommandType.WORKFLOW_CHAIN_SUBMIT.value: ControlRiskLevel.LOW.value,
    ControlCommandType.WORKFLOW_RETRY.value: ControlRiskLevel.MEDIUM.value,
    ControlCommandType.WORKFLOW_CANCEL.value: ControlRiskLevel.HIGH.value,
    ControlCommandType.SYNC_REPAIR.value: ControlRiskLevel.LOW.value,
}
_TRANSITIONS = {
    ControlCommandStatus.REQUESTED.value: frozenset(
        {
            ControlCommandStatus.ACCEPTED.value,
            ControlCommandStatus.REJECTED.value,
        }
    ),
    ControlCommandStatus.ACCEPTED.value: frozenset(
        {
            ControlCommandStatus.RUNNING.value,
            ControlCommandStatus.REJECTED.value,
        }
    ),
    ControlCommandStatus.RUNNING.value: frozenset(
        {
            ControlCommandStatus.SUCCEEDED.value,
            ControlCommandStatus.FAILED.value,
        }
    ),
    ControlCommandStatus.REJECTED.value: frozenset(),
    ControlCommandStatus.SUCCEEDED.value: frozenset(),
    ControlCommandStatus.FAILED.value: frozenset(),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _normalize_bool(
    value: object,
    *,
    field_name: str,
    default_value: bool,
) -> bool:
    if value is None:
        return default_value
    if isinstance(value, bool):
        return value
    raise ControlCommandError(
        "control.command.invalid_value",
        f"{field_name} must be a boolean",
        details={"field": field_name, "value_type": type(value).__name__},
    )


def _normalize_enum_value(
    value: object,
    *,
    field_name: str,
    allowed_values: frozenset[str],
    default_value: str | None = None,
) -> str:
    if value is None:
        if default_value is None:
            raise ControlCommandError(
                "control.command.invalid_value",
                f"{field_name} must be a non-empty string",
                details={"field": field_name, "value_type": "NoneType"},
            )
        value = default_value

    if isinstance(value, Enum):
        value = value.value

    if not isinstance(value, str) or not value.strip():
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )

    normalized = value.strip()
    if normalized not in allowed_values:
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must be one of {sorted(allowed_values)}",
            details={"field": field_name, "value": normalized},
        )
    return normalized


def _normalize_payload(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    json_value = _json_compatible(dict(value))
    if not isinstance(json_value, Mapping):
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must normalize to a mapping",
            details={"field": field_name, "value_type": type(json_value).__name__},
        )
    return cast(dict[str, Any], dict(json_value))


def _normalize_workflow_retry_explanation(
    payload: Mapping[str, Any],
    *,
    field_name: str,
) -> dict[str, Any]:
    normalized = dict(payload)
    previous_failure = _normalize_text(
        normalized.get("previous_failure"),
        field_name=f"{field_name}.previous_failure",
    )
    retry_delta = _normalize_text(
        normalized.get("retry_delta"),
        field_name=f"{field_name}.retry_delta",
    )
    normalized["previous_failure"] = previous_failure
    normalized["retry_delta"] = retry_delta
    return normalized


def _normalize_retry_guard(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    run_id = _normalize_text(value.get("run_id"), field_name=f"{field_name}.run_id")
    label = _normalize_text(value.get("label"), field_name=f"{field_name}.label")
    status = _normalize_text(value.get("status"), field_name=f"{field_name}.status")
    try:
        attempt = int(value.get("attempt") or 0)
    except (TypeError, ValueError) as exc:
        raise ControlCommandError(
            "control.command.invalid_value",
            f"{field_name}.attempt must be an integer",
            details={"field": f"{field_name}.attempt", "value": value.get("attempt")},
        ) from exc
    job_id = value.get("job_id")
    if job_id is not None:
        try:
            job_id = int(job_id)
        except (TypeError, ValueError) as exc:
            raise ControlCommandError(
                "control.command.invalid_value",
                f"{field_name}.job_id must be an integer or null",
                details={"field": f"{field_name}.job_id", "value": value.get("job_id")},
            ) from exc
    return {
        "run_id": run_id,
        "label": label,
        "job_id": job_id,
        "status": status,
        "attempt": attempt,
    }


def workflow_retry_guard(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    label: str,
) -> dict[str, Any]:
    """Return the current job state a retry command must bind to."""

    normalized_run_id = _normalize_text(run_id, field_name="run_id")
    normalized_label = _normalize_text(label, field_name="label")
    if not hasattr(conn, "execute"):
        raise ControlCommandError(
            "control.command.workflow_retry_state_unreadable",
            "workflow retry requires readable job state before command creation",
            details={
                "run_id": normalized_run_id,
                "label": normalized_label,
                "reason": "connection has no execute method",
            },
        )
    try:
        rows = conn.execute(
            """SELECT id, run_id, label, status, attempt
               FROM workflow_jobs
               WHERE run_id = $1 AND label = $2
               ORDER BY id ASC
               LIMIT 1""",
            normalized_run_id,
            normalized_label,
        )
    except Exception as exc:
        raise ControlCommandError(
            "control.command.workflow_retry_state_unreadable",
            "workflow retry requires readable job state before command creation",
            details={
                "run_id": normalized_run_id,
                "label": normalized_label,
                "error": str(exc),
            },
        ) from exc
    if not rows:
        return {
            "run_id": normalized_run_id,
            "label": normalized_label,
            "job_id": None,
            "status": "missing",
            "attempt": 0,
        }
    row = dict(rows[0])
    return _normalize_retry_guard(
        {
            "run_id": row.get("run_id") or normalized_run_id,
            "label": row.get("label") or normalized_label,
            "job_id": row.get("id"),
            "status": row.get("status") or "unknown",
            "attempt": row.get("attempt") or 0,
        },
        field_name="retry_guard",
    )


def workflow_retry_payload_with_guard(
    conn: "SyncPostgresConnection",
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach the moment-in-time job state to a workflow.retry payload."""

    normalized = _normalize_payload(payload, field_name="payload")
    normalized = _normalize_workflow_retry_explanation(
        normalized,
        field_name="payload",
    )
    run_id = _normalize_text(normalized.get("run_id"), field_name="payload.run_id")
    label = _normalize_text(normalized.get("label"), field_name="payload.label")
    guarded = dict(normalized)
    guarded["retry_guard"] = workflow_retry_guard(conn, run_id=run_id, label=label)
    return guarded


def workflow_retry_idempotency_key(
    *,
    requested_by_kind: str,
    payload: Mapping[str, Any],
) -> str:
    """Build a retry key bound to the job state observed by the command."""

    requester = _normalize_text(requested_by_kind, field_name="requested_by_kind")
    normalized = _normalize_payload(payload, field_name="payload")
    normalized = _normalize_workflow_retry_explanation(
        normalized,
        field_name="payload",
    )
    run_id = _normalize_text(normalized.get("run_id"), field_name="payload.run_id")
    guard = _normalize_retry_guard(
        normalized.get("retry_guard"),
        field_name="payload.retry_guard",
    )
    guard_hash = canonical_hash(
        {
            "run_id": run_id,
            "label": guard["label"],
            "retry_guard": guard,
            "previous_failure": normalized["previous_failure"],
            "retry_delta": normalized["retry_delta"],
        }
    )[:24]
    return f"workflow.retry.{requester}.{run_id}.{guard['status']}.{guard['attempt']}.{guard_hash}"


def assert_workflow_retry_guard_current(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    label: str,
    retry_guard: Mapping[str, Any],
    command_id: str | None = None,
) -> dict[str, Any]:
    """Fail a retry command when its bound job-state snapshot is stale."""

    expected = _normalize_retry_guard(retry_guard, field_name="payload.retry_guard")
    current = workflow_retry_guard(conn, run_id=run_id, label=label)
    mismatches = {
        key: {"expected": expected.get(key), "actual": current.get(key)}
        for key in ("run_id", "label", "job_id", "status", "attempt")
        if expected.get(key) != current.get(key)
    }
    if mismatches:
        raise ControlCommandExecutionError(
            "control.command.workflow_retry_guard_stale",
            "workflow retry command was created for stale job state",
            details={
                "command_id": command_id,
                "run_id": run_id,
                "label": label,
                "mismatches": mismatches,
                "expected_retry_guard": expected,
                "actual_retry_guard": current,
            },
        )
    return current


def _ensure_transition_allowed(previous_status: str, next_status: str) -> None:
    if previous_status == next_status:
        return
    allowed = _TRANSITIONS.get(previous_status, frozenset())
    if next_status not in allowed:
        raise ControlCommandTransitionError(
            "control.command.invalid_transition",
            f"invalid control-command transition: {previous_status} -> {next_status}",
            details={
                "previous_status": previous_status,
                "next_status": next_status,
            },
        )


def _row_value(row: Mapping[str, Any], key: str) -> Any:
    value = row.get(key)
    if isinstance(value, str) and key == "payload":
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _row_to_record(row: Mapping[str, Any]) -> "ControlCommandRecord":
    record = ControlCommandRecord(
        command_id=_normalize_text(_row_value(row, "command_id"), field_name="command_id"),
        command_type=_normalize_enum_value(
            _row_value(row, "command_type"),
            field_name="command_type",
            allowed_values=_COMMAND_TYPES,
        ),
        command_status=_normalize_enum_value(
            _row_value(row, "command_status"),
            field_name="command_status",
            allowed_values=_COMMAND_STATUSES,
        ),
        requested_by_kind=_normalize_text(
            _row_value(row, "requested_by_kind"),
            field_name="requested_by_kind",
        ),
        requested_by_ref=_normalize_text(
            _row_value(row, "requested_by_ref"),
            field_name="requested_by_ref",
        ),
        requested_at=_row_value(row, "requested_at"),
        approved_at=_row_value(row, "approved_at"),
        approved_by=(
            None
            if _row_value(row, "approved_by") is None
            else _normalize_text(_row_value(row, "approved_by"), field_name="approved_by")
        ),
        idempotency_key=_normalize_text(
            _row_value(row, "idempotency_key"),
            field_name="idempotency_key",
        ),
        risk_level=_normalize_enum_value(
            _row_value(row, "risk_level"),
            field_name="risk_level",
            allowed_values=_RISK_LEVELS,
        ),
        payload=_normalize_payload(_row_value(row, "payload"), field_name="payload"),
        result_ref=(
            None
            if _row_value(row, "result_ref") is None
            else _normalize_text(_row_value(row, "result_ref"), field_name="result_ref")
        ),
        error_code=(
            None
            if _row_value(row, "error_code") is None
            else _normalize_text(_row_value(row, "error_code"), field_name="error_code")
        ),
        error_detail=(
            None
            if _row_value(row, "error_detail") is None
            else _normalize_text(_row_value(row, "error_detail"), field_name="error_detail")
        ),
        created_at=_row_value(row, "created_at"),
        updated_at=_row_value(row, "updated_at"),
    )
    return record


@lru_cache(maxsize=len(_SCHEMA_FILENAMES))
def _schema_statements(filename: str) -> tuple[str, ...]:
    try:
        return workflow_migration_statements(filename)
    except WorkflowMigrationError as exc:
        reason_code = (
            "control.command.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "control.command.schema_missing"
        )
        message = (
            "control-command schema file did not contain executable statements"
            if reason_code == "control.command.schema_empty"
            else "control-command schema file could not be resolved from the canonical workflow migration root"
        )
        raise ControlCommandError(reason_code, message, details=exc.details) from exc


def bootstrap_control_commands_schema(conn: "SyncPostgresConnection") -> None:
    """Apply the durable command schema and the workflow.* type cutover."""

    sql_statements: list[str] = []
    for filename in _SCHEMA_FILENAMES:
        sql_statements.extend(_schema_statements(filename))

    sql_text = ";\n".join(sql_statements) + ";"
    if hasattr(conn, "execute_script"):
        conn.execute_script(sql_text)
        return
    for statement in sql_statements:
        conn.execute(statement)


@dataclass(frozen=True, slots=True)
class ControlIntent:
    """Typed request contract for one durable command."""

    command_type: str | ControlCommandType
    requested_by_kind: str
    requested_by_ref: str
    idempotency_key: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    risk_level: str | ControlRiskLevel | None = None
    plan_envelope_hash: str | None = None

    def __post_init__(self) -> None:
        command_type = _normalize_enum_value(
            self.command_type,
            field_name="command_type",
            allowed_values=_COMMAND_TYPES,
        )
        requested_by_kind = _normalize_text(self.requested_by_kind, field_name="requested_by_kind")
        requested_by_ref = _normalize_text(self.requested_by_ref, field_name="requested_by_ref")
        idempotency_key = _normalize_text(self.idempotency_key, field_name="idempotency_key")
        payload = _normalize_payload(self.payload, field_name="payload")
        if command_type == ControlCommandType.WORKFLOW_RETRY.value:
            payload = _normalize_workflow_retry_explanation(
                payload,
                field_name="payload",
            )
            _normalize_retry_guard(
                payload.get("retry_guard"),
                field_name="payload.retry_guard",
            )
        risk_level = _normalize_enum_value(
            self.risk_level,
            field_name="risk_level",
            allowed_values=_RISK_LEVELS,
            default_value=_DEFAULT_RISK_LEVELS[command_type],
        )
        plan_envelope_hash = None
        raw_plan_hash = self.plan_envelope_hash or payload.get("plan_envelope_hash")
        if raw_plan_hash is not None:
            plan_envelope_hash = _normalize_text(raw_plan_hash, field_name="plan_envelope_hash")
            payload = dict(payload)
            payload["plan_envelope_hash"] = plan_envelope_hash

        object.__setattr__(self, "command_type", command_type)
        object.__setattr__(self, "requested_by_kind", requested_by_kind)
        object.__setattr__(self, "requested_by_ref", requested_by_ref)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "risk_level", risk_level)
        object.__setattr__(self, "plan_envelope_hash", plan_envelope_hash)

    def to_json(self) -> dict[str, Any]:
        return {
            "command_type": self.command_type,
            "requested_by_kind": self.requested_by_kind,
            "requested_by_ref": self.requested_by_ref,
            "idempotency_key": self.idempotency_key,
            "risk_level": self.risk_level,
            "plan_envelope_hash": self.plan_envelope_hash,
            "payload": _json_compatible(dict(self.payload)),
        }

    def signature(self) -> str:
        return _intent_payload_signature(self)


@dataclass(frozen=True, slots=True)
class ControlPolicyDecision:
    """Policy outcome for a requested control command."""

    mode: str
    risk_level: str
    reason_code: str
    approved_by: str = _AUTO_APPROVAL_REF

    @property
    def auto_execute(self) -> bool:
        return self.mode == ControlExecutionMode.AUTO_EXECUTE.value

    @property
    def confirm_required(self) -> bool:
        return self.mode == ControlExecutionMode.CONFIRM_REQUIRED.value


@dataclass(frozen=True, slots=True)
class ControlCommandRecord:
    """Durable control-command row."""

    command_id: str
    command_type: str
    command_status: str
    requested_by_kind: str
    requested_by_ref: str
    requested_at: datetime
    approved_at: datetime | None
    approved_by: str | None
    idempotency_key: str
    risk_level: str
    payload: Mapping[str, Any]
    result_ref: str | None
    error_code: str | None
    error_detail: str | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "command_id": self.command_id,
            "command_type": self.command_type,
            "command_status": self.command_status,
            "requested_by_kind": self.requested_by_kind,
            "requested_by_ref": self.requested_by_ref,
            "requested_at": self.requested_at.isoformat(),
            "approved_at": None if self.approved_at is None else self.approved_at.isoformat(),
            "approved_by": self.approved_by,
            "idempotency_key": self.idempotency_key,
            "risk_level": self.risk_level,
            "payload": _json_compatible(dict(self.payload)),
            "result_ref": self.result_ref,
            "error_code": self.error_code,
            "error_detail": self.error_detail,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    def signature(self) -> str:
        return _record_payload_signature(self)

    def to_intent(self) -> ControlIntent:
        return ControlIntent(
            command_type=self.command_type,
            requested_by_kind=self.requested_by_kind,
            requested_by_ref=self.requested_by_ref,
            idempotency_key=self.idempotency_key,
            risk_level=self.risk_level,
            payload=dict(self.payload),
            plan_envelope_hash=(
                str(self.payload.get("plan_envelope_hash"))
                if isinstance(self.payload.get("plan_envelope_hash"), str)
                else None
            ),
        )


def _build_control_plan_envelope(intent: ControlIntent):
    return build_plan_envelope(
        command_type=intent.command_type,
        requested_by_kind=intent.requested_by_kind,
        requested_by_ref=intent.requested_by_ref,
        risk_level=intent.risk_level,
        payload=intent.payload,
    )


def stamp_control_intent(intent: ControlIntent) -> ControlIntent:
    """Attach the canonical plan-envelope hash to the control intent."""

    envelope = _build_control_plan_envelope(intent)
    existing_hash = intent.plan_envelope_hash
    if existing_hash is not None and existing_hash != envelope.plan_hash:
        raise ControlCommandPolicyError(
            "control.command.plan_hash_mismatch",
            "control intent plan envelope hash does not match its payload",
            details={
                "expected_plan_envelope_hash": envelope.plan_hash,
                "actual_plan_envelope_hash": existing_hash,
            },
        )
    payload = dict(intent.payload)
    payload["plan_envelope_hash"] = envelope.plan_hash
    return ControlIntent(
        command_type=intent.command_type,
        requested_by_kind=intent.requested_by_kind,
        requested_by_ref=intent.requested_by_ref,
        idempotency_key=intent.idempotency_key,
        risk_level=intent.risk_level,
        payload=payload,
        plan_envelope_hash=envelope.plan_hash,
    )


def _local_policy_covers(intent: ControlIntent) -> bool:
    return (
        intent.command_type in _LOW_RISK_LOCAL_COMMAND_TYPES
        and intent.risk_level == ControlRiskLevel.LOW.value
        and intent.requested_by_kind in _LOCAL_AUTO_EXECUTE_REQUESTER_KINDS
    )


def classify_control_intent(intent: ControlIntent) -> ControlPolicyDecision:
    """Return the policy disposition for one intent."""

    payload = dict(intent.payload)
    if (
        isinstance(payload.get("grant_ref"), str)
        and isinstance(payload.get("plan_envelope_hash"), str)
        and payload.get("grant_coverage_status") == "covered"
    ):
        return ControlPolicyDecision(
            mode=ControlExecutionMode.AUTO_EXECUTE.value,
            risk_level=intent.risk_level,
            reason_code="control.policy.capability_grant_covered",
        )
    if _local_policy_covers(intent):
        return ControlPolicyDecision(
            mode=ControlExecutionMode.AUTO_EXECUTE.value,
            risk_level=intent.risk_level,
            reason_code="control.policy.local_operator_authority",
        )
    return ControlPolicyDecision(
        mode=ControlExecutionMode.CONFIRM_REQUIRED.value,
        risk_level=intent.risk_level,
        reason_code="control.policy.capability_grant_required",
    )


def evaluate_control_intent_policy(
    conn: "SyncPostgresConnection",
    intent: ControlIntent,
) -> ControlPolicyDecision:
    """Evaluate DB-backed policy for one intent."""

    stamped = stamp_control_intent(intent)
    envelope = _build_control_plan_envelope(stamped)
    payload = dict(stamped.payload)
    grant_ref = payload.get("grant_ref")
    principal_ref = payload.get("principal_ref")
    device_id = payload.get("device_id")
    resolution: GrantResolution | None = None
    if isinstance(grant_ref, str) and grant_ref.strip():
        resolution = resolve_capability_grant(
            conn,
            envelope=envelope,
            principal_ref=principal_ref if isinstance(principal_ref, str) else None,
            device_id=device_id if isinstance(device_id, str) else None,
            grant_ref=grant_ref,
        )
    if resolution is not None and resolution.covered:
        return ControlPolicyDecision(
            mode=ControlExecutionMode.AUTO_EXECUTE.value,
            risk_level=stamped.risk_level,
            reason_code=resolution.reason_code,
            approved_by=resolution.grant_ref or _AUTO_APPROVAL_REF,
        )
    if _local_policy_covers(stamped):
        return ControlPolicyDecision(
            mode=ControlExecutionMode.AUTO_EXECUTE.value,
            risk_level=stamped.risk_level,
            reason_code="control.policy.local_operator_authority",
        )
    return ControlPolicyDecision(
        mode=ControlExecutionMode.CONFIRM_REQUIRED.value,
        risk_level=stamped.risk_level,
        reason_code=(
            "control.policy.capability_grant_required"
            if resolution is None
            else resolution.reason_code
        ),
    )


def is_safe_to_auto_execute(intent: ControlIntent) -> bool:
    return classify_control_intent(intent).auto_execute


def requires_confirmation(intent: ControlIntent) -> bool:
    return not is_safe_to_auto_execute(intent)


def load_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
) -> ControlCommandRecord | None:
    rows = conn.execute(
        """SELECT command_id, command_type, command_status, requested_by_kind, requested_by_ref,
                  requested_at, approved_at, approved_by, idempotency_key, risk_level,
                  payload, result_ref, error_code, error_detail, created_at, updated_at
           FROM control_commands
           WHERE command_id = $1
           LIMIT 1""",
        _normalize_text(command_id, field_name="command_id"),
    )
    if not rows:
        return None
    return _row_to_record(cast(Mapping[str, Any], rows[0]))


def load_control_command_by_idempotency_key(
    conn: "SyncPostgresConnection",
    idempotency_key: str,
) -> ControlCommandRecord | None:
    rows = conn.execute(
        """SELECT command_id, command_type, command_status, requested_by_kind, requested_by_ref,
                  requested_at, approved_at, approved_by, idempotency_key, risk_level,
                  payload, result_ref, error_code, error_detail, created_at, updated_at
           FROM control_commands
           WHERE idempotency_key = $1
           LIMIT 1""",
        _normalize_text(idempotency_key, field_name="idempotency_key"),
    )
    if not rows:
        return None
    return _row_to_record(cast(Mapping[str, Any], rows[0]))


def list_control_commands(
    conn: "SyncPostgresConnection",
    *,
    command_type: str | ControlCommandType | None = None,
    command_status: str | ControlCommandStatus | None = None,
    requested_by_kind: str | None = None,
    limit: int = 100,
) -> list[ControlCommandRecord]:
    if limit < 1:
        raise ControlCommandError(
            "control.command.invalid_value",
            "limit must be a positive integer",
            details={"field": "limit", "value": limit},
        )

    clauses: list[str] = []
    args: list[object] = []

    if command_type is not None:
        args.append(
            _normalize_enum_value(
                command_type,
                field_name="command_type",
                allowed_values=_COMMAND_TYPES,
            )
        )
        clauses.append(f"command_type = ${len(args)}")

    if command_status is not None:
        args.append(
            _normalize_enum_value(
                command_status,
                field_name="command_status",
                allowed_values=_COMMAND_STATUSES,
            )
        )
        clauses.append(f"command_status = ${len(args)}")

    if requested_by_kind is not None:
        args.append(_normalize_text(requested_by_kind, field_name="requested_by_kind"))
        clauses.append(f"requested_by_kind = ${len(args)}")

    args.append(limit)
    clauses_sql = ""
    if clauses:
        clauses_sql = "WHERE " + " AND ".join(clauses)

    rows = conn.execute(
        f"""SELECT command_id, command_type, command_status, requested_by_kind, requested_by_ref,
                   requested_at, approved_at, approved_by, idempotency_key, risk_level,
                   payload, result_ref, error_code, error_detail, created_at, updated_at
            FROM control_commands
            {clauses_sql}
            ORDER BY requested_at DESC, created_at DESC, command_id DESC
            LIMIT ${len(args)}""",
        *args,
    )
    return [_row_to_record(cast(Mapping[str, Any], row)) for row in rows or []]


def _insert_control_command_row(
    conn: "SyncPostgresConnection",
    *,
    command_id: str,
    intent: ControlIntent,
    requested_at: datetime,
) -> ControlCommandRecord | None:
    row = PostgresCommandRepository(conn).insert_control_command(
        command_id=command_id,
        command_type=intent.command_type,
        command_status=ControlCommandStatus.REQUESTED.value,
        requested_by_kind=intent.requested_by_kind,
        requested_by_ref=intent.requested_by_ref,
        requested_at=requested_at,
        approved_at=None,
        approved_by=None,
        idempotency_key=intent.idempotency_key,
        risk_level=intent.risk_level,
        payload=intent.payload,
        result_ref=None,
        error_code=None,
        error_detail=None,
        created_at=requested_at,
        updated_at=requested_at,
    )
    if row is None:
        return None
    return _row_to_record(cast(Mapping[str, Any], row))


def create_control_command(
    conn: "SyncPostgresConnection",
    intent: ControlIntent,
    *,
    command_id: str | None = None,
    requested_at: datetime | None = None,
    auto_execute: bool = True,
) -> ControlCommandRecord:
    """Create a durable command row, optionally auto-executing safe intents."""

    requested_at = requested_at or _utc_now()
    intent = stamp_control_intent(intent)
    policy = evaluate_control_intent_policy(conn, intent)
    command_id = command_id or f"control.command.{uuid.uuid4().hex[:12]}"
    signature = intent.signature()

    created = _insert_control_command_row(
        conn,
        command_id=command_id,
        intent=intent,
        requested_at=requested_at,
    )
    if created is None:
        existing = load_control_command_by_idempotency_key(conn, intent.idempotency_key)
        if existing is None:
            raise ControlCommandError(
                "control.command.insert_failed",
                "failed to load control command after idempotency conflict",
                details={"idempotency_key": intent.idempotency_key},
            )
        if existing.signature() != signature:
            raise ControlCommandIdempotencyConflict(
                intent.idempotency_key,
                existing.command_id,
                existing.requested_at,
            )
        logger.info(
            "Idempotent replay: returning existing control command %s",
            existing.command_id,
        )
        return existing

    _emit_system_event(
        conn,
        "control.command.requested",
        created,
        previous_status=None,
        extra={"policy": policy.mode, "policy_reason_code": policy.reason_code},
    )

    if auto_execute and policy.auto_execute:
        return execute_control_command(conn, created.command_id)

    return created


def update_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
    *,
    command_status: str | ControlCommandStatus | None = None,
    approved_at: datetime | None | object = _UNSET,
    approved_by: str | None | object = _UNSET,
    payload: Mapping[str, Any] | None | object = _UNSET,
    result_ref: str | None | object = _UNSET,
    error_code: str | None | object = _UNSET,
    error_detail: str | None | object = _UNSET,
) -> ControlCommandRecord:
    """Update one command row, enforcing lifecycle transitions."""

    current = load_control_command(conn, command_id)
    if current is None:
        raise ControlCommandError(
            "control.command.not_found",
            f"control command not found: {command_id}",
            details={"command_id": command_id},
        )

    next_status = current.command_status
    if command_status is not None:
        next_status = _normalize_enum_value(
            command_status,
            field_name="command_status",
            allowed_values=_COMMAND_STATUSES,
        )
    _ensure_transition_allowed(current.command_status, next_status)

    next_approved_at = current.approved_at if approved_at is _UNSET else approved_at
    next_approved_by = current.approved_by if approved_by is _UNSET else (
        None if approved_by is None else _normalize_text(approved_by, field_name="approved_by")
    )
    next_payload = current.payload if payload is _UNSET else _normalize_payload(payload, field_name="payload")
    next_result_ref = current.result_ref if result_ref is _UNSET else (
        None if result_ref is None else _normalize_text(result_ref, field_name="result_ref")
    )
    next_error_code = current.error_code if error_code is _UNSET else (
        None if error_code is None else _normalize_text(error_code, field_name="error_code")
    )
    next_error_detail = current.error_detail if error_detail is _UNSET else (
        None if error_detail is None else _normalize_text(error_detail, field_name="error_detail")
    )

    if next_status == ControlCommandStatus.ACCEPTED.value:
        if next_approved_by is None:
            raise ControlCommandTransitionError(
                "control.command.approval_required",
                "accepted commands must record approved_by",
                details={"command_id": command_id},
            )
        if next_approved_at is None:
            next_approved_at = _utc_now()

    if next_status in {
        ControlCommandStatus.RUNNING.value,
        ControlCommandStatus.SUCCEEDED.value,
        ControlCommandStatus.FAILED.value,
    } and next_approved_at is None:
        raise ControlCommandTransitionError(
            "control.command.approval_required",
            "running or terminal commands must already be approved",
            details={"command_id": command_id, "status": next_status},
        )

    try:
        row = PostgresCommandRepository(conn).update_control_command(
            command_id=command_id,
            command_status=next_status,
            approved_at=next_approved_at,
            approved_by=next_approved_by,
            payload=next_payload,
            result_ref=next_result_ref,
            error_code=next_error_code,
            error_detail=next_error_detail,
        )
    except RuntimeError as exc:
        raise ControlCommandError(
            "control.command.update_failed",
            f"control command could not be updated: {command_id}",
            details={"command_id": command_id, "cause_type": type(exc).__name__},
        ) from exc

    updated = _row_to_record(cast(Mapping[str, Any], row))
    if updated.command_status != current.command_status:
        _emit_system_event(
            conn,
            _event_type_for_status(updated.command_status),
            updated,
            previous_status=current.command_status,
        )
    return updated


def accept_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
    *,
    approved_by: str,
    approved_at: datetime | None = None,
) -> ControlCommandRecord:
    return update_control_command(
        conn,
        command_id,
        command_status=ControlCommandStatus.ACCEPTED.value,
        approved_at=approved_at or _utc_now(),
        approved_by=approved_by,
    )


def reject_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
    *,
    error_code: str = "control.command.rejected",
    error_detail: str | None = None,
) -> ControlCommandRecord:
    return update_control_command(
        conn,
        command_id,
        command_status=ControlCommandStatus.REJECTED.value,
        result_ref=None,
        error_code=error_code,
        error_detail=error_detail or "control command rejected",
    )


def start_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
) -> ControlCommandRecord:
    return update_control_command(
        conn,
        command_id,
        command_status=ControlCommandStatus.RUNNING.value,
    )


def complete_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
    *,
    result_ref: str,
) -> ControlCommandRecord:
    return update_control_command(
        conn,
        command_id,
        command_status=ControlCommandStatus.SUCCEEDED.value,
        result_ref=result_ref,
        error_code=None,
        error_detail=None,
    )


def fail_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
    *,
    error_code: str,
    error_detail: str,
    result_ref: str | None = None,
) -> ControlCommandRecord:
    return update_control_command(
        conn,
        command_id,
        command_status=ControlCommandStatus.FAILED.value,
        result_ref=result_ref,
        error_code=error_code,
        error_detail=error_detail,
    )


def execute_control_command(
    conn: "SyncPostgresConnection",
    command_id: str,
) -> ControlCommandRecord:
    """Execute one durable command deterministically."""

    command = load_control_command(conn, command_id)
    if command is None:
        raise ControlCommandError(
            "control.command.not_found",
            f"control command not found: {command_id}",
            details={"command_id": command_id},
        )

    if command.command_status == ControlCommandStatus.REQUESTED.value:
        policy = evaluate_control_intent_policy(conn, command.to_intent())
        if not policy.auto_execute:
            raise ControlCommandPolicyError(
                "control.command.confirmation_required",
                f"command requires confirmation before execution: {command.command_type}",
                details={
                    "command_id": command.command_id,
                    "command_type": command.command_type,
                    "risk_level": command.risk_level,
                    "policy_reason_code": policy.reason_code,
                },
            )
        command = accept_control_command(
            conn,
            command.command_id,
            approved_by=policy.approved_by,
        )
    elif command.command_status != ControlCommandStatus.ACCEPTED.value:
        raise ControlCommandTransitionError(
            "control.command.invalid_transition",
            f"command cannot be executed from status {command.command_status}",
            details={
                "command_id": command.command_id,
                "command_status": command.command_status,
            },
        )

    command = start_control_command(conn, command.command_id)
    try:
        result_ref = _run_command_handler(conn, command)
        return complete_control_command(
            conn,
            command.command_id,
            result_ref=result_ref,
        )
    except ControlCommandExecutionError as exc:
        return fail_control_command(
            conn,
            command.command_id,
            error_code=exc.reason_code,
            error_detail=str(exc),
            result_ref=exc.result_ref,
        )
    except Exception as exc:
        return fail_control_command(
            conn,
            command.command_id,
            error_code="control.command.execution_failed",
            error_detail=str(exc),
        )


def request_control_command(
    conn: "SyncPostgresConnection",
    intent: ControlIntent,
    *,
    command_id: str | None = None,
    requested_at: datetime | None = None,
) -> ControlCommandRecord:
    """Create a command and auto-execute safe intents."""

    return create_control_command(
        conn,
        intent,
        command_id=command_id,
        requested_at=requested_at,
        auto_execute=True,
    )


def execute_control_intent(
    conn: "SyncPostgresConnection",
    intent: ControlIntent,
    *,
    approved_by: str,
    command_id: str | None = None,
    requested_at: datetime | None = None,
    approved_at: datetime | None = None,
) -> ControlCommandRecord:
    """Create, explicitly approve, and execute one intent through the bus."""

    approver = _normalize_text(approved_by, field_name="approved_by")
    command = create_control_command(
        conn,
        intent,
        command_id=command_id,
        requested_at=requested_at,
        auto_execute=False,
    )
    if command.command_status == ControlCommandStatus.REQUESTED.value:
        command = accept_control_command(
            conn,
            command.command_id,
            approved_by=approver,
            approved_at=approved_at,
        )
    if command.command_status == ControlCommandStatus.ACCEPTED.value:
        return execute_control_command(conn, command.command_id)
    return command


__all__ = [
    "ControlCommandError",
    "ControlCommandExecutionError",
    "ControlCommandIdempotencyConflict",
    "ControlCommandPolicyError",
    "ControlCommandRecord",
    "ControlCommandStatus",
    "ControlCommandTransitionError",
    "ControlCommandType",
    "ControlExecutionMode",
    "ControlIntent",
    "ControlPolicyDecision",
    "ControlRiskLevel",
    "accept_control_command",
    "assert_workflow_retry_guard_current",
    "bootstrap_control_commands_schema",
    "classify_control_intent",
    "complete_control_command",
    "create_control_command",
    "evaluate_control_intent_policy",
    "execute_control_intent",
    "execute_control_command",
    "fail_control_command",
    "is_safe_to_auto_execute",
    "list_control_commands",
    "load_control_command",
    "load_control_command_by_idempotency_key",
    "reject_control_command",
    "render_control_command_response",
    "render_control_command_failure",
    "request_control_command",
    "submit_workflow_chain_command",
    "render_workflow_chain_submit_response",
    "request_workflow_chain_submit_command",
    "render_workflow_spawn_response",
    "request_workflow_spawn_command",
    "render_workflow_submit_response",
    "request_workflow_submit_command",
    "submit_workflow_command",
    "requires_confirmation",
    "stamp_control_intent",
    "start_control_command",
    "workflow_cancel_proof",
    "workflow_retry_guard",
    "workflow_retry_idempotency_key",
    "workflow_retry_payload_with_guard",
    "update_control_command",
]
