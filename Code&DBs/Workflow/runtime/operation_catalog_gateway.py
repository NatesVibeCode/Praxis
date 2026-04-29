"""Shared execution gateway for DB-backed operation catalog bindings."""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
from collections.abc import Callable, Mapping
from contextlib import contextmanager
import contextvars
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
import inspect
import json
import threading
import time
from typing import Any
from uuid import uuid4

from storage.postgres import SyncPostgresConnection, get_workflow_pool

from .operation_catalog import get_resolved_operation_definition
from .operation_catalog_bindings import (
    ResolvedHttpOperationBinding,
    resolve_http_operation_binding,
)
from .posture import Posture
from .crypto_authority import canonical_digest_hex


@dataclass(slots=True)
class _EnvironmentBackedSubsystems:
    env: Mapping[str, str]
    conn: Any

    def get_pg_conn(self) -> Any:
        return self.conn

    def _postgres_env(self) -> dict[str, str]:
        return dict(self.env)


@dataclass(frozen=True, slots=True)
class CallerContext:
    """Causal-tracing handle threaded through nested gateway calls.

    A handler that triggers downstream gateway calls passes the parent
    receipt's context so child receipts get cause_receipt_id and inherit
    correlation_id. When ``caller_context`` is None at an entry point
    (HTTP/CLI/MCP boundary), the gateway mints a fresh correlation_id
    and the receipt is the root of a new trace tree.
    """

    cause_receipt_id: str | None
    correlation_id: str


def caller_context_from_receipt(receipt: Mapping[str, Any]) -> CallerContext:
    """Build a child ``CallerContext`` from a parent receipt dict.

    Use this in handlers that call ``execute_operation_from_subsystems``
    on behalf of a parent operation: pass the parent's receipt mapping
    in, get a context whose ``cause_receipt_id`` points at the parent
    and ``correlation_id`` is inherited.
    """

    receipt_id = receipt.get("receipt_id")
    correlation = receipt.get("correlation_id")
    if receipt_id is None:
        raise ValueError("caller_context_from_receipt: receipt is missing receipt_id")
    if correlation is None:
        raise ValueError("caller_context_from_receipt: receipt is missing correlation_id")
    return CallerContext(cause_receipt_id=str(receipt_id), correlation_id=str(correlation))


# ContextVar holding the caller context that nested gateway calls should
# inherit. The gateway sets this to the *child* context for the duration
# of a handler invocation: nested execute_operation_from_subsystems calls
# read it and produce receipts whose cause_receipt_id points at the
# parent receipt and whose correlation_id matches the parent trace.
#
# Asyncio Task creation copies the current Context, so nested calls in
# coroutines spawned via `asyncio.create_task(...)` from a handler also
# inherit. Phase 2 will harden the propagation across additional spawn
# boundaries (run_in_executor, queue handoffs).
CURRENT_CALLER_CONTEXT: ContextVar[CallerContext | None] = ContextVar(
    "praxis_gateway_caller_context",
    default=None,
)


def current_caller_context() -> CallerContext | None:
    """Return the caller context the current code path should inherit."""
    return CURRENT_CALLER_CONTEXT.get()


def spawn_threaded(
    target: Callable[..., Any],
    *args: Any,
    name: str | None = None,
    daemon: bool = True,
    **kwargs: Any,
) -> threading.Thread:
    """Start a daemon thread that inherits the current ContextVar context.

    Plain ``threading.Thread`` runs its target in a fresh thread with empty
    ContextVars, so any nested gateway call inside the target produces a
    receipt with no cause_receipt_id and a fresh correlation_id —
    disconnected from the caller's trace. This helper snapshots
    ``contextvars.copy_context()`` and runs the target inside
    ``ctx.run(target, ...)`` so CURRENT_CALLER_CONTEXT (and any other
    ContextVar) flows in.

    Use this in any handler that does fire-and-forget background work via
    raw threads — kickoff dispatchers, async worker spawns, post-receipt
    hooks. asyncio.create_task already handles inheritance automatically;
    this is the threading-side analogue.
    """

    ctx = contextvars.copy_context()
    thread = threading.Thread(
        target=ctx.run,
        args=(target, *args),
        kwargs=kwargs,
        name=name,
        daemon=daemon,
    )
    thread.start()
    return thread


@dataclass(frozen=True, slots=True)
class AuthorityGateway:
    """Universal CQRS front door over registered authority operations."""

    subsystems: Any

    def execute(
        self,
        operation_name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        caller_context: CallerContext | None = None,
    ) -> Any:
        return execute_operation_from_subsystems(
            self.subsystems,
            operation_name=operation_name,
            payload=payload,
            requested_mode="command",
            caller_context=caller_context,
        )

    def query(
        self,
        operation_name: str,
        payload: Mapping[str, Any] | None = None,
        *,
        caller_context: CallerContext | None = None,
    ) -> Any:
        return execute_operation_from_subsystems(
            self.subsystems,
            operation_name=operation_name,
            payload=payload,
            requested_mode="query",
            caller_context=caller_context,
        )


class OperationIdempotencyConflict(RuntimeError):
    """Raised when an idempotency key is reused with different input."""

    def __init__(self, *, operation_ref: str, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency conflict for {operation_ref}: key={idempotency_key} "
            "exists with different input"
        )
        self.operation_ref = operation_ref
        self.idempotency_key = idempotency_key


class OperationModeViolation(RuntimeError):
    """Raised when a caller-requested execution mode does not admit an operation."""

    def __init__(
        self,
        *,
        operation_ref: str,
        requested_mode: str,
        operation_kind: str,
        posture: str,
    ) -> None:
        super().__init__(
            f"Operation {operation_ref} is {operation_kind}/{posture} and cannot run as {requested_mode}"
        )
        self.operation_ref = operation_ref
        self.requested_mode = requested_mode
        self.operation_kind = operation_kind
        self.posture = posture


def _normalized_requested_mode(requested_mode: str | None) -> str:
    return str(requested_mode or "call").strip().lower().replace("-", "_")


def _assert_mode_admits_operation(
    binding: ResolvedHttpOperationBinding,
    *,
    requested_mode: str | None,
) -> None:
    mode = _normalized_requested_mode(requested_mode)
    if mode == "call":
        return

    operation_kind = str(_binding_value(binding, "operation_kind", "") or "").strip()
    posture = str(_binding_value(binding, "posture", "") or "").strip()
    idempotency_policy = str(_binding_value(binding, "idempotency_policy", "") or "").strip()
    if mode == "query":
        if (
            operation_kind == "query"
            and (posture == Posture.OBSERVE.value or idempotency_policy == "read_only")
        ):
            return
    elif mode == "command" and operation_kind == "command":
        return

    raise OperationModeViolation(
        operation_ref=binding.operation_ref,
        requested_mode=mode,
        operation_kind=operation_kind,
        posture=posture,
    )


def resolve_named_operation_binding(
    conn: Any,
    *,
    operation_name: str,
) -> ResolvedHttpOperationBinding:
    definition = get_resolved_operation_definition(conn, operation_name=operation_name)
    return resolve_http_operation_binding(definition)


def build_operation_command(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    command_data = dict(payload or {})
    return binding.command_class(**command_data)


def _stable_hash(value: Any) -> str:
    return canonical_digest_hex(value, purpose="operation_catalog_gateway.stable_hash")


def _binding_value(binding: Any, name: str, default: Any) -> Any:
    return getattr(binding, name, default)


import threading as _threading
import time as _time
from pathlib import Path as _Path

_GATEWAY_CALL_COUNTERS: dict[str, list[float]] = {}
_GATEWAY_DRIFT_WINDOW_S = 90.0
_GATEWAY_DRIFT_THRESHOLD = 5
_GATEWAY_DRIFT_LOCK = _threading.Lock()
_GATEWAY_REFUSAL_COUNTERS: dict[str, list[float]] = {}


# ──────────────────────────────────────────────────────────────────────────
# Code-drift signal — stale-process detector
#
# Long-running container processes (api-server, workflow-worker, scheduler,
# semantic-backend) and the host MCP daemon all import Python modules at
# process start. Edits to bind-mounted source files don't reach those
# processes until they restart. This session's history shows that gap:
# multiple "fixed code" cycles where the edits were on disk but the running
# process kept executing the old version, leading to silent wrong behavior.
#
# This detector captures `_PROCESS_STARTED_AT_EPOCH` at module import. On
# every gateway response it walks the runtime code roots and compares max
# mtime. If on-disk code is newer than the process, it attaches a
# `code_out_of_date` signal on the response — fail-open: the call still
# returns its result, but the model/operator sees that the result was
# computed by stale code and should bounce the container.
#
# Cached for 30s to avoid stat-walking on every call.
# ──────────────────────────────────────────────────────────────────────────

_PROCESS_STARTED_AT_EPOCH: float = _time.time()

_CODE_DRIFT_ROOT_NAMES: tuple[str, ...] = (
    "runtime",
    "registry",
    "surfaces",
    "storage",
    "adapters",
)
_CODE_DRIFT_CACHE: dict[str, float] = {"checked_at": 0.0, "max_mtime": 0.0}
_CODE_DRIFT_CACHE_TTL_S: float = 30.0
_CODE_DRIFT_LOCK = _threading.Lock()


def _workflow_code_root() -> _Path:
    """Return the Code&DBs/Workflow directory (parent of runtime/)."""
    return _Path(__file__).resolve().parents[1]


def _max_code_mtime_cached() -> float:
    """Walk the code roots and return the max .py mtime. Fail-open: any
    OSError → 0.0 (no signal). Cached 30s."""
    now = _time.time()
    with _CODE_DRIFT_LOCK:
        if now - _CODE_DRIFT_CACHE["checked_at"] < _CODE_DRIFT_CACHE_TTL_S:
            return _CODE_DRIFT_CACHE["max_mtime"]
    try:
        root = _workflow_code_root()
        max_mtime = 0.0
        for root_name in _CODE_DRIFT_ROOT_NAMES:
            sub = root / root_name
            if not sub.is_dir():
                continue
            for py_file in sub.rglob("*.py"):
                try:
                    m = py_file.stat().st_mtime
                    if m > max_mtime:
                        max_mtime = m
                except OSError:
                    continue
    except OSError:
        return 0.0
    with _CODE_DRIFT_LOCK:
        _CODE_DRIFT_CACHE["checked_at"] = now
        _CODE_DRIFT_CACHE["max_mtime"] = max_mtime
    return max_mtime


def _check_code_drift_signal() -> dict[str, Any] | None:
    """Return a `code_out_of_date` payload when on-disk code is newer than
    process start, or None when the process is up-to-date. Fails open: any
    error returns None (no false-positive drift signal)."""
    try:
        max_mtime = _max_code_mtime_cached()
        if max_mtime <= _PROCESS_STARTED_AT_EPOCH:
            return None
        drift_seconds = int(max_mtime - _PROCESS_STARTED_AT_EPOCH)
        return {
            "code_out_of_date": True,
            "process_started_at_epoch": _PROCESS_STARTED_AT_EPOCH,
            "latest_code_mtime_epoch": max_mtime,
            "drift_seconds": drift_seconds,
            "scanned_roots": list(_CODE_DRIFT_ROOT_NAMES),
            "hint": (
                "This process is running stale code — files under "
                f"{', '.join(_CODE_DRIFT_ROOT_NAMES)} have been modified "
                f"{drift_seconds}s after the process started. The result "
                "above was computed by the OLD code; restart the container "
                "(`docker compose restart api-server workflow-worker scheduler`) "
                "or bounce the host MCP daemon before trusting subsequent results."
            ),
        }
    except Exception:  # noqa: BLE001 — fail open
        return None


def _record_gateway_call_and_check_drift(operation_ref: str) -> dict[str, Any] | None:
    """Track repeated calls per operation_ref. When the same op fires N times
    in a sliding window, return a `model_drift_signal` dict the response
    builder will surface to the caller. Tells the model "you keep doing this;
    use a different tool" — putting reality in the model's face per the
    operator request to make platform observability self-defending.

    Pure in-process; no DB hit. Reset across restarts."""
    now = _time.monotonic()
    with _GATEWAY_DRIFT_LOCK:
        timestamps = _GATEWAY_CALL_COUNTERS.setdefault(operation_ref, [])
        # Drop entries outside the window
        cutoff = now - _GATEWAY_DRIFT_WINDOW_S
        timestamps[:] = [t for t in timestamps if t >= cutoff]
        timestamps.append(now)
        recent_count = len(timestamps)
    if recent_count >= _GATEWAY_DRIFT_THRESHOLD:
        # Suggest the next tool the operator should pivot to. Operation-specific
        # routing — for read ops where the model is hammering, suggest the
        # consolidated control-plane / search tool.
        suggested_pivot = _drift_pivot_for_operation(operation_ref)
        return {
            "drift": True,
            "operation_ref": operation_ref,
            "calls_in_window_seconds": int(_GATEWAY_DRIFT_WINDOW_S),
            "calls_in_window": recent_count,
            "threshold": _GATEWAY_DRIFT_THRESHOLD,
            "hint": (
                f"You've called `{operation_ref}` {recent_count} times in the last "
                f"{int(_GATEWAY_DRIFT_WINDOW_S)}s. If the answer hasn't changed, "
                "stop polling and either pivot to a different tool or surface "
                "the unresolved problem to the operator. Repeated identical "
                "calls almost always mean you're stuck in mode-lock."
            ),
            "suggested_pivot": suggested_pivot,
        }
    return None


def _drift_pivot_for_operation(operation_ref: str) -> dict[str, str] | None:
    """Map common 'I'm stuck in this tool' patterns to the better tool."""
    pivots: dict[str, dict[str, str]] = {
        "operator-circuit-states": {
            "tool": "praxis_provider_control_plane",
            "reason": "if you keep listing breaker state, the issue is downstream — control plane shows is_runnable + removal_reasons across every gate",
        },
        "operator-provider-control-plane": {
            "tool": "praxis_circuits / praxis_access_control / praxis_provider_onboard",
            "reason": "if control plane keeps showing is_runnable=false, fix the gate the named tool owns",
        },
        "operator-model-access-control-matrix": {
            "tool": "praxis_provider_control_plane",
            "reason": "matrix is the read; control_plane is the read+removal_reasons. Pivot.",
        },
    }
    return pivots.get(operation_ref)


def _idempotency_key(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None,
    input_hash: str,
    idempotency_key_override: str | None = None,
) -> str | None:
    """Compute the cache key used for replay lookup. Returning None disables replay.

    Policy semantics (2026-04-26 correction):
      * ``idempotent`` — pure function of input; replay is free.
      * ``read_only`` — does not mutate, BUT the underlying state can change
        between calls (live views, time-windowed projections, circuit-breaker
        state, control-plane catalog). Replay would serve stale data, so we
        do NOT replay; each call runs fresh and records its own receipt.
      * ``non_idempotent`` — every call must run.

    Previously ``read_only`` was treated like ``idempotent`` and caused
    ``praxis_circuits list`` etc. to return cached results that disagreed
    with the live projection.
    """
    if idempotency_key_override is not None and idempotency_key_override.strip():
        return idempotency_key_override.strip()
    policy = str(_binding_value(binding, "idempotency_policy", "") or "").strip()
    if policy != "idempotent":
        return None
    fields = _binding_value(binding, "idempotency_key_fields", ()) or ()
    source = dict(payload or {})
    if fields:
        material = {str(field): source.get(str(field)) for field in fields}
    else:
        material = {"input_hash": input_hash}
    return f"{binding.operation_ref}:{_stable_hash(material)}"


def _fetch_existing_idempotent_result(
    conn: Any,
    *,
    operation_ref: str,
    idempotency_key: str | None,
    input_hash: str,
) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    row = conn.fetchrow(
        """
        SELECT receipt_id, input_hash, result_payload
          FROM authority_operation_receipts
         WHERE operation_ref = $1
           AND idempotency_key = $2
           AND execution_status = 'completed'
         ORDER BY created_at DESC
         LIMIT 1
        """,
        operation_ref,
        idempotency_key,
    )
    if row is None:
        return None
    existing_input_hash = row.get("input_hash") if isinstance(row, Mapping) else row["input_hash"]
    if existing_input_hash != input_hash:
        raise OperationIdempotencyConflict(
            operation_ref=operation_ref,
            idempotency_key=idempotency_key,
        )
    payload = row.get("result_payload") if isinstance(row, Mapping) else row["result_payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    return dict(payload) if isinstance(payload, Mapping) else None


def _cached_result_body(cached: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(cached)
    result.pop("operation_receipt", None)
    return result


def _projection_freshness(result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        return {}
    raw = result.get("projection_freshness")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _error_code_for_exception(exc: Exception) -> str:
    reason_code = getattr(exc, "reason_code", None)
    if isinstance(reason_code, str) and reason_code.strip():
        return reason_code.strip()
    return type(exc).__name__


def _error_details_for_exception(exc: Exception) -> dict[str, Any] | None:
    details = getattr(exc, "details", None)
    return dict(details) if isinstance(details, Mapping) else None


def _result_authority_event_ids(result: Any) -> list[str]:
    if not isinstance(result, Mapping):
        return []
    raw = result.get("authority_event_ids")
    if not isinstance(raw, list):
        return []
    event_ids: list[str] = []
    for value in raw:
        if isinstance(value, str) and value.strip():
            event_ids.append(value.strip())
    return event_ids


def _receipt_payload(
    binding: ResolvedHttpOperationBinding,
    *,
    receipt_id: str,
    input_hash: str,
    output_hash: str | None,
    idempotency_key: str | None,
    execution_status: str,
    result_status: str | None,
    error_code: str | None,
    error_detail: str | None,
    event_ids: list[str],
    projection_freshness: Mapping[str, Any],
    duration_ms: int,
    cause_receipt_id: str | None,
    correlation_id: str,
) -> dict[str, Any]:
    return {
        "receipt_id": receipt_id,
        "operation_ref": binding.operation_ref,
        "operation_name": binding.operation_name,
        "operation_kind": binding.operation_kind,
        "source_kind": binding.source_kind,
        "authority_ref": binding.authority_ref,
        "authority_domain_ref": _binding_value(binding, "authority_domain_ref", binding.authority_ref),
        "projection_ref": binding.projection_ref,
        "storage_target_ref": _binding_value(binding, "storage_target_ref", "praxis.primary_postgres"),
        "posture": binding.posture,
        "idempotency_policy": binding.idempotency_policy,
        "idempotency_key": idempotency_key,
        "binding_revision": binding.binding_revision,
        "decision_ref": binding.decision_ref,
        "execution_status": execution_status,
        "result_status": result_status,
        "error_code": error_code,
        "error_detail": error_detail,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "event_ids": list(event_ids),
        "projection_freshness": dict(projection_freshness),
        "duration_ms": duration_ms,
        "cause_receipt_id": cause_receipt_id,
        "correlation_id": correlation_id,
    }


def _insert_operation_receipt(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    receipt: Mapping[str, Any],
    result_payload: Any,
) -> None:
    conn.execute(
        """
        INSERT INTO authority_operation_receipts (
            receipt_id,
            operation_ref,
            operation_name,
            operation_kind,
            authority_domain_ref,
            authority_ref,
            projection_ref,
            storage_target_ref,
            input_hash,
            output_hash,
            idempotency_key,
            caller_ref,
            execution_status,
            result_status,
            error_code,
            error_detail,
            event_ids,
            projection_freshness,
            result_payload,
            duration_ms,
            binding_revision,
            decision_ref,
            cause_receipt_id,
            correlation_id
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb,
            $19::jsonb, $20, $21, $22, $23::uuid, $24::uuid
        )
        """,
        receipt["receipt_id"],
        binding.operation_ref,
        binding.operation_name,
        binding.operation_kind,
        receipt["authority_domain_ref"],
        binding.authority_ref,
        binding.projection_ref,
        receipt["storage_target_ref"],
        receipt["input_hash"],
        receipt["output_hash"],
        receipt["idempotency_key"],
        "authority_gateway",
        receipt["execution_status"],
        receipt["result_status"],
        receipt["error_code"],
        receipt["error_detail"],
        json.dumps(receipt["event_ids"], sort_keys=True, default=str),
        json.dumps(receipt["projection_freshness"], sort_keys=True, default=str),
        json.dumps(result_payload, sort_keys=True, default=str),
        receipt["duration_ms"],
        binding.binding_revision,
        binding.decision_ref,
        receipt.get("cause_receipt_id"),
        receipt.get("correlation_id"),
    )


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    try:
        return row[name]
    except (KeyError, IndexError, TypeError):
        return default


def _decode_json_field(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _fetch_persisted_operation_receipt(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    receipt_id: str,
) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        SELECT receipt_id::text AS receipt_id,
               operation_ref,
               operation_name,
               operation_kind,
               authority_domain_ref,
               authority_ref,
               projection_ref,
               storage_target_ref,
               input_hash,
               output_hash,
               idempotency_key,
               execution_status,
               result_status,
               error_code,
               error_detail,
               event_ids,
               projection_freshness,
               duration_ms,
               binding_revision,
               decision_ref,
               cause_receipt_id::text AS cause_receipt_id,
               correlation_id::text AS correlation_id
          FROM authority_operation_receipts
         WHERE receipt_id = $1::uuid
        """,
        receipt_id,
    )
    if row is None:
        raise RuntimeError(
            "operation receipt persistence failed: "
            f"receipt_id={receipt_id} was not readable after proof write"
        )

    event_ids = _decode_json_field(_row_value(row, "event_ids"), default=[])
    if not isinstance(event_ids, list):
        event_ids = []
    projection_freshness = _decode_json_field(
        _row_value(row, "projection_freshness"),
        default={},
    )
    if not isinstance(projection_freshness, Mapping):
        projection_freshness = {}

    return {
        "receipt_id": str(_row_value(row, "receipt_id")),
        "operation_ref": _row_value(row, "operation_ref"),
        "operation_name": _row_value(row, "operation_name"),
        "operation_kind": _row_value(row, "operation_kind"),
        "source_kind": binding.source_kind,
        "authority_ref": _row_value(row, "authority_ref"),
        "authority_domain_ref": _row_value(row, "authority_domain_ref"),
        "projection_ref": _row_value(row, "projection_ref"),
        "storage_target_ref": _row_value(row, "storage_target_ref"),
        "posture": binding.posture,
        "idempotency_policy": binding.idempotency_policy,
        "idempotency_key": _row_value(row, "idempotency_key"),
        "binding_revision": _row_value(row, "binding_revision"),
        "decision_ref": _row_value(row, "decision_ref"),
        "execution_status": _row_value(row, "execution_status"),
        "result_status": _row_value(row, "result_status"),
        "error_code": _row_value(row, "error_code"),
        "error_detail": _row_value(row, "error_detail"),
        "input_hash": _row_value(row, "input_hash"),
        "output_hash": _row_value(row, "output_hash"),
        "event_ids": [str(value) for value in event_ids],
        "projection_freshness": dict(projection_freshness),
        "duration_ms": _row_value(row, "duration_ms"),
        "cause_receipt_id": _row_value(row, "cause_receipt_id"),
        "correlation_id": _row_value(row, "correlation_id"),
    }


def _insert_authority_event(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    event_id: str | None = None,
    receipt_id: str,
    input_hash: str,
    output_hash: str,
    idempotency_key: str | None,
    result_status: str | None,
    custom_payload: Mapping[str, Any] | None = None,
    correlation_id: str | None = None,
) -> str:
    """Insert one row into ``authority_events``.

    The base payload is a fixed shape (operation_name, input/output hash,
    result_status, recorded_at) so every event carries the same audit
    metadata. When the handler returns an ``event_payload`` field, those
    keys are merged on top so conceptual events can carry decision-
    relevant fields (e.g. ``compose.experiment.completed`` carries
    ``config_count`` / ``winning_config_index`` / ``matrix_summary``).
    Custom keys win on conflict; metadata keys cannot be overridden.
    """
    event_id = event_id or str(uuid4())
    authority_domain_ref = _binding_value(binding, "authority_domain_ref", binding.authority_ref)
    event_type = _binding_value(binding, "event_type", None) or binding.operation_name.replace(".", "_")
    payload: dict[str, Any] = {
        "operation_name": binding.operation_name,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "result_status": result_status,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(custom_payload, Mapping):
        for key, value in custom_payload.items():
            if key in {"operation_name", "input_hash", "output_hash", "recorded_at"}:
                # Audit metadata is gateway-owned; ignore handler attempts
                # to override.
                continue
            payload[key] = value
    conn.execute(
        """
        INSERT INTO authority_events (
            event_id,
            authority_domain_ref,
            aggregate_ref,
            event_type,
            event_payload,
            idempotency_key,
            operation_ref,
            receipt_id,
            emitted_by,
            correlation_id
        ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, $7, $8::uuid, $9, $10::uuid)
        """,
        event_id,
        authority_domain_ref,
        binding.operation_ref,
        event_type,
        json.dumps(payload, sort_keys=True, default=str),
        idempotency_key,
        binding.operation_ref,
        receipt_id,
        "authority_gateway",
        correlation_id,
    )
    return event_id


@contextmanager
def _operation_proof_transaction(conn: Any):
    transaction = getattr(conn, "transaction", None)
    if not callable(transaction):
        raise RuntimeError("operation receipt persistence requires conn.transaction()")
    with transaction() as tx_conn:
        yield tx_conn


def _attach_receipt_to_authority_events(
    conn: Any,
    *,
    receipt_id: str,
    event_ids: list[str],
    correlation_id: str | None = None,
) -> None:
    if not event_ids:
        return
    conn.execute(
        """
        UPDATE authority_events
           SET receipt_id = $1::uuid,
               correlation_id = COALESCE(correlation_id, $3::uuid)
         WHERE event_id = ANY($2::uuid[])
           AND receipt_id IS NULL
        """,
        receipt_id,
        event_ids,
        correlation_id,
    )


def _write_operation_proof(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    receipt: Mapping[str, Any],
    result_payload: Any,
    input_hash: str,
    output_hash: str | None,
    idempotency_key: str | None,
    result_status: str | None,
) -> dict[str, Any]:
    durable_receipt = dict(receipt)
    event_ids = list(durable_receipt.get("event_ids") or [])
    should_create_event = (
        durable_receipt.get("execution_status") == "completed"
        and binding.operation_kind == "command"
        and _binding_value(binding, "event_required", True)
        and not event_ids
    )
    if should_create_event:
        event_ids.append(str(uuid4()))
        durable_receipt["event_ids"] = event_ids

    # Conceptual events MAY carry a rich payload from the handler return.
    # When the result is a Mapping with an ``event_payload`` key, hoist
    # that dict onto the event row so the event itself carries decision-
    # relevant data (per architecture-policy::platform-architecture::
    # conceptual-events-register-through-operation-catalog-registry).
    custom_event_payload: Mapping[str, Any] | None = None
    if isinstance(result_payload, Mapping):
        candidate = result_payload.get("event_payload")
        if isinstance(candidate, Mapping):
            custom_event_payload = candidate

    if not _binding_value(binding, "receipt_required", True):
        raise RuntimeError(
            "operation receipt persistence requires receipt_required=true"
        )

    _insert_operation_receipt(conn, binding, receipt=durable_receipt, result_payload=result_payload)
    if durable_receipt.get("execution_status") == "completed" and event_ids:
        if should_create_event:
            _insert_authority_event(
                conn,
                binding,
                event_id=event_ids[0],
                receipt_id=str(durable_receipt["receipt_id"]),
                input_hash=input_hash,
                output_hash=output_hash or "",
                idempotency_key=idempotency_key,
                result_status=result_status,
                custom_payload=custom_event_payload,
                correlation_id=durable_receipt.get("correlation_id"),
            )
        else:
            _attach_receipt_to_authority_events(
                conn,
                receipt_id=str(durable_receipt["receipt_id"]),
                event_ids=event_ids,
                correlation_id=durable_receipt.get("correlation_id"),
            )
    return _fetch_persisted_operation_receipt(
        conn,
        binding,
        receipt_id=str(durable_receipt["receipt_id"]),
    )


def _prepare_call_context(
    caller_context: CallerContext | None,
) -> tuple[str, str, str | None, CallerContext]:
    """Resolve caller context and pre-mint receipt_id + correlation_id.

    Returns ``(receipt_id, correlation_id, cause_receipt_id, nested_context)``.

    ``nested_context`` is the context that nested gateway calls inherit
    from the ContextVar set around the handler invocation: its
    ``cause_receipt_id`` is the freshly-minted receipt_id (so children
    point at this receipt) and its ``correlation_id`` matches this
    receipt (so the whole tree shares the same correlation).

    When caller_context is None and the ContextVar is also empty, this
    is a root entry-point call: cause_receipt_id is None and a fresh
    correlation_id is minted.
    """

    effective = caller_context if caller_context is not None else CURRENT_CALLER_CONTEXT.get()
    receipt_id = str(uuid4())
    if effective is None:
        correlation_id = str(uuid4())
        cause_receipt_id: str | None = None
    else:
        correlation_id = effective.correlation_id
        cause_receipt_id = effective.cause_receipt_id
    nested_context = CallerContext(
        cause_receipt_id=receipt_id,
        correlation_id=correlation_id,
    )
    return receipt_id, correlation_id, cause_receipt_id, nested_context


def _persist_operation_outcome(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None,
    result: Any,
    input_hash: str,
    idempotency_key: str | None,
    started_ns: int,
    receipt_id: str,
    correlation_id: str,
    cause_receipt_id: str | None,
    execution_status: str = "completed",
    error_code: str | None = None,
    error_detail: str | None = None,
) -> dict[str, Any]:
    output_hash = None if result is None else _stable_hash(result)
    result_status = result.get("status") if isinstance(result, Mapping) else None
    normalized_result_status = str(result_status).strip() if isinstance(result_status, str) else None
    projection_freshness = _projection_freshness(result)
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    event_ids: list[str] = _result_authority_event_ids(result)
    receipt = _receipt_payload(
        binding,
        receipt_id=receipt_id,
        input_hash=input_hash,
        output_hash=output_hash,
        idempotency_key=idempotency_key,
        execution_status=execution_status,
        result_status=normalized_result_status,
        error_code=error_code,
        error_detail=error_detail,
        event_ids=event_ids,
        projection_freshness=projection_freshness,
        duration_ms=duration_ms,
        cause_receipt_id=cause_receipt_id,
        correlation_id=correlation_id,
    )
    with _operation_proof_transaction(conn) as proof_conn:
        receipt = _write_operation_proof(
            proof_conn,
            binding,
            receipt=receipt,
            result_payload=result,
            input_hash=input_hash,
            output_hash=output_hash,
            idempotency_key=idempotency_key,
            result_status=normalized_result_status,
        )
    return dict(receipt)


def _operation_receipt(
    binding: ResolvedHttpOperationBinding,
    *,
    result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    result_status = result.get("status") if isinstance(result, Mapping) else None
    return {
        "operation_ref": binding.operation_ref,
        "operation_name": binding.operation_name,
        "operation_kind": binding.operation_kind,
        "source_kind": binding.source_kind,
        "authority_ref": binding.authority_ref,
        "projection_ref": binding.projection_ref,
        "posture": binding.posture,
        "idempotency_policy": binding.idempotency_policy,
        "binding_revision": binding.binding_revision,
        "decision_ref": binding.decision_ref,
        "execution_status": "completed",
        "result_status": str(result_status).strip() if isinstance(result_status, str) else None,
    }


def _with_operation_receipt(
    binding: ResolvedHttpOperationBinding,
    result: Any,
    *,
    receipt: Mapping[str, Any] | None = None,
) -> Any:
    operation_receipt = dict(receipt) if isinstance(receipt, Mapping) else _operation_receipt(
        binding,
        result=result if isinstance(result, Mapping) else None,
    )
    drift_signal = _record_gateway_call_and_check_drift(binding.operation_ref)
    code_drift = _check_code_drift_signal()
    if isinstance(result, Mapping):
        payload = dict(result)
        if "ok" not in payload:
            payload["ok"] = True
        if binding.operation_kind == "query" and "results" not in payload:
            pass
        payload["operation_receipt"] = operation_receipt
        if drift_signal:
            payload["model_drift_signal"] = drift_signal
        if code_drift:
            payload["code_drift_signal"] = code_drift
        return payload

    if binding.operation_kind == "query" and isinstance(result, list):
        out = {
            "ok": True,
            "results": result,
            "operation_receipt": operation_receipt,
        }
        if drift_signal:
            out["model_drift_signal"] = drift_signal
        if code_drift:
            out["code_drift_signal"] = code_drift
        return out

    out = {
        "ok": True,
        "result": result,
        "operation_receipt": operation_receipt,
    }
    if drift_signal:
        out["model_drift_signal"] = drift_signal
    if code_drift:
        out["code_drift_signal"] = code_drift
    return out


async def _await_handler_result(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


def _run_awaitable_sync(result: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return run_sync_safe(result)
    raise RuntimeError(
        "operation.sync_execution_in_async_boundary: "
        "use aexecute_operation_binding() when invoking async operation handlers "
        "from an active event loop"
    )


async def aexecute_operation_binding(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
    subsystems: Any,
    idempotency_key_override: str | None = None,
    requested_mode: str | None = None,
    caller_context: CallerContext | None = None,
) -> Any:
    _assert_mode_admits_operation(binding, requested_mode=requested_mode)
    conn = subsystems.get_pg_conn()
    input_hash = _stable_hash(payload or {})
    idempotency_key = _idempotency_key(
        binding,
        payload=payload,
        input_hash=input_hash,
        idempotency_key_override=idempotency_key_override,
    )
    receipt_id, correlation_id, cause_receipt_id, nested_context = _prepare_call_context(
        caller_context
    )
    cached = _fetch_existing_idempotent_result(
        conn,
        operation_ref=binding.operation_ref,
        idempotency_key=idempotency_key,
        input_hash=input_hash,
    )
    if cached is not None:
        started_ns = time.monotonic_ns()
        result = _cached_result_body(cached)
        receipt = _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=result,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="replayed",
            receipt_id=receipt_id,
            correlation_id=correlation_id,
            cause_receipt_id=cause_receipt_id,
        )
        return _with_operation_receipt(binding, result, receipt=receipt)
    command = build_operation_command(binding, payload=payload)
    started_ns = time.monotonic_ns()
    token = CURRENT_CALLER_CONTEXT.set(nested_context)
    try:
        result = await _await_handler_result(binding.handler(command, subsystems))
    except Exception as exc:
        error_code = _error_code_for_exception(exc)
        error_details = _error_details_for_exception(exc)
        failure_receipt = _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=None,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="failed",
            error_code=error_code,
            error_detail=str(exc),
            receipt_id=receipt_id,
            correlation_id=correlation_id,
            cause_receipt_id=cause_receipt_id,
        )
        failure = {
            "ok": False,
            "error": str(exc),
            "error_code": error_code,
        }
        if error_details:
            failure["details"] = error_details
        return _with_operation_receipt(binding, failure, receipt=failure_receipt)
    finally:
        CURRENT_CALLER_CONTEXT.reset(token)
    receipt = _persist_operation_outcome(
        conn,
        binding,
        payload=payload,
        result=result,
        input_hash=input_hash,
        idempotency_key=idempotency_key,
        started_ns=started_ns,
        receipt_id=receipt_id,
        correlation_id=correlation_id,
        cause_receipt_id=cause_receipt_id,
    )
    return _with_operation_receipt(binding, result, receipt=receipt)


def execute_operation_binding(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
    subsystems: Any,
    idempotency_key_override: str | None = None,
    requested_mode: str | None = None,
    caller_context: CallerContext | None = None,
) -> Any:
    _assert_mode_admits_operation(binding, requested_mode=requested_mode)
    conn = subsystems.get_pg_conn()
    input_hash = _stable_hash(payload or {})
    idempotency_key = _idempotency_key(
        binding,
        payload=payload,
        input_hash=input_hash,
        idempotency_key_override=idempotency_key_override,
    )
    receipt_id, correlation_id, cause_receipt_id, nested_context = _prepare_call_context(
        caller_context
    )
    cached = _fetch_existing_idempotent_result(
        conn,
        operation_ref=binding.operation_ref,
        idempotency_key=idempotency_key,
        input_hash=input_hash,
    )
    if cached is not None:
        started_ns = time.monotonic_ns()
        result = _cached_result_body(cached)
        receipt = _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=result,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="replayed",
            receipt_id=receipt_id,
            correlation_id=correlation_id,
            cause_receipt_id=cause_receipt_id,
        )
        return _with_operation_receipt(binding, result, receipt=receipt)
    command = build_operation_command(binding, payload=payload)
    started_ns = time.monotonic_ns()
    token = CURRENT_CALLER_CONTEXT.set(nested_context)
    try:
        result = binding.handler(command, subsystems)
        if inspect.isawaitable(result):
            result = _run_awaitable_sync(result)
    except Exception as exc:
        error_code = _error_code_for_exception(exc)
        error_details = _error_details_for_exception(exc)
        failure_receipt = _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=None,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="failed",
            error_code=error_code,
            error_detail=str(exc),
            receipt_id=receipt_id,
            correlation_id=correlation_id,
            cause_receipt_id=cause_receipt_id,
        )
        failure = {
            "ok": False,
            "error": str(exc),
            "error_code": error_code,
        }
        if error_details:
            failure["details"] = error_details
        return _with_operation_receipt(binding, failure, receipt=failure_receipt)
    finally:
        CURRENT_CALLER_CONTEXT.reset(token)
    receipt = _persist_operation_outcome(
        conn,
        binding,
        payload=payload,
        result=result,
        input_hash=input_hash,
        idempotency_key=idempotency_key,
        started_ns=started_ns,
        receipt_id=receipt_id,
        correlation_id=correlation_id,
        cause_receipt_id=cause_receipt_id,
    )
    return _with_operation_receipt(binding, result, receipt=receipt)


async def aexecute_operation_from_subsystems(
    subsystems: Any,
    *,
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
    idempotency_key_override: str | None = None,
    requested_mode: str | None = None,
    caller_context: CallerContext | None = None,
) -> Any:
    if not hasattr(subsystems, "get_pg_conn") or not callable(subsystems.get_pg_conn):
        raise RuntimeError("operation execution requires subsystems.get_pg_conn()")
    binding = resolve_named_operation_binding(
        subsystems.get_pg_conn(),
        operation_name=operation_name,
    )
    return await aexecute_operation_binding(
        binding,
        payload=payload,
        subsystems=subsystems,
        idempotency_key_override=idempotency_key_override,
        requested_mode=requested_mode,
        caller_context=caller_context,
    )


def execute_operation_from_subsystems(
    subsystems: Any,
    *,
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
    idempotency_key_override: str | None = None,
    requested_mode: str | None = None,
    caller_context: CallerContext | None = None,
) -> Any:
    if not hasattr(subsystems, "get_pg_conn") or not callable(subsystems.get_pg_conn):
        raise RuntimeError("operation execution requires subsystems.get_pg_conn()")
    binding = resolve_named_operation_binding(
        subsystems.get_pg_conn(),
        operation_name=operation_name,
    )
    return execute_operation_binding(
        binding,
        payload=payload,
        subsystems=subsystems,
        idempotency_key_override=idempotency_key_override,
        requested_mode=requested_mode,
        caller_context=caller_context,
    )


def execute_operation_from_env(
    *,
    env: Mapping[str, str],
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    source = dict(env)
    conn = SyncPostgresConnection(get_workflow_pool(env=source))
    subsystems = _EnvironmentBackedSubsystems(env=source, conn=conn)
    return execute_operation_from_subsystems(
        subsystems,
        operation_name=operation_name,
        payload=payload,
    )


__all__ = [
    "AuthorityGateway",
    "CallerContext",
    "CURRENT_CALLER_CONTEXT",
    "OperationIdempotencyConflict",
    "OperationModeViolation",
    "aexecute_operation_binding",
    "aexecute_operation_from_subsystems",
    "build_operation_command",
    "caller_context_from_receipt",
    "current_caller_context",
    "execute_operation_binding",
    "execute_operation_from_env",
    "execute_operation_from_subsystems",
    "resolve_named_operation_binding",
    "spawn_threaded",
]
