"""Command execution handlers and event-emission helpers for the control-command bus.

This module contains:
- System-event helpers (_event_payload, _emit_system_event, _event_type_for_status)
- Per-command-type handlers (_workflow_submit, _workflow_chain_submit,
  _workflow_retry, _workflow_cancel, _sync_repair)
- The top-level dispatch router (_run_command_handler)

All handlers receive a live DB connection and a ControlCommandRecord; they
return a result_ref string on success and raise ControlCommandExecutionError
on failure.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

import runtime.post_workflow_sync as post_workflow_sync
from runtime._helpers import _json_compatible
from runtime.command_signatures import _json_dumps

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

_SYSTEM_EVENT_SOURCE_TYPE = "control_command"


# ---------------------------------------------------------------------------
# Deferred import — avoid circular dependency on import
# ---------------------------------------------------------------------------

def _resolve_unified_dispatch_attr(name: str) -> Any:
    # Import lazily so handlers.py can be imported without pulling in unified
    # dispatch at module load time.  This mirrors the proxy in control_commands.
    from runtime.control_commands import _resolve_unified_dispatch_attr as _real
    return _real(name)


# ---------------------------------------------------------------------------
# Event helpers
# ---------------------------------------------------------------------------

def _event_payload(
    command: Any,
    *,
    previous_status: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "command": command.to_json(),
        "transition": {
            "from": previous_status,
            "to": command.command_status,
        },
    }
    if extra:
        payload["extra"] = _json_compatible(dict(extra))
    return cast(dict[str, Any], _json_compatible(payload))


def _emit_system_event(
    conn: "SyncPostgresConnection",
    event_type: str,
    command: Any,
    *,
    previous_status: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> None:
    conn.execute(
        """INSERT INTO system_events (event_type, source_id, source_type, payload)
           VALUES ($1, $2, $3, $4::jsonb)""",
        event_type,
        command.command_id,
        _SYSTEM_EVENT_SOURCE_TYPE,
        _json_dumps(_event_payload(command, previous_status=previous_status, extra=extra)),
    )


def _event_type_for_status(status: str) -> str:
    from runtime.control_commands import ControlCommandError, ControlCommandStatus

    mapping = {
        ControlCommandStatus.REQUESTED.value: "control.command.requested",
        ControlCommandStatus.ACCEPTED.value: "control.command.accepted",
        ControlCommandStatus.REJECTED.value: "control.command.rejected",
        ControlCommandStatus.RUNNING.value: "control.command.started",
        ControlCommandStatus.SUCCEEDED.value: "control.command.completed",
        ControlCommandStatus.FAILED.value: "control.command.failed",
    }
    if status not in mapping:
        raise ControlCommandError(
            "control.command.invalid_status",
            f"unknown command status: {status}",
            details={"status": status},
        )
    return mapping[status]


# ---------------------------------------------------------------------------
# Payload access helper
# ---------------------------------------------------------------------------

def _command_payload_value(command: Any, key: str) -> Any:
    from runtime.control_commands import ControlCommandExecutionError

    if key not in command.payload:
        raise ControlCommandExecutionError(
            "control.command.missing_payload_field",
            f"{command.command_type} requires payload[{key!r}]",
            details={
                "command_id": command.command_id,
                "command_type": command.command_type,
                "field": key,
            },
        )
    return command.payload[key]


# ---------------------------------------------------------------------------
# result_ref helpers
# ---------------------------------------------------------------------------

def _workflow_run_id_from_result_ref(result_ref: str | None) -> str | None:
    if not result_ref or not isinstance(result_ref, str):
        return None
    if not result_ref.startswith("workflow_run:"):
        return None
    run_id = result_ref.split(":", 1)[1].strip()
    return run_id or None


def _workflow_chain_id_from_result_ref(result_ref: str | None) -> str | None:
    if not result_ref or not isinstance(result_ref, str):
        return None
    if not result_ref.startswith("workflow_chain:"):
        return None
    chain_id = result_ref.split(":", 1)[1].strip()
    return chain_id or None


# ---------------------------------------------------------------------------
# Per-command-type handlers
# ---------------------------------------------------------------------------

def _workflow_submit(conn: "SyncPostgresConnection", command: Any) -> str:
    from runtime.control_commands import ControlCommandExecutionError
    from runtime._helpers import _json_compatible as _jc  # noqa: F401 (unused but harmless)
    from runtime.control_commands import _normalize_text, _normalize_bool  # noqa: F401

    # Normalise helpers are still in control_commands for now — import lazily.
    from runtime.control_commands import _normalize_payload as _np, _normalize_text as _nt

    run_id = command.payload.get("run_id")
    if run_id is not None:
        run_id = _nt(run_id, field_name="payload.run_id")

    inline_spec_field = None
    inline_spec_payload = None
    if "inline_spec" in command.payload:
        inline_spec_field = "payload.inline_spec"
        inline_spec_payload = command.payload.get("inline_spec")
    elif "spec" in command.payload:
        inline_spec_field = "payload.spec"
        inline_spec_payload = command.payload.get("spec")

    has_spec_path = "spec_path" in command.payload
    has_inline_spec = inline_spec_payload is not None
    if has_spec_path == has_inline_spec:
        raise ControlCommandExecutionError(
            "control.command.workflow_submit_invalid_payload",
            "workflow.submit requires exactly one of payload.spec_path or payload.inline_spec/payload.spec",
            details={
                "command_id": command.command_id,
                "command_type": command.command_type,
                "has_spec_path": has_spec_path,
                "has_inline_spec": has_inline_spec,
            },
        )

    if has_inline_spec:
        inline_spec = _np(inline_spec_payload, field_name=str(inline_spec_field))
        try:
            result = _resolve_unified_dispatch_attr("submit_workflow_inline")(
                conn,
                inline_spec,
                run_id=run_id,
            )
        except Exception as exc:
            raise ControlCommandExecutionError(
                "control.command.workflow_submit_failed",
                str(exc),
                details={
                    "command_id": command.command_id,
                    "inline_spec_field": inline_spec_field,
                    "run_id": run_id,
                },
            ) from exc
    else:
        spec_path = _nt(
            _command_payload_value(command, "spec_path"),
            field_name="payload.spec_path",
        )
        repo_root = _nt(
            _command_payload_value(command, "repo_root"),
            field_name="payload.repo_root",
        )
        try:
            result = _resolve_unified_dispatch_attr("submit_workflow")(
                conn,
                spec_path,
                repo_root,
                run_id=run_id,
            )
        except Exception as exc:
            raise ControlCommandExecutionError(
                "control.command.workflow_submit_failed",
                str(exc),
                details={
                    "command_id": command.command_id,
                    "spec_path": spec_path,
                    "repo_root": repo_root,
                    "run_id": run_id,
                },
            ) from exc
    result_run_id = result.get("run_id")
    if not result_run_id:
        raise ControlCommandExecutionError(
            "control.command.workflow_submit_failed",
            "workflow.submit did not return a run_id",
            details={
                "command_id": command.command_id,
                "result": result,
            },
        )
    return f"workflow_run:{result_run_id}"


def _workflow_chain_submit(conn: "SyncPostgresConnection", command: Any) -> str:
    from runtime.control_commands import (
        ControlCommandExecutionError,
        _normalize_text as _nt,
        _normalize_bool as _nb,
    )

    coordination_path = _nt(
        _command_payload_value(command, "coordination_path"),
        field_name="payload.coordination_path",
    )
    repo_root = _nt(
        _command_payload_value(command, "repo_root"),
        field_name="payload.repo_root",
    )
    adopt_active = _nb(
        command.payload.get("adopt_active"),
        field_name="payload.adopt_active",
        default_value=True,
    )
    try:
        from runtime.workflow_chain import submit_workflow_chain

        chain_id = submit_workflow_chain(
            conn,
            coordination_path=coordination_path,
            repo_root=repo_root,
            requested_by_kind=command.requested_by_kind,
            requested_by_ref=command.requested_by_ref,
            adopt_active=adopt_active,
            command_id=command.command_id,
        )
    except Exception as exc:
        raise ControlCommandExecutionError(
            "control.command.workflow_chain_submit_failed",
            str(exc),
            details={
                "command_id": command.command_id,
                "coordination_path": coordination_path,
                "repo_root": repo_root,
                "adopt_active": adopt_active,
            },
        ) from exc
    if not chain_id:
        raise ControlCommandExecutionError(
            "control.command.workflow_chain_submit_failed",
            "workflow.chain.submit did not return a chain_id",
            details={
                "command_id": command.command_id,
                "coordination_path": coordination_path,
                "repo_root": repo_root,
                "adopt_active": adopt_active,
            },
        )
    return f"workflow_chain:{chain_id}"


def _workflow_retry(conn: "SyncPostgresConnection", command: Any) -> str:
    from runtime.control_commands import (
        ControlCommandExecutionError,
        _normalize_text as _nt,
    )

    run_id = _nt(
        _command_payload_value(command, "run_id"),
        field_name="payload.run_id",
    )
    label = _nt(
        _command_payload_value(command, "label"),
        field_name="payload.label",
    )
    result = _resolve_unified_dispatch_attr("retry_job")(conn, run_id, label)
    if result.get("error"):
        raise ControlCommandExecutionError(
            "control.command.workflow_retry_failed",
            str(result["error"]),
            details={
                "command_id": command.command_id,
                "run_id": run_id,
                "label": label,
                "result": result,
            },
        )
    return f"workflow_run:{run_id}"


def _workflow_cancel(conn: "SyncPostgresConnection", command: Any) -> str:
    from runtime.control_commands import (
        ControlCommandExecutionError,
        _normalize_text as _nt,
        _normalize_bool as _nb,
    )

    run_id = _nt(
        _command_payload_value(command, "run_id"),
        field_name="payload.run_id",
    )
    include_running = _nb(
        command.payload.get("include_running"),
        field_name="payload.include_running",
        default_value=True,
    )
    result = _resolve_unified_dispatch_attr("cancel_run")(
        conn,
        run_id,
        include_running=include_running,
    )
    cancelled_jobs = int(result.get("cancelled_jobs") or 0)
    run_status = str(result.get("run_status") or "")

    if cancelled_jobs < 1:
        raise ControlCommandExecutionError(
            "control.command.workflow_cancel_noop",
            "workflow cancel did not cancel any jobs",
            details={
                "command_id": command.command_id,
                "run_id": run_id,
                "include_running": include_running,
                "result": result,
            },
            result_ref=f"workflow_run:{run_id}",
        )
    if run_status != "cancelled":
        raise ControlCommandExecutionError(
            "control.command.workflow_cancel_incomplete",
            "workflow cancel did not reach the cancelled state",
            details={
                "command_id": command.command_id,
                "run_id": run_id,
                "include_running": include_running,
                "result": result,
            },
            result_ref=f"workflow_run:{run_id}",
        )
    return f"workflow_run:{run_id}"


def _sync_repair(conn: "SyncPostgresConnection", command: Any) -> str:
    from runtime.control_commands import (
        ControlCommandExecutionError,
        _normalize_text as _nt,
    )

    run_id = _nt(
        _command_payload_value(command, "run_id"),
        field_name="payload.run_id",
    )
    result = post_workflow_sync.repair_workflow_run_sync(run_id=run_id, conn=conn)
    if result.sync_status == "degraded":
        raise ControlCommandExecutionError(
            "control.command.sync_repair_degraded",
            "sync.repair completed with degraded status",
            details={
                "command_id": command.command_id,
                "run_id": run_id,
                "sync_status": result.sync_status,
                "sync_error_count": result.sync_error_count,
                "result": result.to_json(),
            },
            result_ref=f"workflow_run_sync_status:{result.run_id}",
        )
    return f"workflow_run_sync_status:{result.run_id}"


# ---------------------------------------------------------------------------
# Top-level dispatch
# ---------------------------------------------------------------------------

def _run_command_handler(
    conn: "SyncPostgresConnection",
    command: Any,
) -> str:
    from runtime.control_commands import (
        ControlCommandExecutionError,
        ControlCommandType,
    )

    handlers = {
        ControlCommandType.WORKFLOW_SUBMIT.value: _workflow_submit,
        ControlCommandType.WORKFLOW_CHAIN_SUBMIT.value: _workflow_chain_submit,
        ControlCommandType.WORKFLOW_RETRY.value: _workflow_retry,
        ControlCommandType.WORKFLOW_CANCEL.value: _workflow_cancel,
        ControlCommandType.SYNC_REPAIR.value: _sync_repair,
    }
    handler = handlers.get(command.command_type)
    if handler is None:
        raise ControlCommandExecutionError(
            "control.command.unknown_type",
            f"unsupported command type: {command.command_type}",
            details={"command_id": command.command_id, "command_type": command.command_type},
        )
    return handler(conn, command)


# ---------------------------------------------------------------------------
# Cancel proof + response rendering
# ---------------------------------------------------------------------------

def workflow_cancel_proof(
    conn: "SyncPostgresConnection",
    run_id: str,
) -> dict[str, Any]:
    """Return the operator-visible proof for a workflow cancel mutation."""
    from runtime.control_commands import _normalize_text as _nt

    normalized_run_id = _nt(run_id, field_name="run_id")
    status = _resolve_unified_dispatch_attr("get_run_status")(conn, normalized_run_id)
    if status is None:
        return {
            "cancelled_jobs": 0,
            "labels": [],
            "run_status": None,
            "terminal_reason": None,
        }

    cancelled_labels = [
        str(job.get("label") or "").strip()
        for job in status.get("jobs", [])
        if str(job.get("status") or "").strip() == "cancelled"
        and str(job.get("label") or "").strip()
    ]
    return {
        "cancelled_jobs": len(cancelled_labels),
        "labels": cancelled_labels,
        "run_status": str(status.get("status") or "unknown"),
        "terminal_reason": status.get("terminal_reason"),
    }


def _resolve_workflow_cancel_proof() -> Any:
    """Honor the canonical control_commands proof seam when present."""
    from runtime import control_commands as control_commands_mod

    return getattr(control_commands_mod, "workflow_cancel_proof", workflow_cancel_proof)


def render_control_command_response(
    conn: "SyncPostgresConnection | None",
    command: Any,
    *,
    action: str,
    run_id: str | None = None,
    label: str | None = None,
    spec_name: str | None = None,
    total_jobs: int | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Render the canonical operator-facing control-command response."""
    from runtime.control_commands import ControlCommandStatus

    command_json = command.to_json() if hasattr(command, "to_json") else dict(command)
    command_status = str(
        getattr(command, "command_status", command_json.get("command_status", ""))
    )
    command_id = str(getattr(command, "command_id", command_json.get("command_id", "")))
    error_code = getattr(command, "error_code", command_json.get("error_code"))
    error_detail = getattr(command, "error_detail", command_json.get("error_detail"))
    result_ref = getattr(command, "result_ref", command_json.get("result_ref"))
    effective_run_id = run_id or _workflow_run_id_from_result_ref(
        result_ref if isinstance(result_ref, str) else None
    )

    def _base_payload(status: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": status,
            "command_status": command_status,
            "command_id": command_id,
            "approval_required": command_status == ControlCommandStatus.REQUESTED.value,
        }
        if effective_run_id:
            payload["run_id"] = effective_run_id
            payload["stream_url"] = f"/api/workflow-runs/{effective_run_id}/stream"
            payload["status_url"] = f"/api/workflow-runs/{effective_run_id}/status"
        if label:
            payload["label"] = label
        if spec_name:
            payload["spec_name"] = spec_name
        if total_jobs is not None:
            payload["total_jobs"] = total_jobs
        if job_id is not None:
            payload["job_id"] = job_id
        if result_ref:
            payload["result_ref"] = result_ref
        return payload

    if command_status == ControlCommandStatus.FAILED.value:
        payload = _base_payload("failed")
        payload["error"] = str(error_detail or f"workflow {action} command failed")
        payload["error_code"] = error_code
        payload["error_detail"] = error_detail
        payload["command"] = command_json
        if action in {"cancel", "kill_if_idle"} and conn is not None and effective_run_id:
            proof = _resolve_workflow_cancel_proof()(conn, effective_run_id)
            payload.update(proof)
            payload["proof"] = proof
        return cast(dict[str, Any], _json_compatible(payload))

    if command_status == ControlCommandStatus.REQUESTED.value:
        return cast(
            dict[str, Any],
            _json_compatible(_base_payload("approval_required")),
        )

    if action == "run":
        status = "queued" if effective_run_id else command_status
    elif action == "retry":
        status = "requeued"
    elif action in {"cancel", "kill_if_idle"}:
        status = "cancelled"
    else:
        status = command_status

    payload = _base_payload(status)
    if action in {"cancel", "kill_if_idle"}:
        if conn is None or not effective_run_id:
            failure = _base_payload("failed")
            failure["error"] = "workflow cancel proof unavailable"
            failure["error_code"] = "control.command.workflow_cancel_proof_unavailable"
            failure["error_detail"] = "workflow cancel proof unavailable"
            failure["command"] = command_json
            return cast(dict[str, Any], _json_compatible(failure))

        proof = _resolve_workflow_cancel_proof()(conn, effective_run_id)
        payload.update(proof)
        if proof["cancelled_jobs"] < 1 or proof["run_status"] != "cancelled":
            failure = _base_payload("failed")
            failure["error"] = "workflow cancel did not reach the cancelled state"
            failure["error_code"] = "control.command.workflow_cancel_incomplete"
            failure["error_detail"] = "workflow cancel did not reach the cancelled state"
            failure["command"] = command_json
            failure.update(proof)
            failure["proof"] = proof
            return cast(dict[str, Any], _json_compatible(failure))

    return cast(dict[str, Any], _json_compatible(payload))


def render_control_command_failure(
    *,
    error_code: str,
    error_detail: str,
    run_id: str | None = None,
    label: str | None = None,
    spec_name: str | None = None,
    total_jobs: int | None = None,
    job_id: str | None = None,
    command_id: str | None = None,
    result_ref: str | None = None,
    proof: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Render a canonical failed control-command response before or without execution."""
    from runtime.control_commands import ControlCommandStatus

    payload: dict[str, Any] = {
        "status": "failed",
        "command_status": ControlCommandStatus.FAILED.value,
        "approval_required": False,
        "error": error_detail,
        "error_code": error_code,
        "error_detail": error_detail,
    }
    if command_id is not None:
        payload["command_id"] = command_id
    if run_id:
        payload["run_id"] = run_id
        payload["stream_url"] = f"/api/workflow-runs/{run_id}/stream"
        payload["status_url"] = f"/api/workflow-runs/{run_id}/status"
    if label:
        payload["label"] = label
    if spec_name:
        payload["spec_name"] = spec_name
    if total_jobs is not None:
        payload["total_jobs"] = total_jobs
    if job_id is not None:
        payload["job_id"] = job_id
    if result_ref:
        payload["result_ref"] = result_ref
    if proof is not None:
        proof_json = cast(dict[str, Any], _json_compatible(dict(proof)))
        payload.update(proof_json)
        payload["proof"] = proof_json
    if details is not None:
        payload["details"] = _json_compatible(dict(details))
    return cast(dict[str, Any], _json_compatible(payload))


# ---------------------------------------------------------------------------
# Request convenience builders
# ---------------------------------------------------------------------------

def request_workflow_submit_command(
    conn: "SyncPostgresConnection",
    *,
    requested_by_kind: str,
    requested_by_ref: str,
    spec_path: str | None = None,
    repo_root: str | None = None,
    inline_spec: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    idempotency_key: str | None = None,
    command_id: str | None = None,
    requested_at: Any = None,
) -> Any:
    """Create and auto-execute one durable workflow.submit command."""
    import uuid as _uuid
    from runtime.control_commands import (
        ControlCommandError,
        ControlCommandType,
        ControlIntent,
        _normalize_payload as _np,
        _normalize_text as _nt,
        request_control_command,
    )

    n_kind = _nt(requested_by_kind, field_name="requested_by_kind")
    n_ref = _nt(requested_by_ref, field_name="requested_by_ref")
    n_run_id = None if run_id is None else _nt(run_id, field_name="run_id")
    n_ikey = None if idempotency_key is None else _nt(idempotency_key, field_name="idempotency_key")
    has_spec_path = spec_path is not None
    has_inline_spec = inline_spec is not None

    if has_spec_path == has_inline_spec:
        raise ControlCommandError(
            "control.command.invalid_value",
            "request_workflow_submit_command requires exactly one of spec_path or inline_spec",
            details={
                "spec_path_provided": has_spec_path,
                "inline_spec_provided": has_inline_spec,
            },
        )

    _payload: dict[str, Any]
    if has_inline_spec:
        _payload = {"inline_spec": _np(inline_spec, field_name="inline_spec")}
        if repo_root is not None:
            _payload["repo_root"] = _nt(repo_root, field_name="repo_root")
    else:
        n_spec = _nt(spec_path, field_name="spec_path")
        n_root = _nt(repo_root, field_name="repo_root")
        _payload = {"spec_path": n_spec, "repo_root": n_root}
    if n_run_id is not None:
        _payload["run_id"] = n_run_id

    intent = ControlIntent(
        command_type=ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind=n_kind,
        requested_by_ref=n_ref,
        idempotency_key=n_ikey or f"workflow.submit.{n_kind}.{_uuid.uuid4().hex}",
        payload=_payload,
    )
    return request_control_command(conn, intent, command_id=command_id, requested_at=requested_at)


def request_workflow_chain_submit_command(
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
) -> Any:
    """Create and auto-execute one durable workflow.chain.submit command."""
    import uuid as _uuid
    from runtime.control_commands import (
        ControlCommandType,
        ControlIntent,
        _normalize_text as _nt,
        _normalize_bool as _nb,
        request_control_command,
    )

    n_kind = _nt(requested_by_kind, field_name="requested_by_kind")
    n_ref = _nt(requested_by_ref, field_name="requested_by_ref")
    n_coord = _nt(coordination_path, field_name="coordination_path")
    n_root = _nt(repo_root, field_name="repo_root")
    n_adopt = _nb(adopt_active, field_name="adopt_active", default_value=True)
    n_ikey = None if idempotency_key is None else _nt(idempotency_key, field_name="idempotency_key")

    intent = ControlIntent(
        command_type=ControlCommandType.WORKFLOW_CHAIN_SUBMIT,
        requested_by_kind=n_kind,
        requested_by_ref=n_ref,
        idempotency_key=n_ikey or f"workflow.chain.submit.{n_kind}.{_uuid.uuid4().hex}",
        payload={
            "coordination_path": n_coord,
            "repo_root": n_root,
            "adopt_active": n_adopt,
        },
    )
    return request_control_command(conn, intent, command_id=command_id, requested_at=requested_at)


def render_workflow_submit_response(
    command: Any,
    *,
    spec_name: str | None = None,
    total_jobs: int | None = None,
) -> dict[str, Any]:
    """Render the canonical queued-run response for workflow.submit surfaces."""

    command_json = command.to_json() if hasattr(command, "to_json") else dict(command)
    payload = render_control_command_response(
        None,
        command,
        action="run",
        spec_name=spec_name,
        total_jobs=total_jobs,
    )
    if payload.get("status") in {"failed", "approval_required"}:
        return payload
    if payload.get("run_id"):
        return payload

    failed_payload: dict[str, Any] = {
        "status": "failed",
        "command_status": str(command_json.get("command_status") or "failed"),
        "approval_required": False,
        "error": "workflow submit command did not produce a workflow run",
        "error_code": "control.command.workflow_submit_missing_run_id",
        "error_detail": "workflow submit command did not produce a workflow run",
        "command": command_json,
    }
    cmd_id = command_json.get("command_id")
    if cmd_id:
        failed_payload["command_id"] = str(cmd_id)
    result_ref = command_json.get("result_ref")
    if result_ref:
        failed_payload["result_ref"] = str(result_ref)
    if spec_name is not None:
        failed_payload["spec_name"] = spec_name
    if total_jobs is not None:
        failed_payload["total_jobs"] = total_jobs
    return cast(dict[str, Any], _json_compatible(failed_payload))


def render_workflow_chain_submit_response(
    conn: "SyncPostgresConnection",
    command: Any,
    *,
    coordination_path: str | None = None,
) -> dict[str, Any]:
    """Render the canonical durable-chain response for workflow.chain.submit."""
    from runtime.control_commands import ControlCommandStatus

    command_json = command.to_json() if hasattr(command, "to_json") else dict(command)
    command_status = str(
        getattr(command, "command_status", command_json.get("command_status", ""))
    )
    command_id = str(getattr(command, "command_id", command_json.get("command_id", "")))
    result_ref = getattr(command, "result_ref", command_json.get("result_ref"))

    if command_status == ControlCommandStatus.FAILED.value:
        payload = render_control_command_response(conn, command, action="workflow_chain_submit")
        payload["command"] = command_json
        return payload

    if command_status == ControlCommandStatus.REQUESTED.value:
        return {
            "status": "approval_required",
            "command_status": command_status,
            "command_id": command_id,
            "approval_required": True,
            "coordination_path": coordination_path,
            "result_ref": result_ref,
        }

    chain_id = _workflow_chain_id_from_result_ref(
        result_ref if isinstance(result_ref, str) else None
    )
    if not chain_id:
        failed_payload = render_control_command_failure(
            error_code="control.command.workflow_chain_submit_missing_chain_id",
            error_detail="workflow chain submit command did not produce a workflow chain",
            command_id=command_id,
            result_ref=str(result_ref) if result_ref else None,
        )
        failed_payload["command"] = command_json
        if coordination_path is not None:
            failed_payload["coordination_path"] = coordination_path
        return failed_payload

    from runtime.workflow_chain import get_workflow_chain_status

    state = get_workflow_chain_status(conn, chain_id)
    if state is None:
        failed_payload = render_control_command_failure(
            error_code="control.command.workflow_chain_submit_missing_state",
            error_detail=f"workflow chain state not found: {chain_id}",
            command_id=command_id,
            result_ref=str(result_ref) if result_ref else None,
        )
        failed_payload["command"] = command_json
        if coordination_path is not None:
            failed_payload["coordination_path"] = coordination_path
        return failed_payload

    waves = state.get("waves", [])
    waves_total = len(waves) if isinstance(waves, list) else 0
    waves_completed = sum(
        1
        for wave in (waves if isinstance(waves, list) else [])
        if str(wave.get("status") or "") == "succeeded"
    )
    _payload: dict[str, Any] = {
        "status": str(state.get("status") or "queued"),
        "command_status": command_status,
        "command_id": command_id,
        "approval_required": False,
        "chain_id": chain_id,
        "program": state.get("program"),
        "coordination_path": coordination_path or state.get("coordination_path"),
        "current_wave": state.get("current_wave"),
        "waves_total": waves_total,
        "waves_completed": waves_completed,
        "result_ref": result_ref,
    }
    return cast(dict[str, Any], _json_compatible(_payload))
