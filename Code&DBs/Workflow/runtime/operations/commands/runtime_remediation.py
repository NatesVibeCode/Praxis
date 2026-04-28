"""CQRS command for guarded runtime remediation.

This is deliberately not a retry surface. It applies only local authority
repairs whose safety can be proven from the failure type, then returns the
retry delta a caller must present before retrying one job.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.runtime_truth import (
    build_remediation_plan,
    classify_runtime_failure,
    remediation_plan_for_failure,
)


_BLOCKER_TO_FAILURE_TYPE = {
    "context_not_hydrated": "context_not_hydrated",
    "provider_slots_stale": "provider.capacity",
    "provider_capacity": "provider.capacity",
    "expired_host_resource_leases": "host_resource_admission_unavailable",
    "docker_unavailable": "sandbox_error",
    "db_authority_unavailable": "db_pool_pressure",
    "queued_without_fresh_worker_heartbeat": "host_resource_admission_unavailable",
}


class RuntimeRemediationApplyCommand(BaseModel):
    """Input contract for ``operator.remediation_apply``."""

    failure_type: str | None = None
    failure_code: str | None = None
    blocker_code: str | None = None
    stderr: str | None = Field(default=None, max_length=4000)
    run_id: str | None = None
    provider_slug: str | None = None
    stale_after_seconds: int = Field(default=600, ge=1, le=24 * 60 * 60)
    dry_run: bool = True
    confirm: bool = False

    @field_validator("failure_type", "failure_code", "blocker_code", "stderr", "run_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text fields must be non-empty strings when provided")
        return value.strip()

    @field_validator("provider_slug", mode="before")
    @classmethod
    def _normalize_provider_slug(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider_slug must be a non-empty string when provided")
        return value.strip().lower()


def _rows(conn: Any, query: str, *args: Any) -> tuple[list[dict[str, Any]], str | None]:
    if conn is None or not hasattr(conn, "execute"):
        return [], "pg connection unavailable"
    try:
        raw_rows = conn.execute(query, *args)
    except Exception as exc:  # noqa: BLE001 - remediation result must explain failure
        return [], f"{type(exc).__name__}: {exc}"
    rows: list[dict[str, Any]] = []
    for row in raw_rows or []:
        if isinstance(row, Mapping):
            rows.append(dict(row))
        elif hasattr(row, "items"):
            rows.append(dict(row.items()))
        else:
            rows.append({"value": row})
    return rows, None


def _resolve_failure_type(command: RuntimeRemediationApplyCommand, conn: Any) -> str:
    if command.failure_type:
        return command.failure_type
    if command.blocker_code:
        mapped = _BLOCKER_TO_FAILURE_TYPE.get(command.blocker_code)
        if mapped:
            return mapped
    if command.run_id and not command.failure_code:
        plan = build_remediation_plan(conn, run_id=command.run_id)
        if isinstance(plan, Mapping) and plan.get("failure_type"):
            return str(plan["failure_type"])
    return classify_runtime_failure(
        failure_code=command.failure_code,
        stderr=command.stderr,
        outputs={},
    )


def _action(
    *,
    action: str,
    status: str,
    reason: str,
    rows: list[dict[str, Any]] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"action": action, "status": status, "reason": reason}
    if rows is not None:
        payload["rows"] = rows
        payload["row_count"] = len(rows)
    if error:
        payload["error"] = error
    return payload


def _remediate_provider_capacity(
    conn: Any,
    command: RuntimeRemediationApplyCommand,
    *,
    apply: bool,
) -> dict[str, Any]:
    if not apply:
        return _action(
            action="reap_stale_provider_slots",
            status="planned",
            reason="Would clear provider slots older than stale_after_seconds.",
        )
    if command.provider_slug:
        rows, error = _rows(
            conn,
            """
            UPDATE provider_concurrency
               SET active_slots = 0.0,
                   updated_at = NOW()
             WHERE provider_slug = $1
               AND active_slots > 0
               AND updated_at < NOW() - ($2 || ' seconds')::INTERVAL
             RETURNING provider_slug, active_slots, max_concurrent, updated_at
            """,
            command.provider_slug,
            str(command.stale_after_seconds),
        )
    else:
        rows, error = _rows(
            conn,
            """
            UPDATE provider_concurrency
               SET active_slots = 0.0,
                   updated_at = NOW()
             WHERE active_slots > 0
               AND updated_at < NOW() - ($1 || ' seconds')::INTERVAL
             RETURNING provider_slug, active_slots, max_concurrent, updated_at
            """,
            str(command.stale_after_seconds),
        )
    return _action(
        action="reap_stale_provider_slots",
        status="failed" if error else "applied",
        reason="Cleared stale provider slot counters through provider_concurrency authority.",
        rows=rows,
        error=error,
    )


def _remediate_host_resource_leases(
    conn: Any,
    *,
    apply: bool,
) -> dict[str, Any]:
    if not apply:
        return _action(
            action="reap_expired_host_resource_leases",
            status="planned",
            reason="Would delete expired host-resource leases.",
        )
    rows, error = _rows(
        conn,
        """
        DELETE FROM execution_leases
         WHERE resource_key LIKE 'host_resource:%'
           AND expires_at <= NOW()
         RETURNING lease_id, holder_id, resource_key, expires_at
        """,
    )
    return _action(
        action="reap_expired_host_resource_leases",
        status="failed" if error else "applied",
        reason="Deleted expired host-resource leases through execution_leases authority.",
        rows=rows,
        error=error,
    )


def handle_runtime_remediation_apply(
    command: RuntimeRemediationApplyCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    failure_type = _resolve_failure_type(command, conn)
    plan = remediation_plan_for_failure(failure_type)
    apply = bool(command.confirm and not command.dry_run)

    actions: list[dict[str, Any]]
    if plan.get("tier") == "human_gated":
        actions = [
            _action(
                action=str(plan.get("action") or "manual_review"),
                status="blocked",
                reason="Plan is human-gated; automatic remediation is refused.",
            )
        ]
    elif failure_type == "provider.capacity":
        actions = [_remediate_provider_capacity(conn, command, apply=apply)]
    elif failure_type in {"host_resource_capacity", "host_resource_admission_unavailable"}:
        actions = [_remediate_host_resource_leases(conn, apply=apply)]
    else:
        actions = [
            _action(
                action=str(plan.get("action") or "inspect_receipt_before_retry"),
                status="not_automatic",
                reason="This failure type is typed, but the repair is code-owned or diagnostic-only.",
            )
        ]

    failed = any(item.get("status") == "failed" for item in actions)
    blocked = any(item.get("status") == "blocked" for item in actions)
    applied = any(item.get("status") == "applied" for item in actions)
    status = "failed" if failed else ("blocked" if blocked else ("applied" if applied else "planned"))
    writes = []
    if applied:
        action_names = {str(item.get("action") or "") for item in actions}
        if "reap_stale_provider_slots" in action_names:
            writes.append("provider_concurrency")
        if "reap_expired_host_resource_leases" in action_names:
            writes.append("execution_leases")
    payload = {
        "ok": not failed,
        "view": "runtime_remediation_apply",
        "status": status,
        "applied": applied,
        "dry_run": command.dry_run,
        "confirm": command.confirm,
        "failure_type": failure_type,
        "failure_code": command.failure_code,
        "blocker_code": command.blocker_code,
        "run_id": command.run_id,
        "plan": plan,
        "actions": actions,
        "retry_delta_required": plan.get("retry_delta_required"),
        "authority": {
            "operation_name": "operator.remediation_apply",
            "event_type": "runtime.remediation.applied",
            "writes": writes,
        },
    }
    payload["event_payload"] = {
        "status": status,
        "applied": applied,
        "failure_type": failure_type,
        "failure_code": command.failure_code,
        "blocker_code": command.blocker_code,
        "run_id": command.run_id,
        "actions": actions,
        "retry_delta_required": plan.get("retry_delta_required"),
        "source_refs": [
            "operation.operator.remediation_plan",
            "table.provider_concurrency",
            "table.execution_leases",
        ],
    }
    return payload


__all__ = [
    "RuntimeRemediationApplyCommand",
    "handle_runtime_remediation_apply",
]
