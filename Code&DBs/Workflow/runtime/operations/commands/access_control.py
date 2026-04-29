"""Gateway-dispatched mutator for the model-access control panel.

The control-panel data model (migrations 267 + 269) gave us the denial table
``private_provider_model_access_denials`` and the projection that surfaces
denials through ``private_model_access_control_matrix`` plus the routing-side
roll-past in ``runtime.workflow._routing``. This module is the missing
write surface — the seam that turns "first class checkbox" from a data
model into a callable operation.

A denial row matches a ``(runtime_profile_ref, job_type, transport_type,
adapter_type, provider_slug, model_slug)`` tuple. Any field can be ``*`` to
mean "all". Insert one row to disable a column of the matrix; delete the row
to re-enable it.

Actions:
    list    — return the current denial set, optionally filtered by selector
    disable — upsert a denial row (denied=TRUE) and refresh the projection
    enable  — delete the matching denial row and refresh the projection

Refresh runs through ``refresh_private_provider_job_catalog(runtime_profile_ref)``
so the matrix view, work-assignment matrix, and route admission catalog all
see the change without waiting on a deferred refresh.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


_DEFAULT_OPERATOR_MESSAGE = (
    "this Model Access method has been turned off on purpose at the "
    "control panel either for this specific task type, or more broadly, "
    "consult the control panel and do not turn it on without confirming "
    "with the user even if you think that will help you complete your task."
)

_VALID_TRANSPORTS = {"*", "CLI", "API"}
_VALID_ACTIONS = {"list", "disable", "enable"}


class AccessControlCommand(BaseModel):
    action: str = "list"
    runtime_profile_ref: str = "praxis"
    job_type: str = "*"
    transport_type: str = "*"
    adapter_type: str = "*"
    provider_slug: str = "*"
    model_slug: str = "*"
    decision_ref: str | None = None
    operator_message: str | None = None
    reason_code: str = "control_panel.model_access_method_turned_off"
    limit: int = Field(default=200, ge=1, le=1000)

    @field_validator("action")
    @classmethod
    def _check_action(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in _VALID_ACTIONS:
            raise ValueError(
                f"action must be one of {sorted(_VALID_ACTIONS)}; got {value!r}"
            )
        return normalized

    @field_validator("transport_type")
    @classmethod
    def _check_transport(cls, value: str) -> str:
        normalized = (value or "*").strip()
        if normalized not in _VALID_TRANSPORTS:
            raise ValueError(
                f"transport_type must be one of {sorted(_VALID_TRANSPORTS)}; got {value!r}"
            )
        return normalized

    @field_validator(
        "runtime_profile_ref",
        "job_type",
        "adapter_type",
        "provider_slug",
        "model_slug",
        "reason_code",
    )
    @classmethod
    def _strip_nonempty(cls, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("must be a non-empty string (use '*' for wildcard)")
        return cleaned


def _selector_tuple(command: AccessControlCommand) -> tuple[str, str, str, str, str, str]:
    return (
        command.runtime_profile_ref,
        command.job_type,
        command.transport_type,
        command.adapter_type,
        command.provider_slug,
        command.model_slug,
    )


def _list_denials(conn: Any, command: AccessControlCommand) -> list[dict[str, Any]]:
    sql = """
        SELECT runtime_profile_ref, job_type, transport_type, adapter_type,
               provider_slug, model_slug, denied, reason_code, operator_message,
               decision_ref, created_at, updated_at
          FROM private_provider_model_access_denials
         WHERE denied = TRUE
           AND runtime_profile_ref = $1
           AND ($2 = '*' OR job_type = $2)
           AND ($3 = '*' OR transport_type = $3)
           AND ($4 = '*' OR adapter_type = $4)
           AND ($5 = '*' OR provider_slug = $5)
           AND ($6 = '*' OR model_slug = $6)
         ORDER BY transport_type, provider_slug, job_type, model_slug
         LIMIT $7
    """
    rows = conn.fetch(
        sql,
        command.runtime_profile_ref,
        command.job_type,
        command.transport_type,
        command.adapter_type,
        command.provider_slug,
        command.model_slug,
        command.limit,
    )
    return [dict(row) for row in rows]


def _refresh_projection(conn: Any, runtime_profile_ref: str) -> None:
    conn.execute(
        "SELECT refresh_private_provider_job_catalog($1)",
        runtime_profile_ref,
    )


def _upsert_denial(conn: Any, command: AccessControlCommand) -> dict[str, Any]:
    if not command.decision_ref:
        raise ValueError("decision_ref is required for action='disable'")
    sql = """
        INSERT INTO private_provider_model_access_denials (
            runtime_profile_ref, job_type, transport_type, adapter_type,
            provider_slug, model_slug, denied, reason_code, operator_message,
            decision_ref, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, $8, $9, now())
        ON CONFLICT (runtime_profile_ref, job_type, transport_type,
                     adapter_type, provider_slug, model_slug)
        DO UPDATE SET
            denied = TRUE,
            reason_code = EXCLUDED.reason_code,
            operator_message = EXCLUDED.operator_message,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        RETURNING runtime_profile_ref, job_type, transport_type, adapter_type,
                  provider_slug, model_slug, denied, reason_code,
                  operator_message, decision_ref, created_at, updated_at
    """
    row = conn.fetchrow(
        sql,
        command.runtime_profile_ref,
        command.job_type,
        command.transport_type,
        command.adapter_type,
        command.provider_slug,
        command.model_slug,
        command.reason_code,
        command.operator_message or _DEFAULT_OPERATOR_MESSAGE,
        command.decision_ref,
    )
    return dict(row) if row is not None else {}


def _delete_denial(conn: Any, command: AccessControlCommand) -> int:
    sql = """
        WITH deleted AS (
            DELETE FROM private_provider_model_access_denials
             WHERE runtime_profile_ref = $1
               AND job_type = $2
               AND transport_type = $3
               AND adapter_type = $4
               AND provider_slug = $5
               AND model_slug = $6
             RETURNING 1
        )
        SELECT count(*) AS deleted_count FROM deleted
    """
    rows = conn.execute(
        sql,
        command.runtime_profile_ref,
        command.job_type,
        command.transport_type,
        command.adapter_type,
        command.provider_slug,
        command.model_slug,
    )
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, dict):
            return int(first.get("deleted_count") or 0)
        getter = getattr(first, "get", None)
        if callable(getter):
            return int(getter("deleted_count") or 0)
    return 0


def _find_transport_wildcard_denial(
    conn: Any,
    command: AccessControlCommand,
) -> dict[str, Any] | None:
    if command.transport_type not in {"CLI", "API"}:
        return None
    row = conn.fetchrow(
        """
        SELECT runtime_profile_ref, job_type, transport_type, adapter_type,
               provider_slug, model_slug, denied, reason_code, operator_message,
               decision_ref, created_at, updated_at
          FROM private_provider_model_access_denials
         WHERE denied = TRUE
           AND runtime_profile_ref = $1
           AND job_type = $2
           AND transport_type = '*'
           AND adapter_type = $3
           AND provider_slug = $4
           AND model_slug = $5
         LIMIT 1
        """,
        command.runtime_profile_ref,
        command.job_type,
        command.adapter_type,
        command.provider_slug,
        command.model_slug,
    )
    return dict(row) if row is not None else None


def _transport_residual(transport_type: str) -> str | None:
    if transport_type == "CLI":
        return "API"
    if transport_type == "API":
        return "CLI"
    return None


def _rewrite_transport_wildcard_denial(
    conn: Any,
    command: AccessControlCommand,
    source_row: dict[str, Any],
) -> dict[str, Any] | None:
    deleted = _delete_denial(
        conn,
        AccessControlCommand(
            action="enable",
            runtime_profile_ref=str(source_row["runtime_profile_ref"]),
            job_type=str(source_row["job_type"]),
            transport_type=str(source_row["transport_type"]),
            adapter_type=str(source_row["adapter_type"]),
            provider_slug=str(source_row["provider_slug"]),
            model_slug=str(source_row["model_slug"]),
        ),
    )
    if deleted <= 0:
        return None

    residual_transport = _transport_residual(command.transport_type)
    if residual_transport is None:
        return None

    replacement = _upsert_denial(
        conn,
        AccessControlCommand(
            action="disable",
            runtime_profile_ref=str(source_row["runtime_profile_ref"]),
            job_type=str(source_row["job_type"]),
            transport_type=residual_transport,
            adapter_type=str(source_row["adapter_type"]),
            provider_slug=str(source_row["provider_slug"]),
            model_slug=str(source_row["model_slug"]),
            decision_ref=str(source_row["decision_ref"]),
            operator_message=str(source_row["operator_message"] or _DEFAULT_OPERATOR_MESSAGE),
            reason_code=str(source_row["reason_code"] or "control_panel.model_access_method_turned_off"),
        ),
    )
    return {
        "deleted_count": deleted,
        "replacement_row": replacement,
        "rewritten_from_transport": str(source_row["transport_type"]),
        "enabled_transport": command.transport_type,
    }


def handle_access_control(
    command: AccessControlCommand, subsystems: Any
) -> dict[str, Any]:
    """Route disable/enable/list through the gateway.

    The gateway wraps this call in an authority operation receipt; commands
    additionally write an ``access_control.denial.changed`` event on
    completion (registered alongside the operation in migration 280).
    """

    conn = subsystems.get_pg_conn()
    if command.action == "list":
        rows = _list_denials(conn, command)
        return {"ok": True, "action": "list", "rows": rows, "count": len(rows)}

    if command.action == "disable":
        row = _upsert_denial(conn, command)
        _refresh_projection(conn, command.runtime_profile_ref)
        return {
            "ok": True,
            "action": "disable",
            "row": row,
            "event_payload": {
                "runtime_profile_ref": command.runtime_profile_ref,
                "selector": {
                    "job_type": command.job_type,
                    "transport_type": command.transport_type,
                    "adapter_type": command.adapter_type,
                    "provider_slug": command.provider_slug,
                    "model_slug": command.model_slug,
                },
                "denied": True,
                "decision_ref": command.decision_ref,
            },
        }

    deleted = _delete_denial(conn, command)
    rewrite = None
    if deleted <= 0:
        transport_wildcard_row = _find_transport_wildcard_denial(conn, command)
        if transport_wildcard_row is not None:
            rewrite = _rewrite_transport_wildcard_denial(
                conn,
                command,
                transport_wildcard_row,
            )
            if rewrite is not None:
                deleted = int(rewrite.get("deleted_count") or 0)
    _refresh_projection(conn, command.runtime_profile_ref)
    return {
        "ok": True,
        "action": "enable",
        "deleted_count": deleted,
        "rewrite": rewrite,
        "event_payload": {
            "runtime_profile_ref": command.runtime_profile_ref,
            "selector": {
                "job_type": command.job_type,
                "transport_type": command.transport_type,
                "adapter_type": command.adapter_type,
                "provider_slug": command.provider_slug,
                "model_slug": command.model_slug,
            },
            "denied": False,
        },
    }


__all__ = [
    "AccessControlCommand",
    "handle_access_control",
]
