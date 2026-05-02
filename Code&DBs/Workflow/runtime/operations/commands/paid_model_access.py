"""CQRS command for paid-model soft-off state and one-run leases."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from runtime.paid_model_access import (
    PAID_MODEL_POLICY_DECISION_REF,
    bind_paid_model_leases_to_run,
    grant_paid_model_lease,
    require_exact_paid_selector,
)


_VALID_ACTIONS = {
    "preview",
    "status",
    "grant_once",
    "bind_run",
    "revoke",
    "consume",
    "soft_off",
    "soft_on",
}
_VALID_TRANSPORTS = {"CLI", "API"}


class PaidModelAccessCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str = "status"
    selector: dict[str, Any] | None = None
    runtime_profile_ref: str = "praxis"
    job_type: str | None = None
    transport_type: str | None = None
    adapter_type: str | None = None
    provider_slug: str | None = None
    model_slug: str | None = None
    approval_ref: str | None = None
    approved_by: str | None = None
    approval_note: str | None = None
    proposal_hash: str | None = None
    lease_id: str | None = None
    run_id: str | None = None
    expires_at: str | None = None
    ttl_minutes: int = Field(default=30, ge=1, le=24 * 60)
    cost_posture: dict[str, Any] = Field(default_factory=dict)
    route_truth_ref: str | None = None
    decision_ref: str | None = None
    reason_code: str = "paid_model.presentation_soft_off"
    operator_message: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)

    @field_validator("action")
    @classmethod
    def _normalize_action(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in _VALID_ACTIONS:
            raise ValueError(f"action must be one of {sorted(_VALID_ACTIONS)}")
        return normalized

    @field_validator("runtime_profile_ref")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("runtime_profile_ref is required")
        return cleaned

    @field_validator("transport_type", mode="before")
    @classmethod
    def _normalize_transport_type(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        normalized = str(value).strip().upper()
        if normalized not in _VALID_TRANSPORTS:
            raise ValueError("transport_type must be CLI or API")
        return normalized

    @field_validator(
        "job_type",
        "adapter_type",
        "provider_slug",
        "model_slug",
        "approval_ref",
        "approved_by",
        "approval_note",
        "proposal_hash",
        "lease_id",
        "run_id",
        "expires_at",
        "route_truth_ref",
        "decision_ref",
        "reason_code",
        "operator_message",
        mode="before",
    )
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value in (None, ""):
            return None
        cleaned = str(value).strip()
        return cleaned or None


def _selector(command: PaidModelAccessCommand) -> dict[str, Any]:
    nested = dict(command.selector or {})
    explicit_fields = command.model_fields_set
    runtime_profile_ref = (
        command.runtime_profile_ref
        if "runtime_profile_ref" in explicit_fields
        else str(nested.get("runtime_profile_ref") or "").strip()
    ) or "praxis"
    job_type = (
        command.job_type
        if "job_type" in explicit_fields
        else str(nested.get("job_type") or "").strip()
    )
    transport_type = (
        command.transport_type
        if "transport_type" in explicit_fields
        else str(nested.get("transport_type") or "").strip()
    ) or "API"
    transport_type = transport_type.upper()
    if transport_type not in _VALID_TRANSPORTS:
        raise ValueError("transport_type must be CLI or API")
    adapter_type = (
        command.adapter_type
        if "adapter_type" in explicit_fields
        else str(nested.get("adapter_type") or "").strip()
    ) or ("cli_llm" if transport_type == "CLI" else "llm_task")
    provider_slug = (
        command.provider_slug
        if "provider_slug" in explicit_fields
        else str(nested.get("provider_slug") or "").strip()
    )
    model_slug = (
        command.model_slug
        if "model_slug" in explicit_fields
        else str(nested.get("model_slug") or "").strip()
    )
    return {
        "runtime_profile_ref": runtime_profile_ref,
        "job_type": job_type or None,
        "transport_type": transport_type,
        "adapter_type": adapter_type,
        "provider_slug": (provider_slug or "").lower() or None,
        "model_slug": model_slug or None,
    }


def _default_expires_at(command: PaidModelAccessCommand) -> str:
    if command.expires_at:
        parsed = datetime.fromisoformat(command.expires_at)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("expires_at must include a timezone offset")
        return parsed.isoformat()
    return (datetime.now(timezone.utc) + timedelta(minutes=command.ttl_minutes)).isoformat()


def _status_rows(conn: Any, command: PaidModelAccessCommand) -> list[dict[str, Any]]:
    selector = _selector(command)
    nested = dict(command.selector or {})
    rows = conn.execute(
        """
        SELECT *
        FROM private_paid_model_access_state
        WHERE runtime_profile_ref = $1
          AND ($2::text IS NULL OR job_type = $2)
          AND ($3::text IS NULL OR transport_type = $3)
          AND ($4::text IS NULL OR adapter_type = $4)
          AND ($5::text IS NULL OR provider_slug = $5)
          AND ($6::text IS NULL OR model_slug = $6)
        ORDER BY job_type, transport_type, provider_slug, model_slug, lease_created_at DESC NULLS LAST
        LIMIT $7
        """,
        selector["runtime_profile_ref"],
        selector["job_type"],
        selector["transport_type"]
        if command.transport_type or "transport_type" in nested
        else None,
        selector["adapter_type"] if command.adapter_type or "adapter_type" in nested else None,
        selector["provider_slug"],
        selector["model_slug"],
        command.limit,
    )
    return [dict(row) for row in rows or ()]


def _set_soft_state(conn: Any, command: PaidModelAccessCommand, *, enabled: bool) -> dict[str, Any]:
    exact = require_exact_paid_selector(_selector(command))
    if enabled:
        row = conn.fetchrow(
            """
            INSERT INTO private_provider_model_access_soft_offs (
                runtime_profile_ref,
                job_type,
                transport_type,
                adapter_type,
                provider_slug,
                model_slug,
                presentation_state,
                reason_code,
                operator_message,
                decision_ref,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                'soft_off', $7, $8, $9, now()
            )
            ON CONFLICT (
                runtime_profile_ref, job_type, transport_type,
                adapter_type, provider_slug, model_slug
            )
            DO UPDATE SET
                presentation_state = 'soft_off',
                reason_code = EXCLUDED.reason_code,
                operator_message = EXCLUDED.operator_message,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            RETURNING *
            """,
            exact["runtime_profile_ref"],
            exact["job_type"],
            exact["transport_type"],
            exact["adapter_type"],
            exact["provider_slug"],
            exact["model_slug"],
            command.reason_code or "paid_model.presentation_soft_off",
            command.operator_message
            or "Paid model is hidden from default picker surfaces until explicitly requested.",
            command.decision_ref or PAID_MODEL_POLICY_DECISION_REF,
        )
        return dict(row) if row is not None else exact

    rows = conn.execute(
        """
        DELETE FROM private_provider_model_access_soft_offs
        WHERE runtime_profile_ref = $1
          AND job_type = $2
          AND transport_type = $3
          AND adapter_type = $4
          AND provider_slug = $5
          AND model_slug = $6
        RETURNING *
        """,
        exact["runtime_profile_ref"],
        exact["job_type"],
        exact["transport_type"],
        exact["adapter_type"],
        exact["provider_slug"],
        exact["model_slug"],
    )
    deleted = [dict(row) for row in rows or ()]
    return {"selector": exact, "deleted_count": len(deleted), "deleted_rows": deleted}


def _revoke(conn: Any, command: PaidModelAccessCommand) -> list[dict[str, Any]]:
    if not command.lease_id:
        raise ValueError("lease_id is required for action='revoke'")
    rows = conn.execute(
        """
        UPDATE private_paid_model_access_leases
           SET status = 'revoked',
               revoked_at = COALESCE(revoked_at, now()),
               updated_at = now()
         WHERE lease_id = $1
           AND status IN ('active', 'bound')
         RETURNING *
        """,
        command.lease_id,
    )
    return [dict(row) for row in rows or ()]


def _consume(conn: Any, command: PaidModelAccessCommand) -> list[dict[str, Any]]:
    if not command.run_id and not command.lease_id:
        raise ValueError("run_id or lease_id is required for action='consume'")
    rows = conn.execute(
        """
        UPDATE private_paid_model_access_leases
           SET status = 'consumed',
               consumed_runs = max_runs,
               consumed_at = COALESCE(consumed_at, now()),
               updated_at = now()
         WHERE ($1::text IS NULL OR lease_id = $1)
           AND ($2::text IS NULL OR bound_run_id = $2)
           AND status IN ('active', 'bound')
           AND consumed_at IS NULL
         RETURNING *
        """,
        command.lease_id,
        command.run_id,
    )
    return [dict(row) for row in rows or ()]


def handle_paid_model_access(
    command: PaidModelAccessCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()

    if command.action in {"status", "preview"}:
        rows = _status_rows(conn, command)
        return {
            "ok": True,
            "action": command.action,
            "rows": rows,
            "count": len(rows),
            "authority": "private_paid_model_access_state",
        }

    if command.action == "grant_once":
        row = grant_paid_model_lease(
            conn,
            selector=_selector(command),
            approval_ref=command.approval_ref or "",
            approved_by=command.approved_by or "",
            approval_note=command.approval_note,
            proposal_hash=command.proposal_hash or "",
            expires_at=_default_expires_at(command),
            cost_posture=command.cost_posture,
            route_truth_ref=command.route_truth_ref,
            decision_ref=command.decision_ref,
        )
        return {
            "ok": True,
            "action": "grant_once",
            "lease": row,
            "event_payload": {
                "action": "grant_once",
                "lease_id": row.get("lease_id"),
                "selector": _selector(command),
                "approval_ref": command.approval_ref,
                "approved_by": command.approved_by,
                "proposal_hash": command.proposal_hash,
                "expires_at": row.get("expires_at"),
            },
        }

    if command.action == "revoke":
        rows = _revoke(conn, command)
        return {
            "ok": True,
            "action": "revoke",
            "rows": rows,
            "count": len(rows),
            "event_payload": {
                "action": "revoke",
                "lease_id": command.lease_id,
            },
        }

    if command.action == "bind_run":
        if not command.lease_id:
            raise ValueError("lease_id is required for action='bind_run'")
        if not command.run_id:
            raise ValueError("run_id is required for action='bind_run'")
        rows = bind_paid_model_leases_to_run(
            conn,
            lease_ids=[command.lease_id],
            run_id=command.run_id,
        )
        return {
            "ok": True,
            "action": "bind_run",
            "rows": rows,
            "count": len(rows),
            "event_payload": {
                "action": "bind_run",
                "lease_id": command.lease_id,
                "run_id": command.run_id,
            },
        }

    if command.action == "consume":
        rows = _consume(conn, command)
        return {
            "ok": True,
            "action": "consume",
            "rows": rows,
            "count": len(rows),
            "event_payload": {
                "action": "consume",
                "lease_id": command.lease_id,
                "run_id": command.run_id,
            },
        }

    if command.action == "soft_off":
        row = _set_soft_state(conn, command, enabled=True)
        return {
            "ok": True,
            "action": "soft_off",
            "row": row,
            "event_payload": {
                "action": "soft_off",
                "selector": _selector(command),
            },
        }

    row = _set_soft_state(conn, command, enabled=False)
    return {
        "ok": True,
        "action": "soft_on",
        "row": row,
        "event_payload": {
            "action": "soft_on",
            "selector": _selector(command),
        },
    }


__all__ = [
    "PaidModelAccessCommand",
    "handle_paid_model_access",
]
