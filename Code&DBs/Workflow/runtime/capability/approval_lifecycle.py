"""Mobile approval and revocation lifecycle helpers."""

from __future__ import annotations

from contextlib import nullcontext
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from runtime._helpers import _json_compatible

from .plan_envelope import PlanEnvelope


class ApprovalLifecycleError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    else:
        raise ApprovalLifecycleError(
            "approval.assertion_timestamp_required",
            f"{field_name} must be supplied",
            details={"field": field_name},
        )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tx(conn: Any):
    if hasattr(conn, "transaction"):
        return conn.transaction()
    return nullcontext(conn)


def _row(rows: list[Any]) -> dict[str, Any] | None:
    if not rows:
        return None
    return dict(rows[0])


def _ensure_fresh_assertion(
    *,
    assertion_verified_at: object,
    assertion_expires_at: object,
    now: datetime,
    max_age_s: int = 120,
) -> None:
    verified_at = _parse_dt(assertion_verified_at, field_name="assertion_verified_at")
    expires_at = _parse_dt(assertion_expires_at, field_name="assertion_expires_at")
    if verified_at > now + timedelta(seconds=5):
        raise ApprovalLifecycleError(
            "approval.assertion_from_future",
            "assertion verification timestamp is in the future",
        )
    if expires_at <= now:
        raise ApprovalLifecycleError(
            "approval.assertion_stale",
            "assertion freshness window has expired",
        )
    if now - verified_at > timedelta(seconds=max_age_s):
        raise ApprovalLifecycleError(
            "approval.assertion_stale",
            "assertion verification is too old to ratify approval",
        )


def open_approval_request(
    conn: Any,
    *,
    principal_ref: str,
    requested_by_kind: str,
    requested_by_ref: str,
    command_type: str,
    envelope: PlanEnvelope,
    plan_summary: str,
    risk_level: str,
    expires_at: datetime,
    device_id: str | None = None,
    control_command_id: str | None = None,
    blast_radius: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        INSERT INTO approval_requests (
            principal_ref, device_id, requested_by_kind, requested_by_ref,
            command_type, control_command_id, plan_envelope_hash, plan_summary,
            risk_level, blast_radius, expires_at
        ) VALUES (
            $1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11
        )
        RETURNING request_id, request_status, principal_ref, device_id,
                  command_type, plan_envelope_hash, risk_level, expires_at
        """,
        principal_ref,
        device_id,
        requested_by_kind,
        requested_by_ref,
        command_type,
        control_command_id,
        envelope.plan_hash,
        plan_summary,
        risk_level,
        _json_compatible(dict(blast_radius or {})),
        expires_at,
    )
    return dict(rows[0])


def ratify_approval_request(
    conn: Any,
    *,
    request_id: str,
    ratified_by: str,
    assertion_verified_at: object,
    assertion_expires_at: object,
    now: datetime | None = None,
    grant_ttl_s: int = 300,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    _ensure_fresh_assertion(
        assertion_verified_at=assertion_verified_at,
        assertion_expires_at=assertion_expires_at,
        now=effective_now,
    )

    with _tx(conn) as tx:
        approval = _row(
            tx.execute(
                """
                SELECT request_id, request_status, principal_ref, device_id,
                       command_type, plan_envelope_hash, risk_level, blast_radius,
                       expires_at
                FROM approval_requests
                WHERE request_id = $1::uuid
                FOR UPDATE
                """,
                request_id,
            )
        )
        if approval is None:
            raise ApprovalLifecycleError(
                "approval.request_not_found",
                f"approval request not found: {request_id}",
                details={"request_id": request_id},
            )
        if approval["request_status"] != "pending":
            raise ApprovalLifecycleError(
                "approval.request_not_pending",
                "approval request is not pending",
                details={"request_id": request_id, "status": approval["request_status"]},
            )
        if approval["expires_at"] <= effective_now:
            tx.execute(
                "UPDATE approval_requests SET request_status = 'expired' WHERE request_id = $1::uuid",
                request_id,
            )
            raise ApprovalLifecycleError(
                "approval.request_expired",
                "approval request has expired",
                details={"request_id": request_id},
            )

        grant_id = f"mobile.grant.{uuid4().hex[:16]}"
        grant_expires_at = min(
            approval["expires_at"],
            effective_now + timedelta(seconds=max(1, int(grant_ttl_s))),
        )
        capability_scope = {
            "command_types": [approval["command_type"]],
            "approval_request_id": str(approval["request_id"]),
        }
        rows = tx.execute(
            """
            INSERT INTO capability_grants (
                capability_grant_id, workflow_id, run_id, subject_type, subject_id,
                capability_name, grant_state, reason_code, decision_ref, scope_json,
                granted_at, expires_at,
                grant_id, principal_ref, device_id, grant_kind, capability_scope,
                max_risk_level, plan_envelope_hash, approval_request_id, issued_at
            ) VALUES (
                $1, 'mobile', 'mobile.approval', 'principal', $2,
                $3, 'active', 'mobile.approval.ratified', $4, $5::jsonb,
                $6, $7,
                $1, $2, $8::uuid, 'plan', $5::jsonb,
                $9, $10, $11::uuid, $6
            )
            RETURNING grant_id, principal_ref, device_id, grant_kind,
                      capability_scope, max_risk_level, plan_envelope_hash,
                      approval_request_id, issued_at, expires_at
            """,
            grant_id,
            approval["principal_ref"],
            approval["command_type"],
            f"approval_request:{approval['request_id']}",
            _json_compatible(capability_scope),
            effective_now,
            grant_expires_at,
            None if approval.get("device_id") is None else str(approval["device_id"]),
            approval["risk_level"],
            approval["plan_envelope_hash"],
            str(approval["request_id"]),
        )
        grant = dict(rows[0])
        tx.execute(
            """
            UPDATE approval_requests
            SET request_status = 'ratified',
                ratified_at = $2,
                ratified_by = $3,
                grant_ref = $4
            WHERE request_id = $1::uuid
            """,
            request_id,
            effective_now,
            ratified_by,
            grant_id,
        )
        return grant


def revoke_capability_grant(
    conn: Any,
    *,
    grant_ref: str,
    revoked_by: str,
    revoke_reason: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    rows = conn.execute(
        """
        UPDATE capability_grants
        SET revoked_at = COALESCE(revoked_at, $2),
            revoked_by = COALESCE(revoked_by, $3),
            revoke_reason = COALESCE(revoke_reason, $4),
            grant_state = 'revoked'
        WHERE grant_id = $1 OR capability_grant_id = $1
        RETURNING grant_id, capability_grant_id, revoked_at, revoked_by, revoke_reason
        """,
        grant_ref,
        effective_now,
        revoked_by,
        revoke_reason,
    )
    return {"revoked": len(rows), "grant_ref": grant_ref}


def revoke_device_authority(
    conn: Any,
    *,
    device_id: str,
    revoked_by: str,
    revoke_reason: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    with _tx(conn) as tx:
        device_rows = tx.execute(
            """
            UPDATE device_enrollments
            SET revoked_at = COALESCE(revoked_at, $2),
                revoked_by = COALESCE(revoked_by, $3),
                revoke_reason = COALESCE(revoke_reason, $4)
            WHERE device_id = $1::uuid
            RETURNING device_id
            """,
            device_id,
            effective_now,
            revoked_by,
            revoke_reason,
        )
        session_rows = tx.execute(
            """
            UPDATE mobile_sessions
            SET revoked_at = COALESCE(revoked_at, $2),
                revoked_by = COALESCE(revoked_by, $3),
                revoke_reason = COALESCE(revoke_reason, $4)
            WHERE device_id = $1::uuid
              AND revoked_at IS NULL
            RETURNING session_id
            """,
            device_id,
            effective_now,
            revoked_by,
            revoke_reason,
        )
        grant_rows = tx.execute(
            """
            UPDATE capability_grants
            SET revoked_at = COALESCE(revoked_at, $2),
                revoked_by = COALESCE(revoked_by, $3),
                revoke_reason = COALESCE(revoke_reason, $4),
                grant_state = 'revoked'
            WHERE device_id = $1::uuid
              AND revoked_at IS NULL
            RETURNING grant_id
            """,
            device_id,
            effective_now,
            revoked_by,
            revoke_reason,
        )
        approval_rows = tx.execute(
            """
            UPDATE approval_requests
            SET request_status = 'revoked',
                revoked_at = COALESCE(revoked_at, $2),
                revoked_by = COALESCE(revoked_by, $3),
                revoke_reason = COALESCE(revoke_reason, $4)
            WHERE device_id = $1::uuid
              AND request_status = 'pending'
              AND revoked_at IS NULL
            RETURNING request_id
            """,
            device_id,
            effective_now,
            revoked_by,
            revoke_reason,
        )
    return {
        "device_id": device_id,
        "device_rows": len(device_rows),
        "session_rows": len(session_rows),
        "grant_rows": len(grant_rows),
        "approval_rows": len(approval_rows),
    }


__all__ = [
    "ApprovalLifecycleError",
    "open_approval_request",
    "ratify_approval_request",
    "revoke_capability_grant",
    "revoke_device_authority",
]
