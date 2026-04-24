"""Capability grant resolver.

The resolver is deliberately small and fail-closed. Static command type hints
can inform UX, but only this DB-backed grant coverage can authorize operator
control execution.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .plan_envelope import PlanEnvelope

_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True, slots=True)
class GrantResolution:
    covered: bool
    reason_code: str
    grant_ref: str | None = None
    principal_ref: str | None = None
    device_id: str | None = None
    plan_envelope_hash: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "covered": self.covered,
            "reason_code": self.reason_code,
            "grant_ref": self.grant_ref,
            "principal_ref": self.principal_ref,
            "device_id": self.device_id,
            "plan_envelope_hash": self.plan_envelope_hash,
        }


def _row_value(row: Mapping[str, Any], key: str) -> Any:
    value = row.get(key)
    if isinstance(value, str) and key in {"capability_scope", "scope_json"}:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _risk_covers(max_risk_level: object, requested_risk_level: str) -> bool:
    max_rank = _RISK_ORDER.get(str(max_risk_level or "").strip())
    requested_rank = _RISK_ORDER.get(str(requested_risk_level or "").strip())
    return max_rank is not None and requested_rank is not None and requested_rank <= max_rank


def _scope_values(scope: Mapping[str, Any], *keys: str) -> tuple[str, ...]:
    for key in keys:
        value = scope.get(key)
        if isinstance(value, str):
            return (value,)
        if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
            return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def _scope_covers(scope: Mapping[str, Any], envelope: PlanEnvelope) -> bool:
    command_types = _scope_values(scope, "command_types", "commands", "capabilities")
    if command_types and "*" not in command_types and envelope.command_type not in command_types:
        return False

    requested_by_kinds = _scope_values(scope, "requested_by_kinds")
    if requested_by_kinds and "*" not in requested_by_kinds and envelope.requested_by_kind not in requested_by_kinds:
        return False

    target_refs = _scope_values(scope, "target_refs")
    if target_refs and "*" not in target_refs:
        envelope_targets = set(envelope.target_refs)
        if not any(ref in envelope_targets for ref in target_refs):
            return False

    return True


def _resolution(
    *,
    covered: bool,
    reason_code: str,
    row: Mapping[str, Any] | None = None,
    envelope: PlanEnvelope | None = None,
) -> GrantResolution:
    return GrantResolution(
        covered=covered,
        reason_code=reason_code,
        grant_ref=None if row is None else str(_row_value(row, "grant_id") or ""),
        principal_ref=None if row is None else str(_row_value(row, "principal_ref") or ""),
        device_id=None if row is None or _row_value(row, "device_id") is None else str(_row_value(row, "device_id")),
        plan_envelope_hash=None if envelope is None else envelope.plan_hash,
    )


def resolve_capability_grant(
    conn: Any,
    *,
    envelope: PlanEnvelope,
    principal_ref: str | None = None,
    device_id: str | None = None,
    grant_ref: str | None = None,
    now: datetime | None = None,
) -> GrantResolution:
    """Resolve grant coverage for a stamped plan envelope."""

    effective_now = now or datetime.now(timezone.utc)
    try:
        if grant_ref:
            rows = conn.execute(
                """
                SELECT grant_id, principal_ref, device_id, grant_kind, capability_scope,
                       max_risk_level, plan_envelope_hash, expires_at, revoked_at
                FROM capability_grants
                WHERE grant_id = $1
                LIMIT 1
                """,
                grant_ref,
            )
        else:
            rows = conn.execute(
                """
                SELECT grant_id, principal_ref, device_id, grant_kind, capability_scope,
                       max_risk_level, plan_envelope_hash, expires_at, revoked_at
                FROM capability_grants
                WHERE plan_envelope_hash = $1
                  AND revoked_at IS NULL
                  AND (expires_at IS NULL OR expires_at > $2)
                ORDER BY issued_at DESC NULLS LAST
                LIMIT 5
                """,
                envelope.plan_hash,
                effective_now,
            )
    except Exception:
        return GrantResolution(
            covered=False,
            reason_code="capability.grant.authority_unavailable",
            plan_envelope_hash=envelope.plan_hash,
        )

    if not rows:
        return GrantResolution(
            covered=False,
            reason_code="capability.grant.not_found",
            plan_envelope_hash=envelope.plan_hash,
        )

    for raw_row in rows:
        row = dict(raw_row)
        row_principal = _row_value(row, "principal_ref")
        row_device = _row_value(row, "device_id")
        if principal_ref and row_principal and str(row_principal) != principal_ref:
            continue
        if device_id and row_device and str(row_device) != str(device_id):
            continue
        if _row_value(row, "revoked_at") is not None:
            continue
        expires_at = _row_value(row, "expires_at")
        if isinstance(expires_at, datetime) and expires_at <= effective_now:
            continue
        row_plan_hash = _row_value(row, "plan_envelope_hash")
        if row_plan_hash and str(row_plan_hash) != envelope.plan_hash:
            continue
        if not _risk_covers(_row_value(row, "max_risk_level"), envelope.risk_level):
            continue
        scope = _row_value(row, "capability_scope")
        if isinstance(scope, Mapping) and not _scope_covers(scope, envelope):
            continue
        return _resolution(
            covered=True,
            reason_code="capability.grant.covered",
            row=row,
            envelope=envelope,
        )

    return GrantResolution(
        covered=False,
        reason_code="capability.grant.not_covered",
        plan_envelope_hash=envelope.plan_hash,
    )


__all__ = ["GrantResolution", "resolve_capability_grant"]
