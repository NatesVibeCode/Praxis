"""Canonical plan envelope hashing for control authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _json_compatible

_AUTHORITY_PAYLOAD_KEYS = frozenset(
    {
        "approval_request_id",
        "grant_coverage_reason",
        "grant_coverage_status",
        "grant_ref",
        "plan_envelope_hash",
    }
)


def _canonical_json(value: object) -> str:
    return json.dumps(
        _json_compatible(value),
        sort_keys=True,
        separators=(",", ":"),
    )


def _payload_without_authority_keys(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in payload.items()
        if str(key) not in _AUTHORITY_PAYLOAD_KEYS
    }


def canonical_payload_digest(payload: Mapping[str, Any]) -> str:
    """Return a stable digest of the operator-requested payload only."""

    stripped = _payload_without_authority_keys(payload)
    return hashlib.sha256(_canonical_json(stripped).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PlanEnvelope:
    command_type: str
    requested_by_kind: str
    requested_by_ref: str
    risk_level: str
    payload_digest: str
    target_refs: tuple[str, ...] = field(default_factory=tuple)
    blast_radius: Mapping[str, Any] = field(default_factory=dict)
    created_at_bucket: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": 1,
            "command_type": self.command_type,
            "requested_by_kind": self.requested_by_kind,
            "requested_by_ref": self.requested_by_ref,
            "risk_level": self.risk_level,
            "payload_digest": self.payload_digest,
            "target_refs": list(self.target_refs),
            "blast_radius": _json_compatible(dict(self.blast_radius)),
            "created_at_bucket": self.created_at_bucket,
        }

    @property
    def plan_hash(self) -> str:
        digest = hashlib.sha256(_canonical_json(self.to_payload()).encode("utf-8")).hexdigest()
        return f"plan:v1:{digest}"


def _created_at_bucket(created_at: datetime | None) -> str:
    if created_at is None:
        return ""
    value = created_at or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.replace(second=0, microsecond=0).isoformat()


def _target_refs_from_payload(payload: Mapping[str, Any]) -> tuple[str, ...]:
    refs: list[str] = []
    for key in ("run_id", "workflow_id", "spec_path", "coordination_path", "repo_root"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            refs.append(f"{key}:{value.strip()}")
    return tuple(sorted(set(refs)))


def build_plan_envelope(
    *,
    command_type: str,
    requested_by_kind: str,
    requested_by_ref: str,
    risk_level: str,
    payload: Mapping[str, Any],
    target_refs: Sequence[str] | None = None,
    blast_radius: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> PlanEnvelope:
    explicit_targets = tuple(str(ref).strip() for ref in (target_refs or ()) if str(ref).strip())
    return PlanEnvelope(
        command_type=str(command_type).strip(),
        requested_by_kind=str(requested_by_kind).strip(),
        requested_by_ref=str(requested_by_ref).strip(),
        risk_level=str(risk_level).strip(),
        payload_digest=canonical_payload_digest(payload),
        target_refs=tuple(sorted(set(explicit_targets))) or _target_refs_from_payload(payload),
        blast_radius=dict(blast_radius or {}),
        created_at_bucket=_created_at_bucket(created_at),
    )


__all__ = ["PlanEnvelope", "build_plan_envelope", "canonical_payload_digest"]
