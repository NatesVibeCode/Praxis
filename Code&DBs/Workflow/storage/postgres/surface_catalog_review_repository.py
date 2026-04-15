"""DB-native authority for surface catalog review decisions."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid
from typing import Any

from .validators import PostgresWriteError, _encode_jsonb, _optional_text, _require_text

_VALID_TARGET_KINDS = {"catalog_item", "source_policy"}
_VALID_DECISIONS = {"approve", "reject", "defer", "widen", "revoke"}
_VALID_ACTOR_TYPES = {"model", "human", "policy"}


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "surface_catalog_review.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    candidate_payload = payload.get("candidate_payload")
    if isinstance(candidate_payload, str):
        try:
            payload["candidate_payload"] = json.loads(candidate_payload)
        except (TypeError, json.JSONDecodeError):
            payload["candidate_payload"] = None
    return payload


def _normalize_target_kind(value: object) -> str:
    target_kind = _require_text(value, field_name="target_kind").lower()
    if target_kind not in _VALID_TARGET_KINDS:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "target_kind must be catalog_item or source_policy",
            details={"field": "target_kind", "value": target_kind},
        )
    return target_kind


def _normalize_decision(value: object) -> str:
    decision = _require_text(value, field_name="decision").lower()
    if decision not in _VALID_DECISIONS:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "decision must be one of approve, reject, defer, widen, or revoke",
            details={"field": "decision", "value": decision},
        )
    return decision


def _normalize_actor_type(value: object | None) -> str:
    actor_type = _optional_text(value, field_name="actor_type")
    if actor_type is None:
        return "human"
    normalized = actor_type.lower()
    if normalized not in _VALID_ACTOR_TYPES:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "actor_type must be one of model, human, or policy",
            details={"field": "actor_type", "value": actor_type},
        )
    return normalized


def _normalize_decided_at(value: object | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "decided_at must be a datetime when provided",
            details={"field": "decided_at", "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "decided_at must be timezone-aware",
            details={"field": "decided_at"},
        )
    return value.astimezone(timezone.utc)


def record_surface_catalog_review_decision(
    conn: Any,
    *,
    surface_name: str,
    target_kind: str,
    target_ref: str,
    decision: str,
    actor_type: str | None = None,
    actor_ref: str | None = None,
    approval_mode: str | None = None,
    rationale: str | None = None,
    candidate_payload: object | None = None,
    decided_at: datetime | None = None,
    review_decision_id: str | None = None,
) -> dict[str, Any]:
    normalized_review_decision_id = _optional_text(
        review_decision_id,
        field_name="review_decision_id",
    ) or f"scrd_{uuid.uuid4().hex}"
    normalized_surface_name = _require_text(surface_name, field_name="surface_name")
    normalized_target_kind = _normalize_target_kind(target_kind)
    normalized_target_ref = _require_text(target_ref, field_name="target_ref")
    normalized_decision = _normalize_decision(decision)
    normalized_actor_type = _normalize_actor_type(actor_type)
    normalized_actor_ref = _optional_text(actor_ref, field_name="actor_ref") or "surface_catalog"
    normalized_approval_mode = _optional_text(
        approval_mode,
        field_name="approval_mode",
    ) or "manual"
    normalized_rationale = _optional_text(rationale, field_name="rationale")
    normalized_decided_at = _normalize_decided_at(decided_at)
    normalized_candidate_payload = None
    if candidate_payload is not None:
        normalized_candidate_payload = json.loads(
            _encode_jsonb(candidate_payload, field_name="candidate_payload")
        )

    row = conn.fetchrow(
        """
        INSERT INTO surface_catalog_review_decisions (
            review_decision_id,
            surface_name,
            target_kind,
            target_ref,
            decision,
            actor_type,
            actor_ref,
            approval_mode,
            rationale,
            candidate_payload,
            decided_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11
        )
        RETURNING
            review_decision_id,
            surface_name,
            target_kind,
            target_ref,
            decision,
            actor_type,
            actor_ref,
            approval_mode,
            rationale,
            candidate_payload,
            decided_at,
            created_at
        """,
        normalized_review_decision_id,
        normalized_surface_name,
        normalized_target_kind,
        normalized_target_ref,
        normalized_decision,
        normalized_actor_type,
        normalized_actor_ref,
        normalized_approval_mode,
        normalized_rationale,
        _encode_jsonb(normalized_candidate_payload, field_name="candidate_payload")
        if normalized_candidate_payload is not None
        else None,
        normalized_decided_at,
    )
    return _normalize_row(row, operation="record_surface_catalog_review_decision")


def list_latest_surface_catalog_review_decisions(
    conn: Any,
    *,
    surface_name: str,
    target_kind: str | None = None,
    target_ref: str | None = None,
) -> list[dict[str, Any]]:
    normalized_surface_name = _require_text(surface_name, field_name="surface_name")
    normalized_target_kind = (
        _normalize_target_kind(target_kind)
        if target_kind is not None
        else None
    )
    normalized_target_ref = _optional_text(target_ref, field_name="target_ref")

    clauses = ["surface_name = $1"]
    params: list[Any] = [normalized_surface_name]
    if normalized_target_kind is not None:
        clauses.append(f"target_kind = ${len(params) + 1}")
        params.append(normalized_target_kind)
    if normalized_target_ref is not None:
        clauses.append(f"target_ref = ${len(params) + 1}")
        params.append(normalized_target_ref)

    rows = conn.execute(
        f"""
        SELECT DISTINCT ON (target_kind, target_ref)
            review_decision_id,
            surface_name,
            target_kind,
            target_ref,
            decision,
            actor_type,
            actor_ref,
            approval_mode,
            rationale,
            candidate_payload,
            decided_at,
            created_at
        FROM surface_catalog_review_decisions
        WHERE {' AND '.join(clauses)}
        ORDER BY target_kind, target_ref, decided_at DESC, created_at DESC, review_decision_id DESC
        """,
        *params,
    )
    return [_normalize_row(row, operation="list_latest_surface_catalog_review_decisions") for row in rows]


__all__ = [
    "list_latest_surface_catalog_review_decisions",
    "record_surface_catalog_review_decision",
]
