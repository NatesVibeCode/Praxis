"""DB-backed review authority for surface catalog policy."""

from __future__ import annotations

import json
from typing import Any

from runtime.event_log import CHANNEL_SYSTEM, EVENT_REVIEW_DECISION, emit
from storage.postgres.surface_catalog_review_repository import (
    list_latest_surface_catalog_review_decisions,
    record_surface_catalog_review_decision,
)
from storage.postgres.validators import PostgresWriteError

_VALID_TRUTH_CATEGORIES = {"runtime", "persisted", "alias", "partial", "coming_soon"}
_VALID_SURFACE_TIERS = {"primary", "advanced", "hidden"}
_VALID_STATUSES = {"ready", "coming_soon"}


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _normalize_truth(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    category = _text(value.get("category"))
    badge = _text(value.get("badge"))
    detail = _text(value.get("detail"))
    if not category and not badge and not detail:
        return None
    if category not in _VALID_TRUTH_CATEGORIES or not badge or not detail:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "truth must include valid category, badge, and detail",
            details={"field": "truth"},
        )
    return {
        "category": category,
        "badge": badge,
        "detail": detail,
    }


def _normalize_surface_policy(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    tier = _text(value.get("tier"))
    badge = _text(value.get("badge"))
    detail = _text(value.get("detail"))
    hard_choice = _text(value.get("hardChoice"))
    if not tier and not badge and not detail and not hard_choice:
        return None
    if tier not in _VALID_SURFACE_TIERS or not badge or not detail:
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "surfacePolicy must include valid tier, badge, and detail",
            details={"field": "surfacePolicy"},
        )
    normalized = {
        "tier": tier,
        "badge": badge,
        "detail": detail,
    }
    if hard_choice:
        normalized["hardChoice"] = hard_choice
    return normalized


def normalize_surface_catalog_review_payload(
    *,
    target_kind: str,
    candidate_payload: object | None,
) -> dict[str, Any] | None:
    if candidate_payload is None:
        return None
    if not isinstance(candidate_payload, dict):
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "candidate_payload must be an object when provided",
            details={"field": "candidate_payload"},
        )

    normalized: dict[str, Any] = {}
    if target_kind == "catalog_item":
        label = _text(candidate_payload.get("label"))
        icon = _text(candidate_payload.get("icon"))
        status = _text(candidate_payload.get("status"))
        description = _text(candidate_payload.get("description"))
        if label:
            normalized["label"] = label
        if icon:
            normalized["icon"] = icon
        if status:
            if status not in _VALID_STATUSES:
                raise PostgresWriteError(
                    "surface_catalog_review.invalid_input",
                    "status must be ready or coming_soon",
                    details={"field": "status", "value": status},
                )
            normalized["status"] = status
        if description:
            normalized["description"] = description
        if "displayOrder" in candidate_payload:
            display_order = candidate_payload.get("displayOrder")
            if isinstance(display_order, bool) or not isinstance(display_order, int):
                raise PostgresWriteError(
                    "surface_catalog_review.invalid_input",
                    "displayOrder must be an integer when provided",
                    details={"field": "displayOrder"},
                )
            normalized["displayOrder"] = display_order
    elif target_kind != "source_policy":
        raise PostgresWriteError(
            "surface_catalog_review.invalid_input",
            "target_kind must be catalog_item or source_policy",
            details={"field": "target_kind", "value": target_kind},
        )

    truth = _normalize_truth(candidate_payload.get("truth"))
    if truth is not None:
        normalized["truth"] = truth
    surface_policy = _normalize_surface_policy(candidate_payload.get("surfacePolicy"))
    if surface_policy is not None:
        normalized["surfacePolicy"] = surface_policy

    return normalized or None


def record_surface_catalog_review(
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
) -> dict[str, Any]:
    normalized_target_kind = _text(target_kind).lower()
    normalized_candidate_payload = normalize_surface_catalog_review_payload(
        target_kind=normalized_target_kind,
        candidate_payload=candidate_payload,
    )
    record = record_surface_catalog_review_decision(
        conn,
        surface_name=surface_name,
        target_kind=normalized_target_kind,
        target_ref=target_ref,
        decision=decision,
        actor_type=actor_type,
        actor_ref=actor_ref,
        approval_mode=approval_mode,
        rationale=rationale,
        candidate_payload=normalized_candidate_payload,
    )
    emit(
        conn,
        channel=CHANNEL_SYSTEM,
        event_type=EVENT_REVIEW_DECISION,
        entity_id=_text(record.get("surface_name")) or surface_name,
        entity_kind="surface_catalog",
        payload=_json_clone(record),
        emitted_by="runtime.surface_catalog_reviews",
    )
    return record


def list_surface_catalog_reviews(
    conn: Any,
    *,
    surface_name: str,
    target_kind: str | None = None,
    target_ref: str | None = None,
) -> list[dict[str, Any]]:
    normalized_target_kind = _text(target_kind).lower() if target_kind is not None else None
    return list_latest_surface_catalog_review_decisions(
        conn,
        surface_name=surface_name,
        target_kind=normalized_target_kind or None,
        target_ref=target_ref,
    )


__all__ = [
    "list_surface_catalog_reviews",
    "normalize_surface_catalog_review_payload",
    "record_surface_catalog_review",
]
