from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class QueryCircuitStates(BaseModel):
    provider_slug: str | None = None

    @field_validator("provider_slug", mode="before")
    @classmethod
    def _normalize_optional_provider_slug(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider_slug must be a non-empty string when provided")
        return value.strip().lower()


class QueryCircuitHistory(BaseModel):
    provider_slug: str | None = None

    @field_validator("provider_slug", mode="before")
    @classmethod
    def _normalize_optional_provider_slug(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("provider_slug must be a non-empty string when provided")
        return value.strip().lower()


def handle_query_circuit_states(
    query: QueryCircuitStates,
    _subsystems: Any,
) -> dict[str, Any]:
    from runtime.circuit_breaker import get_circuit_breakers

    payload = get_circuit_breakers().all_states()
    if query.provider_slug:
        return {
            "circuits": (
                {query.provider_slug: payload[query.provider_slug]}
                if query.provider_slug in payload
                else {}
            )
        }
    return {"circuits": payload}


def _iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def handle_query_circuit_history(
    query: QueryCircuitHistory,
    subsystems: Any,
) -> dict[str, Any]:
    rows = subsystems.get_pg_conn().execute(
        """
        SELECT
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            created_at,
            updated_at,
            decision_scope_kind,
            decision_scope_ref
        FROM operator_decisions
        WHERE decision_scope_kind = 'provider'
          AND decision_kind IN (
                'circuit_breaker_reset',
                'circuit_breaker_force_open',
                'circuit_breaker_force_closed'
          )
          AND ($1::text = '' OR decision_scope_ref = $1)
        ORDER BY decided_at DESC, created_at DESC, operator_decision_id DESC
        """,
        query.provider_slug or "",
    )
    history: list[dict[str, object]] = []
    for row in rows:
        provider_slug = str(row.get("decision_scope_ref") or "").strip().lower()
        history.append(
            {
                "provider_slug": provider_slug,
                "operator_decision_id": str(row.get("operator_decision_id") or ""),
                "decision_key": str(row.get("decision_key") or ""),
                "decision_kind": str(row.get("decision_kind") or ""),
                "decision_status": str(row.get("decision_status") or ""),
                "rationale": str(row.get("rationale") or ""),
                "decided_by": str(row.get("decided_by") or ""),
                "decision_source": str(row.get("decision_source") or ""),
                "effective_from": _iso(row.get("effective_from")),
                "effective_to": _iso(row.get("effective_to")),
                "decided_at": _iso(row.get("decided_at")),
                "created_at": _iso(row.get("created_at")),
                "updated_at": _iso(row.get("updated_at")),
                "decision_scope_kind": str(row.get("decision_scope_kind") or ""),
                "decision_scope_ref": provider_slug,
            }
        )
    return {"history": history}


__all__ = [
    "QueryCircuitHistory",
    "QueryCircuitStates",
    "handle_query_circuit_history",
    "handle_query_circuit_states",
]
