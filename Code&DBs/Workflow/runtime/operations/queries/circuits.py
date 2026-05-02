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


class QueryProviderControlPlane(BaseModel):
    runtime_profile_ref: str = "praxis"
    provider_slug: str | None = None
    job_type: str | None = None
    transport_type: str | None = None
    model_slug: str | None = None

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            raise ValueError("runtime_profile_ref must be a non-empty string")
        return value.strip()

    @field_validator("provider_slug", "job_type", "model_slug", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("control-plane filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("transport_type", mode="before")
    @classmethod
    def _normalize_transport_type(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("transport_type must be CLI or API when provided")
        normalized = value.strip().upper()
        if normalized not in {"CLI", "API"}:
            raise ValueError("transport_type must be CLI or API")
        return normalized


def handle_query_circuit_states(
    query: QueryCircuitStates,
    _subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_read import ProviderControlPlaneFrontdoor

    return ProviderControlPlaneFrontdoor().query_circuit_states(
        pg=_subsystems.get_pg_conn(),
        provider_slug=query.provider_slug,
    )


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


def handle_query_provider_control_plane(
    query: QueryProviderControlPlane,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_read import ProviderControlPlaneFrontdoor

    return ProviderControlPlaneFrontdoor().query_provider_control_plane(
        pg=subsystems.get_pg_conn(),
        runtime_profile_ref=query.runtime_profile_ref,
        provider_slug=query.provider_slug,
        job_type=query.job_type,
        transport_type=query.transport_type,
        model_slug=query.model_slug,
    )


__all__ = [
    "QueryCircuitHistory",
    "QueryCircuitStates",
    "QueryProviderControlPlane",
    "handle_query_circuit_history",
    "handle_query_circuit_states",
    "handle_query_provider_control_plane",
]
