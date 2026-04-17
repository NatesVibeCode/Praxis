from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator
from surfaces.api.operator_read import TransportSupportFrontdoor


class QueryTransportSupport(BaseModel):
    provider_slug: str | None = None
    model_slug: str | None = None
    runtime_profile_ref: str = "praxis"
    jobs: list[dict[str, Any]] | None = None

    @field_validator("provider_slug", "model_slug", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("transport-support filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            return "praxis"
        return value.strip()


def handle_query_transport_support(
    query: QueryTransportSupport,
    subsystems: Any,
) -> dict[str, Any]:
    return TransportSupportFrontdoor().query_transport_support(
        health_mod=subsystems.get_health_mod(),
        pg=subsystems.get_pg_conn(),
        provider_filter=query.provider_slug,
        model_filter=query.model_slug,
        runtime_profile_ref=query.runtime_profile_ref,
        jobs=query.jobs,
    )


__all__ = [
    "QueryTransportSupport",
    "handle_query_transport_support",
]
