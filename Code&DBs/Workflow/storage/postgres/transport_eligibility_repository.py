"""Explicit Postgres repository for transport-eligibility authority reads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .validators import _optional_text


class PostgresTransportEligibilityRepository:
    """Read the active provider/model transport catalog through one authority seam."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def list_active_transport_models(
        self,
        *,
        provider_slug: str | None = None,
        model_slug: str | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        normalized_provider_slug = _optional_text(provider_slug, field_name="provider_slug")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        rows = self._conn.execute(
            """
            SELECT DISTINCT ON (provider_slug, model_slug)
                   provider_slug,
                   model_slug,
                   capability_tags,
                   route_tier,
                   latency_class
            FROM provider_model_candidates
            WHERE status = 'active'
              AND ($1::text IS NULL OR provider_slug = $1)
              AND ($2::text IS NULL OR model_slug = $2)
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
            """,
            normalized_provider_slug,
            normalized_model_slug,
        )
        return tuple(rows or ())


__all__ = ["PostgresTransportEligibilityRepository"]
