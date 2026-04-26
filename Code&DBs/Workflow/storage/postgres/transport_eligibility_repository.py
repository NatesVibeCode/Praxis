"""Explicit Postgres repository for transport-eligibility authority reads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .validators import _optional_text, _require_text


@dataclass(frozen=True)
class EffectiveProviderJobCatalogRow:
    """Available private provider/model capability for one job type."""

    runtime_profile_ref: str
    job_type: str
    transport_type: str
    adapter_type: str
    provider_slug: str
    model_slug: str
    model_version: str
    cost_structure: str
    cost_metadata: Mapping[str, Any]
    reason_code: str
    candidate_ref: str | None
    provider_ref: str | None
    source_refs: tuple[str, ...]
    projected_at: Any
    projection_ref: str


@dataclass(frozen=True)
class ProviderJobCatalogRow:
    """Provider/model capability matrix row, including disabled audit rows."""

    runtime_profile_ref: str
    job_type: str
    transport_type: str
    adapter_type: str
    provider_slug: str
    model_slug: str
    model_version: str
    cost_structure: str
    cost_metadata: Mapping[str, Any]
    availability_state: str
    reason_code: str
    candidate_ref: str | None
    provider_ref: str | None
    source_refs: tuple[str, ...]
    projected_at: Any
    projection_ref: str


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

    def list_effective_provider_job_catalog(
        self,
        *,
        runtime_profile_ref: str,
        job_type: str | None = None,
        transport_type: str | None = None,
        provider_slug: str | None = None,
        model_slug: str | None = None,
    ) -> tuple[EffectiveProviderJobCatalogRow, ...]:
        normalized_runtime_profile_ref = _require_text(
            runtime_profile_ref, field_name="runtime_profile_ref"
        )
        normalized_job_type = _optional_text(job_type, field_name="job_type")
        normalized_transport_type = _optional_text(
            transport_type, field_name="transport_type"
        )
        normalized_provider_slug = _optional_text(provider_slug, field_name="provider_slug")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        if normalized_transport_type is not None:
            normalized_transport_type = normalized_transport_type.upper()

        rows = self._conn.execute(
            """
            SELECT
                runtime_profile_ref,
                job_type,
                transport_type,
                adapter_type,
                provider_slug,
                model_slug,
                model_version,
                cost_structure,
                cost_metadata,
                reason_code,
                candidate_ref,
                provider_ref,
                source_refs,
                projected_at,
                projection_ref
            FROM effective_private_provider_job_catalog
            WHERE runtime_profile_ref = $1
              AND ($2::text IS NULL OR job_type = $2)
              AND ($3::text IS NULL OR transport_type = $3)
              AND ($4::text IS NULL OR provider_slug = $4)
              AND ($5::text IS NULL OR model_slug = $5)
            ORDER BY job_type, transport_type, provider_slug, model_slug
            """,
            normalized_runtime_profile_ref,
            normalized_job_type,
            normalized_transport_type,
            normalized_provider_slug,
            normalized_model_slug,
        )
        return tuple(_effective_provider_job_catalog_row(row) for row in rows or ())

    def list_provider_job_catalog(
        self,
        *,
        runtime_profile_ref: str,
        job_type: str | None = None,
        transport_type: str | None = None,
        provider_slug: str | None = None,
        model_slug: str | None = None,
    ) -> tuple[ProviderJobCatalogRow, ...]:
        """Read the full provider/job matrix, including disabled audit rows."""

        normalized_runtime_profile_ref = _require_text(
            runtime_profile_ref, field_name="runtime_profile_ref"
        )
        normalized_job_type = _optional_text(job_type, field_name="job_type")
        normalized_transport_type = _optional_text(
            transport_type, field_name="transport_type"
        )
        normalized_provider_slug = _optional_text(provider_slug, field_name="provider_slug")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        if normalized_transport_type is not None:
            normalized_transport_type = normalized_transport_type.upper()

        rows = self._conn.execute(
            """
            SELECT
                runtime_profile_ref,
                job_type,
                transport_type,
                adapter_type,
                provider_slug,
                model_slug,
                model_version,
                cost_structure,
                cost_metadata,
                availability_state,
                reason_code,
                candidate_ref,
                provider_ref,
                source_refs,
                projected_at,
                projection_ref
            FROM private_provider_job_catalog
            WHERE runtime_profile_ref = $1
              AND ($2::text IS NULL OR job_type = $2)
              AND ($3::text IS NULL OR transport_type = $3)
              AND ($4::text IS NULL OR provider_slug = $4)
              AND ($5::text IS NULL OR model_slug = $5)
            ORDER BY job_type, transport_type, provider_slug, model_slug, adapter_type
            """,
            normalized_runtime_profile_ref,
            normalized_job_type,
            normalized_transport_type,
            normalized_provider_slug,
            normalized_model_slug,
        )
        return tuple(_provider_job_catalog_row(row) for row in rows or ())


def _effective_provider_job_catalog_row(
    row: Mapping[str, Any],
) -> EffectiveProviderJobCatalogRow:
    source_refs = row.get("source_refs") or ()
    return EffectiveProviderJobCatalogRow(
        runtime_profile_ref=str(row["runtime_profile_ref"]),
        job_type=str(row["job_type"]),
        transport_type=str(row["transport_type"]),
        adapter_type=str(row["adapter_type"]),
        provider_slug=str(row["provider_slug"]),
        model_slug=str(row["model_slug"]),
        model_version=str(row.get("model_version") or ""),
        cost_structure=str(row["cost_structure"]),
        cost_metadata=dict(row.get("cost_metadata") or {}),
        reason_code=str(row["reason_code"]),
        candidate_ref=(
            str(row["candidate_ref"]) if row.get("candidate_ref") is not None else None
        ),
        provider_ref=str(row["provider_ref"]) if row.get("provider_ref") is not None else None,
        source_refs=tuple(str(item) for item in source_refs),
        projected_at=row.get("projected_at"),
        projection_ref=str(row["projection_ref"]),
    )


def _provider_job_catalog_row(
    row: Mapping[str, Any],
) -> ProviderJobCatalogRow:
    source_refs = row.get("source_refs") or ()
    return ProviderJobCatalogRow(
        runtime_profile_ref=str(row["runtime_profile_ref"]),
        job_type=str(row["job_type"]),
        transport_type=str(row["transport_type"]),
        adapter_type=str(row["adapter_type"]),
        provider_slug=str(row["provider_slug"]),
        model_slug=str(row["model_slug"]),
        model_version=str(row.get("model_version") or ""),
        cost_structure=str(row["cost_structure"]),
        cost_metadata=dict(row.get("cost_metadata") or {}),
        availability_state=str(row["availability_state"]),
        reason_code=str(row["reason_code"]),
        candidate_ref=(
            str(row["candidate_ref"]) if row.get("candidate_ref") is not None else None
        ),
        provider_ref=str(row["provider_ref"]) if row.get("provider_ref") is not None else None,
        source_refs=tuple(str(item) for item in source_refs),
        projected_at=row.get("projected_at"),
        projection_ref=str(row["projection_ref"]),
    )


__all__ = [
    "EffectiveProviderJobCatalogRow",
    "ProviderJobCatalogRow",
    "PostgresTransportEligibilityRepository",
]
