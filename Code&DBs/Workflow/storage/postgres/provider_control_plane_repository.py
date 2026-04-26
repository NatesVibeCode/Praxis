"""Postgres read repository for provider control-plane projections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .validators import _optional_text, _require_text


@dataclass(frozen=True)
class ProjectionFreshnessRecord:
    projection_ref: str
    freshness_status: str
    last_refreshed_at: Any
    error_code: str | None
    error_detail: str | None


@dataclass(frozen=True)
class ProviderCircuitStateRow:
    provider_slug: str
    runtime_state: str
    effective_state: str
    manual_override_state: str | None
    manual_override_reason: str | None
    failure_count: int
    success_count: int
    failure_threshold: int
    recovery_timeout_s: float
    half_open_max_calls: int
    last_failure_at: Any
    opened_at: Any
    half_open_after: Any
    half_open_calls: int
    updated_at: Any
    projected_at: Any
    projection_ref: str


@dataclass(frozen=True)
class ProviderControlPlaneSnapshotRow:
    runtime_profile_ref: str
    job_type: str
    transport_type: str
    adapter_type: str
    provider_slug: str
    model_slug: str
    model_version: str
    cost_structure: str
    cost_metadata: Mapping[str, Any]
    credential_availability_state: str
    credential_sources: tuple[str, ...]
    credential_observations: tuple[Mapping[str, Any], ...]
    capability_state: str
    is_runnable: bool
    breaker_state: str
    manual_override_state: str | None
    primary_removal_reason_code: str | None
    removal_reasons: tuple[Mapping[str, Any], ...]
    candidate_ref: str | None
    provider_ref: str | None
    source_refs: tuple[str, ...]
    projected_at: Any
    projection_ref: str


class PostgresProviderControlPlaneRepository:
    """Read durable provider control-plane and breaker projections."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def get_projection_freshness(self, projection_ref: str) -> ProjectionFreshnessRecord:
        normalized_projection_ref = _require_text(projection_ref, field_name="projection_ref")
        row = self._conn.fetchrow(
            """
            SELECT
                projection_ref,
                freshness_status,
                last_refreshed_at,
                error_code,
                error_detail
            FROM authority_projection_state
            WHERE projection_ref = $1
            """,
            normalized_projection_ref,
        ) or {
            "projection_ref": normalized_projection_ref,
            "freshness_status": "unknown",
            "last_refreshed_at": None,
            "error_code": None,
            "error_detail": None,
        }
        return ProjectionFreshnessRecord(
            projection_ref=str(row["projection_ref"]),
            freshness_status=str(row.get("freshness_status") or "unknown"),
            last_refreshed_at=row.get("last_refreshed_at"),
            error_code=str(row["error_code"]) if row.get("error_code") is not None else None,
            error_detail=str(row["error_detail"]) if row.get("error_detail") is not None else None,
        )

    def list_provider_circuit_states(
        self,
        *,
        provider_slug: str | None = None,
    ) -> tuple[ProviderCircuitStateRow, ...]:
        normalized_provider_slug = _optional_text(provider_slug, field_name="provider_slug")
        rows = self._conn.execute(
            """
            SELECT
                provider_slug,
                runtime_state,
                effective_state,
                manual_override_state,
                manual_override_reason,
                failure_count,
                success_count,
                failure_threshold,
                recovery_timeout_s,
                half_open_max_calls,
                last_failure_at,
                opened_at,
                half_open_after,
                half_open_calls,
                updated_at,
                projected_at,
                projection_ref
            FROM effective_provider_circuit_breaker_state
            WHERE ($1::text IS NULL OR provider_slug = $1)
            ORDER BY provider_slug
            """,
            normalized_provider_slug,
        )
        return tuple(_provider_circuit_state_row(row) for row in rows or ())

    def list_provider_control_plane_rows(
        self,
        *,
        runtime_profile_ref: str,
        job_type: str | None = None,
        transport_type: str | None = None,
        provider_slug: str | None = None,
        model_slug: str | None = None,
    ) -> tuple[ProviderControlPlaneSnapshotRow, ...]:
        normalized_runtime_profile_ref = _require_text(
            runtime_profile_ref,
            field_name="runtime_profile_ref",
        )
        normalized_job_type = _optional_text(job_type, field_name="job_type")
        normalized_transport_type = _optional_text(transport_type, field_name="transport_type")
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
                credential_availability_state,
                credential_sources,
                credential_observations,
                capability_state,
                is_runnable,
                breaker_state,
                manual_override_state,
                primary_removal_reason_code,
                removal_reasons,
                candidate_ref,
                provider_ref,
                source_refs,
                projected_at,
                projection_ref
            FROM private_provider_control_plane_snapshot
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
        return tuple(_provider_control_plane_snapshot_row(row) for row in rows or ())


def _provider_circuit_state_row(row: Mapping[str, Any]) -> ProviderCircuitStateRow:
    return ProviderCircuitStateRow(
        provider_slug=str(row["provider_slug"]),
        runtime_state=str(row["runtime_state"]),
        effective_state=str(row["effective_state"]),
        manual_override_state=(
            str(row["manual_override_state"])
            if row.get("manual_override_state") is not None
            else None
        ),
        manual_override_reason=(
            str(row["manual_override_reason"])
            if row.get("manual_override_reason") is not None
            else None
        ),
        failure_count=int(row.get("failure_count") or 0),
        success_count=int(row.get("success_count") or 0),
        failure_threshold=int(row.get("failure_threshold") or 0),
        recovery_timeout_s=float(row.get("recovery_timeout_s") or 0),
        half_open_max_calls=int(row.get("half_open_max_calls") or 0),
        last_failure_at=row.get("last_failure_at"),
        opened_at=row.get("opened_at"),
        half_open_after=row.get("half_open_after"),
        half_open_calls=int(row.get("half_open_calls") or 0),
        updated_at=row.get("updated_at"),
        projected_at=row.get("projected_at"),
        projection_ref=str(row.get("projection_ref") or "projection.circuit_breakers"),
    )


def _provider_control_plane_snapshot_row(
    row: Mapping[str, Any],
) -> ProviderControlPlaneSnapshotRow:
    removal_reasons_raw = row.get("removal_reasons") or ()
    credential_sources_raw = row.get("credential_sources") or ()
    credential_observations_raw = row.get("credential_observations") or ()
    source_refs_raw = row.get("source_refs") or ()
    normalized_removal_reasons: list[Mapping[str, Any]] = []
    for item in removal_reasons_raw:
        if isinstance(item, Mapping):
            normalized_removal_reasons.append(dict(item))
    normalized_credential_observations: list[Mapping[str, Any]] = []
    for item in credential_observations_raw:
        if isinstance(item, Mapping):
            normalized_credential_observations.append(dict(item))
    return ProviderControlPlaneSnapshotRow(
        runtime_profile_ref=str(row["runtime_profile_ref"]),
        job_type=str(row["job_type"]),
        transport_type=str(row["transport_type"]),
        adapter_type=str(row["adapter_type"]),
        provider_slug=str(row["provider_slug"]),
        model_slug=str(row["model_slug"]),
        model_version=str(row.get("model_version") or ""),
        cost_structure=str(row["cost_structure"]),
        cost_metadata=dict(row.get("cost_metadata") or {}),
        credential_availability_state=str(row.get("credential_availability_state") or "unknown"),
        credential_sources=tuple(str(item) for item in credential_sources_raw),
        credential_observations=tuple(normalized_credential_observations),
        capability_state=str(row["capability_state"]),
        is_runnable=bool(row.get("is_runnable")),
        breaker_state=str(row["breaker_state"]),
        manual_override_state=(
            str(row["manual_override_state"])
            if row.get("manual_override_state") is not None
            else None
        ),
        primary_removal_reason_code=(
            str(row["primary_removal_reason_code"])
            if row.get("primary_removal_reason_code") is not None
            else None
        ),
        removal_reasons=tuple(normalized_removal_reasons),
        candidate_ref=str(row["candidate_ref"]) if row.get("candidate_ref") is not None else None,
        provider_ref=str(row["provider_ref"]) if row.get("provider_ref") is not None else None,
        source_refs=tuple(str(item) for item in source_refs_raw),
        projected_at=row.get("projected_at"),
        projection_ref=str(row.get("projection_ref") or "projection.private_provider_control_plane_snapshot"),
    )


__all__ = [
    "PostgresProviderControlPlaneRepository",
    "ProjectionFreshnessRecord",
    "ProviderCircuitStateRow",
    "ProviderControlPlaneSnapshotRow",
]
