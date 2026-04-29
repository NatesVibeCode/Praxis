"""Postgres read repository for provider control-plane projections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .validators import _optional_text, _require_text

_PROVIDER_JOB_CATALOG_AVAILABILITY_DISABLED = (
    "provider_job_catalog.availability_disabled"
)


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
    route_temperature: float | None
    route_max_tokens: int | None
    route_reasoning_control: Mapping[str, Any]
    route_request_contract_ref: str | None
    route_cache_policy: Mapping[str, Any]
    route_structured_output_policy: Mapping[str, Any]
    route_streaming_policy: Mapping[str, Any]
    control_enabled: bool
    control_state: str
    control_scope: str
    control_is_explicit: bool
    control_reason_code: str
    control_decision_ref: str
    control_operator_message: str
    credential_availability_state: str
    credential_sources: tuple[str, ...]
    credential_observations: tuple[Mapping[str, Any], ...]
    mechanical_capability_state: str
    mechanical_is_runnable: bool
    capability_state: str
    is_runnable: bool
    effective_dispatch_state: str
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
            WITH joined AS (
                SELECT
                    snapshot.runtime_profile_ref,
                    snapshot.job_type,
                    snapshot.transport_type,
                    snapshot.adapter_type,
                    snapshot.provider_slug,
                    snapshot.model_slug,
                    snapshot.model_version,
                    snapshot.cost_structure,
                    snapshot.cost_metadata,
                    route.temperature AS route_temperature,
                    route.max_tokens AS route_max_tokens,
                    route.reasoning_control AS route_reasoning_control,
                    route.request_contract_ref AS route_request_contract_ref,
                    route.cache_policy AS route_cache_policy,
                    route.structured_output_policy AS route_structured_output_policy,
                    route.streaming_policy AS route_streaming_policy,
                    (control.runtime_profile_ref IS NOT NULL) AS control_is_present,
                    COALESCE(control.control_enabled, snapshot.is_runnable) AS control_enabled,
                    COALESCE(
                        control.control_state,
                        CASE WHEN snapshot.is_runnable THEN 'on' ELSE 'off' END
                    ) AS control_state,
                    COALESCE(control.control_scope, 'projection.private_provider_control_plane_snapshot') AS control_scope,
                    COALESCE(control.control_is_explicit, false) AS control_is_explicit,
                    COALESCE(
                        control.control_reason_code,
                        snapshot.primary_removal_reason_code,
                        'catalog.available'
                    ) AS control_reason_code,
                    COALESCE(
                        control.control_decision_ref,
                        'decision.model_access_control.legacy_projection'
                    ) AS control_decision_ref,
                    COALESCE(
                        control.control_operator_message,
                        CASE
                            WHEN snapshot.is_runnable
                            THEN 'this Model Access method is currently enabled by the control panel.'
                            ELSE 'this Model Access method has been turned off on purpose at the control panel either for this specific task type, or more broadly, consult the control panel and do not turn it on without confirming with the user even if you think that will help you complete your task.'
                        END
                    ) AS control_operator_message,
                    snapshot.credential_availability_state,
                    snapshot.credential_sources,
                    snapshot.credential_observations,
                    snapshot.capability_state AS mechanical_capability_state,
                    snapshot.is_runnable AS mechanical_is_runnable,
                    snapshot.breaker_state,
                    snapshot.manual_override_state,
                    snapshot.primary_removal_reason_code,
                    snapshot.removal_reasons,
                    snapshot.candidate_ref,
                    snapshot.provider_ref,
                    snapshot.source_refs,
                    snapshot.projected_at,
                    snapshot.projection_ref
                FROM private_provider_control_plane_snapshot AS snapshot
                LEFT JOIN private_model_access_control_matrix AS control
                  ON control.runtime_profile_ref = snapshot.runtime_profile_ref
                 AND control.job_type = snapshot.job_type
                 AND control.adapter_type = snapshot.adapter_type
                 AND control.provider_slug = snapshot.provider_slug
                 AND control.model_slug = snapshot.model_slug
                LEFT JOIN task_type_routing AS route
                  ON route.task_type = snapshot.job_type
                 AND route.transport_type = snapshot.transport_type
                 AND route.provider_slug = snapshot.provider_slug
                 AND route.model_slug = snapshot.model_slug
                 AND route.sub_task_type = '*'
                WHERE snapshot.runtime_profile_ref = $1
                  AND ($2::text IS NULL OR snapshot.job_type = $2)
                  AND ($3::text IS NULL OR snapshot.transport_type = $3)
                  AND ($4::text IS NULL OR snapshot.provider_slug = $4)
                  AND ($5::text IS NULL OR snapshot.model_slug = $5)
            ),
            effective AS (
                SELECT
                    *,
                    (control_enabled IS TRUE AND mechanical_is_runnable IS TRUE) AS effective_is_runnable
                FROM joined
            )
            SELECT
                snapshot.runtime_profile_ref,
                snapshot.job_type,
                snapshot.transport_type,
                snapshot.adapter_type,
                snapshot.provider_slug,
                snapshot.model_slug,
                snapshot.model_version,
                snapshot.cost_structure,
                snapshot.cost_metadata,
                snapshot.route_temperature,
                snapshot.route_max_tokens,
                snapshot.route_reasoning_control,
                snapshot.route_request_contract_ref,
                snapshot.route_cache_policy,
                snapshot.route_structured_output_policy,
                snapshot.route_streaming_policy,
                snapshot.control_is_present,
                snapshot.control_enabled,
                snapshot.control_state,
                snapshot.control_scope,
                snapshot.control_is_explicit,
                snapshot.control_reason_code,
                snapshot.control_decision_ref,
                snapshot.control_operator_message,
                snapshot.credential_availability_state,
                snapshot.credential_sources,
                snapshot.credential_observations,
                snapshot.mechanical_capability_state,
                snapshot.mechanical_is_runnable,
                CASE
                    WHEN snapshot.effective_is_runnable THEN snapshot.mechanical_capability_state
                    ELSE 'removed'
                END AS capability_state,
                snapshot.effective_is_runnable AS is_runnable,
                CASE
                    WHEN snapshot.effective_is_runnable THEN 'runnable'
                    WHEN snapshot.control_enabled IS FALSE THEN 'disabled'
                    ELSE 'removed'
                END AS effective_dispatch_state,
                snapshot.breaker_state,
                snapshot.manual_override_state,
                CASE
                    WHEN snapshot.control_is_present IS TRUE
                     AND snapshot.control_enabled IS FALSE
                     AND snapshot.mechanical_is_runnable IS TRUE
                    THEN COALESCE(
                        NULLIF(snapshot.control_reason_code, ''),
                        'control_panel.model_access_method_turned_off'
                    )
                    ELSE snapshot.primary_removal_reason_code
                END AS primary_removal_reason_code,
                (
                    CASE
                        WHEN snapshot.control_is_present IS TRUE
                         AND snapshot.control_enabled IS FALSE
                        THEN jsonb_build_array(
                            jsonb_build_object(
                                'reason_code',
                                COALESCE(
                                    NULLIF(snapshot.control_reason_code, ''),
                                    'control_panel.model_access_method_turned_off'
                                ),
                                'source_ref',
                                COALESCE(
                                    NULLIF(snapshot.control_decision_ref, ''),
                                    'projection.private_model_access_control_matrix'
                                ),
                                'details',
                                jsonb_build_object(
                                    'control_state', snapshot.control_state,
                                    'control_scope', snapshot.control_scope,
                                    'control_is_explicit', snapshot.control_is_explicit
                                )
                            )
                        )
                        ELSE '[]'::jsonb
                    END
                    || COALESCE(snapshot.removal_reasons, '[]'::jsonb)
                ) AS removal_reasons,
                snapshot.candidate_ref,
                snapshot.provider_ref,
                snapshot.source_refs,
                snapshot.projected_at,
                snapshot.projection_ref
            FROM effective AS snapshot
            ORDER BY snapshot.job_type, snapshot.transport_type, snapshot.provider_slug, snapshot.model_slug, snapshot.adapter_type
            """,
            normalized_runtime_profile_ref,
            normalized_job_type,
            normalized_transport_type,
            normalized_provider_slug,
            normalized_model_slug,
        )
        return tuple(_provider_control_plane_snapshot_row(row) for row in rows or ())


def _normalize_catalog_availability_reason(
    reason_code: str | None,
    removal_reasons: list[Mapping[str, Any]],
) -> str | None:
    if reason_code != "runtime_profile_route.not_admitted":
        return reason_code
    for item in removal_reasons:
        if not isinstance(item, Mapping):
            continue
        details = item.get("details")
        if (
            item.get("source_ref") == "projection.private_provider_job_catalog"
            and isinstance(details, Mapping)
            and str(details.get("availability_state") or "").strip().lower() == "disabled"
        ):
            return _PROVIDER_JOB_CATALOG_AVAILABILITY_DISABLED
    return reason_code


def _normalize_removal_reason_codes(
    removal_reasons: list[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    normalized: list[Mapping[str, Any]] = []
    for item in removal_reasons:
        if not isinstance(item, Mapping):
            continue
        reason = dict(item)
        details = reason.get("details")
        if (
            reason.get("reason_code") == "runtime_profile_route.not_admitted"
            and reason.get("source_ref") == "projection.private_provider_job_catalog"
            and isinstance(details, Mapping)
            and str(details.get("availability_state") or "").strip().lower() == "disabled"
        ):
            reason["reason_code"] = _PROVIDER_JOB_CATALOG_AVAILABILITY_DISABLED
        normalized.append(reason)
    return normalized


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
    normalized_removal_reasons = _normalize_removal_reason_codes(normalized_removal_reasons)
    normalized_credential_observations: list[Mapping[str, Any]] = []
    for item in credential_observations_raw:
        if isinstance(item, Mapping):
            normalized_credential_observations.append(dict(item))
    control_enabled = bool(row.get("control_enabled", row.get("is_runnable")))
    control_is_present = bool(
        row.get(
            "control_is_present",
            any(
                key in row
                for key in (
                    "control_reason_code",
                    "control_decision_ref",
                    "control_scope",
                    "control_state",
                )
            ),
        )
    )
    mechanical_capability_state = str(
        row.get("mechanical_capability_state")
        or row.get("capability_state")
        or "removed"
    )
    mechanical_is_runnable = bool(row.get("mechanical_is_runnable", row.get("is_runnable")))
    effective_is_runnable = bool(row.get("is_runnable")) and control_enabled
    capability_state = (
        str(row.get("capability_state") or mechanical_capability_state)
        if effective_is_runnable
        else "removed"
    )
    effective_dispatch_state = str(row.get("effective_dispatch_state") or "").strip()
    if not effective_dispatch_state:
        effective_dispatch_state = (
            "runnable"
            if effective_is_runnable
            else ("disabled" if not control_enabled else "removed")
        )
    primary_removal_reason_code = (
        str(row["primary_removal_reason_code"])
        if row.get("primary_removal_reason_code") is not None
        else None
    )
    if not control_enabled and control_is_present:
        control_reason_code = str(row.get("control_reason_code") or "").strip()
        if not control_reason_code or control_reason_code == "catalog.available":
            control_reason_code = "control_panel.model_access_method_turned_off"
        if mechanical_is_runnable or not primary_removal_reason_code:
            primary_removal_reason_code = control_reason_code
        if not any(
            isinstance(item, Mapping) and item.get("reason_code") == control_reason_code
            for item in normalized_removal_reasons
        ):
            normalized_removal_reasons.insert(
                0,
                {
                    "reason_code": control_reason_code,
                    "source_ref": str(
                        row.get("control_decision_ref")
                        or "projection.private_model_access_control_matrix"
                    ),
                    "details": {
                        "control_state": str(row.get("control_state") or "off"),
                        "control_scope": str(row.get("control_scope") or ""),
                        "control_is_explicit": bool(row.get("control_is_explicit")),
                    },
                },
            )
    primary_removal_reason_code = _normalize_catalog_availability_reason(
        primary_removal_reason_code,
        normalized_removal_reasons,
    )
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
        route_temperature=(
            float(row["route_temperature"])
            if row.get("route_temperature") is not None
            else None
        ),
        route_max_tokens=(
            int(row["route_max_tokens"])
            if row.get("route_max_tokens") is not None
            else None
        ),
        route_reasoning_control=dict(row.get("route_reasoning_control") or {}),
        route_request_contract_ref=(
            str(row["route_request_contract_ref"])
            if row.get("route_request_contract_ref") is not None
            else None
        ),
        route_cache_policy=dict(row.get("route_cache_policy") or {}),
        route_structured_output_policy=dict(
            row.get("route_structured_output_policy") or {}
        ),
        route_streaming_policy=dict(row.get("route_streaming_policy") or {}),
        control_enabled=control_enabled,
        control_state=str(row.get("control_state") or "off"),
        control_scope=str(row.get("control_scope") or ""),
        control_is_explicit=bool(row.get("control_is_explicit")),
        control_reason_code=str(row.get("control_reason_code") or ""),
        control_decision_ref=str(row.get("control_decision_ref") or ""),
        control_operator_message=str(row.get("control_operator_message") or ""),
        credential_availability_state=str(row.get("credential_availability_state") or "unknown"),
        credential_sources=tuple(str(item) for item in credential_sources_raw),
        credential_observations=tuple(normalized_credential_observations),
        mechanical_capability_state=mechanical_capability_state,
        mechanical_is_runnable=mechanical_is_runnable,
        capability_state=capability_state,
        is_runnable=effective_is_runnable,
        effective_dispatch_state=effective_dispatch_state,
        breaker_state=str(row["breaker_state"]),
        manual_override_state=(
            str(row["manual_override_state"])
            if row.get("manual_override_state") is not None
            else None
        ),
        primary_removal_reason_code=primary_removal_reason_code,
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
