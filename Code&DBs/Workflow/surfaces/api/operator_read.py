"""Merged operator read surface over canonical rows and repo-local flow entrypoints."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from authority.transport_eligibility import load_transport_eligibility_authority
from storage.postgres import (
    PostgresProviderControlPlaneRepository,
    PostgresTransportEligibilityRepository,
)
from ._operator_repository import (
    NativeOperatorQueryError,
    NativeOperatorQueryFrontdoor,
    NativeOperatorQuerySnapshot,
    OperatorBugRecord,
    OperatorCutoverGateRecord,
    OperatorIssueRecord,
    OperatorRoadmapDependencyRecord,
    OperatorRoadmapItemRecord,
    OperatorRoadmapSemanticNeighborRecord,
    OperatorRoadmapTreeSnapshot,
    OperatorWorkflowRunPacketInspectionRecord,
    OperatorWorkflowRunObservabilitySummary,
    OperatorWorkItemCloseoutRecommendationRecord,
    query_operator_surface,
    query_issue_backlog,
    query_roadmap_tree,
)
from ._smoke_service import (
    NativeSelfHostedSmokeContract,
    load_native_self_hosted_smoke_contract,
    run_local_operator_flow,
    run_native_self_hosted_smoke,
)


@dataclass(slots=True)
class TransportSupportFrontdoor:
    """Thin repo-local frontdoor for transport-support inspection."""

    repository_factory: Callable[[Any], Any] | None = None
    provider_registry_mod: Any | None = None
    task_type_router_factory: Callable[[Any], Any] | None = None

    def _resolve_repository_factory(self) -> Callable[[Any], Any]:
        if self.repository_factory is not None:
            return self.repository_factory
        return PostgresTransportEligibilityRepository

    def query_transport_support(
        self,
        *,
        health_mod: Any,
        pg: Any,
        provider_filter: str | None = None,
        model_filter: str | None = None,
        runtime_profile_ref: str = "praxis",
        jobs: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Read transport support through the canonical authority + Postgres repository seam."""

        authority = load_transport_eligibility_authority(
            repository=self._resolve_repository_factory()(pg),
            health_mod=health_mod,
            pg=pg,
            provider_filter=provider_filter,
            model_filter=model_filter,
            runtime_profile_ref=runtime_profile_ref,
            jobs=jobs,
            provider_registry_mod=self.provider_registry_mod,
            task_type_router_factory=self.task_type_router_factory,
        )
        return authority.to_json()


_DEFAULT_TRANSPORT_SUPPORT_FRONTDOOR = TransportSupportFrontdoor()


@dataclass(slots=True)
class ProviderControlPlaneFrontdoor:
    """Thin repo-local frontdoor for provider control-plane read models."""

    repository_factory: Callable[[Any], Any] | None = None

    def _resolve_repository_factory(self) -> Callable[[Any], Any]:
        if self.repository_factory is not None:
            return self.repository_factory
        return PostgresProviderControlPlaneRepository

    def query_provider_control_plane(
        self,
        *,
        pg: Any,
        runtime_profile_ref: str,
        provider_slug: str | None = None,
        job_type: str | None = None,
        transport_type: str | None = None,
        model_slug: str | None = None,
    ) -> dict[str, Any]:
        repository = self._resolve_repository_factory()(pg)
        rows = repository.list_provider_control_plane_rows(
            runtime_profile_ref=runtime_profile_ref,
            provider_slug=provider_slug,
            job_type=job_type,
            transport_type=transport_type,
            model_slug=model_slug,
        )
        freshness = repository.get_projection_freshness(
            "projection.private_provider_control_plane_snapshot"
        )
        return {
            "control_plane": "operator.provider_control_plane",
            "runtime_profile_ref": runtime_profile_ref,
            "filters": {
                "provider_slug": provider_slug,
                "job_type": job_type,
                "transport_type": transport_type,
                "model_slug": model_slug,
            },
            "rows": [
                {
                    "runtime_profile_ref": row.runtime_profile_ref,
                    "job_type": row.job_type,
                    "transport_type": row.transport_type,
                    "adapter_type": row.adapter_type,
                    "provider_slug": row.provider_slug,
                    "model_slug": row.model_slug,
                    "model_version": row.model_version,
                    "cost_structure": row.cost_structure,
                    "cost_metadata": dict(row.cost_metadata),
                    "credential_availability_state": row.credential_availability_state,
                    "credential_sources": list(row.credential_sources),
                    "credential_observations": [
                        dict(item) for item in row.credential_observations
                    ],
                    "capability_state": row.capability_state,
                    "is_runnable": row.is_runnable,
                    "breaker_state": row.breaker_state,
                    "manual_override_state": row.manual_override_state,
                    "primary_removal_reason_code": row.primary_removal_reason_code,
                    "removal_reasons": [dict(item) for item in row.removal_reasons],
                    "candidate_ref": row.candidate_ref,
                    "provider_ref": row.provider_ref,
                    "source_refs": list(row.source_refs),
                    "projected_at": (
                        row.projected_at.isoformat()
                        if hasattr(row.projected_at, "isoformat")
                        else row.projected_at
                    ),
                    "projection_ref": row.projection_ref,
                }
                for row in rows
            ],
            "projection_freshness": {
                "projection_ref": freshness.projection_ref,
                "freshness_status": freshness.freshness_status,
                "last_refreshed_at": (
                    freshness.last_refreshed_at.isoformat()
                    if hasattr(freshness.last_refreshed_at, "isoformat")
                    else freshness.last_refreshed_at
                ),
                "error_code": freshness.error_code,
                "error_detail": freshness.error_detail,
            },
            "levers": {
                "commands": [
                    "operator.circuit_override",
                    "operator.task_route_eligibility",
                ],
                "queries": [
                    "operator.circuit_states",
                    "operator.circuit_history",
                    "operator.provider_control_plane",
                ],
            },
        }

    def query_circuit_states(
        self,
        *,
        pg: Any,
        provider_slug: str | None = None,
    ) -> dict[str, Any]:
        repository = self._resolve_repository_factory()(pg)
        rows = repository.list_provider_circuit_states(provider_slug=provider_slug)
        freshness = repository.get_projection_freshness("projection.circuit_breakers")
        return {
            "circuits": {
                row.provider_slug: {
                    "provider_slug": row.provider_slug,
                    "state": row.effective_state,
                    "runtime_state": row.runtime_state,
                    "manual_override": (
                        None
                        if row.manual_override_state is None
                        else {
                            "override_state": row.manual_override_state,
                            "rationale": row.manual_override_reason,
                        }
                    ),
                    "failure_count": row.failure_count,
                    "success_count": row.success_count,
                    "failure_threshold": row.failure_threshold,
                    "recovery_timeout_s": row.recovery_timeout_s,
                    "half_open_max_calls": row.half_open_max_calls,
                    "last_failure_at": (
                        row.last_failure_at.isoformat()
                        if hasattr(row.last_failure_at, "isoformat")
                        else row.last_failure_at
                    ),
                    "opened_at": (
                        row.opened_at.isoformat()
                        if hasattr(row.opened_at, "isoformat")
                        else row.opened_at
                    ),
                    "half_open_after": (
                        row.half_open_after.isoformat()
                        if hasattr(row.half_open_after, "isoformat")
                        else row.half_open_after
                    ),
                    "half_open_calls": row.half_open_calls,
                    "updated_at": (
                        row.updated_at.isoformat()
                        if hasattr(row.updated_at, "isoformat")
                        else row.updated_at
                    ),
                    "projection_ref": row.projection_ref,
                }
                for row in rows
            },
            "projection_freshness": {
                "projection_ref": freshness.projection_ref,
                "freshness_status": freshness.freshness_status,
                "last_refreshed_at": (
                    freshness.last_refreshed_at.isoformat()
                    if hasattr(freshness.last_refreshed_at, "isoformat")
                    else freshness.last_refreshed_at
                ),
                "error_code": freshness.error_code,
                "error_detail": freshness.error_detail,
            },
        }


_DEFAULT_PROVIDER_CONTROL_PLANE_FRONTDOOR = ProviderControlPlaneFrontdoor()


def build_transport_support_summary(
    transport_support_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Project canonical transport support into the lightweight summary health surfaces expect."""

    providers_payload = transport_support_payload.get("providers")
    provider_rows = (
        providers_payload
        if isinstance(providers_payload, Sequence) and not isinstance(providers_payload, (str, bytes, bytearray))
        else ()
    )
    registered_providers: list[str] = []
    providers: list[dict[str, Any]] = []
    probe_targets: list[tuple[str, str]] = []
    for raw_provider in provider_rows:
        if not isinstance(raw_provider, Mapping):
            continue
        provider_slug = str(raw_provider.get("provider_slug") or "").strip()
        if not provider_slug:
            continue
        transports = raw_provider.get("transports")
        transport_rows = transports if isinstance(transports, Mapping) else {}
        adapters: list[str] = []
        for adapter_type, raw_support in transport_rows.items():
            if not isinstance(raw_support, Mapping) or not bool(raw_support.get("supported")):
                continue
            normalized_adapter = str(adapter_type or "").strip()
            if not normalized_adapter:
                continue
            adapters.append(normalized_adapter)
            probe_targets.append((provider_slug, normalized_adapter))
        registered_providers.append(provider_slug)
        providers.append(
            {
                "provider_slug": provider_slug,
                "adapters": adapters,
            }
        )
    return {
        "default_provider_slug": str(transport_support_payload.get("default_provider_slug") or "").strip(),
        "default_adapter_type": str(transport_support_payload.get("default_adapter_type") or "").strip(),
        "registered_providers": registered_providers,
        "providers": providers,
        "probe_targets": tuple(probe_targets),
        "support_basis": str(transport_support_payload.get("support_basis") or "").strip() or None,
    }


def query_transport_support(
    *,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read transport support through the canonical authority + Postgres repository seam."""

    return _DEFAULT_TRANSPORT_SUPPORT_FRONTDOOR.query_transport_support(
        health_mod=health_mod,
        pg=pg,
        provider_filter=provider_filter,
        model_filter=model_filter,
        runtime_profile_ref=runtime_profile_ref,
        jobs=jobs,
    )


__all__ = [
    "NativeSelfHostedSmokeContract",
    "NativeOperatorQueryError",
    "NativeOperatorQueryFrontdoor",
    "NativeOperatorQuerySnapshot",
    "OperatorBugRecord",
    "OperatorCutoverGateRecord",
    "OperatorIssueRecord",
    "OperatorRoadmapDependencyRecord",
    "OperatorRoadmapItemRecord",
    "OperatorRoadmapSemanticNeighborRecord",
    "OperatorWorkflowRunPacketInspectionRecord",
    "OperatorRoadmapTreeSnapshot",
    "OperatorWorkflowRunObservabilitySummary",
    "OperatorWorkItemCloseoutRecommendationRecord",
    "ProviderControlPlaneFrontdoor",
    "load_native_self_hosted_smoke_contract",
    "TransportSupportFrontdoor",
    "build_transport_support_summary",
    "query_transport_support",
    "query_issue_backlog",
    "query_roadmap_tree",
    "query_operator_surface",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
