"""Merged operator read surface over canonical rows and repo-local flow entrypoints."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from authority.transport_eligibility import load_transport_eligibility_authority
from storage.postgres import PostgresTransportEligibilityRepository
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
