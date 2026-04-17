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
    "query_transport_support",
    "query_issue_backlog",
    "query_roadmap_tree",
    "query_operator_surface",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
