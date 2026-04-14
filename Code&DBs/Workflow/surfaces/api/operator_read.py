"""Merged operator read surface over canonical rows and repo-local flow entrypoints."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from authority.transport_eligibility import load_transport_eligibility_authority
from storage.postgres import PostgresTransportEligibilityRepository

from ._operator_repository import (
    NativeOperatorQueryError,
    NativeOperatorQueryFrontdoor,
    NativeOperatorQuerySnapshot,
    OperatorBugRecord,
    OperatorCutoverGateRecord,
    OperatorRoadmapDependencyRecord,
    OperatorRoadmapItemRecord,
    OperatorRoadmapTreeSnapshot,
    OperatorWorkflowRunPacketInspectionRecord,
    OperatorWorkflowRunObservabilitySummary,
    OperatorWorkItemCloseoutRecommendationRecord,
    query_operator_surface,
    query_roadmap_tree,
)
from ._smoke_service import (
    NativeSelfHostedSmokeContract,
    load_native_self_hosted_smoke_contract,
    run_local_operator_flow,
    run_native_self_hosted_smoke,
)


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

    authority = load_transport_eligibility_authority(
        repository=PostgresTransportEligibilityRepository(pg),
        health_mod=health_mod,
        pg=pg,
        provider_filter=provider_filter,
        model_filter=model_filter,
        runtime_profile_ref=runtime_profile_ref,
        jobs=jobs,
    )
    return authority.to_json()


__all__ = [
    "NativeSelfHostedSmokeContract",
    "NativeOperatorQueryError",
    "NativeOperatorQueryFrontdoor",
    "NativeOperatorQuerySnapshot",
    "OperatorBugRecord",
    "OperatorCutoverGateRecord",
    "OperatorRoadmapDependencyRecord",
    "OperatorRoadmapItemRecord",
    "OperatorWorkflowRunPacketInspectionRecord",
    "OperatorRoadmapTreeSnapshot",
    "OperatorWorkflowRunObservabilitySummary",
    "OperatorWorkItemCloseoutRecommendationRecord",
    "load_native_self_hosted_smoke_contract",
    "query_transport_support",
    "query_roadmap_tree",
    "query_operator_surface",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
