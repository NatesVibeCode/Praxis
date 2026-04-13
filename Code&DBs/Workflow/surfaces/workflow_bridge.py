"""Thin bridge from DAG runtime authority into workflow-facing consumers.

The bridge reads route truth from runtime and worker-consumable facts from the
outbox subscription seam. It does not compile queues, mutate lifecycle state,
or invent protocol-specific orchestration behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from runtime.claims import ClaimLeaseProposalSnapshot
from runtime.domain import RunState, RuntimeBoundaryError
from runtime.subscriptions import (
    WorkerSubscriptionAcknowledgement,
    WorkerSubscriptionBatch,
    WorkerSubscriptionCursor,
    WorkflowWorkerSubscription,
)
from policy.workflow_lanes import WorkflowLaneCatalog, WorkflowLaneResolution

_BUS_AUTHORITY = "runtime.outbox"
_LIFECYCLE_AUTHORITY = "runtime.claims"


class ClaimRouteReader(Protocol):
    """Minimal runtime route reader required by the workflow bridge."""

    def inspect_route(self, *, run_id: str) -> ClaimLeaseProposalSnapshot:
        ...


class LaneCatalogReader(Protocol):
    """Minimal storage-backed lane catalog reader required by the bridge."""

    async def load_catalog(self, *, as_of: datetime) -> WorkflowLaneCatalog:
        ...


@dataclass(frozen=True, slots=True)
class WorkflowClaimableWork:
    """Workflow-facing read model for one run's currently visible work."""

    route_snapshot: ClaimLeaseProposalSnapshot
    inbox_batch: WorkerSubscriptionBatch
    claimable: bool
    lifecycle_authority: str = _LIFECYCLE_AUTHORITY
    bus_authority: str = _BUS_AUTHORITY


@dataclass(frozen=True, slots=True)
class WorkflowAcknowledgement:
    """Workflow-facing acknowledgement model over one worker batch."""

    route_snapshot: ClaimLeaseProposalSnapshot
    acknowledgement: WorkerSubscriptionAcknowledgement
    lifecycle_authority: str = _LIFECYCLE_AUTHORITY
    bus_authority: str = _BUS_AUTHORITY


@dataclass(frozen=True, slots=True)
class WorkflowBridge:
    """Thin frontdoor that packages runtime truth for workflow consumers."""

    routes: ClaimRouteReader
    subscriptions: WorkflowWorkerSubscription
    lane_catalogs: LaneCatalogReader | None = None

    def claimable_work(
        self,
        *,
        cursor: WorkerSubscriptionCursor,
        limit: int = 100,
    ) -> WorkflowClaimableWork:
        route_snapshot = self.routes.inspect_route(run_id=cursor.run_id)
        if route_snapshot.run_id != cursor.run_id:
            raise RuntimeBoundaryError("route snapshot drifted from the requested cursor run_id")
        inbox_batch = self.subscriptions.read_batch(cursor=cursor, limit=limit)
        return WorkflowClaimableWork(
            route_snapshot=route_snapshot,
            inbox_batch=inbox_batch,
            claimable=self._is_claimable(
                route_snapshot=route_snapshot,
                inbox_batch=inbox_batch,
            ),
        )

    def acknowledge(
        self,
        *,
        work: WorkflowClaimableWork,
        through_evidence_seq: int | None = None,
    ) -> WorkflowAcknowledgement:
        acknowledgement = self.subscriptions.acknowledge(
            batch=work.inbox_batch,
            through_evidence_seq=through_evidence_seq,
        )
        return WorkflowAcknowledgement(
            route_snapshot=work.route_snapshot,
            acknowledgement=acknowledgement,
        )

    async def inspect_lane_catalog(
        self,
        *,
        as_of: datetime,
    ) -> WorkflowLaneCatalog:
        if self.lane_catalogs is None:
            raise RuntimeBoundaryError(
                "lane catalog repository is required to inspect lane semantics",
            )
        catalog = await self.lane_catalogs.load_catalog(as_of=as_of)
        if catalog.as_of != as_of:
            raise RuntimeBoundaryError(
                "lane catalog snapshot drifted from the requested as_of",
            )
        return catalog

    async def inspect_lane_runtime(
        self,
        *,
        as_of: datetime,
        policy_scope: str,
        work_kind: str,
    ) -> WorkflowLaneResolution:
        catalog = await self.inspect_lane_catalog(as_of=as_of)
        return catalog.resolve(
            policy_scope=policy_scope,
            work_kind=work_kind,
        )

    @staticmethod
    def _is_claimable(
        *,
        route_snapshot: ClaimLeaseProposalSnapshot,
        inbox_batch: WorkerSubscriptionBatch,
    ) -> bool:
        return (
            route_snapshot.current_state is RunState.CLAIM_ACCEPTED
            and bool(inbox_batch.facts)
        )


__all__ = [
    "ClaimRouteReader",
    "WorkflowAcknowledgement",
    "WorkflowBridge",
    "WorkflowClaimableWork",
    "WorkflowLaneResolution",
    "LaneCatalogReader",
]
