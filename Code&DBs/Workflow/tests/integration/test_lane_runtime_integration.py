from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from policy.workflow_lanes import (
    WorkflowLaneAuthorityRecord,
    WorkflowLaneCatalog,
    WorkflowLanePolicyAuthorityRecord,
)
from runtime import RuntimeBoundaryError
from surfaces.workflow_bridge import WorkflowBridge


def test_lane_runtime_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_lane_runtime_is_deterministic_and_fail_closed())


async def _exercise_lane_runtime_is_deterministic_and_fail_closed() -> None:
    as_of = datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)
    catalog = WorkflowLaneCatalog.from_records(
        lane_records=(
            WorkflowLaneAuthorityRecord(
                workflow_lane_id="workflow_lane.smoke",
                lane_name="smoke",
                lane_kind="smoke",
                status="active",
                concurrency_cap=2,
                default_route_kind="smoke",
                review_required=False,
                retry_policy={
                    "max_attempts": 3,
                    "backoff": "fast",
                },
                effective_from=as_of - timedelta(hours=1),
                effective_to=None,
                created_at=as_of - timedelta(minutes=30),
            ),
            WorkflowLaneAuthorityRecord(
                workflow_lane_id="workflow_lane.promotion-gated",
                lane_name="promotion-gated",
                lane_kind="promotion-gated",
                status="active",
                concurrency_cap=1,
                default_route_kind="gated",
                review_required=True,
                retry_policy={
                    "max_attempts": 1,
                    "backoff": "none",
                },
                effective_from=as_of - timedelta(hours=1),
                effective_to=None,
                created_at=as_of - timedelta(minutes=20),
            ),
        ),
        lane_policy_records=(
            WorkflowLanePolicyAuthorityRecord(
                workflow_lane_policy_id="workflow_lane_policy.smoke",
                workflow_lane_id="workflow_lane.smoke",
                policy_scope="workflow.smoke",
                work_kind="smoke",
                match_rules={
                    "work_kind": "smoke",
                    "smoke": True,
                },
                lane_parameters={
                    "route_kind": "smoke",
                    "fast_path": True,
                },
                decision_ref="decision:lane-policy:smoke",
                effective_from=as_of - timedelta(hours=1),
                effective_to=None,
                created_at=as_of - timedelta(minutes=10),
            ),
            WorkflowLanePolicyAuthorityRecord(
                workflow_lane_policy_id="workflow_lane_policy.promotion-gated",
                workflow_lane_id="workflow_lane.promotion-gated",
                policy_scope="workflow.gated",
                work_kind="promotion-gated",
                match_rules={
                    "work_kind": "promotion-gated",
                    "promotion_gate": True,
                },
                lane_parameters={
                    "route_kind": "gated",
                    "requires_approval": True,
                },
                decision_ref="decision:lane-policy:promotion-gated",
                effective_from=as_of - timedelta(hours=1),
                effective_to=None,
                created_at=as_of - timedelta(minutes=5),
            ),
        ),
        as_of=as_of,
    )
    reader = _FixedLaneCatalogReader(catalog=catalog)
    bridge = WorkflowBridge(
        routes=_UnusedRouteReader(),
        subscriptions=_UnusedSubscriptionReader(),
        lane_catalogs=reader,
    )

    smoke_decision = await bridge.inspect_lane_runtime(
        as_of=as_of,
        policy_scope="workflow.smoke",
        work_kind="smoke",
    )
    smoke_decision_again = await bridge.inspect_lane_runtime(
        as_of=as_of,
        policy_scope="workflow.smoke",
        work_kind="smoke",
    )

    assert smoke_decision == smoke_decision_again
    assert smoke_decision.as_of == as_of
    assert smoke_decision.lane_name == "smoke"
    assert smoke_decision.lane_kind == "smoke"
    assert smoke_decision.route_kind == "smoke"
    assert smoke_decision.review_required is False
    assert smoke_decision.concurrency_cap == 2
    assert smoke_decision.workflow_lane_id == "workflow_lane.smoke"
    assert smoke_decision.workflow_lane_policy_id == "workflow_lane_policy.smoke"
    assert smoke_decision.policy_scope == "workflow.smoke"
    assert smoke_decision.work_kind == "smoke"
    assert smoke_decision.match_rules == {
        "work_kind": "smoke",
        "smoke": True,
    }
    assert smoke_decision.lane_parameters == {
        "route_kind": "smoke",
        "fast_path": True,
    }
    assert smoke_decision.decision_ref == "decision:lane-policy:smoke"
    assert reader.calls == 2

    gated_decision = await bridge.inspect_lane_runtime(
        as_of=as_of,
        policy_scope="workflow.gated",
        work_kind="promotion-gated",
    )

    assert gated_decision.lane_name == "promotion-gated"
    assert gated_decision.route_kind == "gated"
    assert gated_decision.review_required is True
    assert gated_decision.concurrency_cap == 1

    drifted_bridge = WorkflowBridge(
        routes=_UnusedRouteReader(),
        subscriptions=_UnusedSubscriptionReader(),
        lane_catalogs=_FixedLaneCatalogReader(
            catalog=WorkflowLaneCatalog.from_records(
                lane_records=catalog.lane_records,
                lane_policy_records=catalog.lane_policy_records,
                as_of=as_of + timedelta(minutes=1),
            )
        ),
    )

    with pytest.raises(RuntimeBoundaryError) as drifted_info:
        await drifted_bridge.inspect_lane_runtime(
            as_of=as_of,
            policy_scope="workflow.smoke",
            work_kind="smoke",
        )

    assert "lane catalog snapshot drifted" in str(drifted_info.value)


@dataclass
class _FixedLaneCatalogReader:
    catalog: WorkflowLaneCatalog
    calls: int = 0

    async def load_catalog(self, *, as_of: datetime) -> WorkflowLaneCatalog:
        self.calls += 1
        return self.catalog


class _UnusedRouteReader:
    def inspect_route(self, *, run_id: str):  # pragma: no cover - not used here
        raise AssertionError(f"unexpected route inspection for run_id={run_id!r}")


class _UnusedSubscriptionReader:
    def read_batch(self, *args, **kwargs):  # pragma: no cover - not used here
        raise AssertionError("unexpected subscription read in lane-runtime test")

    def acknowledge(self, *args, **kwargs):  # pragma: no cover - not used here
        raise AssertionError("unexpected subscription acknowledgement in lane-runtime test")
