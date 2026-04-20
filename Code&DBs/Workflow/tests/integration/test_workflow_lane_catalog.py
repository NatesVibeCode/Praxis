from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json

import pytest

from policy.workflow_lanes import (
    WorkflowLaneCatalogError,
    PostgresWorkflowLaneCatalogRepository,
    admit_native_workflow_lane_catalog,
    bootstrap_workflow_lane_catalog_schema,
    load_workflow_lane_catalog,
)
from surfaces.workflow_bridge import WorkflowBridge
from storage.postgres import PostgresConfigurationError, connect_workflow_database


def test_workflow_lane_catalog_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_workflow_lane_catalog_is_deterministic_and_fail_closed())


async def _exercise_workflow_lane_catalog_is_deterministic_and_fail_closed() -> None:
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for workflow lane catalog integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        await bootstrap_workflow_lane_catalog_schema(conn)

        as_of = datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)
        catalog = await admit_native_workflow_lane_catalog(conn, as_of=as_of)

        assert catalog.as_of == as_of
        assert catalog.lane_names == (
            "fanout",
            "loop",
            "promotion-gated",
            "repair",
            "review",
            "smoke",
        )
        assert catalog.policy_keys == (
            ("workflow.fanout", "fanout"),
            ("workflow.gated", "promotion-gated"),
            ("workflow.loop", "loop"),
            ("workflow.repair", "repair"),
            ("workflow.review", "review"),
            ("workflow.smoke", "smoke"),
        )

        bridge = WorkflowBridge(
            routes=_BridgeRouteReader(),
            subscriptions=_BridgeSubscriptionReader(),
            lane_catalogs=PostgresWorkflowLaneCatalogRepository(conn),
        )
        bridge_catalog = await bridge.inspect_lane_catalog(as_of=as_of)

        assert bridge_catalog.as_of == as_of
        assert bridge_catalog.lane_names == catalog.lane_names
        assert bridge_catalog.policy_keys == catalog.policy_keys

        review_resolution = catalog.resolve(
            policy_scope="workflow.review",
            work_kind="review",
        )

        assert review_resolution.lane_name == "review"
        assert review_resolution.lane_kind == "review"
        assert review_resolution.policy_scope == "workflow.review"
        assert review_resolution.work_kind == "review"
        assert review_resolution.workflow_lane.workflow_lane_id == "workflow_lane.review"
        assert review_resolution.workflow_lane.review_required is True
        assert review_resolution.workflow_lane.concurrency_cap == 1
        assert review_resolution.workflow_lane.retry_policy == {
            "max_attempts": 1,
            "backoff": "none",
        }
        assert review_resolution.lane_policy.workflow_lane_policy_id == "workflow_lane_policy.review"
        assert review_resolution.lane_policy.decision_ref == "decision:lane-policy:review"
        assert review_resolution.lane_policy.match_rules == {
            "work_kind": "review",
            "review": True,
        }
        assert review_resolution.lane_policy.lane_parameters == {
            "route_kind": "review",
            "manual_review": True,
        }

        with pytest.raises(WorkflowLaneCatalogError) as missing_info:
            catalog.resolve(
                policy_scope="dispatch.unknown",
                work_kind="unknown",
            )
        assert missing_info.value.reason_code == "workflow_lane.policy_missing"
        assert missing_info.value.details == {
            "policy_scope": "dispatch.unknown",
            "work_kind": "unknown",
        }

        duplicate_transaction = conn.transaction()
        await duplicate_transaction.start()
        try:
            await _insert_lane_policy(
                conn,
                workflow_lane_policy_id="workflow_lane_policy.review.duplicate",
                workflow_lane_id="workflow_lane.review",
                policy_scope="workflow.review",
                work_kind="review",
                match_rules={
                    "work_kind": "review",
                    "review": True,
                    "variant": "duplicate",
                },
                lane_parameters={
                    "route_kind": "review",
                    "manual_review": True,
                    "variant": "duplicate",
                },
                decision_ref="decision:lane-policy:review:duplicate",
                effective_from=as_of - timedelta(hours=2),
                effective_to=as_of + timedelta(hours=2),
                created_at=as_of + timedelta(minutes=1),
            )

            with pytest.raises(WorkflowLaneCatalogError) as ambiguous_info:
                await load_workflow_lane_catalog(conn, as_of=as_of)

            assert ambiguous_info.value.reason_code == "workflow_lane.ambiguous_policy"
            assert ambiguous_info.value.details == {
                "as_of": as_of.isoformat(),
                "policy_scope": "workflow.review",
                "work_kind": "review",
                "workflow_lane_policy_ids": (
                    "workflow_lane_policy.review.duplicate,"
                    "workflow_lane_policy.review"
                ),
            }
        finally:
            await duplicate_transaction.rollback()
    finally:
        await transaction.rollback()
        await conn.close()


async def _insert_lane_policy(
    conn,
    *,
    workflow_lane_policy_id: str,
    workflow_lane_id: str,
    policy_scope: str,
    work_kind: str,
    match_rules: dict[str, object],
    lane_parameters: dict[str, object],
    decision_ref: str,
    effective_from: datetime,
    effective_to: datetime | None,
    created_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_lane_policies (
            workflow_lane_policy_id,
            workflow_lane_id,
            policy_scope,
            work_kind,
            match_rules,
            lane_parameters,
            decision_ref,
            effective_from,
            effective_to,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10
        )
        """,
        workflow_lane_policy_id,
        workflow_lane_id,
        policy_scope,
        work_kind,
        json.dumps(match_rules, sort_keys=True, separators=(",", ":")),
        json.dumps(lane_parameters, sort_keys=True, separators=(",", ":")),
        decision_ref,
        effective_from,
        effective_to,
        created_at,
    )


class _BridgeRouteReader:
    def inspect_route(self, *, run_id: str):  # pragma: no cover - not exercised here
        raise AssertionError(f"unexpected route inspection for run_id={run_id!r}")


class _BridgeSubscriptionReader:
    pass
