from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.recurring_review_repair_flow import (
    RecurringReviewRepairFlowError,
    RecurringReviewRepairFlowRequest,
    resolve_recurring_review_repair_flow,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import connect_workflow_database

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_recurring_review_repair_flow_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_recurring_review_repair_flow_is_deterministic_and_fail_closed())


async def _exercise_recurring_review_repair_flow_is_deterministic_and_fail_closed() -> None:
    env = _workflow_env()
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex[:10]

    conn = await connect_workflow_database(env=env)
    transaction = conn.transaction()
    await transaction.start()
    try:
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")

        review_class_name = f"review-{suffix}"
        review_policy_scope = f"workflow.review.{suffix}"
        review_work_kind = f"review-{suffix}"
        repair_class_name = f"repair-{suffix}"
        repair_policy_scope = f"workflow.repair.{suffix}"
        repair_work_kind = f"repair-{suffix}"
        target_ref = f"workspace.review-repair.{suffix}"

        await _seed_workflow_lane(
            conn,
            workflow_lane_id=f"workflow_lane.review.{suffix}",
            lane_name=f"review-{suffix}",
            lane_kind="review",
            concurrency_cap=1,
            default_route_kind="review",
            review_required=True,
            retry_policy={"max_attempts": 1, "backoff": "none"},
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
            workflow_lane_policy_id=f"workflow_lane_policy.review.{suffix}",
            workflow_lane_id=f"workflow_lane.review.{suffix}",
            policy_scope=review_policy_scope,
            work_kind=review_work_kind,
            match_rules={"work_kind": review_work_kind, "review": True},
            lane_parameters={"route_kind": "review", "manual_review": True},
            decision_ref=f"decision:lane-policy:review:{suffix}",
            as_of=as_of,
        )
        await _seed_workflow_lane(
            conn,
            workflow_lane_id=f"workflow_lane.repair.{suffix}",
            lane_name=f"repair-{suffix}",
            lane_kind="repair",
            concurrency_cap=1,
            default_route_kind="repair",
            review_required=True,
            retry_policy={"max_attempts": 2, "backoff": "linear"},
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
            workflow_lane_policy_id=f"workflow_lane_policy.repair.{suffix}",
            workflow_lane_id=f"workflow_lane.repair.{suffix}",
            policy_scope=repair_policy_scope,
            work_kind=repair_work_kind,
            match_rules={"work_kind": repair_work_kind, "repair": True},
            lane_parameters={"route_kind": "repair", "manual_intervention": True},
            decision_ref=f"decision:lane-policy:repair:{suffix}",
            as_of=as_of,
        )
        review_workflow_class_id = f"workflow_class.review.{suffix}"
        repair_workflow_class_id = f"workflow_class.repair.{suffix}"
        schedule_definition_id = f"schedule_definition.review-repair.{suffix}"
        recurring_run_window_id = f"recurring_run_window.review-repair.{suffix}"

        await _seed_workflow_class(
            conn,
            workflow_class_id=review_workflow_class_id,
            class_name=review_class_name,
            class_kind="review",
            workflow_lane_id=f"workflow_lane.review.{suffix}",
            queue_shape={"max_parallel": 1, "mode": "bounded-review"},
            throttle_policy={"max_attempts": 1, "backoff": "none"},
            review_required=True,
            decision_ref=f"decision:workflow-class:review:{suffix}",
            as_of=as_of,
        )
        await _seed_workflow_class(
            conn,
            workflow_class_id=repair_workflow_class_id,
            class_name=repair_class_name,
            class_kind="repair",
            workflow_lane_id=f"workflow_lane.repair.{suffix}",
            queue_shape={"max_parallel": 1, "mode": "bounded-repair"},
            throttle_policy={"max_attempts": 2, "backoff": "linear"},
            review_required=True,
            decision_ref=f"decision:workflow-class:repair:{suffix}",
            as_of=as_of,
        )
        await _seed_schedule_definition(
            conn,
            schedule_definition_id=schedule_definition_id,
            workflow_class_id=review_workflow_class_id,
            schedule_name=f"review-repair-{suffix}",
            schedule_kind="hourly",
            cadence_policy={"cadence": "P1H", "bounded": True},
            throttle_policy={"capacity_limit": 2},
            target_ref=target_ref,
            decision_ref=f"decision:schedule:review-repair:{suffix}",
            as_of=as_of,
        )
        await _seed_recurring_run_window(
            conn,
            recurring_run_window_id=recurring_run_window_id,
            schedule_definition_id=schedule_definition_id,
            capacity_limit=2,
            capacity_used=1,
            last_workflow_at=as_of - timedelta(minutes=10),
            as_of=as_of,
        )

        request = RecurringReviewRepairFlowRequest(
            target_ref=target_ref,
            schedule_kind="hourly",
            review_class_name=review_class_name,
            review_policy_scope=review_policy_scope,
            review_work_kind=review_work_kind,
            repair_class_name=repair_class_name,
            repair_policy_scope=repair_policy_scope,
            repair_work_kind=repair_work_kind,
        )

        first_resolution = await resolve_recurring_review_repair_flow(
            conn,
            request=request,
            as_of=as_of,
        )
        second_resolution = await resolve_recurring_review_repair_flow(
            conn,
            request=request,
            as_of=as_of,
        )

        assert first_resolution == second_resolution
        assert first_resolution.as_of == as_of
        assert first_resolution.schedule_definition_id == schedule_definition_id
        assert first_resolution.recurring_run_window_id == recurring_run_window_id
        assert first_resolution.capacity_remaining == 1
        assert first_resolution.review_workflow.workflow_class_id == review_workflow_class_id
        assert first_resolution.review_workflow.class_name == review_class_name
        assert first_resolution.review_workflow.workflow_lane_policy_id == (
            f"workflow_lane_policy.review.{suffix}"
        )
        assert first_resolution.repair_workflow.workflow_class_id == repair_workflow_class_id
        assert first_resolution.repair_workflow.class_name == repair_class_name
        assert first_resolution.repair_workflow.workflow_lane_policy_id == (
            f"workflow_lane_policy.repair.{suffix}"
        )
        assert first_resolution.review_workflow.review_required is True
        assert first_resolution.repair_workflow.review_required is True
        assert first_resolution.to_json()["authorities"] == {
            "workflow_class": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
        }

        await conn.execute(
            """
            UPDATE recurring_run_windows
            SET capacity_used = capacity_limit
            WHERE recurring_run_window_id = $1
            """,
            recurring_run_window_id,
        )

        with pytest.raises(RecurringReviewRepairFlowError) as exc_info:
            await resolve_recurring_review_repair_flow(
                conn,
                request=request,
                as_of=as_of,
            )

        assert exc_info.value.reason_code == "recurring_review_repair_flow.window_capacity_exhausted"
        assert exc_info.value.details == {
            "schedule_definition_id": schedule_definition_id,
            "recurring_run_window_id": recurring_run_window_id,
            "capacity_limit": 2,
            "capacity_used": 2,
        }
    finally:
        await transaction.rollback()
        await conn.close()


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except Exception as exc:  # pragma: no cover - bootstrap stays fail closed
                sqlstate = getattr(exc, "sqlstate", None)
                if sqlstate in {"42P07", "42710"}:
                    continue
                raise


async def _seed_workflow_lane(
    conn,
    *,
    workflow_lane_id: str,
    lane_name: str,
    lane_kind: str,
    concurrency_cap: int,
    default_route_kind: str,
    review_required: bool,
    retry_policy: dict[str, object],
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_lanes (
            workflow_lane_id,
            lane_name,
            lane_kind,
            status,
            concurrency_cap,
            default_route_kind,
            review_required,
            retry_policy,
            effective_from,
            effective_to,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
        )
        ON CONFLICT (workflow_lane_id) DO UPDATE SET
            lane_name = EXCLUDED.lane_name,
            lane_kind = EXCLUDED.lane_kind,
            status = EXCLUDED.status,
            concurrency_cap = EXCLUDED.concurrency_cap,
            default_route_kind = EXCLUDED.default_route_kind,
            review_required = EXCLUDED.review_required,
            retry_policy = EXCLUDED.retry_policy,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            created_at = EXCLUDED.created_at
        """,
        workflow_lane_id,
        lane_name,
        lane_kind,
        "active",
        concurrency_cap,
        default_route_kind,
        review_required,
        json.dumps(retry_policy, sort_keys=True, separators=(",", ":")),
        as_of - timedelta(hours=1),
        None,
        as_of,
    )


async def _seed_workflow_lane_policy(
    conn,
    *,
    workflow_lane_policy_id: str,
    workflow_lane_id: str,
    policy_scope: str,
    work_kind: str,
    match_rules: dict[str, object],
    lane_parameters: dict[str, object],
    decision_ref: str,
    as_of: datetime,
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
        ON CONFLICT (workflow_lane_policy_id) DO UPDATE SET
            workflow_lane_id = EXCLUDED.workflow_lane_id,
            policy_scope = EXCLUDED.policy_scope,
            work_kind = EXCLUDED.work_kind,
            match_rules = EXCLUDED.match_rules,
            lane_parameters = EXCLUDED.lane_parameters,
            decision_ref = EXCLUDED.decision_ref,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            created_at = EXCLUDED.created_at
        """,
        workflow_lane_policy_id,
        workflow_lane_id,
        policy_scope,
        work_kind,
        json.dumps(match_rules, sort_keys=True, separators=(",", ":")),
        json.dumps(lane_parameters, sort_keys=True, separators=(",", ":")),
        decision_ref,
        as_of - timedelta(hours=1),
        None,
        as_of,
    )


async def _seed_workflow_class(
    conn,
    *,
    workflow_class_id: str,
    class_name: str,
    class_kind: str,
    workflow_lane_id: str,
    queue_shape: dict[str, object],
    throttle_policy: dict[str, object],
    review_required: bool,
    decision_ref: str,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_classes (
            workflow_class_id,
            class_name,
            class_kind,
            workflow_lane_id,
            status,
            queue_shape,
            throttle_policy,
            review_required,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
        )
        ON CONFLICT (workflow_class_id) DO UPDATE SET
            class_name = EXCLUDED.class_name,
            class_kind = EXCLUDED.class_kind,
            workflow_lane_id = EXCLUDED.workflow_lane_id,
            status = EXCLUDED.status,
            queue_shape = EXCLUDED.queue_shape,
            throttle_policy = EXCLUDED.throttle_policy,
            review_required = EXCLUDED.review_required,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at
        """,
        workflow_class_id,
        class_name,
        class_kind,
        workflow_lane_id,
        "active",
        json.dumps(queue_shape, sort_keys=True, separators=(",", ":")),
        json.dumps(throttle_policy, sort_keys=True, separators=(",", ":")),
        review_required,
        as_of - timedelta(hours=1),
        None,
        decision_ref,
        as_of,
    )


async def _seed_schedule_definition(
    conn,
    *,
    schedule_definition_id: str,
    workflow_class_id: str,
    schedule_name: str,
    schedule_kind: str,
    cadence_policy: dict[str, object],
    throttle_policy: dict[str, object],
    target_ref: str,
    decision_ref: str,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO schedule_definitions (
            schedule_definition_id,
            workflow_class_id,
            schedule_name,
            schedule_kind,
            status,
            cadence_policy,
            throttle_policy,
            target_ref,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
        )
        """,
        schedule_definition_id,
        workflow_class_id,
        schedule_name,
        schedule_kind,
        "active",
        json.dumps(cadence_policy, sort_keys=True, separators=(",", ":")),
        json.dumps(throttle_policy, sort_keys=True, separators=(",", ":")),
        target_ref,
        as_of - timedelta(minutes=30),
        None,
        decision_ref,
        as_of,
    )


async def _seed_recurring_run_window(
    conn,
    *,
    recurring_run_window_id: str,
    schedule_definition_id: str,
    capacity_limit: int,
    capacity_used: int,
    last_workflow_at: datetime | None,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO recurring_run_windows (
            recurring_run_window_id,
            schedule_definition_id,
            window_started_at,
            window_ended_at,
            window_status,
            capacity_limit,
            capacity_used,
            last_workflow_at,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        )
        """,
        recurring_run_window_id,
        schedule_definition_id,
        as_of - timedelta(minutes=5),
        as_of + timedelta(minutes=55),
        "active",
        capacity_limit,
        capacity_used,
        last_workflow_at,
        as_of,
    )


def _workflow_env() -> dict[str, str]:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    return {"WORKFLOW_DATABASE_URL": database_url}
