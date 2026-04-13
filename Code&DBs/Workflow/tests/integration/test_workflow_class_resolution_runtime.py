from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from authority.workflow_class_resolution import (
    WorkflowClassResolutionError,
    load_workflow_class_resolution_runtime,
)
from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from storage.migrations import workflow_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    connect_workflow_database,
    resolve_workflow_database_url,
)

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_workflow_class_resolution_runtime_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_workflow_class_resolution_runtime_is_deterministic_and_fail_closed())


async def _exercise_workflow_class_resolution_runtime_is_deterministic_and_fail_closed() -> None:
    env = _workflow_env()
    as_of = datetime(2001, 4, 2, 20, 0, tzinfo=timezone.utc)
    review_class_name = "review-runtime"

    conn = await connect_workflow_database(env=env)
    transaction = conn.transaction()
    await transaction.start()
    try:
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")

        await _seed_workflow_lane(
            conn,
            workflow_lane_id="workflow_lane.review",
            lane_name="review",
            lane_kind="review",
            concurrency_cap=1,
            default_route_kind="review",
            review_required=True,
            retry_policy={
                "max_attempts": 1,
                "backoff": "none",
            },
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
            workflow_lane_policy_id="workflow_lane_policy.review",
            workflow_lane_id="workflow_lane.review",
            policy_scope="workflow.review",
            work_kind="review",
            match_rules={
                "work_kind": "review",
                "review": True,
            },
            lane_parameters={
                "route_kind": "review",
                "manual_review": True,
            },
            decision_ref="decision:lane-policy:review",
            as_of=as_of,
        )
        await _seed_workflow_lane(
            conn,
            workflow_lane_id="workflow_lane.promotion-gated",
            lane_name="promotion-gated",
            lane_kind="promotion-gated",
            concurrency_cap=1,
            default_route_kind="gated",
            review_required=True,
            retry_policy={
                "max_attempts": 1,
                "backoff": "none",
            },
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
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
            as_of=as_of,
        )
        await _seed_workflow_class(
            conn,
            workflow_class_id="workflow_class.review",
            class_name=review_class_name,
            class_kind="review",
            workflow_lane_id="workflow_lane.review",
            queue_shape={
                "max_parallel": 1,
                "batching": "manual",
            },
            throttle_policy={
                "max_attempts": 1,
                "backoff": "none",
            },
            review_required=True,
            decision_ref="decision:workflow-class:review",
            as_of=as_of,
        )

        runtime = await load_workflow_class_resolution_runtime(conn, as_of=as_of)
        first_resolution = runtime.resolve(
            class_name=review_class_name,
            policy_scope="workflow.review",
            work_kind="review",
        )
        second_resolution = runtime.resolve(
            class_name=review_class_name,
            policy_scope="workflow.review",
            work_kind="review",
        )

        assert first_resolution == second_resolution
        assert runtime.as_of == as_of
        assert first_resolution.workflow_class_id == "workflow_class.review"
        assert first_resolution.workflow_lane_policy_id == "workflow_lane_policy.review"
        assert first_resolution.class_name == review_class_name
        assert first_resolution.class_kind == "review"
        assert first_resolution.workflow_lane_id == "workflow_lane.review"
        assert first_resolution.queue_shape == {
            "max_parallel": 1,
            "batching": "manual",
        }
        assert first_resolution.throttle_policy == {
            "max_attempts": 1,
            "backoff": "none",
        }
        assert first_resolution.review_required is True
        assert first_resolution.policy_scope == "workflow.review"
        assert first_resolution.work_kind == "review"
        assert first_resolution.match_rules == {
            "work_kind": "review",
            "review": True,
        }
        assert first_resolution.lane_parameters == {
            "route_kind": "review",
            "manual_review": True,
        }
        assert first_resolution.decision_ref == "decision:lane-policy:review"

        with pytest.raises(WorkflowClassResolutionError) as mismatch_info:
            runtime.resolve(
                class_name=review_class_name,
                policy_scope="workflow.gated",
                work_kind="promotion-gated",
            )

        assert mismatch_info.value.reason_code == "workflow_class.lane_policy_mismatch"
        assert mismatch_info.value.details == {
            "class_name": review_class_name,
            "workflow_class_id": "workflow_class.review",
            "workflow_lane_id": "workflow_lane.review",
            "workflow_lane_policy_id": "workflow_lane_policy.promotion-gated",
            "lane_policy_workflow_lane_id": "workflow_lane.promotion-gated",
            "policy_scope": "workflow.gated",
            "work_kind": "promotion-gated",
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
            except Exception as exc:  # pragma: no cover - bootstrap is fail closed
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


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for workflow-class resolution runtime integration test: "
            f"{exc.reason_code}"
        )
    return {"WORKFLOW_DATABASE_URL": database_url}
