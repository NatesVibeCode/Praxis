from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.instance import NativeWorkflowInstance
from runtime.recurring_review_repair_flow import (
    RecurringReviewRepairFlowError,
    RecurringReviewRepairFlowRequest,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import connect_workflow_database
from surfaces.api import operator_write

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_operator_path_adopts_recurring_review_repair_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _workflow_env()
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex[:10]

    seeded = asyncio.run(_seed_operator_path(env=env, as_of=as_of, suffix=suffix))
    try:
        monkeypatch.setattr(
            operator_write,
            "resolve_native_instance",
            lambda env=None: _native_instance(),
        )

        request = RecurringReviewRepairFlowRequest(
            target_ref=seeded["target_ref"],
            schedule_kind="hourly",
            review_class_name=seeded["review_class_name"],
            review_policy_scope=seeded["review_policy_scope"],
            review_work_kind=seeded["review_work_kind"],
            repair_class_name=seeded["repair_class_name"],
            repair_policy_scope=seeded["repair_policy_scope"],
            repair_work_kind=seeded["repair_work_kind"],
        )

        frontdoor = operator_write.NativeWorkflowFlowFrontdoor()
        first_payload = frontdoor.inspect_recurring_review_repair_flow(
            env=env,
            request=request,
            as_of=as_of,
        )
        second_payload = frontdoor.inspect_recurring_review_repair_flow(
            env=env,
            request=request,
            as_of=as_of,
        )

        assert first_payload == second_payload
        assert first_payload["native_instance"]["praxis_instance_name"] == "praxis"
        assert first_payload["recurring_flow_authority"] == "runtime.recurring_review_repair_flow"
        assert first_payload["as_of"] == as_of.isoformat()
        assert first_payload["recurring_review_repair_flow"]["authorities"] == {
            "workflow_class": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
        }
        assert first_payload["recurring_review_repair_flow"]["request"] == {
            "target_ref": seeded["target_ref"],
            "schedule_kind": "hourly",
            "review_class_name": seeded["review_class_name"],
            "review_policy_scope": seeded["review_policy_scope"],
            "review_work_kind": seeded["review_work_kind"],
            "repair_class_name": seeded["repair_class_name"],
            "repair_policy_scope": seeded["repair_policy_scope"],
            "repair_work_kind": seeded["repair_work_kind"],
        }
        assert first_payload["recurring_review_repair_flow"]["schedule"] == {
            "schedule_definition_id": seeded["schedule_definition_id"],
            "schedule_name": f"operator-recurring-flow-{suffix}",
            "schedule_kind": "hourly",
            "target_ref": seeded["target_ref"],
            "recurring_run_window_id": seeded["recurring_run_window_id"],
            "window_status": "active",
            "capacity_limit": 2,
            "capacity_used": 1,
            "capacity_remaining": 1,
            "decision_ref": f"decision:schedule:operator-path:{suffix}",
        }
        assert first_payload["recurring_review_repair_flow"]["review"]["workflow_class_id"] == (
            seeded["review_workflow_class_id"]
        )
        assert first_payload["recurring_review_repair_flow"]["review"]["workflow_lane_policy_id"] == (
            seeded["review_workflow_lane_policy_id"]
        )
        assert first_payload["recurring_review_repair_flow"]["repair"]["workflow_class_id"] == (
            seeded["repair_workflow_class_id"]
        )
        assert first_payload["recurring_review_repair_flow"]["repair"]["workflow_lane_policy_id"] == (
            seeded["repair_workflow_lane_policy_id"]
        )

        asyncio.run(
            _mark_window_exhausted(
                env=env,
                recurring_run_window_id=seeded["recurring_run_window_id"],
            )
        )

        with pytest.raises(RecurringReviewRepairFlowError) as exc_info:
            frontdoor.inspect_recurring_review_repair_flow(
                env=env,
                request=request,
                as_of=as_of,
            )

        assert exc_info.value.reason_code == "recurring_review_repair_flow.window_capacity_exhausted"
        assert exc_info.value.details == {
            "schedule_definition_id": seeded["schedule_definition_id"],
            "recurring_run_window_id": seeded["recurring_run_window_id"],
            "capacity_limit": 2,
            "capacity_used": 2,
        }
    finally:
        asyncio.run(_cleanup_operator_path(env=env, suffix=suffix))


async def _seed_operator_path(
    *,
    env: dict[str, str],
    as_of: datetime,
    suffix: str,
) -> dict[str, str]:
    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")

        target_ref = f"workspace.operator-path.{suffix}"
        review_class_name = f"operator-review-{suffix}"
        review_policy_scope = f"dispatch.operator.review.{suffix}"
        review_work_kind = f"operator-review-{suffix}"
        repair_class_name = f"operator-repair-{suffix}"
        repair_policy_scope = f"dispatch.operator.repair.{suffix}"
        repair_work_kind = f"operator-repair-{suffix}"
        review_workflow_lane_id = f"workflow_lane.operator.review.{suffix}"
        repair_workflow_lane_id = f"workflow_lane.operator.repair.{suffix}"
        review_workflow_lane_policy_id = f"workflow_lane_policy.operator.review.{suffix}"
        repair_workflow_lane_policy_id = f"workflow_lane_policy.operator.repair.{suffix}"
        review_workflow_class_id = f"workflow_class.operator.review.{suffix}"
        repair_workflow_class_id = f"workflow_class.operator.repair.{suffix}"
        schedule_definition_id = f"schedule_definition.operator-path.{suffix}"
        recurring_run_window_id = f"recurring_run_window.operator-path.{suffix}"

        await _seed_workflow_lane(
            conn,
            workflow_lane_id=review_workflow_lane_id,
            lane_name=f"operator-review-{suffix}",
            lane_kind="review",
            concurrency_cap=1,
            default_route_kind="review",
            review_required=True,
            retry_policy={"max_attempts": 1, "backoff": "none"},
            as_of=as_of,
        )
        await _seed_workflow_lane(
            conn,
            workflow_lane_id=repair_workflow_lane_id,
            lane_name=f"operator-repair-{suffix}",
            lane_kind="repair",
            concurrency_cap=1,
            default_route_kind="repair",
            review_required=True,
            retry_policy={"max_attempts": 2, "backoff": "linear"},
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
            workflow_lane_policy_id=review_workflow_lane_policy_id,
            workflow_lane_id=review_workflow_lane_id,
            policy_scope=review_policy_scope,
            work_kind=review_work_kind,
            match_rules={"work_kind": review_work_kind, "operator": True},
            lane_parameters={"route_kind": "review", "operator_path": "bounded"},
            decision_ref=f"decision:lane-policy:operator-review:{suffix}",
            as_of=as_of,
        )
        await _seed_workflow_lane_policy(
            conn,
            workflow_lane_policy_id=repair_workflow_lane_policy_id,
            workflow_lane_id=repair_workflow_lane_id,
            policy_scope=repair_policy_scope,
            work_kind=repair_work_kind,
            match_rules={"work_kind": repair_work_kind, "operator": True},
            lane_parameters={"route_kind": "repair", "operator_path": "bounded"},
            decision_ref=f"decision:lane-policy:operator-repair:{suffix}",
            as_of=as_of,
        )
        await _seed_workflow_class(
            conn,
            workflow_class_id=review_workflow_class_id,
            class_name=review_class_name,
            class_kind="review",
            workflow_lane_id=review_workflow_lane_id,
            queue_shape={"mode": "operator-recurring-review", "max_parallel": 1},
            throttle_policy={"max_attempts": 1, "backoff": "none"},
            review_required=True,
            decision_ref=f"decision:workflow-class:operator-review:{suffix}",
            as_of=as_of,
        )
        await _seed_workflow_class(
            conn,
            workflow_class_id=repair_workflow_class_id,
            class_name=repair_class_name,
            class_kind="repair",
            workflow_lane_id=repair_workflow_lane_id,
            queue_shape={"mode": "operator-recurring-repair", "max_parallel": 1},
            throttle_policy={"max_attempts": 2, "backoff": "linear"},
            review_required=True,
            decision_ref=f"decision:workflow-class:operator-repair:{suffix}",
            as_of=as_of,
        )
        await _seed_schedule_definition(
            conn,
            schedule_definition_id=schedule_definition_id,
            workflow_class_id=review_workflow_class_id,
            schedule_name=f"operator-recurring-flow-{suffix}",
            schedule_kind="hourly",
            cadence_policy={"cadence": "PT1H", "bounded_operator_path": True},
            throttle_policy={"capacity_limit": 2},
            target_ref=target_ref,
            decision_ref=f"decision:schedule:operator-path:{suffix}",
            as_of=as_of,
        )
        await _seed_recurring_run_window(
            conn,
            recurring_run_window_id=recurring_run_window_id,
            schedule_definition_id=schedule_definition_id,
            capacity_limit=2,
            capacity_used=1,
            last_workflow_at=as_of - timedelta(minutes=8),
            as_of=as_of,
        )
    finally:
        await conn.close()

    return {
        "target_ref": target_ref,
        "review_class_name": review_class_name,
        "review_policy_scope": review_policy_scope,
        "review_work_kind": review_work_kind,
        "repair_class_name": repair_class_name,
        "repair_policy_scope": repair_policy_scope,
        "repair_work_kind": repair_work_kind,
        "review_workflow_lane_policy_id": review_workflow_lane_policy_id,
        "repair_workflow_lane_policy_id": repair_workflow_lane_policy_id,
        "review_workflow_class_id": review_workflow_class_id,
        "repair_workflow_class_id": repair_workflow_class_id,
        "schedule_definition_id": schedule_definition_id,
        "recurring_run_window_id": recurring_run_window_id,
    }


async def _mark_window_exhausted(
    *,
    env: dict[str, str],
    recurring_run_window_id: str,
) -> None:
    conn = await connect_workflow_database(env=env)
    try:
        await conn.execute(
            """
            UPDATE recurring_run_windows
            SET capacity_used = capacity_limit
            WHERE recurring_run_window_id = $1
            """,
            recurring_run_window_id,
        )
    finally:
        await conn.close()


async def _cleanup_operator_path(
    *,
    env: dict[str, str],
    suffix: str,
) -> None:
    conn = await connect_workflow_database(env=env)
    try:
        await conn.execute(
            "DELETE FROM recurring_run_windows WHERE recurring_run_window_id = $1",
            f"recurring_run_window.operator-path.{suffix}",
        )
        await conn.execute(
            "DELETE FROM schedule_definitions WHERE schedule_definition_id = $1",
            f"schedule_definition.operator-path.{suffix}",
        )
        await conn.execute(
            "DELETE FROM workflow_classes WHERE workflow_class_id = ANY($1::text[])",
            [
                f"workflow_class.operator.review.{suffix}",
                f"workflow_class.operator.repair.{suffix}",
            ],
        )
        await conn.execute(
            "DELETE FROM workflow_lane_policies WHERE workflow_lane_policy_id = ANY($1::text[])",
            [
                f"workflow_lane_policy.operator.review.{suffix}",
                f"workflow_lane_policy.operator.repair.{suffix}",
            ],
        )
        await conn.execute(
            "DELETE FROM workflow_lanes WHERE workflow_lane_id = ANY($1::text[])",
            [
                f"workflow_lane.operator.review.{suffix}",
                f"workflow_lane.operator.repair.{suffix}",
            ],
        )
    finally:
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
            except Exception as exc:  # pragma: no cover
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
        _jsonb(retry_policy),
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
        _jsonb(match_rules),
        _jsonb(lane_parameters),
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
        _jsonb(queue_shape),
        _jsonb(throttle_policy),
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
        _jsonb(cadence_policy),
        _jsonb(throttle_policy),
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


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _workflow_env() -> dict[str, str]:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    return {"WORKFLOW_DATABASE_URL": database_url}


def _native_instance() -> NativeWorkflowInstance:
    return NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_REPO_ROOT,
        workdir=_REPO_ROOT,
        receipts_dir=f"{_REPO_ROOT}/artifacts/runtime_receipts",
        topology_dir=f"{_REPO_ROOT}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_REPO_ROOT}/config/runtime_profiles.json",
    )
