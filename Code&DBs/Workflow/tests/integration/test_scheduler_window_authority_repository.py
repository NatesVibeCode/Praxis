from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from _pg_test_conn import ensure_test_database_ready
from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.scheduler_window_repository import (
    PostgresSchedulerWindowAuthorityRepository,
    SchedulerWindowRepositoryError,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import connect_workflow_database

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_TEST_DATABASE_URL = ensure_test_database_ready()


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in {"42P07", "42710"}


async def _bootstrap_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 30, tzinfo=timezone.utc)


async def _seed_scheduler_window_authority_rows(
    conn,
    *,
    suffix: str,
) -> tuple[str, str]:
    clock = _fixed_clock()
    workflow_lane_id = f"workflow_lane.scheduler-window.{suffix}"
    workflow_class_id = f"workflow_class.scheduler-window.{suffix}"
    schedule_definition_id = f"schedule_definition.scheduler-window.{suffix}"
    recurring_run_window_id = f"recurring_run_window.scheduler-window.{suffix}"

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
        """,
        workflow_lane_id,
        f"scheduler-window-{suffix}",
        "hourly",
        "active",
        1,
        "manual",
        False,
        json.dumps(
            {
                "max_attempts": 1,
                "backoff": "none",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        clock - timedelta(hours=1),
        None,
        clock,
    )
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
        """,
        workflow_class_id,
        f"scheduler-window-{suffix}",
        "hourly",
        workflow_lane_id,
        "active",
        json.dumps(
            {
                "shape": "single-run",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        json.dumps(
            {
                "max_attempts": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        False,
        clock - timedelta(hours=1),
        None,
        f"decision:scheduler-window:{suffix}:workflow-class",
        clock,
    )
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
        f"scheduler-window-{suffix}",
        "hourly",
        "active",
        json.dumps(
            {
                "cadence": "P1H",
                "bounded": True,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        json.dumps(
            {
                "capacity_limit": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        f"workspace.{suffix}",
        clock - timedelta(minutes=30),
        None,
        f"decision:scheduler-window:{suffix}:schedule",
        clock,
    )
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
        clock - timedelta(minutes=5),
        clock + timedelta(minutes=55),
        "active",
        1,
        0,
        None,
        clock,
    )

    return schedule_definition_id, recurring_run_window_id


def test_scheduler_window_authority_repository_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_scheduler_window_authority_repository_is_deterministic_and_fail_closed())


async def _exercise_scheduler_window_authority_repository_is_deterministic_and_fail_closed() -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL},
    )

    transaction = conn.transaction()
    await transaction.start()
    try:
        suffix = _unique_suffix()
        as_of = _fixed_clock()
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        schedule_definition_id, recurring_run_window_id = await _seed_scheduler_window_authority_rows(
            conn,
            suffix=suffix,
        )

        repository = PostgresSchedulerWindowAuthorityRepository(conn)
        authority = await repository.load_authority(as_of=as_of)
        authority_again = await repository.load_authority(as_of=as_of)

        assert authority == authority_again
        assert authority.as_of == as_of
        assert authority.schedule_keys == ((f"workspace.{suffix}", "hourly"),)
        assert authority.schedule_names == (f"scheduler-window-{suffix}",)
        assert authority.window_keys == (
            (schedule_definition_id, as_of - timedelta(minutes=5)),
        )

        resolution = authority.resolve(
            target_ref=f"workspace.{suffix}",
            schedule_kind="hourly",
        )

        assert resolution.schedule_definition_id == schedule_definition_id
        assert resolution.recurring_run_window_id == recurring_run_window_id
        assert resolution.schedule_name == f"scheduler-window-{suffix}"
        assert resolution.window_status == "active"
        assert resolution.capacity_limit == 1
        assert resolution.capacity_used == 0
        assert resolution.decision_ref == f"decision:scheduler-window:{suffix}:schedule"

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
            f"recurring_run_window.scheduler-window.{suffix}.duplicate",
            schedule_definition_id,
            as_of - timedelta(minutes=4),
            as_of + timedelta(minutes=55),
            "active",
            1,
            0,
            None,
            as_of + timedelta(minutes=1),
        )

        ambiguous_authority = await repository.load_authority(as_of=as_of)
        with pytest.raises(SchedulerWindowRepositoryError) as exc_info:
            ambiguous_authority.resolve(
                target_ref=f"workspace.{suffix}",
                schedule_kind="hourly",
            )

        assert exc_info.value.reason_code == "scheduler_window.window_ambiguous"
        assert exc_info.value.details["schedule_definition_id"] == schedule_definition_id
        assert exc_info.value.details["target_ref"] == f"workspace.{suffix}"
        assert exc_info.value.details["schedule_kind"] == "hourly"
        assert exc_info.value.details["as_of"] == as_of.isoformat()
        assert set(exc_info.value.details["recurring_run_window_ids"].split(",")) == {
            recurring_run_window_id,
            f"recurring_run_window.scheduler-window.{suffix}.duplicate",
        }
    finally:
        await transaction.rollback()
        await conn.close()
