from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.work_item_workflow_bindings import (
    PostgresWorkItemWorkflowBindingRepository,
    load_work_item_workflow_binding,
    project_work_item_workflow_binding,
    work_item_workflow_binding_id,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    resolve_workflow_database_url,
)
from surfaces.api import operator_write

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_work_item_workflow_bindings_record_bug_to_workflow_class_binding_is_canonical_and_persisted() -> None:
    asyncio.run(_exercise_work_item_workflow_bindings_record_bug_to_workflow_class_binding_is_canonical_and_persisted())


async def _exercise_work_item_workflow_bindings_record_bug_to_workflow_class_binding_is_canonical_and_persisted() -> None:
    env = _workflow_env()
    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _bootstrap_workflow_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_workflow_migration(conn, "010_operator_control_authority.sql")

        await _seed_workflow_lane(conn, as_of=as_of)
        await _seed_workflow_class(conn, as_of=as_of)
        await _seed_bug(conn, as_of=as_of)
        await _seed_operator_decision(conn, as_of=as_of)

        binding_kwargs = {
            "binding_kind": "governed_by",
            "bug_id": "bug.dispatch-binding.1",
            "workflow_class_id": "workflow_class.review.binding",
            "binding_status": "active",
            "bound_by_decision_id": "operator_decision.dispatch-binding.1",
            "created_at": as_of,
            "updated_at": as_of,
            "env": env,
        }

        first_payload = await operator_write.arecord_work_item_workflow_binding(
            **binding_kwargs,
        )
        second_payload = await operator_write.arecord_work_item_workflow_binding(
            **binding_kwargs,
        )
        binding_id = work_item_workflow_binding_id(
            binding_kind=binding_kwargs["binding_kind"],
            bug_id=binding_kwargs["bug_id"],
            workflow_class_id=binding_kwargs["workflow_class_id"],
        )

        assert first_payload == second_payload
        assert first_payload["binding"]["work_item_workflow_binding_id"] == binding_id
        assert first_payload["binding"]["source"] == {
            "kind": "bug",
            "id": "bug.dispatch-binding.1",
            "bug_id": "bug.dispatch-binding.1",
        }
        assert first_payload["binding"]["targets"] == {
            "workflow_class_id": "workflow_class.review.binding",
        }
        assert first_payload["binding"]["bound_by_decision_id"] == "operator_decision.dispatch-binding.1"

        row = await conn.fetchrow(
            """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                bug_id,
                roadmap_item_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
            WHERE work_item_workflow_binding_id = $1
            """,
            binding_id,
        )
        assert row is not None
        assert row["work_item_workflow_binding_id"] == binding_id
        assert row["binding_kind"] == "governed_by"
        assert row["binding_status"] == "active"
        assert row["bug_id"] == "bug.dispatch-binding.1"
        assert row["workflow_class_id"] == "workflow_class.review.binding"
        assert row["bound_by_decision_id"] == "operator_decision.dispatch-binding.1"
        assert row["updated_at"] == as_of

        repository = PostgresWorkItemWorkflowBindingRepository(conn)
        loaded = await repository.load_binding(work_item_workflow_binding_id=binding_id)
        assert loaded is not None
        assert loaded == await load_work_item_workflow_binding(
            conn,
            work_item_workflow_binding_id=binding_id,
        )
        assert project_work_item_workflow_binding(first_payload["binding"]) == loaded
        assert loaded.work_item_workflow_binding_id == binding_id
        assert loaded.source_kind == "bug"
        assert loaded.source_id == "bug.dispatch-binding.1"
        assert loaded.target_refs == {
            "workflow_class_id": "workflow_class.review.binding",
        }

        duplicate_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM work_item_workflow_bindings
            WHERE work_item_workflow_binding_id = $1
            """,
            binding_id,
        )
        assert duplicate_count == 1
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
            except Exception as exc:  # pragma: no cover - fail closed in integration setup
                sqlstate = getattr(exc, "sqlstate", None)
                if sqlstate in {"42P07", "42710"}:
                    continue
                raise


async def _seed_workflow_lane(conn, *, as_of: datetime) -> None:
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
        "workflow_lane.review.binding",
        "review-binding",
        "review",
        "active",
        1,
        "manual",
        True,
        '{"max_attempts": 1}',
        as_of,
        None,
        as_of,
    )


async def _seed_workflow_class(conn, *, as_of: datetime) -> None:
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
        "workflow_class.review.binding",
        "review",
        "review",
        "workflow_lane.review.binding",
        "active",
        '{"shape":"single-run"}',
        '{"max_attempts":1}',
        False,
        as_of,
        None,
        "decision:workflow-class:review-binding",
        as_of,
    )


async def _seed_bug(conn, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO bugs (
            bug_id,
            bug_key,
            title,
            status,
            severity,
            priority,
            summary,
            source_kind,
            decision_ref,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12
        )
        ON CONFLICT (bug_id) DO UPDATE SET
            bug_key = EXCLUDED.bug_key,
            title = EXCLUDED.title,
            status = EXCLUDED.status,
            severity = EXCLUDED.severity,
            priority = EXCLUDED.priority,
            summary = EXCLUDED.summary,
            source_kind = EXCLUDED.source_kind,
            decision_ref = EXCLUDED.decision_ref,
            opened_at = EXCLUDED.opened_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        "bug.dispatch-binding.1",
        "bug-key.dispatch-binding.1",
        "Binding test bug",
        "open",
        "medium",
        "p2",
        "Work item binding test bug",
        "manual",
        "decision:bug:dispatch-binding.1",
        as_of,
        as_of,
        as_of,
    )


async def _seed_operator_decision(conn, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO operator_decisions (
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            title,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12
        )
        ON CONFLICT (operator_decision_id) DO UPDATE SET
            decision_key = EXCLUDED.decision_key,
            decision_kind = EXCLUDED.decision_kind,
            decision_status = EXCLUDED.decision_status,
            title = EXCLUDED.title,
            rationale = EXCLUDED.rationale,
            decided_by = EXCLUDED.decided_by,
            decision_source = EXCLUDED.decision_source,
            effective_from = EXCLUDED.effective_from,
            decided_at = EXCLUDED.decided_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        "operator_decision.dispatch-binding.1",
        "decision:binding:dispatch-binding.1",
        "binding",
        "recorded",
        "Binding test decision",
        "Authorize one explicit work-item binding",
        "operator",
        "manual",
        as_of,
        as_of,
        as_of,
        as_of,
    )


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for the work-item dispatch binding integration test: "
            f"{exc.reason_code}"
        )
    return {"WORKFLOW_DATABASE_URL": database_url}
