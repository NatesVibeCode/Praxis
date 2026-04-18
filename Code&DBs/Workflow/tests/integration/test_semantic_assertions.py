from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from _pg_test_conn import ensure_test_database_ready
from storage.migrations import workflow_bootstrap_migration_statements
from storage.postgres import (
    bootstrap_control_plane_schema,
    connect_workflow_database,
)
from surfaces.api import semantic_assertions
from surfaces.api.semantic_assertions import SemanticAssertionFrontdoor
from runtime.semantic_projection_subscriber import aconsume_semantic_projection_events

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_TEST_DATABASE_URL = ensure_test_database_ready()


def test_semantic_assertions_frontdoor_persists_events_and_projection() -> None:
    asyncio.run(_exercise_semantic_assertions_frontdoor_persists_events_and_projection())


async def _exercise_semantic_assertions_frontdoor_persists_events_and_projection() -> None:
    env = _workflow_env()
    first_as_of = datetime(2026, 4, 16, 20, 0, tzinfo=timezone.utc)
    second_as_of = first_as_of + timedelta(minutes=10)
    third_as_of = second_as_of + timedelta(minutes=10)

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        for filename in (
            "010_operator_control_authority.sql",
            "082_event_log.sql",
            "146_semantic_assertion_substrate.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        await conn.execute("DELETE FROM semantic_current_assertions")
        await conn.execute("DELETE FROM semantic_assertions")
        await conn.execute("DELETE FROM semantic_predicates")
        await conn.execute("DELETE FROM event_log WHERE channel = 'semantic_assertion'")

        predicate_payload = await semantic_assertions.aregister_predicate(
            predicate_slug="grouped_in",
            subject_kind_allowlist=("bug",),
            object_kind_allowlist=("functional_area",),
            cardinality_mode="single_active_per_subject",
            description="One bug belongs to one functional area at a time.",
            created_at=first_as_of,
            updated_at=first_as_of,
            env=env,
        )
        first_payload = await semantic_assertions.arecord_assertion(
            predicate_slug="grouped_in",
            subject_kind="bug",
            subject_ref="bug.semantic.checkout",
            object_kind="functional_area",
            object_ref="functional_area.checkout",
            qualifiers_json={"confidence": 0.9},
            source_kind="operator",
            source_ref="tests",
            valid_from=first_as_of,
            created_at=first_as_of,
            updated_at=first_as_of,
            env=env,
        )
        second_payload = await semantic_assertions.arecord_assertion(
            predicate_slug="grouped_in",
            subject_kind="bug",
            subject_ref="bug.semantic.checkout",
            object_kind="functional_area",
            object_ref="functional_area.payments",
            qualifiers_json={"confidence": 1.0},
            source_kind="operator",
            source_ref="tests",
            valid_from=second_as_of,
            created_at=second_as_of,
            updated_at=second_as_of,
            env=env,
        )

        current_payload = await semantic_assertions.alist_assertions(
            predicate_slug="grouped_in",
            subject_ref="bug.semantic.checkout",
            env=env,
        )
        historical_payload = await semantic_assertions.alist_assertions(
            predicate_slug="grouped_in",
            subject_ref="bug.semantic.checkout",
            as_of=first_as_of + timedelta(minutes=5),
            env=env,
        )

        retracted_payload = await semantic_assertions.aretract_assertion(
            semantic_assertion_id=second_payload["semantic_assertion"]["semantic_assertion_id"],
            retracted_at=third_as_of,
            updated_at=third_as_of,
            env=env,
        )
        rebuilt_payload = await SemanticAssertionFrontdoor().rebuild_current_projection_async(
            as_of=third_as_of + timedelta(minutes=1),
            env=env,
        )
        current_after_retract = await semantic_assertions.alist_assertions(
            predicate_slug="grouped_in",
            subject_ref="bug.semantic.checkout",
            env=env,
        )

        predicate_row = await conn.fetchrow(
            """
            SELECT predicate_slug, cardinality_mode
            FROM semantic_predicates
            WHERE predicate_slug = 'grouped_in'
            """,
        )
        assert predicate_row is not None
        assert predicate_row["predicate_slug"] == "grouped_in"
        assert predicate_row["cardinality_mode"] == "single_active_per_subject"

        first_row = await conn.fetchrow(
            """
            SELECT assertion_status, valid_to
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            """,
            first_payload["semantic_assertion"]["semantic_assertion_id"],
        )
        assert first_row is not None
        assert first_row["assertion_status"] == "superseded"
        assert first_row["valid_to"] == second_as_of

        current_rows = await conn.fetch(
            """
            SELECT semantic_assertion_id
            FROM semantic_current_assertions
            ORDER BY semantic_assertion_id
            """,
        )
        assert current_rows == []

        event_rows = await conn.fetch(
            """
            SELECT channel, event_type, entity_kind
            FROM event_log
            WHERE channel = 'semantic_assertion'
            ORDER BY id
            """,
        )
        assert [row["event_type"] for row in event_rows] == [
            "semantic_predicate_registered",
            "semantic_assertion_recorded",
            "semantic_assertion_recorded",
            "semantic_assertion_retracted",
            "semantic_projection_rebuilt",
        ]
        assert all(row["entity_kind"] for row in event_rows)

        assert predicate_payload["semantic_predicate"]["predicate_slug"] == "grouped_in"
        assert first_payload["semantic_assertion"]["object"]["ref"] == "functional_area.checkout"
        assert second_payload["superseded_assertions"][0]["semantic_assertion_id"] == (
            first_payload["semantic_assertion"]["semantic_assertion_id"]
        )
        assert current_payload["projection_source"] == "semantic_current_assertions"
        assert current_payload["semantic_assertions"][0]["object"]["ref"] == "functional_area.payments"
        assert historical_payload["projection_source"] == "semantic_assertions"
        assert historical_payload["semantic_assertions"][0]["object"]["ref"] == "functional_area.checkout"
        assert retracted_payload["semantic_assertion"]["assertion_status"] == "retracted"
        assert rebuilt_payload["row_count"] == 0
        assert current_after_retract["semantic_assertions"] == []
    finally:
        await conn.close()


def test_semantic_projection_subscriber_rebuilds_projection_from_event_cursors() -> None:
    asyncio.run(_exercise_semantic_projection_subscriber_rebuilds_projection_from_event_cursors())


async def _exercise_semantic_projection_subscriber_rebuilds_projection_from_event_cursors() -> None:
    env = _workflow_env()
    recorded_at = datetime(2026, 4, 16, 21, 0, tzinfo=timezone.utc)
    refresh_as_of = recorded_at + timedelta(minutes=5)

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        for filename in (
            "010_operator_control_authority.sql",
            "082_event_log.sql",
            "146_semantic_assertion_substrate.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        await conn.execute("DELETE FROM semantic_current_assertions")
        await conn.execute("DELETE FROM semantic_assertions")
        await conn.execute("DELETE FROM semantic_predicates")
        await conn.execute("DELETE FROM event_log WHERE channel = 'semantic_assertion'")
        await conn.execute(
            """
            DELETE FROM event_log_cursors
            WHERE subscriber_id = 'semantic_projection_refresher'
              AND channel = 'semantic_assertion'
            """
        )

        await semantic_assertions.aregister_predicate(
            predicate_slug="grouped_in",
            subject_kind_allowlist=("bug",),
            object_kind_allowlist=("functional_area",),
            cardinality_mode="single_active_per_subject",
            description="One bug belongs to one functional area at a time.",
            created_at=recorded_at,
            updated_at=recorded_at,
            env=env,
        )
        recorded_payload = await semantic_assertions.arecord_assertion(
            predicate_slug="grouped_in",
            subject_kind="bug",
            subject_ref="bug.semantic.cursor",
            object_kind="functional_area",
            object_ref="functional_area.checkout",
            qualifiers_json={"confidence": 1.0},
            source_kind="operator",
            source_ref="tests",
            valid_from=recorded_at,
            created_at=recorded_at,
            updated_at=recorded_at,
            env=env,
        )

        await conn.execute("DELETE FROM semantic_current_assertions")

        refreshed = await aconsume_semantic_projection_events(
            limit=20,
            as_of=refresh_as_of,
            env=env,
        )

        current_rows = await conn.fetch(
            """
            SELECT semantic_assertion_id, object_ref
            FROM semantic_current_assertions
            ORDER BY semantic_assertion_id
            """,
        )
        cursor_row = await conn.fetchrow(
            """
            SELECT last_event_id
            FROM event_log_cursors
            WHERE subscriber_id = 'semantic_projection_refresher'
              AND channel = 'semantic_assertion'
            """
        )
        projection_event = await conn.fetchrow(
            """
            SELECT event_type, entity_id, emitted_by, payload
            FROM event_log
            WHERE channel = 'semantic_assertion'
              AND event_type = 'semantic_projection_rebuilt'
            ORDER BY id DESC
            LIMIT 1
            """
        )

        assert refreshed["subscriber_id"] == "semantic_projection_refresher"
        assert refreshed["refreshed"] is True
        assert refreshed["scanned_count"] == 2
        assert refreshed["relevant_count"] == 1
        assert refreshed["row_count"] == 1
        assert refreshed["projection_event_id"] is not None
        assert [
            (row["semantic_assertion_id"], row["object_ref"])
            for row in current_rows
        ] == [
            (
                recorded_payload["semantic_assertion"]["semantic_assertion_id"],
                "functional_area.checkout",
            )
        ]
        assert cursor_row is not None
        assert cursor_row["last_event_id"] == refreshed["ending_cursor"]
        assert projection_event is not None
        assert projection_event["entity_id"] == "semantic_current_assertions"
        assert projection_event["event_type"] == "semantic_projection_rebuilt"
        assert projection_event["emitted_by"] == "semantic_projection_subscriber.consume"
        projection_payload = projection_event["payload"]
        if isinstance(projection_payload, str):
            projection_payload = json.loads(projection_payload)
        assert projection_payload["subscriber_id"] == "semantic_projection_refresher"
    finally:
        await conn.close()


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_bootstrap_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in {"42P07", "42710"}:
                    continue
                raise


def _workflow_env() -> dict[str, str]:
    return {"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL}
