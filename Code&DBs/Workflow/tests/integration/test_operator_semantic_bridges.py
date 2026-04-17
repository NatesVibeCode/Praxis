from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from runtime.event_log import CHANNEL_SEMANTIC_ASSERTION
from runtime.operator_object_relations import operator_object_relation_id
from runtime.semantic_assertions import semantic_assertion_id
from storage.migrations import workflow_bootstrap_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    resolve_workflow_database_url,
)
from surfaces.api import operator_write

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_operator_decision_write_bridges_scoped_decisions_only() -> None:
    asyncio.run(_exercise_operator_decision_write_bridges_scoped_decisions_only())


async def _exercise_operator_decision_write_bridges_scoped_decisions_only() -> None:
    env = _workflow_env()
    as_of = datetime(2026, 4, 16, 23, 0, tzinfo=timezone.utc)

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        for filename in (
            "010_operator_control_authority.sql",
            "082_event_log.sql",
            "124_operator_decision_scope_authority.sql",
            "126_operator_decision_scope_policy.sql",
            "146_semantic_assertion_substrate.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        await conn.execute("DELETE FROM semantic_current_assertions")
        await conn.execute("DELETE FROM semantic_assertions")
        await conn.execute("DELETE FROM semantic_predicates")
        await conn.execute("DELETE FROM event_log WHERE channel = 'semantic_assertion'")

        semantic_event_row = await conn.fetchrow(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM event_log
            WHERE channel = $1
            """,
            CHANNEL_SEMANTIC_ASSERTION,
        )
        assert semantic_event_row is not None
        starting_semantic_event_id = int(semantic_event_row["max_id"])

        scoped_payload = await operator_write.arecord_operator_decision(
            decision_key="architecture-policy::semantic-bridge::write-time",
            decision_kind="architecture_policy",
            title="Write-time semantic bridge coverage",
            rationale="Scoped operator decisions should mirror into semantic assertions.",
            decided_by="integration-tests",
            decision_source="integration-tests",
            decision_scope_kind="authority_domain",
            decision_scope_ref="semantic_bridge_write_time",
            effective_from=as_of,
            env=env,
        )
        unscoped_payload = await operator_write.arecord_operator_decision(
            decision_key="decision.query.semantic-bridge.write-time",
            decision_kind="query",
            title="Unscoped decision coverage",
            rationale="Unscoped decisions should stay out of the semantic bridge.",
            decided_by="integration-tests",
            decision_source="integration-tests",
            effective_from=as_of + timedelta(minutes=1),
            env=env,
        )

        scoped_decision_id = scoped_payload["operator_decision"]["operator_decision_id"]
        unscoped_decision_id = unscoped_payload["operator_decision"]["operator_decision_id"]
        scoped_assertion_id = semantic_assertion_id(
            predicate_slug="architecture_policy",
            subject_kind="authority_domain",
            subject_ref="semantic_bridge_write_time",
            object_kind="operator_decision",
            object_ref=scoped_decision_id,
            source_kind="operator_decision",
            source_ref=scoped_decision_id,
        )

        decision_rows = await conn.fetch(
            """
            SELECT operator_decision_id, decision_kind, decision_scope_kind, decision_scope_ref
            FROM operator_decisions
            WHERE operator_decision_id = ANY($1::text[])
            ORDER BY operator_decision_id
            """,
            [scoped_decision_id, unscoped_decision_id],
        )
        assert [(row["operator_decision_id"], row["decision_kind"]) for row in decision_rows] == [
            (scoped_decision_id, "architecture_policy"),
            (unscoped_decision_id, "query"),
        ]

        scoped_assertion_row = await conn.fetchrow(
            """
            SELECT
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                source_kind,
                source_ref
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            """,
            scoped_assertion_id,
        )
        assert scoped_assertion_row is not None
        assert scoped_assertion_row["predicate_slug"] == "architecture_policy"
        assert scoped_assertion_row["assertion_status"] == "active"
        assert scoped_assertion_row["subject_kind"] == "authority_domain"
        assert scoped_assertion_row["subject_ref"] == "semantic_bridge_write_time"
        assert scoped_assertion_row["object_ref"] == scoped_decision_id
        assert scoped_assertion_row["source_kind"] == "operator_decision"
        assert scoped_assertion_row["source_ref"] == scoped_decision_id

        current_rows = await conn.fetch(
            """
            SELECT semantic_assertion_id
            FROM semantic_current_assertions
            ORDER BY semantic_assertion_id
            """,
        )
        assert [row["semantic_assertion_id"] for row in current_rows] == [scoped_assertion_id]

        unscoped_bridge_row = await conn.fetchrow(
            """
            SELECT semantic_assertion_id
            FROM semantic_assertions
            WHERE source_kind = 'operator_decision'
              AND source_ref = $1
            LIMIT 1
            """,
            unscoped_decision_id,
        )
        assert unscoped_bridge_row is None

        semantic_events = await conn.fetch(
            """
            SELECT channel, event_type, entity_id
            FROM event_log
            WHERE channel = $1
              AND id > $2
            ORDER BY id
            """,
            CHANNEL_SEMANTIC_ASSERTION,
            starting_semantic_event_id,
        )
        assert [
            (row["channel"], row["event_type"], row["entity_id"])
            for row in semantic_events
        ] == [
            (
                CHANNEL_SEMANTIC_ASSERTION,
                "semantic_predicate_registered",
                "architecture_policy",
            ),
            (
                CHANNEL_SEMANTIC_ASSERTION,
                "semantic_assertion_recorded",
                scoped_assertion_id,
            ),
        ]
    finally:
        await conn.close()


def test_backfill_semantic_bridges_replays_legacy_operator_rows() -> None:
    asyncio.run(_exercise_backfill_semantic_bridges_replays_legacy_operator_rows())


async def _exercise_backfill_semantic_bridges_replays_legacy_operator_rows() -> None:
    env = _workflow_env()
    created_at = datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)
    updated_at = created_at + timedelta(minutes=5)
    as_of = created_at + timedelta(days=1)

    scoped_decision_id = "operator_decision.architecture_policy.legacy.semantic_graph"
    unscoped_decision_id = "operator_decision.query.legacy.read_surface"
    active_relation_id = operator_object_relation_id(
        relation_kind="grouped_in",
        source_kind="bug",
        source_ref="bug.legacy.active",
        target_kind="functional_area",
        target_ref="functional_area.checkout",
    )
    inactive_relation_id = operator_object_relation_id(
        relation_kind="grouped_in",
        source_kind="bug",
        source_ref="bug.legacy.inactive",
        target_kind="functional_area",
        target_ref="functional_area.payments",
    )
    scoped_assertion_id = semantic_assertion_id(
        predicate_slug="architecture_policy",
        subject_kind="authority_domain",
        subject_ref="semantic_graph",
        object_kind="operator_decision",
        object_ref=scoped_decision_id,
        source_kind="operator_decision",
        source_ref=scoped_decision_id,
    )
    active_relation_assertion_id = semantic_assertion_id(
        predicate_slug="grouped_in",
        subject_kind="bug",
        subject_ref="bug.legacy.active",
        object_kind="functional_area",
        object_ref="functional_area.checkout",
        source_kind="operator_object_relation",
        source_ref=active_relation_id,
    )
    inactive_relation_assertion_id = semantic_assertion_id(
        predicate_slug="grouped_in",
        subject_kind="bug",
        subject_ref="bug.legacy.inactive",
        object_kind="functional_area",
        object_ref="functional_area.payments",
        source_kind="operator_object_relation",
        source_ref=inactive_relation_id,
    )

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        for filename in (
            "010_operator_control_authority.sql",
            "082_event_log.sql",
            "124_operator_decision_scope_authority.sql",
            "126_operator_decision_scope_policy.sql",
            "134_operator_object_relations.sql",
            "146_semantic_assertion_substrate.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        await conn.execute("DELETE FROM semantic_current_assertions")
        await conn.execute("DELETE FROM semantic_assertions")
        await conn.execute("DELETE FROM semantic_predicates")
        await conn.execute("DELETE FROM event_log WHERE channel = 'semantic_assertion'")

        semantic_event_row = await conn.fetchrow(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM event_log
            WHERE channel = $1
            """,
            CHANNEL_SEMANTIC_ASSERTION,
        )
        assert semantic_event_row is not None
        starting_semantic_event_id = int(semantic_event_row["max_id"])

        await _seed_functional_area(
            conn,
            functional_area_id="functional_area.checkout",
            area_slug="checkout",
            title="Checkout",
            summary="Legacy functional area for semantic bridge replay.",
            as_of=created_at,
        )
        await _seed_functional_area(
            conn,
            functional_area_id="functional_area.payments",
            area_slug="payments",
            title="Payments",
            summary="Legacy functional area for inactive semantic bridge replay.",
            as_of=created_at,
        )
        await _seed_operator_decision(
            conn,
            operator_decision_id=scoped_decision_id,
            decision_key="architecture-policy::legacy::semantic-graph",
            decision_kind="architecture_policy",
            decision_status="decided",
            title="Legacy semantic graph policy",
            rationale="Legacy scoped operator decision for replay coverage.",
            decided_by="tests",
            decision_source="tests",
            effective_from=created_at,
            decided_at=created_at,
            created_at=created_at,
            updated_at=created_at,
            decision_scope_kind="authority_domain",
            decision_scope_ref="semantic_graph",
        )
        await _seed_operator_decision(
            conn,
            operator_decision_id=unscoped_decision_id,
            decision_key="decision.query.legacy.read-surface",
            decision_kind="query",
            decision_status="decided",
            title="Legacy query decision",
            rationale="Legacy unscoped operator decision for replay coverage.",
            decided_by="tests",
            decision_source="tests",
            effective_from=created_at,
            decided_at=created_at,
            created_at=created_at,
            updated_at=created_at,
            decision_scope_kind=None,
            decision_scope_ref=None,
        )
        await _seed_operator_object_relation(
            conn,
            operator_object_relation_id=active_relation_id,
            relation_kind="grouped_in",
            relation_status="active",
            source_kind="bug",
            source_ref="bug.legacy.active",
            target_kind="functional_area",
            target_ref="functional_area.checkout",
            relation_metadata={"origin": "legacy-replay"},
            bound_by_decision_id=scoped_decision_id,
            created_at=created_at,
            updated_at=created_at,
        )
        await _seed_operator_object_relation(
            conn,
            operator_object_relation_id=inactive_relation_id,
            relation_kind="grouped_in",
            relation_status="inactive",
            source_kind="bug",
            source_ref="bug.legacy.inactive",
            target_kind="functional_area",
            target_ref="functional_area.payments",
            relation_metadata={"origin": "legacy-replay"},
            bound_by_decision_id=None,
            created_at=created_at,
            updated_at=updated_at,
        )

        payload = await operator_write.abackfill_semantic_bridges(
            include_object_relations=True,
            include_operator_decisions=True,
            as_of=as_of,
            env=env,
        )

        assert payload["semantic_bridge_backfill"] == {
            "as_of": as_of.isoformat(),
            "object_relations": {
                "processed": 2,
                "recorded": 1,
                "retracted": 0,
                "tombstoned": 1,
            },
            "operator_decisions": {
                "processed": 2,
                "recorded": 1,
                "skipped_unscoped": 1,
            },
        }

        active_relation_row = await conn.fetchrow(
            """
            SELECT assertion_status, bound_decision_id
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            """,
            active_relation_assertion_id,
        )
        assert active_relation_row is not None
        assert active_relation_row["assertion_status"] == "active"
        assert active_relation_row["bound_decision_id"] == scoped_decision_id

        inactive_relation_row = await conn.fetchrow(
            """
            SELECT assertion_status, valid_to
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            """,
            inactive_relation_assertion_id,
        )
        assert inactive_relation_row is not None
        assert inactive_relation_row["assertion_status"] == "retracted"
        assert inactive_relation_row["valid_to"] == updated_at

        scoped_decision_row = await conn.fetchrow(
            """
            SELECT assertion_status, subject_kind, subject_ref, object_ref
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            """,
            scoped_assertion_id,
        )
        assert scoped_decision_row is not None
        assert scoped_decision_row["assertion_status"] == "active"
        assert scoped_decision_row["subject_kind"] == "authority_domain"
        assert scoped_decision_row["subject_ref"] == "semantic_graph"
        assert scoped_decision_row["object_ref"] == scoped_decision_id

        unscoped_bridge_row = await conn.fetchrow(
            """
            SELECT semantic_assertion_id
            FROM semantic_assertions
            WHERE source_kind = 'operator_decision'
              AND source_ref = $1
            LIMIT 1
            """,
            unscoped_decision_id,
        )
        assert unscoped_bridge_row is None

        current_rows = await conn.fetch(
            """
            SELECT semantic_assertion_id
            FROM semantic_current_assertions
            ORDER BY semantic_assertion_id
            """,
        )
        assert {row["semantic_assertion_id"] for row in current_rows} == {
            active_relation_assertion_id,
            scoped_assertion_id,
        }

        semantic_events = await conn.fetch(
            """
            SELECT event_type, entity_id
            FROM event_log
            WHERE channel = $1
              AND id > $2
            ORDER BY id
            """,
            CHANNEL_SEMANTIC_ASSERTION,
            starting_semantic_event_id,
        )
        assert [(row["event_type"], row["entity_id"]) for row in semantic_events] == [
            ("semantic_predicate_registered", "grouped_in"),
            ("semantic_assertion_recorded", active_relation_assertion_id),
            ("semantic_assertion_recorded", inactive_relation_assertion_id),
            ("semantic_predicate_registered", "architecture_policy"),
            ("semantic_assertion_recorded", scoped_assertion_id),
            ("semantic_bridge_backfilled", "operator_control"),
        ]
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
                if getattr(exc, "sqlstate", None) in {"42P07", "42701", "42710"}:
                    continue
                raise


async def _seed_functional_area(
    conn,
    *,
    functional_area_id: str,
    area_slug: str,
    title: str,
    summary: str,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO functional_areas (
            functional_area_id,
            area_slug,
            title,
            area_status,
            summary,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, 'active', $4, $5, $6
        )
        ON CONFLICT (functional_area_id) DO UPDATE SET
            area_slug = EXCLUDED.area_slug,
            title = EXCLUDED.title,
            area_status = EXCLUDED.area_status,
            summary = EXCLUDED.summary,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        functional_area_id,
        area_slug,
        title,
        summary,
        as_of,
        as_of,
    )


async def _seed_operator_decision(
    conn,
    *,
    operator_decision_id: str,
    decision_key: str,
    decision_kind: str,
    decision_status: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
    effective_from: datetime,
    decided_at: datetime,
    created_at: datetime,
    updated_at: datetime,
    decision_scope_kind: str | None,
    decision_scope_ref: str | None,
) -> None:
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
            updated_at,
            decision_scope_kind,
            decision_scope_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12, $13, $14
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
            effective_to = EXCLUDED.effective_to,
            decided_at = EXCLUDED.decided_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            decision_scope_kind = EXCLUDED.decision_scope_kind,
            decision_scope_ref = EXCLUDED.decision_scope_ref
        """,
        operator_decision_id,
        decision_key,
        decision_kind,
        decision_status,
        title,
        rationale,
        decided_by,
        decision_source,
        effective_from,
        decided_at,
        created_at,
        updated_at,
        decision_scope_kind,
        decision_scope_ref,
    )


async def _seed_operator_object_relation(
    conn,
    *,
    operator_object_relation_id: str,
    relation_kind: str,
    relation_status: str,
    source_kind: str,
    source_ref: str,
    target_kind: str,
    target_ref: str,
    relation_metadata: dict[str, object],
    bound_by_decision_id: str | None,
    created_at: datetime,
    updated_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_object_relations (
            operator_object_relation_id,
            relation_kind,
            relation_status,
            source_kind,
            source_ref,
            target_kind,
            target_ref,
            relation_metadata,
            bound_by_decision_id,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
        )
        ON CONFLICT (operator_object_relation_id) DO UPDATE SET
            relation_kind = EXCLUDED.relation_kind,
            relation_status = EXCLUDED.relation_status,
            source_kind = EXCLUDED.source_kind,
            source_ref = EXCLUDED.source_ref,
            target_kind = EXCLUDED.target_kind,
            target_ref = EXCLUDED.target_ref,
            relation_metadata = EXCLUDED.relation_metadata,
            bound_by_decision_id = EXCLUDED.bound_by_decision_id,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        operator_object_relation_id,
        relation_kind,
        relation_status,
        source_kind,
        source_ref,
        target_kind,
        target_ref,
        json.dumps(relation_metadata),
        bound_by_decision_id,
        created_at,
        updated_at,
    )


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for operator semantic bridge integration tests: "
            f"{exc.reason_code}"
        )
    return {"WORKFLOW_DATABASE_URL": database_url}
