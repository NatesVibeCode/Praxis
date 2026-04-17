from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from storage.migrations import (
    workflow_bootstrap_migration_statements,
    workflow_migration_statements,
)
from storage.postgres import PostgresConfigurationError, connect_workflow_database
from surfaces.api import operator_write

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741002


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


async def _bootstrap_migration(conn, filename: str) -> None:
    statements = (
        workflow_bootstrap_migration_statements(filename)
        if filename == "082_event_log.sql"
        else workflow_migration_statements(filename)
    )
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in statements:
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise


async def _seed_roadmap_item(
    conn,
    *,
    roadmap_item_id: str,
    title: str,
    parent_roadmap_item_id: str | None,
    phase_order: str,
) -> None:
    clock = _fixed_clock()
    await conn.execute(
        """
        INSERT INTO roadmap_items (
            roadmap_item_id,
            roadmap_key,
            title,
            item_kind,
            status,
            priority,
            parent_roadmap_item_id,
            source_bug_id,
            summary,
            acceptance_criteria,
            decision_ref,
            target_start_at,
            target_end_at,
            completed_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NULL, $8, $9::jsonb, $10, NULL, NULL, NULL, $11, $12
        )
        """,
        roadmap_item_id,
        roadmap_item_id.replace("roadmap_item.", "roadmap."),
        title,
        "capability",
        "active",
        "p1",
        parent_roadmap_item_id,
        title,
        json.dumps(
            {
                "tier": "tier_1",
                "phase_ready": False,
                "approval_tag": "seeded-test",
                "outcome_gate": title,
                "phase_order": phase_order,
            },
            sort_keys=True,
        ),
        f"decision.test.{roadmap_item_id.rsplit('.', 1)[-1]}",
        clock,
        clock,
    )


class _ConnectionProxy:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def execute(self, query: str, *args: object):
        return await self._conn.execute(query, *args)

    async def fetch(self, query: str, *args: object):
        return await self._conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args: object):
        return await self._conn.fetchrow(query, *args)

    def transaction(self):
        return self._conn.transaction()

    async def close(self) -> None:
        return None


def test_roadmap_write_gate_previews_and_commits_package() -> None:
    asyncio.run(_exercise_roadmap_write_gate_previews_and_commits_package())


async def _exercise_roadmap_write_gate_previews_and_commits_package() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(
            env={"WORKFLOW_DATABASE_URL": database_url},
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for roadmap write gate integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_migration(conn, "082_event_log.sql")
        await _bootstrap_migration(conn, "146_semantic_assertion_substrate.sql")

        suffix = _unique_suffix()
        parent_id = f"roadmap_item.test.operator_write.{suffix}"
        blocker_id = f"{parent_id}.validation_review"

        await _seed_roadmap_item(
            conn,
            roadmap_item_id=parent_id,
            title=f"Parent roadmap {suffix}",
            parent_roadmap_item_id=None,
            phase_order="1",
        )
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=blocker_id,
            title=f"Validation review {suffix}",
            parent_roadmap_item_id=parent_id,
            phase_order="1.1",
        )

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=lambda env=None: asyncio.sleep(0, result=_ConnectionProxy(conn)),
        )
        preview = await frontdoor.roadmap_write_async(
            action="preview",
            title="Unified operator write validation gate",
            intent_brief="Single preview-first validation gate for roadmap and operator writes",
            template="hard_cutover_program",
            priority="p1",
            parent_roadmap_item_id=parent_id,
            depends_on=[blocker_id],
        )

        assert preview["committed"] is False
        assert preview["blocking_errors"] == []
        assert preview["normalized_payload"]["template"] == "hard_cutover_program"
        assert preview["normalized_payload"]["parent_roadmap_item_id"] == parent_id
        assert preview["normalized_payload"]["depends_on"] == [blocker_id]
        assert preview["auto_fixes"][0] == (
            "slug generated from title: unified.operator.write.validation.gate"
        )
        assert preview["auto_fixes"][1].startswith(
            "approval_tag generated: operator-write-"
        )
        assert preview["auto_fixes"][2].endswith(
            ".unified-operator-write-validation-gate"
        )
        assert preview["auto_fixes"][3] == "phase_order assigned: 1.2"
        assert len(preview["preview"]["roadmap_items"]) == 6
        assert preview["preview"]["roadmap_items"][0]["roadmap_item_id"].startswith(
            f"{parent_id}.unified.operator.write.validation.gate"
        )
        assert preview["preview"]["roadmap_item_dependencies"][0][
            "depends_on_roadmap_item_id"
        ] == blocker_id

        committed = await frontdoor.roadmap_write_async(
            action="commit",
            title="Unified operator write validation gate",
            intent_brief="Single preview-first validation gate for roadmap and operator writes",
            template="hard_cutover_program",
            priority="p1",
            parent_roadmap_item_id=parent_id,
            depends_on=[blocker_id],
        )

        assert committed["committed"] is True
        created_item_ids = committed["commit_summary"]["roadmap_item_ids"]
        created_dependency_ids = committed["commit_summary"]["roadmap_item_dependency_ids"]
        assert len(created_item_ids) == 6
        assert len(created_dependency_ids) == 6
        assert committed["semantic_bridge_summary"]["processed"] == len(created_item_ids)
        assert committed["semantic_bridge_summary"]["retracted"] == 0

        rows = await conn.fetch(
            """
            SELECT roadmap_item_id
            FROM roadmap_items
            WHERE roadmap_item_id = ANY($1::text[])
            ORDER BY roadmap_item_id
            """,
            created_item_ids,
        )
        assert [str(row["roadmap_item_id"]) for row in rows] == sorted(created_item_ids)

        dependency_rows = await conn.fetch(
            """
            SELECT roadmap_item_dependency_id
            FROM roadmap_item_dependencies
            WHERE roadmap_item_dependency_id = ANY($1::text[])
            ORDER BY roadmap_item_dependency_id
            """,
            created_dependency_ids,
        )
        assert [str(row["roadmap_item_dependency_id"]) for row in dependency_rows] == sorted(
            created_dependency_ids
        )

        semantic_rows = await conn.fetch(
            """
            SELECT source_ref, predicate_slug, object_ref
            FROM semantic_assertions
            WHERE source_kind = 'roadmap_item'
              AND source_ref = ANY($1::text[])
            ORDER BY source_ref, predicate_slug, object_ref
            """,
            created_item_ids,
        )
        assert {str(row["source_ref"]) for row in semantic_rows} == set(created_item_ids)
        assert committed["semantic_bridge_summary"]["recorded"] == len(semantic_rows)
    finally:
        await transaction.rollback()
        await conn.close()
