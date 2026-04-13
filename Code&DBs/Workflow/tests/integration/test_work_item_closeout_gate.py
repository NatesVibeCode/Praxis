from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from storage.migrations import workflow_migration_statements
from storage.postgres import PostgresConfigurationError, connect_workflow_database
from surfaces.api import operator_write

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741003


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 9, 16, 0, tzinfo=timezone.utc)


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


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


async def _seed_bug(conn, *, bug_id: str, suffix: str, clock: datetime) -> None:
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
            resolution_summary,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, 'OPEN', 'high', 'p1', $4, 'test', $5, NULL, $6, NULL, $7, $8
        )
        """,
        bug_id,
        f"bug-key.{suffix}",
        f"Closeout bug {suffix}",
        "Bug seeded for work-item closeout testing.",
        f"decision.{suffix}.closeout",
        clock,
        clock,
        clock,
    )


async def _seed_bug_evidence(
    conn,
    *,
    bug_id: str,
    suffix: str,
    clock: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO bug_evidence_links (
            bug_evidence_link_id,
            bug_id,
            evidence_kind,
            evidence_ref,
            evidence_role,
            created_at,
            created_by,
            notes
        ) VALUES (
            $1, $2, 'receipt', $3, 'validates_fix', $4, 'test.work_item_closeout_gate', $5
        )
        """,
        f"bug_evidence_link.{suffix}.1",
        bug_id,
        f"receipt.{suffix}.fix",
        clock,
        "Explicit proof row for auto-close testing.",
    )


async def _seed_roadmap_item(
    conn,
    *,
    roadmap_item_id: str,
    bug_id: str,
    suffix: str,
    clock: datetime,
) -> None:
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
            $1, $2, $3, 'initiative', 'active', 'p1', NULL, $4, $5, $6::jsonb, $7, NULL, NULL, NULL, $8, $9
        )
        """,
        roadmap_item_id,
        f"roadmap.{suffix}",
        f"Closeout roadmap {suffix}",
        bug_id,
        "Roadmap item seeded for proof-backed closeout testing.",
        json.dumps(
            {"must_have": ["explicit-proof", "linked-source-bug"]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        f"decision.{suffix}.roadmap",
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


def test_work_item_closeout_gate_previews_and_commits_from_explicit_fix_proof() -> None:
    asyncio.run(
        _exercise_work_item_closeout_gate_previews_and_commits_from_explicit_fix_proof()
    )


async def _exercise_work_item_closeout_gate_previews_and_commits_from_explicit_fix_proof() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(
            env={"WORKFLOW_DATABASE_URL": database_url},
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for work-item closeout integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")

        suffix = _unique_suffix()
        clock = _fixed_clock()
        bug_id = f"bug.{suffix}.closeout"
        roadmap_item_id = f"roadmap_item.{suffix}.closeout"

        await _seed_bug(conn, bug_id=bug_id, suffix=suffix, clock=clock)
        await _seed_bug_evidence(conn, bug_id=bug_id, suffix=suffix, clock=clock)
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=roadmap_item_id,
            bug_id=bug_id,
            suffix=suffix,
            clock=clock,
        )

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=lambda env=None: asyncio.sleep(0, result=_ConnectionProxy(conn)),
        )

        preview = await frontdoor.reconcile_work_item_closeout_async(
            action="preview",
            bug_ids=[bug_id],
        )

        assert preview["committed"] is False
        assert preview["proof_threshold"]["bug_requires_evidence_role"] == "validates_fix"
        assert [candidate["bug_id"] for candidate in preview["candidates"]["bugs"]] == [bug_id]
        assert [
            candidate["roadmap_item_id"]
            for candidate in preview["candidates"]["roadmap_items"]
        ] == [roadmap_item_id]
        assert preview["applied"] == {"bugs": [], "roadmap_items": []}

        committed = await frontdoor.reconcile_work_item_closeout_async(
            action="commit",
            bug_ids=[bug_id],
        )

        assert committed["committed"] is True
        assert [row["bug_id"] for row in committed["applied"]["bugs"]] == [bug_id]
        assert [
            row["roadmap_item_id"] for row in committed["applied"]["roadmap_items"]
        ] == [roadmap_item_id]

        bug_row = await conn.fetchrow(
            """
            SELECT status, resolved_at, resolution_summary
            FROM bugs
            WHERE bug_id = $1
            """,
            bug_id,
        )
        assert bug_row is not None
        assert str(bug_row["status"]) == "FIXED"
        assert bug_row["resolved_at"] is not None
        assert "validates_fix proof" in str(bug_row["resolution_summary"])

        roadmap_row = await conn.fetchrow(
            """
            SELECT status, completed_at
            FROM roadmap_items
            WHERE roadmap_item_id = $1
            """,
            roadmap_item_id,
        )
        assert roadmap_row is not None
        assert str(roadmap_row["status"]) == "completed"
        assert roadmap_row["completed_at"] is not None
    finally:
        await transaction.rollback()
        await conn.close()
