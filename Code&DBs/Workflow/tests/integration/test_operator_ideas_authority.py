from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

from _pg_test_conn import bootstrap_workflow_migration, ensure_test_database_ready
from storage.postgres import connect_workflow_database
from surfaces.api import operator_write

_SCHEMA_BOOTSTRAP_LOCK_ID = 741195
_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 21, 5, 20, tzinfo=timezone.utc)


async def _bootstrap_migration(conn, filename: str) -> None:
    await bootstrap_workflow_migration(
        conn,
        filename,
        schema_bootstrap_lock_id=_SCHEMA_BOOTSTRAP_LOCK_ID,
    )


async def _seed_roadmap_item(conn, *, roadmap_item_id: str, clock: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO roadmap_items (
            roadmap_item_id,
            roadmap_key,
            title,
            item_kind,
            status,
            lifecycle,
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
            $1, $2, 'Idea promotion target', 'capability', 'active', 'planned',
            'p2', NULL, NULL, 'Roadmap item promoted from an idea.',
            $3::jsonb, $4, NULL, NULL, NULL, $5, $5
        )
        """,
        roadmap_item_id,
        roadmap_item_id.replace("roadmap_item.", "roadmap."),
        json.dumps(
            {
                "tier": "tier_1",
                "phase_ready": False,
                "approval_tag": "operator-ideas-test",
                "outcome_gate": "Roadmap item promoted from an idea.",
                "phase_order": "1",
            },
            sort_keys=True,
        ),
        f"decision.{roadmap_item_id}",
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


def test_operator_ideas_file_resolve_and_promote_are_pre_commitment_authority() -> None:
    asyncio.run(_exercise_operator_ideas_authority())


async def _exercise_operator_ideas_authority() -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL},
    )

    transaction = conn.transaction()
    await transaction.start()
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_migration(conn, "136_operation_catalog_authority.sql")
        await _bootstrap_migration(conn, "195_operator_ideas_authority.sql")

        suffix = _unique_suffix()
        clock = _fixed_clock()
        roadmap_item_id = f"roadmap_item.operator_ideas.{suffix}"
        await _seed_roadmap_item(conn, roadmap_item_id=roadmap_item_id, clock=clock)

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=lambda env=None: asyncio.sleep(0, result=_ConnectionProxy(conn)),
        )

        filed = await frontdoor.operator_ideas_async(
            action="file",
            title=f"Try a risky pre-commitment idea {suffix}",
            summary="Explore an idea before it becomes roadmap commitment.",
            source_kind="conversation",
            source_ref=f"conversation.{suffix}",
            opened_at=clock,
            created_at=clock,
            updated_at=clock,
        )
        idea = filed["idea"]
        assert idea["status"] == "open"
        assert idea["idea_id"].startswith("operator_idea.")

        rejected = await frontdoor.operator_ideas_async(
            action="resolve",
            idea_id=idea["idea_id"],
            status="rejected",
            resolution_summary="Rejected before roadmap commitment.",
            resolved_at=clock,
        )
        assert rejected["idea"]["status"] == "rejected"
        assert rejected["idea"]["resolution_summary"] == "Rejected before roadmap commitment."

        promoted_source = await frontdoor.operator_ideas_async(
            action="file",
            title=f"Promote an accepted pre-commitment idea {suffix}",
            summary="This idea is ready to feed a roadmap item.",
            opened_at=clock,
            created_at=clock,
            updated_at=clock,
        )
        promoted = await frontdoor.operator_ideas_async(
            action="promote",
            idea_id=promoted_source["idea"]["idea_id"],
            roadmap_item_id=roadmap_item_id,
            promoted_by="operator_ideas_test",
            promoted_at=clock,
        )

        assert promoted["idea"]["status"] == "promoted"
        assert promoted["promotion"]["roadmap_item_id"] == roadmap_item_id
        assert promoted["roadmap_item"]["source_idea_id"] == promoted_source["idea"]["idea_id"]

        roadmap_statuses = await conn.fetch(
            "SELECT DISTINCT status FROM roadmap_items WHERE roadmap_item_id = $1",
            roadmap_item_id,
        )
        assert {str(row["status"]) for row in roadmap_statuses} == {"active"}
        assert "canceled" not in {str(row["status"]) for row in roadmap_statuses}

        blocked_preview = await frontdoor.roadmap_write_async(
            action="preview",
            title=f"Legacy roadmap idea lane should not be used {suffix}",
            intent_brief="This should stay in operator_ideas until committed.",
            lifecycle="idea",
        )
        assert blocked_preview["committed"] is False
        assert blocked_preview["blocking_errors"] == [
            "roadmap lifecycle 'idea' is retired for new roadmap writes; "
            "record pre-commitment work through praxis_operator_ideas and "
            "promote it into roadmap when committed"
        ]
    finally:
        await transaction.rollback()
        await conn.close()
