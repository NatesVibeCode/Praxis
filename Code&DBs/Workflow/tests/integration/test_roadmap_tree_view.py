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
from surfaces.api import operator_read, operator_write
from surfaces.api._operator_repository import _render_roadmap_tree_markdown

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741003


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc)


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


def _is_postgres_unavailable_error(error: BaseException) -> bool:
    return isinstance(error, (PostgresConfigurationError, PermissionError)) or (
        isinstance(error, OSError) and getattr(error, "errno", None) == 1
    )


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
            $1, $2, $3, 'capability', 'active', 'p1', $4, NULL, $5, $6::jsonb, $7, NULL, NULL, NULL, $8, $9
        )
        ON CONFLICT (roadmap_item_id) DO UPDATE SET
            roadmap_key = EXCLUDED.roadmap_key,
            title = EXCLUDED.title,
            parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
            summary = EXCLUDED.summary,
            acceptance_criteria = EXCLUDED.acceptance_criteria,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = EXCLUDED.updated_at
        """,
        roadmap_item_id,
        roadmap_item_id.replace("roadmap_item.", "roadmap."),
        title,
        parent_roadmap_item_id,
        title,
        json.dumps(
            {
                "phase_order": phase_order,
                "outcome_gate": title,
                "approval_tag": "tree-test",
            },
            sort_keys=True,
        ),
        f"decision.test.{roadmap_item_id.rsplit('.', 1)[-1]}",
        clock,
        clock,
    )


async def _seed_dependency(
    conn,
    *,
    dependency_id: str,
    roadmap_item_id: str,
    depends_on_roadmap_item_id: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO roadmap_item_dependencies (
            roadmap_item_dependency_id,
            roadmap_item_id,
            depends_on_roadmap_item_id,
            dependency_kind,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, 'blocks', $4, $5
        )
        ON CONFLICT (roadmap_item_dependency_id) DO UPDATE SET
            roadmap_item_id = EXCLUDED.roadmap_item_id,
            depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
            decision_ref = EXCLUDED.decision_ref
        """,
        dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        "decision.test.tree",
        _fixed_clock(),
    )


async def _seed_tree_rows() -> tuple[dict[str, str], dict[str, str]]:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    suffix = _unique_suffix()
    root_id = f"roadmap_item.test.tree.{suffix}"
    child_id = f"{root_id}.child"
    blocker_id = f"roadmap_item.test.tree.blocker.{suffix}"
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=root_id,
            title=f"Roadmap root {suffix}",
            parent_roadmap_item_id=None,
            phase_order="1",
        )
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=child_id,
            title=f"Roadmap child {suffix}",
            parent_roadmap_item_id=root_id,
            phase_order="1.1",
        )
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=blocker_id,
            title=f"External blocker {suffix}",
            parent_roadmap_item_id=None,
            phase_order="2",
        )
        await _seed_dependency(
            conn,
            dependency_id=f"roadmap_item_dependency.test.tree.{suffix}.external",
            roadmap_item_id=root_id,
            depends_on_roadmap_item_id=blocker_id,
        )
        await _seed_dependency(
            conn,
            dependency_id=f"roadmap_item_dependency.test.tree.{suffix}.internal",
            roadmap_item_id=child_id,
            depends_on_roadmap_item_id=root_id,
        )
        return (
            {"WORKFLOW_DATABASE_URL": database_url},
            {
                "root_id": root_id,
                "child_id": child_id,
                "blocker_id": blocker_id,
                "suffix": suffix,
            },
        )
    finally:
        await conn.close()


async def _cleanup_tree_rows(env: dict[str, str], ids: dict[str, str]) -> None:
    conn = await connect_workflow_database(env=env)
    try:
        await conn.execute(
            "DELETE FROM roadmap_item_dependencies WHERE roadmap_item_id LIKE $1 OR depends_on_roadmap_item_id LIKE $1",
            f"roadmap_item.test.tree.%{ids['suffix']}%",
        )
        await conn.execute(
            "DELETE FROM roadmap_items WHERE roadmap_item_id LIKE $1",
            f"roadmap_item.test.tree.%{ids['suffix']}%",
        )
    finally:
        await conn.close()


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


class _FailingRoadmapWriteConnectionProxy(_ConnectionProxy):
    def __init__(self, conn, *, fail_after_item_inserts: int) -> None:
        super().__init__(conn)
        self._fail_after_item_inserts = fail_after_item_inserts
        self._roadmap_item_inserts = 0

    async def execute(self, query: str, *args: object):
        normalized_query = " ".join(query.lower().split())
        if normalized_query.startswith("insert into roadmap_items"):
            self._roadmap_item_inserts += 1
            if self._roadmap_item_inserts > self._fail_after_item_inserts:
                raise RuntimeError("simulated roadmap write failure")
        return await self._conn.execute(query, *args)


def test_roadmap_tree_renderer_orders_phase_tokens_numerically() -> None:
    root = operator_read.OperatorRoadmapItemRecord(
        roadmap_item_id="roadmap_item.root",
        roadmap_key="roadmap.root",
        title="Root",
        item_kind="capability",
        status="active",
        priority="p1",
        parent_roadmap_item_id=None,
        source_bug_id=None,
        registry_paths=(),
        summary="Root summary",
        acceptance_criteria={"phase_order": "1", "outcome_gate": "Root summary"},
        decision_ref="decision.root",
        target_start_at=None,
        target_end_at=None,
        completed_at=None,
        created_at=datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
    )
    child_late = operator_read.OperatorRoadmapItemRecord(
        roadmap_item_id="roadmap_item.root.child_late",
        roadmap_key="roadmap.root.child_late",
        title="Child late",
        item_kind="capability",
        status="active",
        priority="p1",
        parent_roadmap_item_id=root.roadmap_item_id,
        source_bug_id=None,
        registry_paths=(),
        summary="Late child summary",
        acceptance_criteria={"phase_order": "1.10", "outcome_gate": "Late child summary"},
        decision_ref="decision.child_late",
        target_start_at=None,
        target_end_at=None,
        completed_at=None,
        created_at=datetime(2026, 4, 8, 20, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 8, 20, 1, tzinfo=timezone.utc),
    )
    child_early = operator_read.OperatorRoadmapItemRecord(
        roadmap_item_id="roadmap_item.root.child_early",
        roadmap_key="roadmap.root.child_early",
        title="Child early",
        item_kind="capability",
        status="active",
        priority="p1",
        parent_roadmap_item_id=root.roadmap_item_id,
        source_bug_id=None,
        registry_paths=(),
        summary="Early child summary",
        acceptance_criteria={"phase_order": "1.2", "outcome_gate": "Early child summary"},
        decision_ref="decision.child_early",
        target_start_at=None,
        target_end_at=None,
        completed_at=None,
        created_at=datetime(2026, 4, 8, 20, 2, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 8, 20, 2, tzinfo=timezone.utc),
    )

    rendered = _render_roadmap_tree_markdown(
        root_item=root,
        roadmap_items=(root, child_late, child_early),
        roadmap_item_dependencies=(),
    )

    assert rendered.index("Child early [1.2]") < rendered.index("Child late [1.10]")


def test_query_roadmap_tree_reads_subtree_and_dependencies() -> None:
    try:
        env, ids = asyncio.run(_seed_tree_rows())
    except Exception as exc:
        if not _is_postgres_unavailable_error(exc):
            raise
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for roadmap tree integration test: "
            f"{getattr(exc, 'reason_code', type(exc).__name__)}"
        )

    try:
        payload = operator_read.query_roadmap_tree(
            env=env,
            root_roadmap_item_id=ids["root_id"],
        )
        assert payload["kind"] == "roadmap_tree"
        assert payload["instruction_authority"]["kind"] == "roadmap_tree_instruction_authority"
        assert payload["instruction_authority"]["roadmap_truth"]["root_roadmap_item_id"] == ids["root_id"]
        assert payload["root_roadmap_item_id"] == ids["root_id"]
        assert payload["counts"] == {
            "roadmap_items": 2,
            "roadmap_item_dependencies": 2,
            "semantic_neighbors": 0,
        }
        assert payload["semantic_neighbors_reason_code"] in {
            "roadmap.semantic_neighbors.none",
            "roadmap.semantic_neighbors.schema_unavailable",
        }
        assert payload["semantic_neighbors"] == []
        assert payload["root_item"]["roadmap_item_id"] == ids["root_id"]
        assert [row["roadmap_item_id"] for row in payload["roadmap_items"]] == [
            ids["root_id"],
            ids["child_id"],
        ]
        assert ids["blocker_id"] in payload["rendered_markdown"]
        assert "Roadmap child" in payload["rendered_markdown"]
    finally:
        asyncio.run(_cleanup_tree_rows(env, ids))


def test_roadmap_write_preview_matches_tree_view_after_commit() -> None:
    try:
        asyncio.run(_exercise_roadmap_write_preview_parity())
    except Exception as exc:
        if not _is_postgres_unavailable_error(exc):
            raise
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for roadmap write parity test: "
            f"{getattr(exc, 'reason_code', type(exc).__name__)}"
        )


async def _exercise_roadmap_write_preview_parity() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    suffix = _unique_suffix()
    parent_id = f"roadmap_item.test.tree.write.{suffix}"
    blocker_id = f"roadmap_item.test.tree.write.blocker.{suffix}"
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=parent_id,
            title=f"Roadmap parent {suffix}",
            parent_roadmap_item_id=None,
            phase_order="1",
        )
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=blocker_id,
            title=f"Blocking roadmap {suffix}",
            parent_roadmap_item_id=None,
            phase_order="2",
        )

        async def _connect_database(env=None):
            del env
            return _ConnectionProxy(conn)

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=_connect_database,
        )
        preview = await frontdoor.roadmap_write_async(
            action="preview",
            title="Roadmap write preview parity",
            intent_brief="Preview parity between write gate and tree read model",
            template="hard_cutover_program",
            priority="p1",
            parent_roadmap_item_id=parent_id,
            depends_on=[blocker_id],
        )
        committed = await frontdoor.roadmap_write_async(
            action="commit",
            title="Roadmap write preview parity",
            intent_brief="Preview parity between write gate and tree read model",
            template="hard_cutover_program",
            priority="p1",
            parent_roadmap_item_id=parent_id,
            depends_on=[blocker_id],
        )

        assert preview["committed"] is False
        assert committed["committed"] is True

        root_roadmap_item_id = committed["preview"]["roadmap_items"][0]["roadmap_item_id"]
        payload = await asyncio.to_thread(
            operator_read.query_roadmap_tree,
            env={"WORKFLOW_DATABASE_URL": database_url},
            root_roadmap_item_id=root_roadmap_item_id,
        )

        assert payload["root_item"] == committed["preview"]["roadmap_items"][0]
        assert payload["roadmap_items"] == committed["preview"]["roadmap_items"]
        dependency_sort_key = lambda row: (
            row["roadmap_item_id"],
            row["created_at"],
            row["roadmap_item_dependency_id"],
        )
        assert sorted(payload["roadmap_item_dependencies"], key=dependency_sort_key) == sorted(
            committed["preview"]["roadmap_item_dependencies"],
            key=dependency_sort_key,
        )
        assert payload["counts"] == {
            "roadmap_items": len(committed["preview"]["roadmap_items"]),
            "roadmap_item_dependencies": len(
                committed["preview"]["roadmap_item_dependencies"]
            ),
            "semantic_neighbors": len(payload["semantic_neighbors"]),
        }
        assert "Roadmap write preview parity" in payload["rendered_markdown"]
        assert any(
            dependency["depends_on_roadmap_item_id"] == blocker_id
            for dependency in payload["roadmap_item_dependencies"]
        )
    finally:
        await conn.close()
        await _cleanup_tree_rows({"WORKFLOW_DATABASE_URL": database_url}, {"suffix": suffix})


def test_roadmap_write_rollback_clears_partial_tree_after_failure() -> None:
    try:
        asyncio.run(_exercise_roadmap_write_transaction_rollback())
    except Exception as exc:
        if not _is_postgres_unavailable_error(exc):
            raise
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for roadmap write rollback test: "
            f"{getattr(exc, 'reason_code', type(exc).__name__)}"
        )


async def _exercise_roadmap_write_transaction_rollback() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    suffix = _unique_suffix()
    parent_id = f"roadmap_item.test.tree.rollback.{suffix}"
    blocker_id = f"roadmap_item.test.tree.rollback.blocker.{suffix}"
    try:
        await _bootstrap_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=parent_id,
            title=f"Rollback parent {suffix}",
            parent_roadmap_item_id=None,
            phase_order="1",
        )
        await _seed_roadmap_item(
            conn,
            roadmap_item_id=blocker_id,
            title=f"Rollback blocker {suffix}",
            parent_roadmap_item_id=None,
            phase_order="2",
        )

        async def _connect_database(env=None):
            del env
            return _FailingRoadmapWriteConnectionProxy(conn, fail_after_item_inserts=1)

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=_connect_database,
        )
        with pytest.raises(RuntimeError, match="simulated roadmap write failure"):
            await frontdoor.roadmap_write_async(
                action="commit",
                title="Rollback roadmap write",
                intent_brief="Fail after one insert to prove transaction rollback",
                template="hard_cutover_program",
                priority="p1",
                parent_roadmap_item_id=parent_id,
                depends_on=[blocker_id],
            )

        rows = await conn.fetch(
            """
            SELECT roadmap_item_id
            FROM roadmap_items
            WHERE roadmap_item_id LIKE $1
            ORDER BY roadmap_item_id
            """,
            f"{parent_id}.%",
        )
        dependency_rows = await conn.fetch(
            """
            SELECT roadmap_item_dependency_id
            FROM roadmap_item_dependencies
            WHERE roadmap_item_id LIKE $1
            ORDER BY roadmap_item_dependency_id
            """,
            f"{parent_id}.%",
        )

        assert rows == []
        assert dependency_rows == []
    finally:
        await conn.close()
        await _cleanup_tree_rows({"WORKFLOW_DATABASE_URL": database_url}, {"suffix": suffix})
