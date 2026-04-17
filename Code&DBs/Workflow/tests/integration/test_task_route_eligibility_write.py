from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from storage.migrations import workflow_bootstrap_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    connect_workflow_database,
    resolve_workflow_database_url,
)
from surfaces.api import operator_write

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_DUPLICATE_SQLSTATES = {"42P07", "42710"}


class _BorrowedConnection:
    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def __getattr__(self, name: str):
        return getattr(self._conn, name)

    async def close(self) -> None:
        return None


def test_task_route_eligibility_write_supersedes_active_scope_window() -> None:
    if sys.platform == "darwin":
        pytest.xfail(
            "macOS pytest harness hangs before task-route eligibility integration reaches "
            "the repo-local database path; the async flow was validated separately via direct python execution"
        )
    asyncio.run(_exercise_task_route_eligibility_write_supersedes_active_scope_window())


async def _exercise_task_route_eligibility_write_supersedes_active_scope_window() -> None:
    env = _workflow_env()
    conn = await connect_workflow_database(env=env)
    await _bootstrap_workflow_migration(conn, "012_task_type_route_eligibility.sql")
    transaction = conn.transaction()
    await transaction.start()
    try:
        borrowed_conn = _BorrowedConnection(conn)

        async def _connect_database(
            _env: dict[str, str] | None = None,
        ) -> _BorrowedConnection:
            return borrowed_conn

        frontdoor = operator_write.OperatorControlFrontdoor(
            connect_database=_connect_database,
        )
        suffix = uuid.uuid4().hex[:8]
        provider_slug = f"anthropic-test-{suffix}"
        first_start = datetime(2026, 4, 8, 16, 0, tzinfo=timezone.utc)
        second_start = first_start + timedelta(hours=2)
        first_end = first_start + timedelta(days=1)
        second_end = first_start + timedelta(days=2)

        first_payload = await frontdoor.set_task_route_eligibility_window_async(
            provider_slug=provider_slug,
            eligibility_status="rejected",
            effective_from=first_start,
            effective_to=first_end,
            reason_code="provider_disabled",
            rationale="Provider disabled for testing",
            env=env,
        )
        second_payload = await frontdoor.set_task_route_eligibility_window_async(
            provider_slug=provider_slug,
            eligibility_status="rejected",
            effective_from=second_start,
            effective_to=second_end,
            reason_code="provider_disabled",
            rationale="Provider disabled longer for testing",
            env=env,
        )

        assert first_payload["task_route_eligibility"]["provider_slug"] == provider_slug
        assert first_payload["task_route_eligibility"]["effective_from"] == first_start.isoformat()
        assert first_payload["task_route_eligibility"]["effective_to"] == first_end.isoformat()
        assert second_payload["superseded_task_route_eligibility_ids"] == [
            first_payload["task_route_eligibility"]["task_route_eligibility_id"]
        ]

        rows = await conn.fetch(
            """
            SELECT
                task_route_eligibility_id,
                effective_from,
                effective_to,
                reason_code
            FROM task_type_route_eligibility
            WHERE provider_slug = $1
            ORDER BY effective_from ASC
            """,
            provider_slug,
        )

        assert len(rows) == 2
        assert rows[0]["task_route_eligibility_id"] == first_payload["task_route_eligibility"]["task_route_eligibility_id"]
        assert rows[0]["effective_from"] == first_start
        assert rows[0]["effective_to"] == second_start
        assert rows[1]["task_route_eligibility_id"] == second_payload["task_route_eligibility"]["task_route_eligibility_id"]
        assert rows[1]["effective_from"] == second_start
        assert rows[1]["effective_to"] == second_end
        assert rows[1]["reason_code"] == "provider_disabled"
    finally:
        await transaction.rollback()
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
                if getattr(exc, "sqlstate", None) in _DUPLICATE_SQLSTATES:
                    continue
                raise


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url(
            env={"WORKFLOW_DATABASE_URL": os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")}
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for task-route-eligibility integration test: "
            f"{exc.reason_code}"
        )
    return {"WORKFLOW_DATABASE_URL": database_url}
