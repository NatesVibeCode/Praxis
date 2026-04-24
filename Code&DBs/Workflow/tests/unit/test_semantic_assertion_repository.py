from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from storage.postgres.semantic_assertion_repository import (
    PostgresSemanticAssertionRepository,
)


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object):
        self.executed.append((query, args))
        return {"row_count": 1}


def test_rebuild_current_assertions_deduplicates_and_upserts_projection() -> None:
    asyncio.run(_exercise_rebuild_current_assertions_deduplicates_and_upserts_projection())


async def _exercise_rebuild_current_assertions_deduplicates_and_upserts_projection() -> None:
    conn = _FakeConn()
    repository = PostgresSemanticAssertionRepository(conn)  # type: ignore[arg-type]
    as_of = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)

    rebuilt_count = await repository.rebuild_current_assertions(as_of=as_of)

    assert rebuilt_count == 1
    assert len(conn.executed) == 1
    rebuild_sql, rebuild_args = conn.executed[0]
    assert rebuild_args == (as_of,)
    assert "SELECT DISTINCT ON (semantic_assertion_id)" in rebuild_sql
    assert "FROM current_assertions" in rebuild_sql
    assert "ON CONFLICT (semantic_assertion_id) DO UPDATE SET" in rebuild_sql
    assert "DELETE FROM semantic_current_assertions current_assertion" in rebuild_sql
