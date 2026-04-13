"""Shared Postgres test connection helpers for unit and integration tests.

All tests use the local Postgres instance. The database is always running
via launchd (com.praxis.postgres).

`get_test_conn()` returns the long-lived shared auto-commit wrapper.
Tests that must avoid durable writes should use `get_isolated_conn()`,
which opens a dedicated connection inside a transaction and rolls it back
when closed.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path

_wf = str(Path(__file__).resolve().parent)
if _wf not in sys.path:
    sys.path.insert(0, _wf)

_pool = None
_conn = None
_AUTHORITY_UNAVAILABLE = "postgres.authority_unavailable"


def _resolve_test_env() -> dict[str, str]:
    return {
        "WORKFLOW_DATABASE_URL": os.environ.get(
            "WORKFLOW_DATABASE_URL",
            "postgresql://test@localhost:5432/praxis_test",
        ),
        "PATH": os.environ.get("PATH", ""),
    }


def _get_pool():
    global _pool
    if _pool is None:
        from storage.postgres.connection import get_workflow_pool
        try:
            _pool = get_workflow_pool(env=_resolve_test_env())
        except Exception as exc:
            _skip_for_unavailable_authority(exc)
            raise
    return _pool


def _skip_for_unavailable_authority(exc: BaseException) -> None:
    reason_code = getattr(exc, "reason_code", "")
    if reason_code != _AUTHORITY_UNAVAILABLE:
        return
    import pytest

    pytest.skip(
        "repo-local Postgres authority unavailable for DB-backed tests: "
        f"{reason_code}"
    )


def get_test_conn():
    """Get a shared Postgres connection for testing.

    Return a shared, auto-commit connection to the configured workflow database.

    WARNING: writes are durable. Use transactional_test_conn() or
    get_isolated_conn() in tests that insert or update authority tables.
    """
    global _conn
    if _conn is None:
        from storage.postgres import SyncPostgresConnection
        _conn = SyncPostgresConnection(_get_pool())
    return _conn


class _IsolatedSyncPostgresConnection:
    """Dedicated connection wrapper that rolls back all writes on close."""

    def __init__(self) -> None:
        from storage.postgres.connection import _run_sync, connect_workflow_database

        self._run_sync = _run_sync
        try:
            self._conn = self._run_sync(connect_workflow_database(env=_resolve_test_env()))
        except Exception as exc:
            _skip_for_unavailable_authority(exc)
            raise
        self._transaction = self._conn.transaction()
        self._run_sync(self._transaction.start())
        self._closed = False

    def execute(self, query: str, *args) -> list:
        async def _do():
            return await self._conn.fetch(query, *args)

        return self._run_sync(_do())

    def fetchrow(self, query: str, *args):
        async def _do():
            return await self._conn.fetchrow(query, *args)

        return self._run_sync(_do())

    def fetchval(self, query: str, *args):
        async def _do():
            return await self._conn.fetchval(query, *args)

        return self._run_sync(_do())

    def execute_many(self, query: str, args_list: list):
        async def _do():
            await self._conn.executemany(query, args_list)

        self._run_sync(_do())

    def execute_script(self, sql: str):
        async def _do():
            await self._conn.execute(sql)

        self._run_sync(_do())

    def close(self) -> None:
        if self._closed:
            return

        async def _do():
            await self._transaction.rollback()
            await self._conn.close()

        self._run_sync(_do())
        self._closed = True

    def commit(self) -> None:
        """Tests using this helper should not persist writes."""


def get_isolated_conn():
    """Get a dedicated connection whose writes are rolled back on close."""

    return _IsolatedSyncPostgresConnection()


@contextmanager
def transactional_test_conn():
    """Provide a rollback-on-exit connection for tests that write durable rows."""
    conn = get_isolated_conn()
    try:
        yield conn
    finally:
        conn.close()
