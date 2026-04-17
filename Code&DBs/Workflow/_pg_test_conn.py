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
from urllib.parse import urlsplit

import asyncpg

_wf = str(Path(__file__).resolve().parent)
if _wf not in sys.path:
    sys.path.insert(0, _wf)

_pool = None
_conn = None
_AUTHORITY_UNAVAILABLE = "postgres.authority_unavailable"
_DEFAULT_TEST_DATABASE = "praxis_test"
_DEFAULT_TEST_DATABASE_URL = f"postgresql://localhost:5432/{_DEFAULT_TEST_DATABASE}"
_DEFAULT_ADMIN_DATABASE_URL = "postgresql://localhost:5432/postgres"


def _database_name_from_url(database_url: str) -> str:
    parsed = urlsplit(database_url)
    return parsed.path.lstrip("/") or _DEFAULT_TEST_DATABASE


async def _create_database_if_missing(*, admin_database_url: str, database_name: str) -> None:
    from storage.postgres.connection import connect_workflow_database

    conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": admin_database_url})
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if exists:
            return
        escaped_identifier = '"' + database_name.replace('"', '""') + '"'
        try:
            await conn.execute(f"CREATE DATABASE {escaped_identifier}")
        except (asyncpg.DuplicateDatabaseError, asyncpg.UniqueViolationError):
            # Parallel test collectors may race on the first create; that's fine.
            pass
    finally:
        await conn.close()


def _default_test_database_url() -> str:
    from storage.postgres.connection import resolve_workflow_database_url

    return resolve_workflow_database_url(
        env={"WORKFLOW_DATABASE_URL": _DEFAULT_TEST_DATABASE_URL},
    )


def ensure_test_database_ready() -> str:
    from storage.postgres import ensure_postgres_available
    from storage.postgres.connection import _run_sync, resolve_workflow_database_url

    database_url = _default_test_database_url()
    admin_database_url = resolve_workflow_database_url(
        env={"WORKFLOW_DATABASE_URL": _DEFAULT_ADMIN_DATABASE_URL},
    )
    database_name = _database_name_from_url(database_url)
    create_database = _create_database_if_missing(
        admin_database_url=admin_database_url,
        database_name=database_name,
    )
    try:
        _run_sync(create_database)
        ensure_postgres_available(
            env={"WORKFLOW_DATABASE_URL": database_url},
        ).close()
    except Exception as exc:
        try:
            create_database.close()
        except Exception:
            pass
        _skip_for_unavailable_authority(exc)
        raise
    return database_url


def _resolve_test_env() -> dict[str, str]:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL")
    if database_url is None or not str(database_url).strip():
        database_url = ensure_test_database_ready()
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        "PATH": os.environ.get("PATH", ""),
    }


def get_test_env() -> dict[str, str]:
    """Return the canonical environment for DB-backed workflow tests."""

    return dict(_resolve_test_env())


def _get_pool():
    global _pool
    if _pool is not None and bool(
        getattr(_pool, "_closed", False) or getattr(_pool, "closed", False)
    ):
        _pool = None
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
        f"{reason_code}",
        allow_module_level=True,
    )


def get_test_conn():
    """Get a shared Postgres connection for testing.

    Return a shared, auto-commit connection to the configured workflow database.

    WARNING: writes are durable. Use transactional_test_conn() or
    get_isolated_conn() in tests that insert or update authority tables.
    """
    global _conn
    if _conn is not None and bool(
        getattr(getattr(_conn, "_pool", None), "_closed", False)
        or getattr(getattr(_conn, "_pool", None), "closed", False)
    ):
        _conn = None
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
