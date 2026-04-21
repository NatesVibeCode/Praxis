from __future__ import annotations

import _pg_test_conn as pg_test_conn
import pytest

import storage.postgres as storage_postgres
import storage.postgres.connection as pg_connection
import runtime._workflow_database as runtime_db
from storage.postgres.validators import PostgresConfigurationError


def test_resolve_test_env_uses_canonical_test_database_even_when_runtime_env_is_set(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql:///praxis?host=/tmp&port=5432")
    monkeypatch.setattr(
        pg_test_conn,
        "ensure_test_database_ready",
        lambda: "postgresql://postgres@localhost:5432/praxis_test",
    )

    env = pg_test_conn._resolve_test_env()

    assert env["WORKFLOW_DATABASE_URL"] == "postgresql://postgres@localhost:5432/praxis_test"
    assert "PATH" in env


def test_resolve_test_env_bootstraps_default_database_when_runtime_override_missing(
    monkeypatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        pg_test_conn,
        "ensure_test_database_ready",
        lambda: "postgresql://postgres@localhost:5432/praxis_test",
    )

    env = pg_test_conn._resolve_test_env()

    assert env["WORKFLOW_DATABASE_URL"] == "postgresql://postgres@localhost:5432/praxis_test"


def test_resolve_test_env_bootstraps_default_database_when_runtime_override_blank(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "   ")
    monkeypatch.setattr(
        pg_test_conn,
        "ensure_test_database_ready",
        lambda: "postgresql://postgres@localhost:5432/praxis_test",
    )

    env = pg_test_conn._resolve_test_env()

    assert env["WORKFLOW_DATABASE_URL"] == "postgresql://postgres@localhost:5432/praxis_test"


def test_default_test_database_url_derives_from_runtime_authority(
    monkeypatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_TEST_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        runtime_db,
        "resolve_runtime_database_url",
        lambda required=True: "postgresql://nate@localhost:5432/praxis?sslmode=disable",
    )

    assert (
        pg_test_conn._default_test_database_url()
        == "postgresql://nate@localhost:5432/praxis_test?sslmode=disable"
    )


def test_default_test_database_url_honors_explicit_test_authority(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "WORKFLOW_TEST_DATABASE_URL",
        "postgresql://tester@localhost:5432/custom_test",
    )
    monkeypatch.setattr(
        runtime_db,
        "resolve_runtime_database_url",
        lambda required=True: "postgresql://nate@localhost:5432/praxis",
    )

    assert (
        pg_test_conn._default_test_database_url()
        == "postgresql://tester@localhost:5432/custom_test"
    )


def test_transactional_test_conn_closes_connection(monkeypatch) -> None:
    closed: list[bool] = []

    class _FakeConn:
        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(pg_test_conn, "get_isolated_conn", lambda: _FakeConn())

    with pg_test_conn.transactional_test_conn() as conn:
        assert isinstance(conn, _FakeConn)

    assert closed == [True]


def test_get_isolated_conn_skips_when_authority_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(pg_connection, "connect_workflow_database", lambda env=None: object())

    def _boom(_coro) -> None:
        raise PostgresConfigurationError(
            "postgres.authority_unavailable",
            "repo-local Postgres authority unavailable",
        )

    monkeypatch.setattr(pg_connection, "_run_sync", _boom)

    with pytest.raises(pytest.skip.Exception, match="repo-local Postgres authority unavailable"):
        pg_test_conn.get_isolated_conn()


def test_get_test_conn_rebuilds_wrapper_when_cached_pool_is_closed(monkeypatch) -> None:
    class _FakePool:
        def __init__(self, closed: bool) -> None:
            self._closed = closed

    created: list[object] = []

    class _FakeSyncConn:
        def __init__(self, pool) -> None:
            self._pool = pool
            created.append(self)

    stale = _FakeSyncConn(_FakePool(closed=True))
    pg_test_conn._conn = stale

    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda env=None: _FakePool(closed=False))
    monkeypatch.setattr(storage_postgres, "SyncPostgresConnection", _FakeSyncConn)

    try:
        refreshed = pg_test_conn.get_test_conn()
    finally:
        pg_test_conn._conn = None

    assert refreshed is created[-1]
    assert refreshed is not stale
    assert refreshed._pool._closed is False


def test_get_pool_reopens_closed_cached_pool(monkeypatch) -> None:
    class _FakePool:
        def __init__(self, closed: bool) -> None:
            self._closed = closed

    closed_pool = _FakePool(closed=True)
    fresh_pool = _FakePool(closed=False)
    pg_test_conn._pool = closed_pool

    monkeypatch.setattr(pg_connection, "get_workflow_pool", lambda env=None: fresh_pool)

    try:
        resolved = pg_test_conn._get_pool()
    finally:
        pg_test_conn._pool = None

    assert resolved is fresh_pool
