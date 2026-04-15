from __future__ import annotations

import _pg_test_conn as pg_test_conn
import pytest

import storage.postgres.connection as pg_connection
from storage.postgres.validators import PostgresConfigurationError


def test_resolve_test_env_prefers_runtime_override(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql:///praxis?host=/tmp&port=5432")

    env = pg_test_conn._resolve_test_env()

    assert env["WORKFLOW_DATABASE_URL"] == "postgresql:///praxis?host=/tmp&port=5432"
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
