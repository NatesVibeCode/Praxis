from __future__ import annotations

import pytest

from storage.postgres import connection as connection_mod
from storage.postgres.validators import PostgresConfigurationError


class _FakePool:
    def __init__(self, label: str) -> None:
        self.label = label
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_get_workflow_pool_rotates_when_dsn_changes(monkeypatch) -> None:
    connection_mod.shutdown_workflow_pool()
    created: list[_FakePool] = []

    async def _fake_create_workflow_pool(env=None, **kwargs):
        del kwargs
        pool = _FakePool(env["WORKFLOW_DATABASE_URL"])
        created.append(pool)
        return pool

    monkeypatch.setattr(connection_mod, "create_workflow_pool", _fake_create_workflow_pool)

    first = connection_mod.get_workflow_pool(
        env={"WORKFLOW_DATABASE_URL": "postgresql://first"}
    )
    second = connection_mod.get_workflow_pool(
        env={"WORKFLOW_DATABASE_URL": "postgresql://second"}
    )

    assert first is created[0]
    assert second is created[1]
    assert created[0].closed is True
    assert created[1].closed is False
    connection_mod.shutdown_workflow_pool()


def test_resolve_workflow_database_url_preserves_missing_user() -> None:
    assert connection_mod.resolve_workflow_database_url(
        env={"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis"}
    ) == "postgresql://localhost:5432/praxis"


def test_shutdown_workflow_pool_clears_cached_state() -> None:
    connection_mod.shutdown_workflow_pool()
    pool = _FakePool("postgresql://example")
    loop = connection_mod._get_bg_loop()
    connection_mod._workflow_pool = pool
    connection_mod._workflow_pool_dsn = pool.label

    connection_mod.shutdown_workflow_pool()

    assert pool.closed is True
    assert connection_mod._workflow_pool is None
    assert connection_mod._workflow_pool_dsn is None
    assert connection_mod._bg_loop is None
    assert connection_mod._bg_thread is None


def test_get_workflow_pool_reopens_closed_cached_pool(monkeypatch) -> None:
    connection_mod.shutdown_workflow_pool()
    created: list[_FakePool] = []

    async def _fake_create_workflow_pool(env=None, **kwargs):
        del kwargs
        pool = _FakePool(env["WORKFLOW_DATABASE_URL"])
        created.append(pool)
        return pool

    monkeypatch.setattr(connection_mod, "create_workflow_pool", _fake_create_workflow_pool)

    first = connection_mod.get_workflow_pool(
        env={"WORKFLOW_DATABASE_URL": "postgresql://example"}
    )
    first.closed = True

    second = connection_mod.get_workflow_pool(
        env={"WORKFLOW_DATABASE_URL": "postgresql://example"}
    )

    assert first is created[0]
    assert second is created[1]
    assert second.closed is False
    connection_mod.shutdown_workflow_pool()


def test_sync_connection_close_delegates_to_shutdown(monkeypatch) -> None:
    called: list[bool] = []
    conn = connection_mod.SyncPostgresConnection(_FakePool("postgresql://example"))

    monkeypatch.setattr(
        connection_mod,
        "shutdown_workflow_pool",
        lambda: called.append(True),
    )

    conn.close()

    assert called == [True]


def test_sync_connection_rebinds_closed_singleton_pool(monkeypatch) -> None:
    connection_mod.shutdown_workflow_pool()
    first = _FakePool("postgresql://example")
    second = _FakePool("postgresql://example")
    first.closed = True
    connection_mod._workflow_pool = first
    connection_mod._workflow_pool_dsn = first.label

    conn = connection_mod.SyncPostgresConnection(first)

    monkeypatch.setattr(
        connection_mod,
        "get_workflow_pool",
        lambda env=None: second,
    )

    assert conn._pool_handle() is second


def test_resolve_workflow_authority_cache_key_sanitizes_database_identity() -> None:
    cache_key = connection_mod.resolve_workflow_authority_cache_key(
        env={
            "WORKFLOW_DATABASE_URL": (
                "postgresql://s3cr3t-user:top-secret@db.internal.example:5432/praxis"
                "?sslmode=require&application_name=worker"
            )
        }
    )

    assert cache_key.startswith("workflow_pool:")
    assert "top-secret" not in cache_key
    assert "db.internal.example" not in cache_key


def test_create_workflow_pool_wraps_permission_errors_as_authority_unavailable(monkeypatch) -> None:
    async def _blocked_create_pool(*args, **kwargs):
        del args, kwargs
        raise PermissionError("[Errno 1] Operation not permitted")

    monkeypatch.setattr(connection_mod.asyncpg, "create_pool", _blocked_create_pool)

    import asyncio

    with pytest.raises(PostgresConfigurationError) as exc_info:
        asyncio.run(
            connection_mod.create_workflow_pool(
                env={"WORKFLOW_DATABASE_URL": "postgresql://sandboxed"}
            )
        )

    assert exc_info.value.reason_code == "postgres.authority_unavailable"
    assert exc_info.value.details["cause_type"] == "PermissionError"
    assert "Operation not permitted" in str(exc_info.value)


def test_create_workflow_pool_wraps_asyncpg_auth_errors_as_authority_unavailable(monkeypatch) -> None:
    async def _blocked_create_pool(*args, **kwargs):
        del args, kwargs
        raise connection_mod.asyncpg.InvalidAuthorizationSpecificationError(
            'role "test" does not exist'
        )

    monkeypatch.setattr(connection_mod.asyncpg, "create_pool", _blocked_create_pool)

    import asyncio

    with pytest.raises(PostgresConfigurationError) as exc_info:
        asyncio.run(
            connection_mod.create_workflow_pool(
                env={"WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test"}
            )
        )

    assert exc_info.value.reason_code == "postgres.authority_unavailable"
    assert exc_info.value.details["cause_type"] == "InvalidAuthorizationSpecificationError"
    assert 'role "test" does not exist' in str(exc_info.value)


def test_connect_workflow_database_wraps_asyncpg_auth_errors_as_authority_unavailable(
    monkeypatch,
) -> None:
    async def _blocked_connect(*args, **kwargs):
        del args, kwargs
        raise connection_mod.asyncpg.InvalidAuthorizationSpecificationError(
            'role "test" does not exist'
        )

    monkeypatch.setattr(connection_mod.asyncpg, "connect", _blocked_connect)

    import asyncio

    with pytest.raises(PostgresConfigurationError) as exc_info:
        asyncio.run(
            connection_mod.connect_workflow_database(
                env={"WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test"}
            )
        )

    assert exc_info.value.reason_code == "postgres.authority_unavailable"
    assert exc_info.value.details["cause_type"] == "InvalidAuthorizationSpecificationError"
    assert 'role "test" does not exist' in str(exc_info.value)


def test_ensure_postgres_available_wraps_bootstrap_errors_as_authority_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        connection_mod,
        "get_workflow_pool",
        lambda env=None: _FakePool(env["WORKFLOW_DATABASE_URL"]),
    )

    def _boom(coro):
        coro.close()
        raise RuntimeError("schema boom")

    monkeypatch.setattr(connection_mod, "_run_sync", _boom)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        connection_mod.ensure_postgres_available(
            env={"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"}
        )

    assert exc_info.value.reason_code == "postgres.authority_unavailable"
    assert exc_info.value.details["operation"] == "bootstrap_workflow_schema"
    assert exc_info.value.details["database_url"] == "postgresql://repo.test/workflow"
    assert "schema boom" in str(exc_info.value)
