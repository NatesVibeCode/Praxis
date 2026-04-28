from __future__ import annotations

import asyncio
import concurrent.futures
import json

import pytest

from storage.postgres import connection as connection_mod
from storage.postgres.validators import PostgresConfigurationError


class _FakePool:
    def __init__(self, label: str) -> None:
        self.label = label
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _TimeoutAcquireContext:
    async def __aenter__(self):
        raise TimeoutError("no pooled connections available")

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _ExhaustedPool(_FakePool):
    def __init__(self, label: str) -> None:
        super().__init__(label)
        self.acquire_timeout = None

    def acquire(self, *, timeout=None):
        self.acquire_timeout = timeout
        return _TimeoutAcquireContext()


class _CodecConn:
    def __init__(self) -> None:
        self.codecs: list[tuple[str, dict[str, object]]] = []

    async def set_type_codec(self, typename: str, **kwargs: object) -> None:
        self.codecs.append((typename, kwargs))


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


def test_run_sync_closes_coroutine_when_bridge_submission_fails(monkeypatch) -> None:
    async def _pending():
        return "ok"

    coro = _pending()

    def _fail_submit(awaitable, loop):
        assert awaitable is coro
        assert loop == "loop"
        raise RuntimeError("bridge down")

    monkeypatch.setattr(connection_mod, "_get_bg_loop", lambda: "loop")
    monkeypatch.setattr(connection_mod.asyncio, "run_coroutine_threadsafe", _fail_submit)

    with pytest.raises(RuntimeError, match="bridge down"):
        connection_mod._run_sync(coro)

    assert coro.cr_frame is None


def test_run_sync_cancels_and_closes_unstarted_coroutine_on_timeout(monkeypatch) -> None:
    async def _pending():
        return "ok"

    class _StoppedLoop:
        def is_running(self) -> bool:
            return False

    class _TimeoutFuture:
        cancelled = False

        def result(self, timeout=None):
            del timeout
            raise concurrent.futures.TimeoutError()

        def cancel(self) -> bool:
            self.cancelled = True
            return True

        def done(self) -> bool:
            return False

    future = _TimeoutFuture()
    coro = _pending()

    monkeypatch.setattr(connection_mod, "_get_bg_loop", lambda: _StoppedLoop())
    monkeypatch.setattr(
        connection_mod.asyncio,
        "run_coroutine_threadsafe",
        lambda awaitable, loop: future,
    )

    with pytest.raises(concurrent.futures.TimeoutError):
        connection_mod._run_sync(coro)

    assert future.cancelled is True
    assert coro.cr_frame is None


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


def test_sync_connection_pool_acquire_timeout_is_typed(monkeypatch) -> None:
    monkeypatch.setenv(connection_mod.WORKFLOW_POOL_ACQUIRE_TIMEOUT_ENV, "0.25")
    pool = _ExhaustedPool("postgresql://example")
    conn = connection_mod.SyncPostgresConnection(pool)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        conn.fetchval("SELECT 1")

    assert pool.acquire_timeout == 0.25
    assert exc_info.value.reason_code == "postgres.pool_acquire_timeout"
    assert exc_info.value.details["operation"] == "fetchval"
    assert exc_info.value.details["timeout_s"] == 0.25
    assert "after 0.25s" in str(exc_info.value)


def test_json_codec_encoder_preserves_preencoded_json_text() -> None:
    assert connection_mod._encode_json_value("[]") == "[]"
    assert connection_mod._encode_json_value("{}") == "{}"
    assert connection_mod._encode_json_value('{"event_ids":["evt-1"]}') == (
        '{"event_ids":["evt-1"]}'
    )
    assert json.loads(connection_mod._encode_json_value(["evt-1"])) == ["evt-1"]
    assert json.loads(connection_mod._encode_json_value({"event_ids": ["evt-1"]})) == {
        "event_ids": ["evt-1"]
    }
    assert json.loads(connection_mod._encode_json_value("plain text")) == "plain text"


def test_register_jsonb_codec_uses_tolerant_encoder_for_json_and_jsonb() -> None:
    conn = _CodecConn()

    asyncio.run(connection_mod._register_jsonb_codec(conn))

    registered = {typename: kwargs for typename, kwargs in conn.codecs}
    assert set(registered) == {"jsonb", "json"}
    for typename in ("jsonb", "json"):
        assert registered[typename]["schema"] == "pg_catalog"
        assert registered[typename]["format"] == "text"
        assert registered[typename]["encoder"] is connection_mod._encode_json_value
        assert registered[typename]["decoder"]("[]") == []


def test_create_workflow_pool_defaults_to_single_min_connection(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_create_pool(database_url, **kwargs):
        captured.update({"database_url": database_url, **kwargs})
        return _FakePool(database_url)

    monkeypatch.setattr(connection_mod.asyncpg, "create_pool", _fake_create_pool)

    pool = asyncio.run(
        connection_mod.create_workflow_pool(
            env={connection_mod.WORKFLOW_DATABASE_URL_ENV: "postgresql://pool"}
        )
    )

    assert isinstance(pool, _FakePool)
    assert captured["min_size"] == 1
    assert captured["max_size"] == 8


def test_create_workflow_pool_reads_size_overrides_from_env_mapping(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_create_pool(database_url, **kwargs):
        captured.update({"database_url": database_url, **kwargs})
        return _FakePool(database_url)

    monkeypatch.setattr(connection_mod.asyncpg, "create_pool", _fake_create_pool)

    asyncio.run(
        connection_mod.create_workflow_pool(
            env={
                connection_mod.WORKFLOW_DATABASE_URL_ENV: "postgresql://pool",
                connection_mod.WORKFLOW_POOL_MIN_SIZE_ENV: "3",
                connection_mod.WORKFLOW_POOL_MAX_SIZE_ENV: "2",
            }
        )
    )

    assert captured["min_size"] == 3
    assert captured["max_size"] == 3


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
