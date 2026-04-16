"""Connection management, pool, and sync wrapper for the Postgres control plane."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import contextmanager
import hashlib
import os
import threading as _threading
from urllib.parse import urlsplit

import asyncpg

from .validators import PostgresConfigurationError, PostgresStorageError
from storage.migrations import workflow_compile_authority_readiness_tables

WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
_POSTGRES_SCHEMES = ("postgresql://", "postgres://")

_workflow_pool: asyncpg.Pool | None = None
_workflow_pool_dsn: str | None = None
_workflow_authority_scope: "_WorkflowAuthorityScope | None" = None
_bg_loop: asyncio.AbstractEventLoop | None = None
_bg_thread: _threading.Thread | None = None
_AUTHORITY_UNAVAILABLE_EXCEPTIONS = (
    PermissionError,
    OSError,
    asyncpg.PostgresError,
)


class _WorkflowAuthorityScope:
    """Weakref-safe identity object for process-local workflow authority caches."""

    __slots__ = ("cache_key", "__weakref__")

    def __init__(self, cache_key: str) -> None:
        self.cache_key = cache_key


def _normalize_authority_error(
    exc: BaseException,
    *,
    database_url: str | None,
) -> PostgresConfigurationError:
    """Wrap sandboxed/connectivity authority failures in a typed config error."""
    return PostgresConfigurationError(
        "postgres.authority_unavailable",
        (
            f"{WORKFLOW_DATABASE_URL_ENV} authority unavailable: "
            f"{type(exc).__name__}: {exc}"
        ),
        details={
            "environment_variable": WORKFLOW_DATABASE_URL_ENV,
            "database_url": database_url or "",
            "cause_type": type(exc).__name__,
            "cause_message": str(exc),
        },
    )


def workflow_authority_cache_key(database_url: str) -> str:
    """Return a stable, sanitized cache key for one workflow authority."""
    parsed = urlsplit(database_url)
    hostname = (parsed.hostname or "localhost").lower()
    port = f":{parsed.port}" if parsed.port is not None else ""
    identity = f"{parsed.scheme}://{hostname}{port}{parsed.path or '/'}?{parsed.query}"
    digest = hashlib.blake2s(identity.encode("utf-8"), digest_size=12).hexdigest()
    return f"workflow_pool:{digest}"


def resolve_workflow_database_url(
    env: Mapping[str, str] | None = None,
) -> str:
    """Resolve the workflow database URL from explicit configuration."""

    source = env if env is not None else os.environ
    raw_value = source.get(WORKFLOW_DATABASE_URL_ENV)
    if raw_value is None:
        raise PostgresConfigurationError(
            "postgres.config_missing",
            f"{WORKFLOW_DATABASE_URL_ENV} must be set to a Postgres DSN",
            details={"environment_variable": WORKFLOW_DATABASE_URL_ENV},
        )
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise PostgresConfigurationError(
            "postgres.config_invalid",
            f"{WORKFLOW_DATABASE_URL_ENV} must be a non-empty postgres:// or postgresql:// DSN",
            details={
                "environment_variable": WORKFLOW_DATABASE_URL_ENV,
                "value_type": type(raw_value).__name__,
            },
        )

    database_url = raw_value.strip()
    if not database_url.startswith(_POSTGRES_SCHEMES):
        raise PostgresConfigurationError(
            "postgres.config_invalid",
            f"{WORKFLOW_DATABASE_URL_ENV} must use a postgres:// or postgresql:// DSN",
            details={
                "environment_variable": WORKFLOW_DATABASE_URL_ENV,
                "value": database_url,
            },
        )

    return database_url


def resolve_workflow_authority_cache_key(
    env: Mapping[str, str] | None = None,
) -> str:
    """Resolve the sanitized cache key for the configured workflow authority."""
    return workflow_authority_cache_key(resolve_workflow_database_url(env=env))


async def connect_workflow_database(
    env: Mapping[str, str] | None = None,
) -> asyncpg.Connection:
    """Open a Postgres connection using only explicit workflow configuration."""

    database_url = resolve_workflow_database_url(env=env)
    try:
        return await asyncpg.connect(database_url)
    except _AUTHORITY_UNAVAILABLE_EXCEPTIONS as exc:
        raise _normalize_authority_error(exc, database_url=database_url) from exc


def _get_bg_loop() -> asyncio.AbstractEventLoop:
    """Get a dedicated background event loop for sync-to-async bridging.

    The pool and all connections live on this loop so acquire/release
    always happen on the same loop that created the pool.
    """
    global _bg_loop, _bg_thread
    if _bg_loop is not None and _bg_loop.is_running():
        return _bg_loop

    _bg_loop = asyncio.new_event_loop()

    def _run():
        asyncio.set_event_loop(_bg_loop)
        _bg_loop.run_forever()

    _bg_thread = _threading.Thread(target=_run, daemon=True, name="pg-sync-bridge")
    _bg_thread.start()
    return _bg_loop


def _run_sync(coro):
    """Bridge async to sync using the dedicated background loop."""
    loop = _get_bg_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=30)


async def create_workflow_pool(
    env: Mapping[str, str] | None = None,
    *,
    # Sized for unified dispatch worker (64 API + 4 CLI threads + heartbeat threads share this pool)
    min_size: int = 4,
    max_size: int = 40,
) -> asyncpg.Pool:
    """Create a shared asyncpg connection pool for the workflow database."""
    database_url = resolve_workflow_database_url(env=env)
    try:
        return await asyncpg.create_pool(database_url, min_size=min_size, max_size=max_size)
    except _AUTHORITY_UNAVAILABLE_EXCEPTIONS as exc:
        raise _normalize_authority_error(exc, database_url=database_url) from exc


def get_workflow_pool(env: Mapping[str, str] | None = None) -> asyncpg.Pool:
    """Get or create the singleton workflow connection pool."""
    global _workflow_pool, _workflow_pool_dsn, _workflow_authority_scope
    database_url = resolve_workflow_database_url(env=env)
    if _workflow_pool is not None and _workflow_pool_dsn != database_url:
        shutdown_workflow_pool()
    if _workflow_pool is None:
        _workflow_pool = _run_sync(create_workflow_pool(env=env))
        _workflow_pool_dsn = database_url
        _workflow_authority_scope = _WorkflowAuthorityScope(
            cache_key=resolve_workflow_authority_cache_key(env=env),
        )
    return _workflow_pool


def shutdown_workflow_pool() -> None:
    """Close the shared pool and stop the bridge loop."""
    global _workflow_pool, _workflow_pool_dsn, _workflow_authority_scope, _bg_loop, _bg_thread
    pool = _workflow_pool
    loop = _bg_loop
    thread = _bg_thread
    _workflow_pool = None
    _workflow_pool_dsn = None
    _workflow_authority_scope = None
    _bg_loop = None
    _bg_thread = None

    if pool is not None and loop is not None and loop.is_running():
        future = asyncio.run_coroutine_threadsafe(pool.close(), loop)
        future.result(timeout=30)

    if loop is not None and loop.is_running():
        loop.call_soon_threadsafe(loop.stop)

    if thread is not None and thread.is_alive():
        thread.join(timeout=5)

    if loop is not None and not loop.is_closed():
        loop.close()


class SyncPostgresConnection:
    """Sync wrapper around an asyncpg pool for subsystem access.

    Provides execute/fetchrow/fetchval that subsystems can call
    without knowing about async. Replaces ad hoc sync DB handles in
    subsystem constructors.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._authority_scope = _workflow_authority_scope or _WorkflowAuthorityScope(
            cache_key=f"workflow_pool:{id(pool)}",
        )
        self._authority_cache_key = self._authority_scope.cache_key

    def execute(self, query: str, *args) -> list:
        async def _do():
            async with self._pool.acquire() as conn:
                return await conn.fetch(query, *args)
        return _run_sync(_do())

    def fetchrow(self, query: str, *args):
        async def _do():
            async with self._pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        return _run_sync(_do())

    def fetchval(self, query: str, *args):
        async def _do():
            async with self._pool.acquire() as conn:
                return await conn.fetchval(query, *args)
        return _run_sync(_do())

    @contextmanager
    def transaction(self):
        async def _begin():
            conn = await self._pool.acquire()
            tx = conn.transaction()
            await tx.start()
            return conn, tx

        raw_conn, tx = _run_sync(_begin())
        pinned = _PinnedSyncPostgresConnection(
            pool=self._pool,
            conn=raw_conn,
            tx=tx,
            authority_scope=self._authority_scope,
        )
        try:
            yield pinned
        except Exception:
            pinned.rollback()
            raise
        else:
            pinned.commit()

    def execute_many(self, query: str, args_list: list):
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.executemany(query, args_list)
        _run_sync(_do())

    def execute_script(self, sql: str):
        async def _do():
            async with self._pool.acquire() as conn:
                await conn.execute(sql)
        _run_sync(_do())

    def close(self) -> None:
        shutdown_workflow_pool()

    def commit(self) -> None:
        """No-op — asyncpg auto-commits each statement."""


class _PinnedSyncPostgresConnection:
    """Sync wrapper pinned to one asyncpg connection/transaction."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        conn: asyncpg.Connection,
        tx: asyncpg.transaction.Transaction,
        authority_scope: _WorkflowAuthorityScope,
    ) -> None:
        self._pool = pool
        self._conn = conn
        self._tx = tx
        self._closed = False
        self._authority_scope = authority_scope
        self._authority_cache_key = authority_scope.cache_key

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("workflow transaction connection is closed")

    def execute(self, query: str, *args) -> list:
        self._ensure_open()

        async def _do():
            return await self._conn.fetch(query, *args)

        return _run_sync(_do())

    def fetchrow(self, query: str, *args):
        self._ensure_open()

        async def _do():
            return await self._conn.fetchrow(query, *args)

        return _run_sync(_do())

    def fetchval(self, query: str, *args):
        self._ensure_open()

        async def _do():
            return await self._conn.fetchval(query, *args)

        return _run_sync(_do())

    def execute_many(self, query: str, args_list: list):
        self._ensure_open()

        async def _do():
            await self._conn.executemany(query, args_list)

        _run_sync(_do())

    def execute_script(self, sql: str):
        self._ensure_open()

        async def _do():
            await self._conn.execute(sql)

        _run_sync(_do())

    @contextmanager
    def transaction(self):
        yield self

    def commit(self) -> None:
        if self._closed:
            return

        async def _do():
            try:
                await self._tx.commit()
            finally:
                await self._pool.release(self._conn)

        _run_sync(_do())
        self._closed = True

    def rollback(self) -> None:
        if self._closed:
            return

        async def _do():
            try:
                await self._tx.rollback()
            finally:
                await self._pool.release(self._conn)

        _run_sync(_do())
        self._closed = True

    def close(self) -> None:
        self.rollback()

def ensure_postgres_available(
    env: Mapping[str, str] | None = None,
) -> SyncPostgresConnection:
    """Ensure Postgres is reachable and schema is bootstrapped, return a SyncPostgresConnection.

    This is the single entry point for all subsystem access. It:
    1. Creates a connection pool (validates reachability)
    2. Bootstraps the schema if needed (idempotent)
    3. Returns a SyncPostgresConnection wrapping the shared pool

    Native Postgres auto-start is forbidden. Callers must provide an explicit
    reachable database authority from the sandboxed runtime lane.
    """
    pool = get_workflow_pool(env=env)

    # Idempotent schema bootstrap
    async def _bootstrap():
        from .schema import bootstrap_workflow_schema, inspect_workflow_schema
        async with pool.acquire() as conn:
            await bootstrap_workflow_schema(conn)
            readiness = await inspect_workflow_schema(conn)
        critical_objects = tuple(workflow_compile_authority_readiness_tables())
        missing_critical = tuple(
            name for name in readiness.missing_relations if name in critical_objects
        )
        if missing_critical:
            missing = ", ".join(missing_critical[:10])
            raise RuntimeError(
                "workflow schema bootstrap incomplete: "
                f"{len(missing_critical)} critical objects still missing"
                + (f" ({missing})" if missing else "")
            )
    _run_sync(_bootstrap())

    return SyncPostgresConnection(pool)
