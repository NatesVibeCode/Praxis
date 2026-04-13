"""Connection management, pool, and sync wrapper for the Postgres control plane."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import os
import threading as _threading

import asyncpg

from .validators import PostgresConfigurationError, PostgresStorageError

WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
_POSTGRES_SCHEMES = ("postgresql://", "postgres://")

_workflow_pool: asyncpg.Pool | None = None
_workflow_pool_dsn: str | None = None
_bg_loop: asyncio.AbstractEventLoop | None = None
_bg_thread: _threading.Thread | None = None


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


async def connect_workflow_database(
    env: Mapping[str, str] | None = None,
) -> asyncpg.Connection:
    """Open a Postgres connection using only explicit workflow configuration."""

    database_url = resolve_workflow_database_url(env=env)
    try:
        return await asyncpg.connect(database_url)
    except PermissionError as exc:
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
    except PermissionError as exc:
        raise _normalize_authority_error(exc, database_url=database_url) from exc


def get_workflow_pool(env: Mapping[str, str] | None = None) -> asyncpg.Pool:
    """Get or create the singleton workflow connection pool."""
    global _workflow_pool, _workflow_pool_dsn
    database_url = resolve_workflow_database_url(env=env)
    if _workflow_pool is not None and _workflow_pool_dsn != database_url:
        shutdown_workflow_pool()
    if _workflow_pool is None:
        _workflow_pool = _run_sync(create_workflow_pool(env=env))
        _workflow_pool_dsn = database_url
    return _workflow_pool


def shutdown_workflow_pool() -> None:
    """Close the shared pool and stop the bridge loop."""
    global _workflow_pool, _workflow_pool_dsn, _bg_loop, _bg_thread
    pool = _workflow_pool
    loop = _bg_loop
    thread = _bg_thread
    _workflow_pool = None
    _workflow_pool_dsn = None
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


def ensure_postgres_available(
    env: Mapping[str, str] | None = None,
) -> SyncPostgresConnection:
    """Ensure Postgres is reachable and schema is bootstrapped, return a SyncPostgresConnection.

    This is the single entry point for all subsystem access. It:
    1. Creates a connection pool (validates reachability)
    2. Bootstraps the schema if needed (idempotent)
    3. Returns a SyncPostgresConnection wrapping the shared pool

    If the local cluster is down and dev_postgres helpers are available,
    attempts to start it first.
    """
    # Try to start the local cluster if it's not running.
    # This is best-effort — if dev_postgres helpers fail (e.g. path mismatch),
    # we still attempt a direct connection since Postgres may already be up.
    try:
        from storage.dev_postgres import local_postgres_up
        local_postgres_up(env=env)
    except Exception:
        pass  # Cluster may already be running or managed externally

    pool = get_workflow_pool(env=env)

    # Idempotent schema bootstrap
    async def _bootstrap():
        from .schema import bootstrap_workflow_schema, inspect_workflow_schema
        async with pool.acquire() as conn:
            await bootstrap_workflow_schema(conn)
            readiness = await inspect_workflow_schema(conn)
            critical_objects = {
                "compile_artifacts",
                "capability_catalog",
                "verify_refs",
                "verification_registry",
                "compile_index_snapshots",
                "execution_packets",
                "repo_snapshots",
                "verifier_registry",
                "healer_registry",
                "verifier_healer_bindings",
                "verification_runs",
                "healing_runs",
            }
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
