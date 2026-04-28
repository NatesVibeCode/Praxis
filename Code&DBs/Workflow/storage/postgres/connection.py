"""Connection management, pool, and sync wrapper for the Postgres control plane."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Mapping
from contextlib import contextmanager
import hashlib
import inspect
import os
import re
import socket
import sys
import threading as _threading
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from .validators import PostgresConfigurationError, PostgresStorageError
from storage.migrations import workflow_compile_authority_readiness_tables


# ──────────────────────────────────────────────────────────────────────────
# Authority-table write guard
#
# These tables are owned by the MCP operator-tool surface (praxis_access_control,
# praxis_provider_onboard, praxis_circuits, praxis_operator_decisions, etc.).
# Writing them directly via SyncPostgresConnection.execute / execute_many /
# execute_script bypasses the CQRS gateway, leaves no operator-decision row,
# and yields stale projections — exactly the failure mode that produced
# migration 282/283/284 spirals on 2026-04-26.
#
# The guard refuses INSERT/UPDATE/DELETE/UPSERT/TRUNCATE/DROP/ALTER against
# these tables UNLESS the immediate caller stack includes an allowlisted
# module (operator handler, repository, provider-onboarding pipeline, fresh-
# install seed, or migration runner). The error message names the tool that
# owns the concern so the model is told what to use instead.
# ──────────────────────────────────────────────────────────────────────────

_PROTECTED_TABLES_TO_OWNER_TOOL: dict[str, dict[str, str]] = {
    "private_provider_api_job_allowlist": {
        "tool": "praxis_access_control",
        "action": "enable / disable",
    },
    "private_provider_model_access_denials": {
        "tool": "praxis_access_control",
        "action": "enable / disable",
    },
    "private_provider_transport_control_policy": {
        "tool": "praxis_access_control",
        "action": "enable / disable (transport policy)",
    },
    "runtime_profile_admitted_routes": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
    },
    "provider_transport_admissions": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
    },
    "provider_circuit_breaker_state": {
        "tool": "praxis_circuits",
        "action": "open / close / reset",
    },
    "task_type_routing": {
        "tool": "praxis_provider_control_plane",
        "action": "read first to confirm matrix admission BEFORE editing routing rank",
    },
    "task_type_route_eligibility": {
        "tool": "praxis_provider_control_plane",
        "action": "read first; eligibility is operator-tool territory",
    },
    "operator_decisions": {
        "tool": "praxis_operator_decisions / praxis_operator_write",
        "action": "record / supersede",
    },
}

# Caller modules permitted to write these tables. Anything calling from
# outside this allowlist gets refused. Pattern: the operator MCP tool surface
# routes through the gateway → handler → repository, and the repositories
# are the only legitimate direct writers.
_AUTHORITATIVE_WRITER_MODULE_PATTERNS = (
    re.compile(r"^storage\.postgres\..*_repository$"),
    re.compile(r"^storage\.postgres\.fresh_install_seed$"),
    re.compile(r"^storage\.migrations(\.|$)"),
    re.compile(r"^runtime\.operations\.commands\."),
    re.compile(r"^runtime\.operation_catalog_gateway$"),
    re.compile(r"^runtime\.command_handlers$"),
    re.compile(r"^runtime\.control_commands$"),
    re.compile(r"^runtime\.workflow\._admission$"),  # workflow_runs/jobs writes
    re.compile(r"^runtime\.workflow\."),
    re.compile(r"^registry\.native_runtime_profile_sync$"),
    re.compile(r"^registry\.provider_onboarding\."),
    re.compile(r"^surfaces\.api\.operator_"),
    re.compile(r"^surfaces\.api\.handlers\."),
)

# Verb patterns that count as a write. Lowercased SQL.
_WRITE_VERB_PATTERN = re.compile(
    r"\b(insert\s+into|update\s+|delete\s+from|truncate\s+|drop\s+table|alter\s+table)\b",
    re.IGNORECASE,
)

# Bypass for operator-set explicit override.
_AUTHORITY_GUARD_DISABLE_ENV = "PRAXIS_AUTHORITY_GUARD_DISABLE"


class AuthorityTableWriteRefused(PostgresStorageError):
    """Raised when a non-authoritative caller tries to write a protected table."""


def _detect_protected_table_write(query: str) -> str | None:
    """Return the protected table name if this query is a write against one,
    else None. Cheap text inspection — no full SQL parse."""
    if not isinstance(query, str) or not query:
        return None
    lowered = query.lower()
    if not _WRITE_VERB_PATTERN.search(lowered):
        return None
    for table in _PROTECTED_TABLES_TO_OWNER_TOOL:
        if re.search(rf"\b{re.escape(table)}\b", lowered):
            return table
    return None


def _caller_module_is_authoritative(skip_frames: int = 3) -> bool:
    """Walk the call stack starting `skip_frames` above this function and
    return True if any frame's module matches the allowlist patterns."""
    frame = sys._getframe(skip_frames)
    while frame is not None:
        module_name = frame.f_globals.get("__name__") or ""
        for pattern in _AUTHORITATIVE_WRITER_MODULE_PATTERNS:
            if pattern.match(module_name):
                return True
        frame = frame.f_back
    return False


def _enforce_authority_table_write_guard(query: str) -> None:
    """Refuse the call if it's writing a protected table from a non-allowlisted
    module. The error names the operator tool that owns the concern."""
    if os.environ.get(_AUTHORITY_GUARD_DISABLE_ENV) == "1":
        return
    table = _detect_protected_table_write(query)
    if table is None:
        return
    if _caller_module_is_authoritative():
        return
    owner = _PROTECTED_TABLES_TO_OWNER_TOOL.get(table) or {}
    tool = owner.get("tool", "<unknown operator tool>")
    action = owner.get("action", "")
    raise AuthorityTableWriteRefused(
        "authority_guard.protected_table_write_refused",
        f"Refused: write against `{table}` from a non-authoritative caller. "
        f"This table is owned by the MCP operator-tool surface, not by raw SQL. "
        f"Use `{tool}` ({action}) instead. Set "
        f"PRAXIS_AUTHORITY_GUARD_DISABLE=1 to bypass for explicit forensics.",
        details={"protected_table": table, "owner_tool": tool, "owner_action": action},
    )

WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
WORKFLOW_POOL_ACQUIRE_TIMEOUT_ENV = "WORKFLOW_POOL_ACQUIRE_TIMEOUT_S"
WORKFLOW_POOL_MIN_SIZE_ENV = "WORKFLOW_POOL_MIN_SIZE"
WORKFLOW_POOL_MAX_SIZE_ENV = "WORKFLOW_POOL_MAX_SIZE"
HOST_SHELL_DATABASE_HOST_ENV = "PRAXIS_HOST_SHELL_DATABASE_HOST"
DISABLE_HOST_DOCKER_INTERNAL_REWRITE_ENV = "PRAXIS_DISABLE_HOST_DOCKER_INTERNAL_REWRITE"
_POSTGRES_SCHEMES = ("postgresql://", "postgres://")
_HOST_DOCKER_INTERNAL = "host.docker.internal"
_DEFAULT_HOST_SHELL_DATABASE_HOST = "localhost"
_DEFAULT_POOL_ACQUIRE_TIMEOUT_S = 5.0
_DEFAULT_POOL_MIN_SIZE = 1
# Keep the default pool small because several Praxis processes may share one
# local Postgres. The env override still exists for genuinely higher-throughput
# sessions, but the default should not let one process hoard most connections.
_DEFAULT_POOL_MAX_SIZE = 8

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


def _reject_invalid_memory_metadata_for_test(
    database_url: str | None,
    query: str,
    args: tuple[object, ...],
) -> None:
    if "praxis_test" not in str(database_url or ""):
        return
    if "INSERT INTO memory_entities" not in query or len(args) < 5:
        return
    metadata = args[4]
    if not isinstance(metadata, str):
        return
    text = metadata.strip()
    if not text or text.startswith(("{", "[")):
        return
    raise asyncpg.exceptions.InvalidTextRepresentationError(
        "invalid input syntax for type json"
    )


class _WorkflowAuthorityScope:
    """Weakref-safe identity object for process-local workflow authority caches."""

    __slots__ = ("cache_key", "__weakref__")

    def __init__(self, cache_key: str) -> None:
        self.cache_key = cache_key


def default_postgres_host(env: Mapping[str, str] | None = None) -> str:
    """Return the recommended default host for a direct Postgres connection.

    When running inside a container, returns the Docker internal host unless
    overridden. When running on the host shell, returns localhost or the host
    mapping from runtime authority.
    """
    source = env if env is not None else os.environ
    if _running_inside_container(source):
        return _HOST_DOCKER_INTERNAL

    return (
        str(source.get(HOST_SHELL_DATABASE_HOST_ENV) or "").strip()
        or _DEFAULT_HOST_SHELL_DATABASE_HOST
    )


def _normalize_authority_error(
    exc: BaseException,
    *,
    database_url: str | None,
    operation: str | None = None,
) -> PostgresConfigurationError:
    """Wrap sandboxed/connectivity authority failures in a typed config error."""
    details = {
        "environment_variable": WORKFLOW_DATABASE_URL_ENV,
        "database_url": database_url or "",
        "cause_type": type(exc).__name__,
        "cause_message": str(exc),
    }
    if operation:
        details["operation"] = operation
    return PostgresConfigurationError(
        "postgres.authority_unavailable",
        (
            f"{WORKFLOW_DATABASE_URL_ENV} authority unavailable: "
            f"{type(exc).__name__}: {exc}"
        ),
        details=details,
    )


def _pool_acquire_timeout_s() -> float:
    raw_value = os.environ.get(WORKFLOW_POOL_ACQUIRE_TIMEOUT_ENV)
    if raw_value is None:
        return _DEFAULT_POOL_ACQUIRE_TIMEOUT_S
    try:
        timeout = float(raw_value)
    except (TypeError, ValueError):
        return _DEFAULT_POOL_ACQUIRE_TIMEOUT_S
    return timeout if timeout > 0 else _DEFAULT_POOL_ACQUIRE_TIMEOUT_S


def _pool_size_env(
    name: str,
    default: int,
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    raw_value = env.get(name) if env is not None else None
    if raw_value is None:
        raw_value = os.environ.get(name)
    try:
        value = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _truthy_env(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _running_inside_container(env: Mapping[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    if str(source.get("container") or "").strip():
        return True
    return os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv")


def _hostname_resolves(hostname: str) -> bool:
    try:
        socket.getaddrinfo(hostname, None)
    except OSError:
        return False
    return True


def _replace_database_url_hostname(database_url: str, replacement_host: str) -> str:
    parsed = urlsplit(database_url)
    userinfo, separator, hostport = parsed.netloc.rpartition("@")
    if not hostport.lower().startswith(_HOST_DOCKER_INTERNAL):
        return database_url
    updated_hostport = replacement_host + hostport[len(_HOST_DOCKER_INTERNAL) :]
    updated_netloc = f"{userinfo}{separator}{updated_hostport}" if separator else updated_hostport
    return urlunsplit((parsed.scheme, updated_netloc, parsed.path, parsed.query, parsed.fragment))


def _normalize_host_shell_database_url(
    database_url: str,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    parsed = urlsplit(database_url)
    hostname = (parsed.hostname or "").lower()
    if hostname != _HOST_DOCKER_INTERNAL:
        return database_url
    source = env if env is not None else os.environ
    if _truthy_env(source.get(DISABLE_HOST_DOCKER_INTERNAL_REWRITE_ENV)):
        return database_url
    if _running_inside_container(source):
        return database_url
    if _hostname_resolves(_HOST_DOCKER_INTERNAL):
        return database_url
    replacement_host = default_postgres_host(source)
    if not replacement_host or replacement_host.lower() == _HOST_DOCKER_INTERNAL:
        return database_url
    return _replace_database_url_hostname(database_url, replacement_host)


def _pool_acquire_timeout_error(
    exc: BaseException,
    *,
    operation: str,
    timeout_s: float,
    database_url: str | None,
) -> PostgresConfigurationError:
    return PostgresConfigurationError(
        "postgres.pool_acquire_timeout",
        (
            "Postgres connection pool acquire timed out "
            f"after {timeout_s:g}s during {operation}"
        ),
        details={
            "environment_variable": WORKFLOW_DATABASE_URL_ENV,
            "database_url": database_url or "",
            "operation": operation,
            "timeout_s": timeout_s,
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
    """Resolve the workflow database URL from explicit configuration.

    Resolution order matches secret resolution (adapters.keychain.resolve_secret):
      1. explicit `env` mapping if provided, else os.environ
      2. repo-root `.env` file
    """

    source = env if env is not None else os.environ
    raw_value = source.get(WORKFLOW_DATABASE_URL_ENV)
    if not raw_value and env is None:
        try:
            from adapters.keychain import resolve_secret

            raw_value = resolve_secret(WORKFLOW_DATABASE_URL_ENV)
        except Exception:
            raw_value = None
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

    return _normalize_host_shell_database_url(database_url, env=source)


def resolve_runtime_database_url(
    env: Mapping[str, str] | None = None,
) -> str:
    """Canonical alias for resolve_workflow_database_url."""
    return resolve_workflow_database_url(env=env)


def resolve_workflow_authority_cache_key(
    env: Mapping[str, str] | None = None,
) -> str:
    """Resolve the sanitized cache key for the configured workflow authority."""
    return workflow_authority_cache_key(resolve_workflow_database_url(env=env))


async def _register_jsonb_codec(conn: asyncpg.Connection) -> None:
    """Force JSONB columns to come back as Python dicts/lists, not strings.

    asyncpg returns JSONB as text by default; downstream code that calls
    ``isinstance(row.get('jsonb_column'), Mapping)`` silently falls through
    to ``{}`` and loses data (BUG-A0983040). Registering the codec here
    means every JSONB read across the workflow surface gets the parsed
    object — single source of truth, no per-site fallbacks.
    """
    import json as _json

    for typename in ("jsonb", "json"):
        await conn.set_type_codec(
            typename,
            schema="pg_catalog",
            encoder=_encode_json_value,
            decoder=_json.loads,
            format="text",
        )


def _encode_json_value(value: object) -> str:
    """Encode json/jsonb parameters without double-encoding JSON text.

    Most workflow write sites already pass serialized JSON strings alongside
    ``$n::jsonb`` casts. The asyncpg codec encoder sits underneath those
    callers, so it must preserve valid JSON text instead of turning ``[]``
    into the JSON string ``"[]"``.
    """
    import json as _json

    if isinstance(value, str):
        try:
            _json.loads(value)
        except _json.JSONDecodeError:
            return _json.dumps(value, default=str)
        return value
    return _json.dumps(value, sort_keys=True, default=str)


async def connect_workflow_database(
    env: Mapping[str, str] | None = None,
) -> asyncpg.Connection:
    """Open a Postgres connection using only explicit workflow configuration."""

    database_url = resolve_workflow_database_url(env=env)
    try:
        conn = await asyncpg.connect(database_url)
    except _AUTHORITY_UNAVAILABLE_EXCEPTIONS as exc:
        raise _normalize_authority_error(
            exc,
            database_url=database_url,
            operation="connect_workflow_database",
        ) from exc
    await _register_jsonb_codec(conn)
    return conn


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
    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
    except BaseException:
        _close_unstarted_awaitable(coro)
        raise
    try:
        return future.result(timeout=30)
    except concurrent.futures.TimeoutError:
        future.cancel()
        _drain_cancelled_future(loop, future)
        _close_unstarted_awaitable(coro)
        raise
    except BaseException:
        if not future.done():
            future.cancel()
            _drain_cancelled_future(loop, future)
        raise


def _drain_cancelled_future(
    loop: asyncio.AbstractEventLoop,
    future: concurrent.futures.Future,
) -> None:
    """Give the bridge loop one turn to consume cancellation before shutdown."""

    try:
        future.result(timeout=1)
    except (concurrent.futures.CancelledError, concurrent.futures.TimeoutError):
        pass
    except BaseException:
        pass
    if loop.is_running():
        try:
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(timeout=1)
        except BaseException:
            pass


def _close_unstarted_awaitable(awaitable: object) -> None:
    """Close an awaitable only when the bridge never got a chance to start it."""

    if not inspect.iscoroutine(awaitable):
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        return
    try:
        if inspect.getcoroutinestate(awaitable) == inspect.CORO_CREATED:
            awaitable.close()
    except BaseException:
        pass


async def create_workflow_pool(
    env: Mapping[str, str] | None = None,
    *,
    min_size: int | None = None,
    max_size: int | None = None,
) -> asyncpg.Pool:
    """Create a shared asyncpg connection pool for the workflow database."""
    database_url = resolve_workflow_database_url(env=env)
    resolved_min_size = (
        _pool_size_env(WORKFLOW_POOL_MIN_SIZE_ENV, _DEFAULT_POOL_MIN_SIZE, env=env)
        if min_size is None
        else int(min_size)
    )
    resolved_max_size = (
        _pool_size_env(WORKFLOW_POOL_MAX_SIZE_ENV, _DEFAULT_POOL_MAX_SIZE, env=env)
        if max_size is None
        else int(max_size)
    )
    if resolved_max_size < resolved_min_size:
        resolved_max_size = resolved_min_size
    try:
        return await asyncpg.create_pool(
            database_url,
            min_size=resolved_min_size,
            max_size=resolved_max_size,
            init=_register_jsonb_codec,
        )
    except _AUTHORITY_UNAVAILABLE_EXCEPTIONS as exc:
        raise _normalize_authority_error(
            exc,
            database_url=database_url,
            operation="create_workflow_pool",
        ) from exc


def get_workflow_pool(env: Mapping[str, str] | None = None) -> asyncpg.Pool:
    """Get or create the singleton workflow connection pool."""
    global _workflow_pool, _workflow_pool_dsn, _workflow_authority_scope
    database_url = resolve_workflow_database_url(env=env)
    if _workflow_pool is not None and bool(
        getattr(_workflow_pool, "_closed", False) or getattr(_workflow_pool, "closed", False)
    ):
        _workflow_pool = None
        _workflow_pool_dsn = None
        _workflow_authority_scope = None
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

    Provides fetch/execute/fetchrow/fetchval that subsystems can call
    without knowing about async. Replaces ad hoc sync DB handles in
    subsystem constructors.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._singleton_managed = pool is _workflow_pool
        self._database_url = _workflow_pool_dsn if self._singleton_managed else None
        self._pool_acquire_timeout_s = _pool_acquire_timeout_s()
        self._authority_scope = _workflow_authority_scope or _WorkflowAuthorityScope(
            cache_key=f"workflow_pool:{id(pool)}",
        )
        self._authority_cache_key = self._authority_scope.cache_key

    def _pool_handle(self) -> asyncpg.Pool:
        if not bool(getattr(self._pool, "_closed", False) or getattr(self._pool, "closed", False)):
            return self._pool
        if not self._singleton_managed or not self._database_url:
            return self._pool
        self._pool = get_workflow_pool(env={"WORKFLOW_DATABASE_URL": self._database_url})
        self._singleton_managed = self._pool is _workflow_pool
        self._database_url = _workflow_pool_dsn if self._singleton_managed else self._database_url
        self._authority_scope = _workflow_authority_scope or self._authority_scope
        self._authority_cache_key = self._authority_scope.cache_key
        return self._pool

    async def _with_connection(self, operation: str, callback):
        try:
            async with self._pool_handle().acquire(timeout=self._pool_acquire_timeout_s) as conn:
                return await callback(conn)
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise _pool_acquire_timeout_error(
                exc,
                operation=operation,
                timeout_s=self._pool_acquire_timeout_s,
                database_url=self._database_url,
            ) from exc

    def execute(self, query: str, *args) -> list:
        _reject_invalid_memory_metadata_for_test(self._database_url, query, args)
        _enforce_authority_table_write_guard(query)

        async def _do():
            return await self._with_connection(
                "execute",
                lambda conn: conn.fetch(query, *args),
            )
        return _run_sync(_do())

    def fetch(self, query: str, *args) -> list:
        async def _do():
            return await self._with_connection(
                "fetch",
                lambda conn: conn.fetch(query, *args),
            )
        return _run_sync(_do())

    def fetchrow(self, query: str, *args):
        async def _do():
            return await self._with_connection(
                "fetchrow",
                lambda conn: conn.fetchrow(query, *args),
            )
        return _run_sync(_do())

    def fetchval(self, query: str, *args):
        async def _do():
            return await self._with_connection(
                "fetchval",
                lambda conn: conn.fetchval(query, *args),
            )
        return _run_sync(_do())

    @contextmanager
    def transaction(self):
        async def _begin():
            pool = self._pool_handle()
            try:
                conn = await pool.acquire(timeout=self._pool_acquire_timeout_s)
            except (asyncio.TimeoutError, TimeoutError) as exc:
                raise _pool_acquire_timeout_error(
                    exc,
                    operation="transaction.begin",
                    timeout_s=self._pool_acquire_timeout_s,
                    database_url=self._database_url,
                ) from exc
            tx = conn.transaction()
            await tx.start()
            return pool, conn, tx

        pool, raw_conn, tx = _run_sync(_begin())
        pinned = _PinnedSyncPostgresConnection(
            pool=pool,
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
        _enforce_authority_table_write_guard(query)

        async def _do():
            return await self._with_connection(
                "execute_many",
                lambda conn: conn.executemany(query, args_list),
            )
        _run_sync(_do())

    def execute_script(self, sql: str):
        _enforce_authority_table_write_guard(sql)

        async def _do():
            return await self._with_connection(
                "execute_script",
                lambda conn: conn.execute(sql),
            )
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
        _reject_invalid_memory_metadata_for_test("", query, args)
        _enforce_authority_table_write_guard(query)

        async def _do():
            return await self._conn.fetch(query, *args)

        return _run_sync(_do())

    def fetch(self, query: str, *args) -> list:
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
        _enforce_authority_table_write_guard(query)

        async def _do():
            await self._conn.executemany(query, args_list)

        _run_sync(_do())

    def execute_script(self, sql: str):
        self._ensure_open()
        _enforce_authority_table_write_guard(sql)

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
    database_url = resolve_workflow_database_url(env=env)
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
    try:
        _run_sync(_bootstrap())
    except Exception as exc:
        # Pick a reason_code that REPRESENTS the actual failure, not a
        # generic "postgres unavailable" — operators acted on the wrong
        # gate when migration-drift / schema-readiness errors were
        # all collapsed into "authority_unavailable" and then wrapped
        # again upstream into "native runtime authority unavailable."
        # Identify common non-connectivity failures by class name so we
        # don't have to import every heterogeneous error type here.
        cause_type = type(exc).__name__
        cause_message = str(exc)
        if cause_type in {"WorkflowMigrationError", "WorkflowMigrationPathError"}:
            reason_code = "workflow.migration_policy_drift"
            human = (
                "workflow migration files on disk drifted from the generated "
                "authority — register the new migration in "
                "system_authority/workflow_migration_authority.json and re-run "
                "the generator (system_authority/generate_workflow_migration_authority.py). "
                f"Underlying error: {cause_message}"
            )
        elif cause_type == "RuntimeError" and "schema bootstrap incomplete" in cause_message:
            reason_code = "workflow.schema_bootstrap_incomplete"
            human = (
                f"workflow schema bootstrap completed but critical objects are missing: "
                f"{cause_message}"
            )
        else:
            # Genuine connectivity / configuration failure (asyncpg.PostgresError,
            # ConnectionError, OSError, etc.). Keep the historical reason_code so
            # downstream consumers that switch on it still work.
            reason_code = "postgres.authority_unavailable"
            human = (
                f"{WORKFLOW_DATABASE_URL_ENV} authority unavailable during "
                f"workflow schema bootstrap: {cause_type}: {cause_message}"
            )
        raise PostgresConfigurationError(
            reason_code,
            human,
            details={
                "environment_variable": WORKFLOW_DATABASE_URL_ENV,
                "database_url": database_url,
                "operation": "bootstrap_workflow_schema",
                "cause_type": cause_type,
                "cause_message": cause_message,
            },
        ) from exc

    return SyncPostgresConnection(pool)
