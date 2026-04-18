"""Centralized configuration registry backed by Postgres.

Replaces hardcoded buffer sizes, timeouts, model context windows,
truncation limits, and other magic numbers scattered across modules
with a single registry that reads from the ``platform_config`` table.

The registry does not seed fallback rows. If authority is missing, lookups
fail explicitly.

Singleton access:  ``get_config()``
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime._workflow_database import resolve_runtime_database_url
from storage.postgres.validators import PostgresConfigurationError

_log = logging.getLogger(__name__)

_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"

# ---------------------------------------------------------------------------
# ConfigEntry — frozen authority record for one configuration value
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ConfigEntry:
    """One configuration entry in the registry."""

    key: str
    value: float | int | str
    category: str  # routing, execution, observability, context
    description: str
    min_value: float | None
    max_value: float | None
    updated_at: datetime


_CACHE_TTL_S = 60.0


# ---------------------------------------------------------------------------
# ConfigRegistry
# ---------------------------------------------------------------------------

def _coerce_value(raw: str, value_type: str) -> float | int | str:
    """Convert a stored string value back to its typed representation."""
    if value_type == "int":
        return int(float(raw))
    if value_type == "float":
        return float(raw)
    return raw


def _value_type_label(value: Any) -> str:
    """Derive the value_type column label from a Python value."""
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _resolve_database_url() -> str | None:
    """Return the Postgres DSN for configuration authority.

    Uses the canonical runtime resolver so process env, launchd plist, repo
    .env, and docker-compose fallbacks all share one code path.
    """
    try:
        return resolve_runtime_database_url(required=False)
    except PostgresConfigurationError:
        return None


def _require_database_url() -> str:
    """Return the authoritative Postgres DSN or fail closed."""
    try:
        dsn = resolve_runtime_database_url(required=True)
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            f"config_registry requires explicit {_DATABASE_URL_ENV} Postgres authority"
        ) from exc
    if dsn is None:
        raise RuntimeError(
            f"config_registry requires explicit {_DATABASE_URL_ENV} Postgres authority"
        )
    return dsn


def _run_async(coro: Any) -> Any:
    """Run an async coroutine from synchronous code."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        # Already inside an event loop — schedule on a background thread.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result(timeout=10)
    return asyncio.run(coro)


class ConfigRegistry:
    """Central configuration registry backed by Postgres ``platform_config``.

    On first access the registry loads all rows from Postgres. Cache is
    refreshed after ``_CACHE_TTL_S`` seconds. Missing keys raise instead
    of falling back to hidden defaults.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[str, ConfigEntry] = {}
        self._cache_loaded_at: float = 0.0

    def _ensure_cache(self) -> None:
        """Populate / refresh the cache when stale."""
        now = time.monotonic()
        if self._cache and (now - self._cache_loaded_at) < _CACHE_TTL_S:
            return
        with self._lock:
            # Double-check inside lock.
            if self._cache and (time.monotonic() - self._cache_loaded_at) < _CACHE_TTL_S:
                return
            self._load_from_db()

    def _load_from_db(self) -> None:
        """Load authoritative configuration rows from Postgres."""
        rows = _run_async(self._async_load(_require_database_url()))
        self._cache = {}
        for row in rows:
            key = row["config_key"]
            self._cache[key] = ConfigEntry(
                key=key,
                value=_coerce_value(row["config_value"], row["value_type"]),
                category=row["category"],
                description=row["description"],
                min_value=row["min_value"],
                max_value=row["max_value"],
                updated_at=row["updated_at"],
            )
        self._cache_loaded_at = time.monotonic()

    @staticmethod
    async def _async_load(dsn: str) -> list[dict[str, Any]]:
        import asyncpg
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                "SELECT config_key, config_value, value_type, category, "
                "description, min_value, max_value, updated_at "
                "FROM platform_config ORDER BY config_key"
            )
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    # -- public API ----------------------------------------------------------

    def get(self, key: str) -> Any:
        """Return the value for *key* or fail explicitly when absent."""
        self._ensure_cache()
        entry = self._cache.get(key)
        if entry is None:
            raise RuntimeError(
                f"config_registry missing authoritative value for {key}"
            )
        return entry.value

    def get_int(self, key: str) -> int:
        """Return an integer config value or fail explicitly when absent."""
        return int(self.get(key))

    def get_float(self, key: str) -> float:
        """Return a float config value or fail explicitly when absent."""
        return float(self.get(key))

    def set(
        self,
        key: str,
        value: Any,
        *,
        category: str = "general",
        description: str = "",
    ) -> None:
        """Write one value to Postgres and update the local cache."""
        dsn = _require_database_url()
        vtype = _value_type_label(value)
        now = datetime.now(timezone.utc)
        _run_async(self._async_set(dsn, key, str(value), vtype, category, description))
        with self._lock:
            self._cache[key] = ConfigEntry(
                key=key,
                value=value,
                category=category,
                description=description,
                min_value=None,
                max_value=None,
                updated_at=now,
            )

    @staticmethod
    async def _async_set(
        dsn: str,
        key: str,
        value_str: str,
        value_type: str,
        category: str,
        description: str,
    ) -> None:
        import asyncpg
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                """
                INSERT INTO platform_config
                    (config_key, config_value, value_type, category, description, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (config_key)
                DO UPDATE SET
                    config_value = EXCLUDED.config_value,
                    value_type = EXCLUDED.value_type,
                    category = EXCLUDED.category,
                    description = EXCLUDED.description,
                    updated_at = NOW()
                """,
                key, value_str, value_type, category, description,
            )
        finally:
            await conn.close()

    def all_entries(self) -> dict[str, ConfigEntry]:
        """Return all cached config entries."""
        self._ensure_cache()
        return dict(self._cache)

    def seed_defaults(self, defaults: dict[str, tuple[Any, str, str]] | None = None) -> None:
        """Seeding fallback defaults is no longer supported.

        Use explicit database writes or migrations to establish platform_config
        authority before runtime.
        """
        del defaults
        raise RuntimeError(
            "config_registry no longer seeds fallback defaults; platform_config authority must already be present"
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_CONFIG: ConfigRegistry | None = None
_CONFIG_LOCK = threading.Lock()


def get_config() -> ConfigRegistry:
    """Return the module-level ConfigRegistry singleton."""
    global _CONFIG
    if _CONFIG is None:
        with _CONFIG_LOCK:
            if _CONFIG is None:
                _CONFIG = ConfigRegistry()
    return _CONFIG


__all__ = [
    "ConfigEntry",
    "ConfigRegistry",
    "get_config",
]
