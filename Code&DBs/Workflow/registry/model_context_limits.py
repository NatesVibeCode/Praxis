"""Model context window limits and safe budget calculations.

Maps provider/model pairs to their context window sizes and derives
safe token budgets that leave room for system prompts and responses.

Context windows are authoritative from Postgres
``model_profiles.default_parameters->'context_window'``. This module
fails closed when ``WORKFLOW_DATABASE_URL`` is missing or when the
requested provider/model pair has no active authoritative profile.

Budget ratio reads from the config registry (``context.budget_ratio``)
and fails explicitly when the row is absent.
"""

from __future__ import annotations

import logging

from runtime._workflow_database import resolve_runtime_database_url
from storage.postgres.validators import PostgresConfigurationError

_log = logging.getLogger(__name__)

_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"

# ---------------------------------------------------------------------------
# DB-backed model_profiles lookup (cached)
# ---------------------------------------------------------------------------

_model_profiles_cache: dict[tuple[str, str], int] | None = None
_model_profiles_loaded: bool = False


def _require_database_url() -> str:
    try:
        dsn = resolve_runtime_database_url(required=True)
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "model_context_limits requires explicit WORKFLOW_DATABASE_URL Postgres authority"
        ) from exc
    if dsn is None:
        raise RuntimeError(
            "model_context_limits requires explicit WORKFLOW_DATABASE_URL Postgres authority"
        )
    return dsn


def _load_model_profiles_context_windows() -> dict[tuple[str, str], int]:
    """Query ``model_profiles.default_parameters->'context_window'`` from Postgres.

    Returns a dict mapping ``(provider_name, model_name)`` to context window
    size. Fails closed on authority or query errors.
    """
    global _model_profiles_cache, _model_profiles_loaded
    if _model_profiles_loaded:
        return _model_profiles_cache or {}

    dsn = _require_database_url()

    try:
        import asyncio
        import asyncpg

        async def _fetch() -> dict[tuple[str, str], int]:
            conn = await asyncpg.connect(dsn)
            try:
                rows = await conn.fetch(
                    """
                    SELECT provider_name, model_name, default_parameters
                    FROM model_profiles
                    WHERE status = 'active'
                      AND default_parameters ? 'context_window'
                    ORDER BY effective_from DESC
                    """
                )
                result: dict[tuple[str, str], int] = {}
                for row in rows:
                    key = (row["provider_name"], row["model_name"])
                    if key not in result:
                        params = row["default_parameters"]
                        if isinstance(params, str):
                            import json
                            params = json.loads(params)
                        cw = params.get("context_window")
                        if cw is not None:
                            result[key] = int(cw)
                return result
            finally:
                await conn.close()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                _model_profiles_cache = pool.submit(asyncio.run, _fetch()).result(timeout=10)
        else:
            _model_profiles_cache = asyncio.run(_fetch())
        _model_profiles_loaded = True
        return _model_profiles_cache or {}

    except Exception as exc:
        _log.debug("model_context_limits: model_profiles lookup failed: %s", exc)
        _model_profiles_cache = None
        _model_profiles_loaded = False
        raise RuntimeError(
            "model_context_limits failed to load authoritative model_profiles context windows"
        ) from exc


def context_window_for_model(
    provider_slug: str,
    model_slug: str | None = None,
) -> int:
    """Return the context window size (in tokens) for a provider/model pair.

    Context windows are authoritative from Postgres model_profiles.
    """
    if not provider_slug or not model_slug:
        raise RuntimeError(
            "model_context_limits requires provider_slug and model_slug for context-window lookup"
        )

    db_windows = _load_model_profiles_context_windows()
    db_val = db_windows.get((provider_slug, model_slug))
    if db_val is None:
        raise RuntimeError(
            f"model_context_limits missing authoritative context window for {provider_slug}/{model_slug}"
        )
    return db_val


def _budget_ratio() -> float:
    """Return the authoritative context budget ratio from Postgres."""
    from registry.config_registry import get_config

    return float(get_config().get("context.budget_ratio"))


def safe_context_budget(
    provider_slug: str,
    model_slug: str | None = None,
) -> int:
    """Return the adaptive fraction of the context window as the safe
    budget for accumulated pipeline context, leaving the remainder for
    system prompt and response generation.
    """
    window = context_window_for_model(provider_slug, model_slug)
    return int(window * _budget_ratio())


__all__ = [
    "context_window_for_model",
    "safe_context_budget",
]
