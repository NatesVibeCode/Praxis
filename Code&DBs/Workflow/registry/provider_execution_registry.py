"""Provider execution registry authority.

This module owns the DB-backed provider execution catalog:

- loading provider CLI profiles from Postgres
- caching registry state and loader health
- parsing authoritative profile rows and adapter config
- exposing the canonical provider profile lookup surface

Transport mechanics remain in ``adapters.provider_transport`` and callers that
need the legacy import path should continue using ``adapters.provider_registry``.
"""

from __future__ import annotations

import enum
import json
import logging
import os
import threading
import time
from typing import Any

from adapters import provider_transport
from adapters.provider_types import ProviderAdapterContract, ProviderCLIProfile
from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

__all__ = [
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryLoadTimeout",
    "RegistryLoadStatus",
    "reload_from_db",
    "registry_health",
    "resolve_adapter_config",
    "get_profile",
    "get_all_profiles",
    "registered_providers",
    "resolve_provider_from_alias",
    "default_provider_slug",
    "default_llm_adapter_type",
    "default_model_for_provider",
    "resolve_adapter_economics",
    "resolve_api_endpoint",
    "resolve_api_protocol_family",
    "resolve_api_key_env_vars",
    "resolve_mcp_args_template",
    "resolve_lane_policy",
    "resolve_adapter_contract",
    "supports_adapter",
    "supports_model_adapter",
    "resolve_binary",
    "build_command",
    "validate_profiles",
]

logger = logging.getLogger(__name__)

try:
    import asyncpg as _asyncpg  # type: ignore[import-untyped]

    _ASYNCPG_AVAILABLE = True
except ImportError:
    _asyncpg = None  # type: ignore[assignment]
    _ASYNCPG_AVAILABLE = False
    logger.info("asyncpg not installed — provider registry will use built-in profiles only")


class ProviderRegistryError(RuntimeError):
    """Base exception for provider registry failures."""


class ProviderRegistryLoadError(ProviderRegistryError):
    """Database unavailable or connection refused."""


class ProviderRegistrySchemaError(ProviderRegistryError):
    """DB schema does not match expected columns."""


class ProviderRegistryDataError(ProviderRegistryError):
    """Row data failed validation."""


class ProviderRegistryLoadTimeout(ProviderRegistryError):
    """DB load exceeded timeout."""


class RegistryLoadStatus(enum.Enum):
    UNLOADED = "unloaded"
    LOADED_FROM_DB = "loaded_from_db"
    DEGRADED_BUILTIN = "degraded_builtin"
    LOAD_FAILED = "load_failed"


_REGISTRY: dict[str, ProviderCLIProfile] = {}
_ALIAS_MAP: dict[str, str] = {}
_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"

_ADAPTER_CONFIG: dict[str, Any] = {}
_ADAPTER_FAILURE_MAPPINGS: dict[str, dict[str, str]] = {}

_DB_LOAD_LOCK = threading.Lock()
_load_status: RegistryLoadStatus = RegistryLoadStatus.UNLOADED
_load_error: str | None = None
_load_timestamp: float | None = None
_LOAD_TIMEOUT_ENV = "PRAXIS_PROVIDER_REGISTRY_LOAD_TIMEOUT"
_DEFAULT_LOAD_TIMEOUT = 30
_DB_LOADED = False


def _register(profile: ProviderCLIProfile) -> None:
    _REGISTRY[profile.provider_slug] = profile
    _ALIAS_MAP[profile.binary] = profile.provider_slug
    for alias in profile.aliases:
        _ALIAS_MAP[alias] = profile.provider_slug


for _builtin_profile in provider_transport.BUILTIN_PROVIDER_PROFILES:
    _register(_builtin_profile)


def _require_database_url() -> str:
    try:
        return resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "provider_registry requires explicit WORKFLOW_DATABASE_URL Postgres authority"
        ) from exc


def _row_text_tuple(value: Any, fallback: tuple[str, ...] = ()) -> tuple[str, ...]:
    if value is None:
        return tuple(fallback)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            value = [value]
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(fallback)


def _parse_mcp_args_template(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    if isinstance(value, list) and value:
        return [str(item) for item in value if item]
    return None


def _row_json_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    return dict(value) if isinstance(value, dict) else None


def _row_json_mapping(value: Any) -> dict[str, dict[str, Any]] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(value, dict):
        return None
    normalized: dict[str, dict[str, Any]] = {}
    for key, item in value.items():
        if isinstance(item, dict):
            normalized[str(key)] = dict(item)
    return normalized


def _load_timeout_seconds() -> int:
    try:
        return max(5, int(os.environ.get(_LOAD_TIMEOUT_ENV, _DEFAULT_LOAD_TIMEOUT)))
    except (ValueError, TypeError):
        return _DEFAULT_LOAD_TIMEOUT


def _run_async(coro: Any) -> Any:
    import asyncio
    import concurrent.futures

    timeout = _load_timeout_seconds()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            try:
                return pool.submit(asyncio.run, coro).result(timeout=timeout)
            except concurrent.futures.TimeoutError as exc:
                raise ProviderRegistryLoadTimeout(
                    f"DB load timed out after {timeout}s "
                    f"(set {_LOAD_TIMEOUT_ENV} to increase)"
                ) from exc
    return asyncio.run(coro)


async def _fetch_from_db(db_url: str) -> tuple[list[Any], list[Any], list[Any]]:
    """Fetch all registry tables in a single connection."""

    conn = await _asyncpg.connect(db_url)
    try:
        provider_rows = await conn.fetch(
            """
            SELECT
                profile.provider_slug,
                profile.binary_name,
                profile.base_flags,
                profile.model_flag,
                profile.system_prompt_flag,
                profile.json_schema_flag,
                profile.output_format,
                profile.output_envelope_key,
                profile.forbidden_flags,
                profile.default_timeout,
                profile.aliases,
                profile.default_model,
                profile.api_endpoint,
                profile.api_protocol_family,
                profile.api_key_env_vars,
                profile.adapter_economics,
                profile.prompt_mode,
                profile.mcp_config_style,
                profile.mcp_args_template,
                profile.sandbox_env_overrides,
                profile.exclude_from_rotation,
                COALESCE(admission.lane_policies, '{}'::jsonb) AS lane_policies
            FROM provider_cli_profiles AS profile
            LEFT JOIN (
                SELECT
                    provider_slug,
                    jsonb_object_agg(
                        adapter_type,
                        jsonb_build_object(
                            'provider_transport_admission_id', provider_transport_admission_id,
                            'adapter_type', adapter_type,
                            'transport_kind', transport_kind,
                            'execution_topology', execution_topology,
                            'admitted_by_policy', admitted_by_policy,
                            'policy_reason', policy_reason,
                            'decision_ref', decision_ref,
                            'docs_urls', docs_urls,
                            'credential_sources', credential_sources,
                            'probe_contract', probe_contract,
                            'status', status
                        )
                    ) AS lane_policies
                FROM provider_transport_admissions
                WHERE status = 'active'
                GROUP BY provider_slug
            ) AS admission
              ON admission.provider_slug = profile.provider_slug
            WHERE profile.status = 'active'
            ORDER BY profile.provider_slug ASC
            """
        )
        try:
            config_rows = await conn.fetch(
                "SELECT config_key, config_value FROM adapter_config"
            )
        except Exception as exc:
            logger.warning("provider_registry: adapter_config load failed: %s", exc)
            config_rows = []
        try:
            failure_rows = await conn.fetch(
                "SELECT transport_kind, failure_code, mapped_code "
                "FROM adapter_failure_mappings"
            )
        except Exception as exc:
            logger.warning("provider_registry: adapter_failure_mappings load failed: %s", exc)
            failure_rows = []
        return provider_rows, config_rows, failure_rows
    finally:
        await conn.close()


def _parse_profile_row(row: Any) -> ProviderCLIProfile:
    provider_slug = str(row["provider_slug"]).strip()
    if not provider_slug:
        raise ProviderRegistryDataError("provider_slug is empty")
    binary_name = str(row["binary_name"]).strip()
    if not binary_name:
        raise ProviderRegistryDataError(f"{provider_slug}: binary_name is empty")
    return ProviderCLIProfile(
        provider_slug=provider_slug,
        binary=binary_name,
        default_model=(
            str(row["default_model"]).strip()
            if row["default_model"] not in (None, "")
            else None
        ),
        api_endpoint=(
            str(row["api_endpoint"]).strip()
            if row["api_endpoint"] not in (None, "")
            else None
        ),
        api_protocol_family=(
            str(row["api_protocol_family"]).strip()
            if row["api_protocol_family"] not in (None, "")
            else None
        ),
        api_key_env_vars=_row_text_tuple(row["api_key_env_vars"]),
        adapter_economics=_row_json_mapping(row["adapter_economics"]),
        lane_policies=_row_json_mapping(row["lane_policies"]),
        prompt_mode=(
            str(row["prompt_mode"]).strip().lower()
            if row["prompt_mode"] not in (None, "")
            else "stdin"
        ),
        base_flags=_row_text_tuple(row["base_flags"]),
        model_flag=(
            str(row["model_flag"]).strip()
            if row["model_flag"] not in (None, "")
            else None
        ),
        system_prompt_flag=(
            str(row["system_prompt_flag"]).strip()
            if row["system_prompt_flag"] not in (None, "")
            else None
        ),
        json_schema_flag=(
            str(row["json_schema_flag"]).strip()
            if row["json_schema_flag"] not in (None, "")
            else None
        ),
        output_format=(
            str(row["output_format"]).strip()
            if row["output_format"] not in (None, "")
            else "json"
        ),
        output_envelope_key=(
            str(row["output_envelope_key"]).strip()
            if row["output_envelope_key"] not in (None, "")
            else "result"
        ),
        forbidden_flags=_row_text_tuple(row["forbidden_flags"]),
        default_timeout=int(row["default_timeout"]),
        mcp_config_style=(
            str(row["mcp_config_style"]).strip()
            if row.get("mcp_config_style") not in (None, "")
            else None
        ),
        mcp_args_template=_parse_mcp_args_template(row.get("mcp_args_template")),
        sandbox_env_overrides=_row_json_dict(row.get("sandbox_env_overrides")),
        exclude_from_rotation=bool(row.get("exclude_from_rotation")),
        aliases=_row_text_tuple(row["aliases"]),
    )


def _load_adapter_config(config_rows: list[Any]) -> None:
    loaded: dict[str, Any] = {}
    for row in config_rows:
        key = str(row["config_key"])
        value = row["config_value"]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                pass
        loaded[key] = value
    _ADAPTER_CONFIG.clear()
    _ADAPTER_CONFIG.update(loaded)


def _load_failure_mappings(failure_rows: list[Any]) -> None:
    loaded: dict[str, dict[str, str]] = {}
    for row in failure_rows:
        kind = str(row["transport_kind"])
        loaded.setdefault(kind, {})[str(row["failure_code"])] = str(row["mapped_code"])
    _ADAPTER_FAILURE_MAPPINGS.clear()
    _ADAPTER_FAILURE_MAPPINGS.update(loaded)


def _load_from_db() -> None:
    """Load provider CLI profiles from Postgres."""

    global _DB_LOADED, _load_status, _load_error, _load_timestamp
    if _DB_LOADED:
        return

    with _DB_LOAD_LOCK:
        if _DB_LOADED:
            return

        if not _ASYNCPG_AVAILABLE:
            logger.warning("provider_registry: asyncpg unavailable — using built-in profiles")
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = "asyncpg not installed"
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return

        try:
            db_url = _require_database_url()
        except RuntimeError as exc:
            logger.warning("provider_registry: %s — using built-in profiles", exc)
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = str(exc)
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return

        try:
            rows, config_rows, failure_rows = _run_async(_fetch_from_db(db_url))
        except ProviderRegistryLoadTimeout as exc:
            logger.error("provider_registry: %s — using built-in profiles", exc)
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = str(exc)
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return
        except Exception as exc:
            logger.error(
                "provider_registry: DB fetch failed (%s: %s) — using built-in profiles",
                type(exc).__name__,
                exc,
            )
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = f"{type(exc).__name__}: {exc}"
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return

        if not rows:
            logger.warning(
                "provider_registry: no active provider_cli_profiles in DB — using built-in profiles"
            )
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = "no active rows"
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return

        loaded_registry: dict[str, ProviderCLIProfile] = {}
        loaded_aliases: dict[str, str] = {}
        parse_errors: list[str] = []
        for row in rows:
            try:
                profile = _parse_profile_row(row)
            except Exception as exc:
                slug = row.get("provider_slug", "<unknown>")
                parse_errors.append(f"{slug}: {exc}")
                logger.warning("provider_registry: skipping bad row %s: %s", slug, exc)
                continue
            loaded_registry[profile.provider_slug] = profile
            loaded_aliases[profile.binary] = profile.provider_slug
            for alias in profile.aliases:
                if alias in loaded_aliases and loaded_aliases[alias] != profile.provider_slug:
                    logger.warning(
                        "provider_registry: alias %r collision (%s overwrites %s)",
                        alias,
                        profile.provider_slug,
                        loaded_aliases[alias],
                    )
                loaded_aliases[alias] = profile.provider_slug

        if not loaded_registry:
            logger.error(
                "provider_registry: all %d DB rows failed validation — using built-in profiles. "
                "Errors: %s",
                len(rows),
                "; ".join(parse_errors),
            )
            _load_status = RegistryLoadStatus.DEGRADED_BUILTIN
            _load_error = f"all {len(rows)} rows failed validation"
            _load_timestamp = time.monotonic()
            _DB_LOADED = True
            return

        _REGISTRY.clear()
        _REGISTRY.update(loaded_registry)
        _ALIAS_MAP.clear()
        _ALIAS_MAP.update(loaded_aliases)
        _load_adapter_config(config_rows)
        _load_failure_mappings(failure_rows)

        _load_status = RegistryLoadStatus.LOADED_FROM_DB
        _load_error = f"{len(parse_errors)} row(s) skipped" if parse_errors else None
        _load_timestamp = time.monotonic()
        _DB_LOADED = True

        logger.info(
            "provider_registry: loaded %d provider(s) from DB%s",
            len(loaded_registry),
            f" ({len(parse_errors)} skipped)" if parse_errors else "",
        )


def reload_from_db() -> None:
    """Force a fresh read of provider_cli_profiles on the next lookup."""

    global _DB_LOADED, _load_status, _load_error, _load_timestamp
    with _DB_LOAD_LOCK:
        _DB_LOADED = False
        _load_status = RegistryLoadStatus.UNLOADED
        _load_error = None
        _load_timestamp = None
    _load_from_db()


def registry_health() -> dict[str, Any]:
    """Return current load status, error details, and provider count."""

    _load_from_db()
    return {
        "status": _load_status.value,
        "error": _load_error,
        "loaded_at": _load_timestamp,
        "provider_count": len(_REGISTRY),
        "providers": sorted(_REGISTRY.keys()),
        "asyncpg_available": _ASYNCPG_AVAILABLE,
    }


def resolve_adapter_config(key: str, default: Any = None) -> Any:
    """Read a value from the DB-backed adapter_config table."""

    _load_from_db()
    value = _ADAPTER_CONFIG.get(key)
    return value if value is not None else default


def get_profile(provider_slug: str) -> ProviderCLIProfile | None:
    _load_from_db()
    return _REGISTRY.get(provider_slug)


def get_all_profiles() -> dict[str, ProviderCLIProfile]:
    _load_from_db()
    return dict(_REGISTRY)


def registered_providers() -> list[str]:
    _load_from_db()
    return sorted(_REGISTRY.keys())


def resolve_provider_from_alias(alias: str) -> str | None:
    _load_from_db()
    return _ALIAS_MAP.get(alias)


def default_provider_slug() -> str:
    _load_from_db()
    if not _REGISTRY:
        raise RuntimeError("provider_registry has no authoritative provider profiles")
    return sorted(_REGISTRY)[0]


def default_llm_adapter_type() -> str:
    _load_from_db()
    return provider_transport.default_llm_adapter_type(
        _REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
    )


def default_model_for_provider(provider_slug: str) -> str | None:
    _load_from_db()
    return provider_transport.default_model_for_provider(provider_slug, _REGISTRY)


def resolve_adapter_economics(provider_slug: str, adapter_type: str) -> dict[str, Any]:
    _load_from_db()
    return provider_transport.resolve_adapter_economics(
        provider_slug,
        adapter_type,
        profiles=_REGISTRY,
    )


def resolve_api_endpoint(provider_slug: str, model_slug: str | None = None) -> str | None:
    _load_from_db()
    return provider_transport.resolve_api_endpoint(
        provider_slug,
        profiles=_REGISTRY,
        model_slug=model_slug,
        logger=logger,
    )


def resolve_api_protocol_family(provider_slug: str) -> str | None:
    _load_from_db()
    return provider_transport.resolve_api_protocol_family(provider_slug, profiles=_REGISTRY)


def resolve_api_key_env_vars(provider_slug: str) -> tuple[str, ...]:
    _load_from_db()
    return provider_transport.resolve_api_key_env_vars(provider_slug, profiles=_REGISTRY)


def resolve_mcp_args_template(provider_slug: str) -> list[str]:
    _load_from_db()
    return provider_transport.resolve_mcp_args_template(provider_slug, profiles=_REGISTRY)


def resolve_lane_policy(provider_slug: str, adapter_type: str) -> dict[str, Any] | None:
    _load_from_db()
    return provider_transport.resolve_lane_policy(
        provider_slug,
        adapter_type,
        profiles=_REGISTRY,
    )


def resolve_adapter_contract(
    provider_slug: str,
    adapter_type: str,
) -> ProviderAdapterContract | None:
    _load_from_db()
    return provider_transport.resolve_adapter_contract(
        provider_slug,
        adapter_type,
        profiles=_REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
    )


def supports_adapter(provider_slug: str, adapter_type: str) -> bool:
    _load_from_db()
    return provider_transport.supports_adapter(
        provider_slug,
        adapter_type,
        profiles=_REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
    )


def supports_model_adapter(provider_slug: str, model_slug: str, adapter_type: str) -> bool:
    _load_from_db()
    return provider_transport.supports_model_adapter(
        provider_slug,
        model_slug,
        adapter_type,
        profiles=_REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
    )


def resolve_binary(provider_slug: str) -> str | None:
    _load_from_db()
    return provider_transport.resolve_binary(provider_slug, profiles=_REGISTRY)


def build_command(
    provider_slug: str,
    model: str | None = None,
    *,
    binary_override: str | None = None,
    system_prompt: str | None = None,
    json_schema: str | None = None,
) -> list[str]:
    _load_from_db()
    return provider_transport.build_command(
        provider_slug,
        profiles=_REGISTRY,
        model=model,
        binary_override=binary_override,
        system_prompt=system_prompt,
        json_schema=json_schema,
    )


def validate_profiles() -> dict[str, dict[str, Any]]:
    _load_from_db()
    return provider_transport.validate_profiles(
        _REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
        adapter_types=tuple(provider_transport.KNOWN_LLM_ADAPTER_TYPES),
    )
