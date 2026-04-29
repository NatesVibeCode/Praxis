"""Provider execution registry authority.

This module owns the DB-backed provider execution catalog:

- loading provider CLI profiles from Postgres
- caching registry state and loader health
- parsing authoritative profile rows and adapter config
- exposing the canonical provider profile lookup surface

Transport mechanics remain in ``adapters.provider_transport``.
"""

from __future__ import annotations

from runtime.async_bridge import run_sync_safe

import enum
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from adapters import provider_transport
from adapters.provider_types import ProviderAdapterContract, ProviderCLIProfile
from runtime._workflow_database import resolve_runtime_database_url
from runtime.workspace_paths import repo_root as workspace_repo_root
from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

__all__ = [
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryAliasConflictError",
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
    "default_adapter_type_for_provider",
    "resolve_default_adapter_type",
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
    "transport_support_report",
]

logger = logging.getLogger(__name__)

try:
    import asyncpg as _asyncpg  # type: ignore[import-untyped]

    _ASYNCPG_AVAILABLE = True
except ImportError:
    _asyncpg = None  # type: ignore[assignment]
    _ASYNCPG_AVAILABLE = False
    logger.info("asyncpg not installed — provider registry cannot load DB authority")


class ProviderRegistryError(RuntimeError):
    """Base exception for provider registry failures."""


class ProviderRegistryLoadError(ProviderRegistryError):
    """Database unavailable or connection refused."""


class ProviderRegistrySchemaError(ProviderRegistryError):
    """DB schema does not match expected columns."""


class ProviderRegistryDataError(ProviderRegistryError):
    """Row data failed validation."""


class ProviderRegistryAliasConflictError(ProviderRegistryDataError):
    """Two or more provider rows claim the same alias/binary.

    Closes BUG-49388D90. Previously the loader logged a warning and overwrote
    ``loaded_aliases[alias]`` last-writer-wins, which silently remapped
    ``resolve_provider_from_alias(...)`` based on DB row order. That turned
    alias ownership into an implicit routing switch. Now the loader fails
    closed: any alias claimed by >1 distinct ``provider_slug`` refuses the
    entire load so operators must disambiguate rather than getting unlucky.
    """

    def __init__(
        self,
        message: str,
        *,
        conflicts: dict[str, tuple[str, ...]] | None = None,
    ) -> None:
        super().__init__(message)
        self.conflicts = {
            alias: tuple(claimants)
            for alias, claimants in (conflicts or {}).items()
        }


class ProviderRegistryLoadTimeout(ProviderRegistryError):
    """DB load exceeded timeout."""


class RegistryLoadStatus(enum.Enum):
    UNLOADED = "unloaded"
    LOADED_FROM_DB = "loaded_from_db"
    # BUG-F8283CC1: partial loads (some DB rows skipped due to parse errors)
    # must not masquerade as authoritative. A degraded status keeps the
    # successfully-parsed providers usable (routing continues to work) while
    # making the degradation visible to operators rather than hiding it
    # behind the clean "loaded_from_db" label.
    LOADED_FROM_DB_PARTIAL = "loaded_from_db_partial"
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
# BUG-F8283CC1: parse errors observed during the last load. Surfaces in
# registry_health so partial-load degradation is visible rather than hidden.
_load_skipped_rows: tuple[str, ...] = ()
# BUG-867CA639: auxiliary authority tables also participate in dispatch.
# Their load failures must degrade registry health instead of being erased as
# empty optional config.
_load_auxiliary_errors: tuple[str, ...] = ()
_LOAD_TIMEOUT_ENV = "PRAXIS_PROVIDER_REGISTRY_LOAD_TIMEOUT"
_DEFAULT_LOAD_TIMEOUT = 30
_DB_LOADED = False
_DEFAULT_PROVIDER_PRIORITY: tuple[str, ...] = (
    "openai",
    "google",
    "anthropic",
    "cursor",
)


def _clear_registry_state() -> None:
    _REGISTRY.clear()
    _ALIAS_MAP.clear()
    _ADAPTER_CONFIG.clear()
    _ADAPTER_FAILURE_MAPPINGS.clear()


def _repo_root() -> Path:
    return workspace_repo_root()


def _read_repo_env_file(path: Path) -> dict[str, str]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    parsed: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and value:
            parsed[key] = value
    return parsed


def _require_database_url() -> str:
    try:
        return str(resolve_runtime_database_url(repo_root=_repo_root(), required=True))
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "provider execution registry requires explicit WORKFLOW_DATABASE_URL Postgres authority"
        ) from exc


def _record_load_failure(
    error: str,
    *,
    log_level: str,
    message: str,
    emit_log: bool = True,
) -> None:
    global _load_status, _load_error, _load_timestamp, _DB_LOADED, _load_skipped_rows
    global _load_auxiliary_errors

    _clear_registry_state()
    if emit_log:
        if log_level == "error":
            logger.error(message)
        elif log_level == "warning":
            logger.warning(message)
        else:
            logger.info(message)
    _load_status = RegistryLoadStatus.LOAD_FAILED
    _load_error = error
    _load_timestamp = time.monotonic()
    _load_skipped_rows = ()
    _load_auxiliary_errors = ()
    _DB_LOADED = True


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
    return run_sync_safe(coro)


async def _fetch_from_db(
    db_url: str,
) -> tuple[list[Any], list[Any], list[Any], list[str]]:
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
        auxiliary_errors: list[str] = []
        try:
            config_rows = await conn.fetch(
                "SELECT config_key, config_value FROM adapter_config"
            )
        except Exception as exc:
            message = f"adapter_config: {type(exc).__name__}: {exc}"
            logger.warning("provider execution registry: adapter_config load failed: %s", exc)
            auxiliary_errors.append(message)
            config_rows = []
        try:
            failure_rows = await conn.fetch(
                "SELECT transport_kind, failure_code, mapped_code "
                "FROM adapter_failure_mappings"
            )
        except Exception as exc:
            message = f"adapter_failure_mappings: {type(exc).__name__}: {exc}"
            logger.warning(
                "provider execution registry: adapter_failure_mappings load failed: %s",
                exc,
            )
            auxiliary_errors.append(message)
            failure_rows = []
        return provider_rows, config_rows, failure_rows, auxiliary_errors
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

    global _DB_LOADED, _load_status, _load_error, _load_timestamp, _load_skipped_rows
    global _load_auxiliary_errors
    if _DB_LOADED:
        return

    with _DB_LOAD_LOCK:
        if _DB_LOADED:
            return

        if not _ASYNCPG_AVAILABLE:
            _record_load_failure(
                "asyncpg not installed",
                log_level="warning",
                message=(
                    "provider execution registry: asyncpg unavailable; "
                    "DB authority cannot be loaded"
                ),
                emit_log=False,
            )
            return

        try:
            db_url = _require_database_url()
        except RuntimeError as exc:
            _record_load_failure(
                str(exc),
                log_level="warning",
                message=f"provider execution registry: {exc}; DB authority cannot be loaded",
                emit_log=False,
            )
            return

        try:
            rows, config_rows, failure_rows, auxiliary_errors = _run_async(
                _fetch_from_db(db_url)
            )
        except ProviderRegistryLoadTimeout as exc:
            _record_load_failure(
                str(exc),
                log_level="error",
                message=f"provider execution registry: {exc}; DB authority cannot be loaded",
                emit_log=False,
            )
            return
        except Exception as exc:
            _record_load_failure(
                f"{type(exc).__name__}: {exc}",
                log_level="error",
                message=(
                    "provider execution registry: DB fetch failed "
                    f"({type(exc).__name__}: {exc}); DB authority cannot be loaded"
                ),
                emit_log=False,
            )
            return

        if not rows:
            _record_load_failure(
                "no active rows",
                log_level="warning",
                message=(
                    "provider execution registry: no active provider_cli_profiles in DB "
                    "so no provider authority is available"
                ),
                emit_log=False,
            )
            return

        loaded_registry: dict[str, ProviderCLIProfile] = {}
        # alias -> set of provider_slugs that claim it. Collected first, then
        # validated: any alias claimed by >1 distinct slug fails the load
        # closed (BUG-49388D90). A provider can safely claim the same alias
        # multiple times (e.g. binary == an entry in aliases) because set
        # dedupes.
        alias_claimants: dict[str, set[str]] = {}
        parse_errors: list[str] = []
        for row in rows:
            try:
                profile = _parse_profile_row(row)
            except Exception as exc:
                slug = row.get("provider_slug", "<unknown>")
                parse_errors.append(f"{slug}: {exc}")
                logger.warning("provider execution registry: skipping bad row %s: %s", slug, exc)
                continue
            loaded_registry[profile.provider_slug] = profile
            alias_claimants.setdefault(profile.binary, set()).add(profile.provider_slug)
            for alias in profile.aliases:
                alias_claimants.setdefault(alias, set()).add(profile.provider_slug)

        if not loaded_registry:
            _record_load_failure(
                f"all {len(rows)} rows failed validation",
                log_level="error",
                message=(
                    "provider execution registry: all "
                    f"{len(rows)} DB rows failed validation; no provider authority is available. "
                    f"Errors: {'; '.join(parse_errors)}"
                ),
            )
            return

        # BUG-49388D90: detect alias ownership conflicts before installing.
        # If two distinct provider_slugs claim the same alias or binary, the
        # old code wrote loaded_aliases[alias] = second_slug and silently
        # remapped resolve_provider_from_alias by DB row order. Fail closed
        # instead so operators must disambiguate at the authority level.
        conflicts: dict[str, tuple[str, ...]] = {
            alias: tuple(sorted(claimants))
            for alias, claimants in alias_claimants.items()
            if len(claimants) > 1
        }
        if conflicts:
            formatted = "; ".join(
                f"{alias!r}:{list(claimants)}" for alias, claimants in sorted(conflicts.items())
            )
            _record_load_failure(
                f"{len(conflicts)} alias ownership conflict(s)",
                log_level="error",
                message=(
                    "provider execution registry: alias ownership conflicts "
                    "between provider rows — load refused to prevent silent "
                    f"last-writer-wins remap (aliases: {formatted}). "
                    "Closes BUG-49388D90."
                ),
            )
            return

        # Single-claimant aliases are safe to install.
        loaded_aliases: dict[str, str] = {
            alias: next(iter(claimants))
            for alias, claimants in alias_claimants.items()
        }

        _REGISTRY.clear()
        _REGISTRY.update(loaded_registry)
        _ALIAS_MAP.clear()
        _ALIAS_MAP.update(loaded_aliases)
        _load_adapter_config(config_rows)
        _load_failure_mappings(failure_rows)

        # BUG-F8283CC1 / BUG-867CA639: partial authority includes skipped
        # provider rows and failed auxiliary tables. The successfully parsed
        # providers stay installed so routing keeps working, but health must
        # report degraded authority until every dispatch input loaded.
        if parse_errors or auxiliary_errors:
            _load_status = RegistryLoadStatus.LOADED_FROM_DB_PARTIAL
            error_parts: list[str] = []
            if parse_errors:
                error_parts.append(f"{len(parse_errors)} row(s) skipped")
            if auxiliary_errors:
                error_parts.append(f"{len(auxiliary_errors)} auxiliary table(s) failed")
            _load_error = "; ".join(error_parts)
            _load_skipped_rows = tuple(parse_errors)
            _load_auxiliary_errors = tuple(auxiliary_errors)
        else:
            _load_status = RegistryLoadStatus.LOADED_FROM_DB
            _load_error = None
            _load_skipped_rows = ()
            _load_auxiliary_errors = ()
        _load_timestamp = time.monotonic()
        _DB_LOADED = True

        if parse_errors:
            logger.warning(
                "provider execution registry: loaded %d provider(s) from DB with "
                "%d row(s) skipped — partial load, authority incomplete (%s). "
                "Closes BUG-F8283CC1.",
                len(loaded_registry),
                len(parse_errors),
                "; ".join(parse_errors),
            )
        if auxiliary_errors:
            logger.warning(
                "provider execution registry: loaded %d provider(s) from DB with "
                "%d auxiliary authority failure(s) — partial load, authority "
                "incomplete (%s). Closes BUG-867CA639.",
                len(loaded_registry),
                len(auxiliary_errors),
                "; ".join(auxiliary_errors),
            )
        if not parse_errors and not auxiliary_errors:
            logger.info(
                "provider execution registry: loaded %d provider(s) from DB",
                len(loaded_registry),
            )


def reload_from_db() -> None:
    """Force a fresh read of provider_cli_profiles on the next lookup."""

    global _DB_LOADED, _load_status, _load_error, _load_timestamp, _load_skipped_rows
    global _load_auxiliary_errors
    with _DB_LOAD_LOCK:
        _DB_LOADED = False
        _load_status = RegistryLoadStatus.UNLOADED
        _load_error = None
        _load_timestamp = None
        _load_skipped_rows = ()
        _load_auxiliary_errors = ()
    _load_from_db()


def registry_health() -> dict[str, Any]:
    """Return current load status, error details, and provider count."""

    _load_from_db()
    # ``authority_available`` remains True for partial loads because the
    # providers that DID parse are still authoritative — routing can keep
    # going for them. ``authority_complete`` is the stricter signal added by
    # BUG-F8283CC1: True only when zero rows were skipped. Operator dashboards
    # and the /health surface use ``authority_complete`` + ``skipped_rows`` to
    # see that a partial load is in effect, rather than trusting the old
    # ``status == "loaded_from_db"`` shortcut which hid the degradation.
    authority_available = _load_status in {
        RegistryLoadStatus.LOADED_FROM_DB,
        RegistryLoadStatus.LOADED_FROM_DB_PARTIAL,
    }
    authority_complete = _load_status == RegistryLoadStatus.LOADED_FROM_DB
    return {
        "status": _load_status.value,
        "error": _load_error,
        "loaded_at": _load_timestamp,
        "provider_count": len(_REGISTRY),
        "providers": sorted(_REGISTRY.keys()),
        "asyncpg_available": _ASYNCPG_AVAILABLE,
        "authority_available": authority_available,
        "authority_complete": authority_complete,
        "skipped_rows": list(_load_skipped_rows),
        "auxiliary_errors": list(_load_auxiliary_errors),
        "authority_source": "provider_cli_profiles" if authority_available else None,
        "fallback_active": False,
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
        raise RuntimeError("provider execution registry has no authoritative provider profiles")
    # Only explicit default-priority providers establish the runtime default.
    # Registry insertion order is transport data, not authority.
    for provider_slug in _DEFAULT_PROVIDER_PRIORITY:
        if provider_slug in _REGISTRY:
            return provider_slug
    raise RuntimeError(
        "provider execution registry has no configured default provider; registry order is not authoritative"
    )


def default_llm_adapter_type() -> str:
    _load_from_db()
    return provider_transport.default_llm_adapter_type(
        _REGISTRY,
        adapter_config=_ADAPTER_CONFIG,
        failure_mappings=_ADAPTER_FAILURE_MAPPINGS,
    )


def default_adapter_type_for_provider(provider_slug: str) -> str | None:
    _load_from_db()
    return provider_transport.default_adapter_type_for_provider(
        provider_slug,
        profiles=_REGISTRY,
    )


def resolve_default_adapter_type(provider_slug: str | None = None) -> str:
    """Resolve adapter defaults through one provider-aware registry authority."""

    normalized_provider_slug = str(provider_slug or "").strip()
    if (
        normalized_provider_slug
        and "/" not in normalized_provider_slug
        and not normalized_provider_slug.startswith("auto/")
    ):
        provider_default = default_adapter_type_for_provider(normalized_provider_slug)
        if provider_default is not None:
            return provider_default
    return default_llm_adapter_type()


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


def transport_support_report(
    *,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble the operator-facing transport support report from canonical authority."""

    from authority.transport_eligibility import load_transport_eligibility_authority
    from storage.postgres import PostgresTransportEligibilityRepository

    authority = load_transport_eligibility_authority(
        repository=PostgresTransportEligibilityRepository(pg),
        health_mod=health_mod,
        pg=pg,
        provider_filter=provider_filter,
        model_filter=model_filter,
        runtime_profile_ref=runtime_profile_ref,
        jobs=jobs,
        provider_registry_mod=__import__(__name__, fromlist=["transport_support_report"]),
    )
    return authority.to_json()
