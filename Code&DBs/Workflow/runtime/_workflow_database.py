"""Shared workflow database authority helpers for runtime modules."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
_WORKFLOW_DATABASE_AUTHORITY_SOURCE_ENV = "WORKFLOW_DATABASE_AUTHORITY_SOURCE"
_WORKFLOW_LAUNCHD_DIR_ENV = "PRAXIS_LAUNCHD_DIR"


@dataclass(frozen=True, slots=True)
class WorkflowDatabaseAuthority:
    """Resolved workflow database authority plus provenance."""

    database_url: str | None
    source: str


def _runtime_repo_root() -> Path:
    from runtime.workspace_paths import repo_root as workspace_repo_root

    return workspace_repo_root()


def _postgres_configuration_error(*args, **kwargs):
    from storage.postgres import PostgresConfigurationError

    return PostgresConfigurationError(*args, **kwargs)


def _resolve_workflow_database_url(*, env: Mapping[str, str]) -> str:
    from storage.postgres import resolve_workflow_database_url

    return resolve_workflow_database_url(env=env)


def _redact_database_url(value: object) -> str | None:
    from runtime.primitive_contracts import redact_url

    return redact_url(value)


def launch_agents_root(*, env: Mapping[str, str] | None = None) -> Path:
    source = env if env is not None else os.environ
    configured = str(source.get(_WORKFLOW_LAUNCHD_DIR_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home().joinpath("Library", "LaunchAgents")


def _read_repo_env_file(path: Path) -> dict[str, str]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    resolved: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and value:
            resolved[key] = value
    return resolved


def resolve_runtime_database_authority(
    database_url: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
    required: bool = True,
) -> WorkflowDatabaseAuthority:
    """Resolve runtime database authority plus the source that provided it.

    The resolver is intentionally explicit: DB authority must arrive through an
    argument, process/registry environment, or a repo env file. It must not
    discover launchd, Docker, or localhost Postgres instances because those can
    silently point future agents at stale operator state.
    """

    if database_url is not None:
        raw_database_url = str(database_url).strip()
        if not raw_database_url:
            if required:
                raise _postgres_configuration_error(
                    "postgres.config_missing",
                    f"{_WORKFLOW_DATABASE_URL_ENV} must be set to a Postgres DSN",
                    details={"environment_variable": _WORKFLOW_DATABASE_URL_ENV},
                )
            return WorkflowDatabaseAuthority(database_url=None, source="unconfigured")
        return WorkflowDatabaseAuthority(
            database_url=_resolve_workflow_database_url(
                env={_WORKFLOW_DATABASE_URL_ENV: raw_database_url}
            ),
            source="argument",
        )

    source = env if env is not None else os.environ
    if _WORKFLOW_DATABASE_URL_ENV in source:
        raw_database_url = source.get(_WORKFLOW_DATABASE_URL_ENV)
        if (not isinstance(raw_database_url, str) or not raw_database_url.strip()) and not required:
            return WorkflowDatabaseAuthority(database_url=None, source="unconfigured")
        return WorkflowDatabaseAuthority(
            database_url=_resolve_workflow_database_url(env=source),
            source="process_env",
        )

    resolved_repo_root = repo_root if repo_root is not None else _runtime_repo_root()
    repo_env_path = resolved_repo_root / ".env"
    repo_env = _read_repo_env_file(repo_env_path)
    if _WORKFLOW_DATABASE_URL_ENV in repo_env:
        return WorkflowDatabaseAuthority(
            database_url=_resolve_workflow_database_url(env=repo_env),
            source=f"repo_env:{repo_env_path}",
        )

    # Try launcher resolution fallback if .env failed
    try:
        from runtime.launcher_authority import read_launcher_seed_config, resolve_launcher_workspace
        seed = read_launcher_seed_config(env=source)
        resolution = resolve_launcher_workspace(seed, env=source)
        if seed.database_url:
            return WorkflowDatabaseAuthority(
                database_url=resolve_workflow_database_url(
                    env={_WORKFLOW_DATABASE_URL_ENV: seed.database_url}
                ),
                source=f"launcher_resolution:{seed.config_path}",
            )
    except Exception:
        pass

    if required:
        raise _postgres_configuration_error(
            "postgres.config_missing",
            (
                f"{_WORKFLOW_DATABASE_URL_ENV} must be provided by the registry/runtime "
                f"environment or declared explicitly in {repo_env_path}"
            ),
            details={
                "environment_variable": _WORKFLOW_DATABASE_URL_ENV,
                "repo_env_path": str(repo_env_path),
            },
        )
    return WorkflowDatabaseAuthority(database_url=None, source="unconfigured")


def resolve_runtime_database_url(
    database_url: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
    required: bool = True,
) -> str | None:
    """Resolve runtime database authority from explicit input, process env, or repo .env."""
    return resolve_runtime_database_authority(
        database_url=database_url,
        env=env,
        repo_root=repo_root,
        required=required,
    ).database_url


def workflow_database_authority_fingerprint(database_url: str | None) -> str | None:
    """Return a stable, redaction-safe identity for one workflow DB authority."""

    normalized_database_url = str(database_url or "").strip()
    if not normalized_database_url:
        return None
    from storage.postgres import workflow_authority_cache_key

    return workflow_authority_cache_key(normalized_database_url)


def workflow_database_authority_payload(
    *,
    database_url: str | None,
    source: str | None,
    observed_database_url: str | None = None,
) -> dict[str, Any]:
    """Return the public, redacted authority block for DB-backed surfaces."""

    resolved_database_url = str(database_url or "").strip() or None
    observed_live_database_url = str(observed_database_url or "").strip() or None
    fingerprint = workflow_database_authority_fingerprint(resolved_database_url)
    observed_fingerprint = workflow_database_authority_fingerprint(observed_live_database_url)
    payload: dict[str, Any] = {
        "kind": "workflow_database_authority",
        "status": "ready" if fingerprint else "unconfigured",
        "authority_source": str(source or "").strip() or "unknown",
        "fingerprint": fingerprint,
        "redacted_url": _redact_database_url(resolved_database_url),
        "comparison_field": "fingerprint",
    }
    if observed_fingerprint:
        payload["observed_fingerprint"] = observed_fingerprint
    if observed_live_database_url:
        payload["observed_redacted_url"] = _redact_database_url(observed_live_database_url)
    if not fingerprint:
        payload.update(
            {
                "reason_code": "workflow_database.authority_unconfigured",
                "message": "Workflow database authority is not configured.",
            }
        )
    elif observed_fingerprint and observed_fingerprint != fingerprint:
        payload.update(
            {
                "status": "degraded",
                "reason_code": "workflow_database.authority_fingerprint_mismatch",
                "message": (
                    "Resolved workflow DB authority does not match the live surface "
                    "connection fingerprint; treat results as degraded until the surface "
                    "is rebound to the canonical authority."
                ),
            }
        )
    return payload


def workflow_database_authority_payload_from_env(
    env: Mapping[str, str] | None,
    *,
    observed_database_url: str | None = None,
) -> dict[str, Any]:
    """Build the public authority block from a resolved workflow env mapping."""

    source = None if env is None else env.get(_WORKFLOW_DATABASE_AUTHORITY_SOURCE_ENV)
    database_url = None if env is None else env.get(_WORKFLOW_DATABASE_URL_ENV)
    return workflow_database_authority_payload(
        database_url=database_url,
        source=source,
        observed_database_url=observed_database_url,
    )


def workflow_database_url_is_configured(
    workflow_env: Mapping[str, str] | None = None,
) -> bool:
    """Return True when ``WORKFLOW_DATABASE_URL`` is present and non-empty.

    Consumers that only need a presence check (e.g. heartbeat modules that
    want to no-op when the workflow DB isn't wired up) should use this helper
    instead of reading ``os.environ`` directly. It honors an explicit
    ``workflow_env`` override first — falling back to the process environment —
    which matches the pattern runtime modules already use when they accept an
    injected env mapping for testability.
    """

    if workflow_env is not None:
        candidate = workflow_env.get(_WORKFLOW_DATABASE_URL_ENV)
        if isinstance(candidate, str) and candidate.strip():
            return True
    value = os.environ.get(_WORKFLOW_DATABASE_URL_ENV)
    return bool(isinstance(value, str) and value.strip())


__all__ = [
    "WorkflowDatabaseAuthority",
    "workflow_database_authority_fingerprint",
    "workflow_database_authority_payload",
    "workflow_database_authority_payload_from_env",
    "resolve_runtime_database_authority",
    "resolve_runtime_database_url",
    "workflow_database_url_is_configured",
]
