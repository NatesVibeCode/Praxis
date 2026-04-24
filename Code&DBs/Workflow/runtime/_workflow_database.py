"""Shared workflow database authority helpers for runtime modules."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

_WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
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
    "resolve_runtime_database_authority",
    "resolve_runtime_database_url",
    "workflow_database_url_is_configured",
]
