"""Shared workflow database authority helpers for runtime modules."""

from __future__ import annotations

import asyncio
import os
import plistlib
import subprocess
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import asyncpg

from storage.postgres import PostgresConfigurationError, resolve_workflow_database_url

_WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"
_WORKFLOW_DATABASE_PROBE_TIMEOUT_S = 3.0
_WORKFLOW_DATABASE_LAUNCHD_LABELS = (
    "com.praxis.engine",
    "com.praxis.postgres",
    "com.praxis.api-server",
    "com.praxis.workflow-api",
    "com.praxis.workflow-worker",
    "com.praxis.scheduler",
    "com.praxis.queue-worker",
)


@dataclass(frozen=True, slots=True)
class WorkflowDatabaseAuthority:
    """Resolved workflow database authority plus provenance."""

    database_url: str | None
    source: str


def _runtime_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _launch_agents_root() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


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


def _probe_workflow_database_url(database_url: str) -> bool:
    normalized_database_url = resolve_workflow_database_url(
        env={_WORKFLOW_DATABASE_URL_ENV: database_url}
    )

    async def _probe() -> bool:
        conn = await asyncpg.connect(
            normalized_database_url,
            timeout=_WORKFLOW_DATABASE_PROBE_TIMEOUT_S,
        )
        try:
            return True
        finally:
            await conn.close()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            return asyncio.run(_probe())
        except Exception:
            return False

    result: dict[str, bool] = {}

    def _run_probe() -> None:
        try:
            result["reachable"] = asyncio.run(_probe())
        except Exception:
            result["reachable"] = False

    probe_thread = threading.Thread(target=_run_probe, daemon=True, name="workflow-db-probe")
    probe_thread.start()
    probe_thread.join(timeout=_WORKFLOW_DATABASE_PROBE_TIMEOUT_S + 1.0)
    return bool(result.get("reachable"))


def _try_resolve_launchd_database_url(repo_root: Path) -> tuple[str, str] | None:
    launch_agents = _launch_agents_root()
    if not launch_agents.is_dir():
        return None

    for label in _WORKFLOW_DATABASE_LAUNCHD_LABELS:
        plist_path = launch_agents / f"{label}.plist"
        if not plist_path.exists():
            continue
        try:
            payload = plistlib.loads(plist_path.read_bytes())
        except Exception:
            continue
        env_vars = payload.get("EnvironmentVariables")
        if not isinstance(env_vars, dict):
            continue
        candidate = env_vars.get(_WORKFLOW_DATABASE_URL_ENV)
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        candidate = candidate.strip()
        if _probe_workflow_database_url(candidate):
            return candidate, label
    return None


def _try_resolve_docker_database_url(repo_root: Path) -> str | None:
    compose_file = repo_root / "docker-compose.yml"
    if not compose_file.is_file():
        return None

    try:
        postgres_container = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "ps", "-q", "postgres"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    if not postgres_container:
        return None

    try:
        container_state = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                postgres_container,
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    if container_state not in {"healthy", "running"}:
        return None

    try:
        published = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "port", "postgres", "5432"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    if not published or ":" not in published:
        return None

    docker_host, docker_port = published.rsplit(":", 1)
    docker_host = docker_host.strip().lstrip("[").rstrip("]")
    if docker_host in {"", "0.0.0.0", "::"}:
        docker_host = "127.0.0.1"
    if not docker_port.strip():
        return None
    return f"postgresql://postgres@{docker_host}:{docker_port.strip()}/praxis"


def resolve_runtime_database_authority(
    database_url: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
    required: bool = True,
) -> WorkflowDatabaseAuthority:
    """Resolve runtime database authority plus the source that provided it."""

    if database_url is not None:
        raw_database_url = str(database_url).strip()
        if not raw_database_url:
            if required:
                raise PostgresConfigurationError(
                    "postgres.config_missing",
                    f"{_WORKFLOW_DATABASE_URL_ENV} must be set to a Postgres DSN",
                    details={"environment_variable": _WORKFLOW_DATABASE_URL_ENV},
                )
            return WorkflowDatabaseAuthority(database_url=None, source="unconfigured")
        return WorkflowDatabaseAuthority(
            database_url=resolve_workflow_database_url(
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
            database_url=resolve_workflow_database_url(env=source),
            source="process_env",
        )

    resolved_repo_root = repo_root if repo_root is not None else _runtime_repo_root()
    launchd_authority = _try_resolve_launchd_database_url(resolved_repo_root)
    if launchd_authority is not None:
        launchd_database_url, launchd_label = launchd_authority
        return WorkflowDatabaseAuthority(
            database_url=launchd_database_url,
            source=f"launchd:{launchd_label}",
        )

    repo_env_path = resolved_repo_root / ".env"
    repo_env = _read_repo_env_file(repo_env_path)
    if _WORKFLOW_DATABASE_URL_ENV in repo_env:
        return WorkflowDatabaseAuthority(
            database_url=resolve_workflow_database_url(env=repo_env),
            source=f"repo_env:{repo_env_path}",
        )

    docker_database_url = _try_resolve_docker_database_url(resolved_repo_root)
    if docker_database_url is not None:
        return WorkflowDatabaseAuthority(
            database_url=docker_database_url,
            source="docker",
        )

    if required:
        raise PostgresConfigurationError(
            "postgres.config_missing",
            (
                f"{_WORKFLOW_DATABASE_URL_ENV} must be set in process env "
                f"or declared in {repo_env_path}"
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
