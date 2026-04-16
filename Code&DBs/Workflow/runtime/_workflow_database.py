"""Shared workflow database authority helpers for runtime modules."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from storage.postgres import PostgresConfigurationError, resolve_workflow_database_url

_WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


def _runtime_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


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
    return f"postgresql://{docker_host}:{docker_port.strip()}/praxis"


def resolve_runtime_database_url(
    database_url: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
    required: bool = True,
) -> str | None:
    """Resolve runtime database authority from explicit input, process env, or repo .env."""

    if database_url is not None:
        raw_database_url = str(database_url).strip()
        if not raw_database_url:
            if required:
                raise PostgresConfigurationError(
                    "postgres.config_missing",
                    f"{_WORKFLOW_DATABASE_URL_ENV} must be set to a Postgres DSN",
                    details={"environment_variable": _WORKFLOW_DATABASE_URL_ENV},
                )
            return None
        return resolve_workflow_database_url(env={_WORKFLOW_DATABASE_URL_ENV: raw_database_url})

    source = env if env is not None else os.environ
    if _WORKFLOW_DATABASE_URL_ENV in source:
        raw_database_url = source.get(_WORKFLOW_DATABASE_URL_ENV)
        if (not isinstance(raw_database_url, str) or not raw_database_url.strip()) and not required:
            return None
        return resolve_workflow_database_url(env=source)

    resolved_repo_root = repo_root if repo_root is not None else _runtime_repo_root()
    repo_env_path = resolved_repo_root / ".env"
    repo_env = _read_repo_env_file(repo_env_path)
    if _WORKFLOW_DATABASE_URL_ENV in repo_env:
        return resolve_workflow_database_url(env=repo_env)
    docker_database_url = _try_resolve_docker_database_url(resolved_repo_root)
    if docker_database_url is not None:
        return docker_database_url

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
    return None


__all__ = ["resolve_runtime_database_url"]
