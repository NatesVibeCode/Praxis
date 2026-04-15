"""Shared workflow database authority helpers for surface entrypoints."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from storage.postgres import PostgresConfigurationError, resolve_workflow_database_url


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


def _try_resolve_database_url(source: Mapping[str, str]) -> str | None:
    try:
        return resolve_workflow_database_url(env=source)
    except PostgresConfigurationError:
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


def workflow_database_env_for_repo(
    repo_root: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve the surface database env without freezing stale import-time state."""

    source = env if env is not None else os.environ
    if "WORKFLOW_DATABASE_URL" in source:
        database_url = resolve_workflow_database_url(env=source)
    else:
        repo_env_path = repo_root / ".env"
        repo_env = _read_repo_env_file(repo_env_path)
        database_url = _try_resolve_database_url(repo_env)
        if database_url is None:
            database_url = _try_resolve_docker_database_url(repo_root)
        if database_url is None:
            raise PostgresConfigurationError(
                "postgres.config_missing",
                (
                    "WORKFLOW_DATABASE_URL must be set in process env "
                    f"or declared in {repo_env_path}"
                ),
                details={
                    "environment_variable": "WORKFLOW_DATABASE_URL",
                    "repo_env_path": str(repo_env_path),
                },
            )
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        "PATH": str(source.get("PATH", "")),
    }


__all__ = [
    "workflow_database_env_for_repo",
]
