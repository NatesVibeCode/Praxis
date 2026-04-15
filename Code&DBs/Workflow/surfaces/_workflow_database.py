"""Shared workflow database authority helpers for surface entrypoints."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from storage.postgres import PostgresConfigurationError, resolve_workflow_database_url

DEFAULT_REPO_LOCAL_DATABASE_URL = "postgresql://postgres@localhost:5432/praxis"


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


def workflow_database_env_for_repo(
    repo_root: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve the surface database env without freezing stale import-time state."""

    source = env if env is not None else os.environ
    database_url = _try_resolve_database_url(source)
    if database_url is None:
        database_url = _try_resolve_database_url(_read_repo_env_file(repo_root / ".env"))
    if database_url is None:
        database_url = DEFAULT_REPO_LOCAL_DATABASE_URL
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        "PATH": str(source.get("PATH", "")),
    }


__all__ = [
    "DEFAULT_REPO_LOCAL_DATABASE_URL",
    "workflow_database_env_for_repo",
]
