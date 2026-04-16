"""Shared workflow database authority helpers for surface entrypoints."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from runtime._workflow_database import resolve_runtime_database_url


def workflow_database_url_for_repo(
    repo_root: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    """Resolve the canonical workflow DB URL for a surface entrypoint."""

    source = env if env is not None else os.environ
    return resolve_runtime_database_url(
        env=source,
        repo_root=repo_root,
        required=True,
    )


def workflow_database_env_for_repo(
    repo_root: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve one explicit DB authority for a surface entrypoint."""

    source = env if env is not None else os.environ
    database_url = workflow_database_url_for_repo(repo_root, env=source)
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        "PATH": str(source.get("PATH", "")),
    }


__all__ = ["workflow_database_env_for_repo", "workflow_database_url_for_repo"]
