"""Shared DB authority helpers for CLI frontdoors."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

from surfaces._workflow_database import workflow_database_env_for_repo


def cli_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def cli_database_env() -> dict[str, str]:
    return workflow_database_env_for_repo(cli_repo_root())


def cli_sync_conn() -> Any:
    return SyncPostgresConnection(get_workflow_pool(env=cli_database_env()))


__all__ = ["cli_database_env", "cli_repo_root", "cli_sync_conn"]
