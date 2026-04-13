"""Lazy-loaded subsystem container singleton."""
from __future__ import annotations

import logging
import os
from pathlib import Path

from .._subsystems_base import _BaseSubsystems

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Repo root and path resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[4]
_WORKFLOW_ROOT = _REPO_ROOT / "Code&DBs" / "Workflow"
_DEFAULT_REPO_LOCAL_DATABASE_URL = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis")

# Default config paths — all subsystems use Postgres now
_RECEIPTS_DIR = str(_REPO_ROOT / "artifacts" / "workflow_receipts")


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


def _is_valid_pg_url(value: str) -> bool:
    return value.startswith(("postgresql://", "postgres://"))


def workflow_database_env() -> dict[str, str]:
    # Only accept a full connection URL — bare database names (e.g. the stale
    # stale value left in the env after a DB rename) are silently skipped
    # so the correct default or .env value wins.
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "").strip()
    if not _is_valid_pg_url(database_url):
        database_url = _read_repo_env_file(_REPO_ROOT / ".env").get(
            "WORKFLOW_DATABASE_URL",
            "",
        ).strip()
    if not _is_valid_pg_url(database_url):
        database_url = _DEFAULT_REPO_LOCAL_DATABASE_URL
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        "PATH": os.environ.get("PATH", ""),
    }


# ---------------------------------------------------------------------------
# Lazy subsystem holder — initialized once on first tool call
# ---------------------------------------------------------------------------

class _Subsystems(_BaseSubsystems):
    """Lazy-loaded subsystem instances."""

    def __init__(self) -> None:
        super().__init__(
            repo_root=_REPO_ROOT,
            workflow_root=_WORKFLOW_ROOT,
            receipts_dir=_RECEIPTS_DIR,
            default_database_url=_DEFAULT_REPO_LOCAL_DATABASE_URL,
            logger=logger,
        )

    def _postgres_env(self) -> dict[str, str]:
        return workflow_database_env()

    def _handle_reference_catalog_sync_error(self, exc: Exception) -> None:
        logger.debug("reference catalog sync skipped: %s", exc)

    def _build_bug_tracker(self):
        from runtime.bug_tracker import BugTracker

        return BugTracker(self.get_pg_conn(), self.get_embedding_service())

    def _build_heartbeat_runner(self):
        from runtime.heartbeat_runner import HeartbeatRunner

        return HeartbeatRunner(
            conn=self.get_pg_conn(),
            embedder=self.get_embedding_service(),
        )


# Singleton subsystems instance
_subs = _Subsystems()

# Re-export path constants for modules that need them
REPO_ROOT = _REPO_ROOT
WORKFLOW_ROOT = _WORKFLOW_ROOT
