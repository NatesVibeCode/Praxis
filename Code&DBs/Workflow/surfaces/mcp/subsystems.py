"""Lazy-loaded subsystem container singleton."""
from __future__ import annotations

import logging
from pathlib import Path

from .._subsystems_base import _BaseSubsystems

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Repo root and path resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[4]
_WORKFLOW_ROOT = _REPO_ROOT / "Code&DBs" / "Workflow"

# Default config paths — all subsystems use Postgres now
_RECEIPTS_DIR = str(_REPO_ROOT / "artifacts" / "workflow_receipts")


def workflow_database_env() -> dict[str, str]:
    from .._workflow_database import workflow_database_env_for_repo

    return workflow_database_env_for_repo(_REPO_ROOT)


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
            logger=logger,
        )

    def _postgres_env(self) -> dict[str, str]:
        return workflow_database_env()

    def _should_start_heartbeat_background(self) -> bool:
        # MCP and API surfaces now rely on shared surface startup hooks to
        # maintain heartbeat-driven maintenance in long-lived process lanes.
        return True

    def _handle_reference_catalog_sync_error(self, exc: Exception) -> None:
        logger.debug("reference catalog sync skipped: %s", exc)


# Singleton subsystems instance
_subs = _Subsystems()

# Re-export path constants for modules that need them
REPO_ROOT = _REPO_ROOT
WORKFLOW_ROOT = _WORKFLOW_ROOT
