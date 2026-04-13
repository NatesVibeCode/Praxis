"""Lazy-loaded subsystem container for the workflow HTTP API."""

from __future__ import annotations

import sys

from ._shared import (
    RECEIPTS_DIR,
    REPO_ROOT,
    WORKFLOW_ROOT,
)

try:
    from surfaces._subsystems_base import _BaseSubsystems
except ModuleNotFoundError:
    if str(WORKFLOW_ROOT) not in sys.path:
        sys.path.insert(0, str(WORKFLOW_ROOT))
    from surfaces._subsystems_base import _BaseSubsystems


class _Subsystems(_BaseSubsystems):
    """Lazy-loaded subsystem instances (same shape as the MCP server)."""

    def __init__(self) -> None:
        super().__init__(
            repo_root=REPO_ROOT,
            workflow_root=WORKFLOW_ROOT,
            receipts_dir=RECEIPTS_DIR,
            default_database_url=None,
        )

    def _should_start_heartbeat_background(self) -> bool:
        """Keep API surfaces request-only; background heartbeats belong to worker lanes."""
        return False


__all__ = ["_Subsystems"]
