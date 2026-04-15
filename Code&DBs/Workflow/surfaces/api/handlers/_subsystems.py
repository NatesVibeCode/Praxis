"""Lazy-loaded subsystem container for the workflow HTTP API."""

from __future__ import annotations

import sys

from ._shared import (
    RECEIPTS_DIR,
    REPO_ROOT,
    WORKFLOW_ROOT,
)
from surfaces._workflow_database import workflow_database_env_for_repo

try:
    from surfaces._subsystems_base import _BaseSubsystems
except ModuleNotFoundError:
    if str(WORKFLOW_ROOT) not in sys.path:
        sys.path.insert(0, str(WORKFLOW_ROOT))
    from surfaces._subsystems_base import _BaseSubsystems


def workflow_database_env() -> dict[str, str]:
    return workflow_database_env_for_repo(REPO_ROOT)


class _Subsystems(_BaseSubsystems):
    """Lazy-loaded subsystem instances (same shape as the MCP server)."""

    def __init__(self) -> None:
        super().__init__(
            repo_root=REPO_ROOT,
            workflow_root=WORKFLOW_ROOT,
            receipts_dir=RECEIPTS_DIR,
        )

    def _postgres_env(self) -> dict[str, str]:
        return workflow_database_env()

    def _should_start_heartbeat_background(self) -> bool:
        # API surface is long-lived and now gets heartbeat-driven maintenance
        # during startup, so trigger processing remains active.
        return True


__all__ = ["_Subsystems", "workflow_database_env"]
