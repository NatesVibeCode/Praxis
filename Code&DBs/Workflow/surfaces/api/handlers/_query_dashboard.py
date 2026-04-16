"""Dashboard and status route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_dashboard_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_dashboard_get(request, path)


def _handle_leaderboard_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_leaderboard_get(request, path)


def _handle_runs_recent_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_runs_recent_get(request, path)


def _handle_status_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_status_get(request, path)


__all__ = [
    "_handle_dashboard_get",
    "_handle_leaderboard_get",
    "_handle_runs_recent_get",
    "_handle_status_get",
]
