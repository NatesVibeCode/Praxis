"""Bug route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_bugs(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    from . import workflow_query as _legacy

    return _legacy._handle_bugs(subs, body)


def _handle_bugs_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_bugs_get(request, path)


def _handle_bugs_replay_ready_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_bugs_replay_ready_get(request, path)


__all__ = [
    "_handle_bugs",
    "_handle_bugs_get",
    "_handle_bugs_replay_ready_get",
]
