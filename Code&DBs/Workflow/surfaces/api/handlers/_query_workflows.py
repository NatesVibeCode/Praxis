"""Workflow route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_build_stream(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_build_stream(request, path)


def _handle_trigger_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_trigger_post(request, path)


def _handle_workflow_build_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflow_build_get(request, path)


def _handle_workflow_delete(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflow_delete(request, path)


def _handle_workflow_triggers_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflow_triggers_get(request, path)


def _handle_workflow_triggers_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflow_triggers_post(request, path)


def _handle_workflows_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflows_get(request, path)


def _handle_workflows_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflows_post(request, path)


def _handle_workflows_runs_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_workflows_runs_get(request, path)


__all__ = [
    "_handle_build_stream",
    "_handle_trigger_post",
    "_handle_workflow_build_get",
    "_handle_workflow_delete",
    "_handle_workflow_triggers_get",
    "_handle_workflow_triggers_post",
    "_handle_workflows_get",
    "_handle_workflows_post",
    "_handle_workflows_runs_get",
]
