"""File route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_files_delete(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_files_delete(request, path)


def _handle_files_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_files_get(request, path)


def _handle_files_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_files_post(request, path)


__all__ = [
    "_handle_files_delete",
    "_handle_files_get",
    "_handle_files_post",
]
