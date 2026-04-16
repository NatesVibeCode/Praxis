"""Object and document route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_documents_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_documents_get(request, path)


def _handle_documents_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_documents_post(request, path)


def _handle_object_types_delete(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_object_types_delete(request, path)


def _handle_object_types_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_object_types_get(request, path)


def _handle_object_types_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_object_types_post(request, path)


def _handle_object_types_put(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_object_types_put(request, path)


def _handle_objects_delete(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_objects_delete(request, path)


def _handle_objects_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_objects_get(request, path)


def _handle_objects_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_objects_post(request, path)


def _handle_objects_put(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_objects_put(request, path)


__all__ = [
    "_handle_documents_get",
    "_handle_documents_post",
    "_handle_object_types_delete",
    "_handle_object_types_get",
    "_handle_object_types_post",
    "_handle_object_types_put",
    "_handle_objects_delete",
    "_handle_objects_get",
    "_handle_objects_post",
    "_handle_objects_put",
]
