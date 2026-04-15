"""Grouped handler registries for the workflow HTTP API."""

from __future__ import annotations

import json
from typing import Any

from ._shared import _ClientError, _read_json_body
from ._surface_usage import record_api_route_usage as _record_api_route_usage
from .workflow_admin import ADMIN_GET_ROUTES, ADMIN_POST_ROUTES, ADMIN_ROUTES
from .workflow_mcp import MCP_POST_ROUTES
from .workflow_notify import NOTIFY_GET_ROUTES, NOTIFY_POST_ROUTES, NOTIFY_ROUTES
from .workflow_query import (
    QUERY_DELETE_ROUTES,
    QUERY_GET_ROUTES,
    QUERY_POST_ROUTES,
    QUERY_PUT_ROUTES,
    QUERY_ROUTES,
)
from .workflow_run import RUN_GET_ROUTES, RUN_POST_ROUTES, RUN_ROUTES


ROUTES: dict[str, object] = {}
ROUTES.update(ADMIN_ROUTES)
ROUTES.update(RUN_ROUTES)
ROUTES.update(QUERY_ROUTES)
ROUTES.update(NOTIFY_ROUTES)

POST_ROUTE_HANDLERS = [
    *MCP_POST_ROUTES,
    *NOTIFY_POST_ROUTES,
    *RUN_POST_ROUTES,
    *QUERY_POST_ROUTES,
    *ADMIN_POST_ROUTES,
]

PUT_ROUTE_HANDLERS = [
    *QUERY_PUT_ROUTES,
]

GET_ROUTE_HANDLERS = [
    *QUERY_GET_ROUTES,
    *ADMIN_GET_ROUTES,
    *NOTIFY_GET_ROUTES,
    *RUN_GET_ROUTES,
]

DELETE_ROUTE_HANDLERS = [
    *QUERY_DELETE_ROUTES,
]


def _dispatch_dynamic(routes: list[tuple[object, object]], request: Any, path: str) -> bool:
    for matches, handler in routes:
        if matches(path):
            handler(request, path)
            return True
    return False


def _dispatch_standard_post(request: Any, path: str) -> bool:
    handler = ROUTES.get(path)
    if handler is None:
        return False

    try:
        body = _read_json_body(request)
        if not isinstance(body, dict):
            payload = {"error": "Request body must be a JSON object"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                response_payload=payload,
                headers=request.headers,
            )
            return True
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return True

    try:
        result = handler(request.subsystems, body)
        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
        )
    except _ClientError as exc:
        payload = {"error": str(exc)}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
    except Exception as exc:
        payload = {
            "error": f"{type(exc).__name__}: {exc}",
            "error_code": "internal_error",
        }
        request._send_json(
            500,
            payload,
        )
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
    return True


def handle_post_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(POST_ROUTE_HANDLERS, request, path) or _dispatch_standard_post(
        request,
        path,
    )


def handle_get_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(GET_ROUTE_HANDLERS, request, path)


def handle_put_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(PUT_ROUTE_HANDLERS, request, path)


def handle_delete_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(DELETE_ROUTE_HANDLERS, request, path)


def path_is_known(path: str) -> bool:
    if path in ROUTES:
        return True
    return any(
        matches(path)
        for routes in (
            POST_ROUTE_HANDLERS,
            PUT_ROUTE_HANDLERS,
            GET_ROUTE_HANDLERS,
            DELETE_ROUTE_HANDLERS,
        )
        for matches, _handler in routes
    )


__all__ = [
    "ADMIN_ROUTES",
    "DELETE_ROUTE_HANDLERS",
    "GET_ROUTE_HANDLERS",
    "NOTIFY_ROUTES",
    "POST_ROUTE_HANDLERS",
    "PUT_ROUTE_HANDLERS",
    "QUERY_ROUTES",
    "ROUTES",
    "RUN_ROUTES",
    "handle_delete_request",
    "handle_get_request",
    "handle_post_request",
    "handle_put_request",
    "path_is_known",
]
