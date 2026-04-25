"""Dispatch helpers for route composition and standard POST plumbing."""

from __future__ import annotations

import json
from typing import Any, Callable

from ._shared import _ClientError, _read_json_body


RouteEntries = list[tuple[object, object]]
RouteRegistry = dict[str, object]


def _dispatch_dynamic(routes: RouteEntries, request: Any, path: str) -> bool:
    for matches, handler in routes:
        if matches(path):
            handler(request, path)
            return True
    return False


def _dispatch_standard_post(
    request: Any,
    path: str,
    route_map: RouteRegistry,
    *,
    record_api_route_usage: Callable[..., None],
    required_body_paths: set[str] | frozenset[str] | None = None,
) -> bool:
    handler = route_map.get(path)
    if handler is None:
        return False
    body_required = path in (required_body_paths or set())

    try:
        body = _read_json_body(request)
        if not isinstance(body, dict):
            payload = {"error": "Request body must be a JSON object"}
            request._send_json(400, payload)
            record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                response_payload=payload,
                headers=request.headers,
            )
            return True

        if body_required:
            if not body:
                payload = {"error": "Request body is required and must be a non-empty JSON object"}
                request._send_json(400, payload)
                record_api_route_usage(
                    request.subsystems,
                    path=path,
                    method="POST",
                    status_code=400,
                    request_body=body,
                    response_payload=payload,
                    headers=request.headers,
                )
                return True
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        record_api_route_usage(
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
        record_api_route_usage(
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
        record_api_route_usage(
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
        request._send_json(500, payload)
        record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
    return True


__all__ = [
    "_dispatch_dynamic",
    "_dispatch_standard_post",
]
