"""HTTP bridge for the DAG MCP surface."""

from __future__ import annotations

import json
import traceback
from typing import Any

from runtime.workflow.mcp_session import (
    WorkflowMcpSessionError,
    verify_workflow_mcp_session_token,
)
from surfaces.mcp.protocol import handle_request
from surfaces.mcp.runtime_context import workflow_mcp_request_context

from ._shared import RouteEntry, _ClientError, _exact, _query_params, _read_json_body


def _allowed_tool_names(request: Any) -> list[str] | None:
    values: list[str] = []
    raw_params = _query_params(getattr(request, "path", ""))
    values.extend(raw_params.get("allowed_tools", []))
    header_value = request.headers.get("X-DAG-Allowed-MCP-Tools")
    if header_value:
        values.append(header_value)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for part in str(raw_value or "").replace("\n", ",").split(","):
            name = part.strip()
            if not name or name in seen:
                continue
            seen.add(name)
            normalized.append(name)
    return normalized or None


def _workflow_token(request: Any) -> str:
    raw_params = _query_params(getattr(request, "path", ""))
    token = ""
    query_values = raw_params.get("workflow_token", [])
    if query_values:
        token = str(query_values[-1] or "").strip()
    if not token:
        token = str(request.headers.get("X-DAG-Workflow-Token") or "").strip()
    if not token:
        auth_header = str(request.headers.get("Authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
    return token


def _intersect_allowed_tools(token_allowed_tools: list[str], request_allowed_tools: list[str] | None) -> list[str]:
    if not request_allowed_tools:
        return list(token_allowed_tools)
    request_allowed_set = {tool for tool in request_allowed_tools}
    return [tool for tool in token_allowed_tools if tool in request_allowed_set]


def _handle_mcp_post(request: Any, path: str) -> None:
    del path
    workflow_token = _workflow_token(request)
    try:
        body = _read_json_body(request)
        if not isinstance(body, dict):
            raise _ClientError("Request body must be a JSON-RPC object")
    except (json.JSONDecodeError, ValueError, _ClientError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    method = str(body.get("method") or "").strip()
    if method in {"tools/list", "tools/call"}:
        try:
            claims = verify_workflow_mcp_session_token(workflow_token)
        except WorkflowMcpSessionError as exc:
            request._send_json(401, {"error": str(exc), "reason_code": exc.reason_code})
            return
        allowed_tool_names = _intersect_allowed_tools(
            claims.get("allowed_tools", []),
            _allowed_tool_names(request),
        )
        with workflow_mcp_request_context(
            run_id=str(claims.get("run_id") or "").strip() or None,
            workflow_id=str(claims.get("workflow_id") or "").strip() or None,
            job_label=str(claims.get("job_label") or "").strip(),
            allowed_tools=allowed_tool_names,
            expires_at=int(claims.get("exp") or 0),
        ):
            try:
                response = handle_request(
                    body,
                    transport="jsonl",
                    allowed_tool_names=allowed_tool_names,
                    workflow_token=workflow_token,
                )
            except Exception as exc:
                request._send_json(
                    500,
                    {
                        "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc(),
                    },
                )
                return
    else:
        allowed_tool_names = _allowed_tool_names(request)
        try:
            response = handle_request(
                body,
                transport="jsonl",
                allowed_tool_names=allowed_tool_names,
                workflow_token=workflow_token,
            )
        except Exception as exc:
            request._send_json(
                500,
                {
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                },
            )
            return

    if response is None:
        request._send_json(202, {})
        return
    request._send_json(200, response)


MCP_POST_ROUTES: list[RouteEntry] = [
    (_exact("/mcp"), _handle_mcp_post),
]
