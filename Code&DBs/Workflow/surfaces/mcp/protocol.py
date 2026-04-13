"""JSON-RPC 2.0 stdin/stdout transport for MCP.

Accepts both Content-Length framed messages and bare JSON lines.
Response transport is configurable so legacy local clients that expect
JSON lines can keep working while framed MCP clients remain supported.

Supports MCP progress notifications (notifications/progress) so
long-running tools like streaming workflow execution can push per-job updates
to the client while the tool call is in-flight.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import traceback
from collections.abc import Sequence
from typing import Any, Callable, Optional

from storage.postgres import PostgresStorageError

from .catalog import canonical_tool_name, get_tool_catalog, resolve_tool_entry
from .invocation import ToolInvocationError, invoke_tool, normalize_allowed_tool_names


_TRANSPORT_JSONL = "jsonl"
_TRANSPORT_CONTENT_LENGTH = "content-length"
_TRANSPORT_MIRROR = "mirror"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


_MAX_TOOL_RESULT_TEXT_CHARS = _env_int("PRAXIS_MCP_TOOL_RESULT_CHAR_LIMIT", 200_000)
_MAX_LOG_MESSAGE_CHARS = _env_int("PRAXIS_MCP_LOG_MESSAGE_CHAR_LIMIT", 16_000)
_MAX_ERROR_MESSAGE_CHARS = _env_int("PRAXIS_MCP_ERROR_MESSAGE_CHAR_LIMIT", 32_000)


class _TransportDecodeError(Exception):
    """Raised when stdin cannot be decoded as JSON-RPC."""

    def __init__(self, transport: str, message: str) -> None:
        super().__init__(message)
        self.transport = transport
        self.message = message


# ---------------------------------------------------------------------------
# JSON-RPC transport (Content-Length framed)
# ---------------------------------------------------------------------------

def _response_transport(incoming_transport: str | None) -> str:
    """Resolve response transport from env override or incoming transport."""
    configured = os.environ.get("PRAXIS_MCP_STDIO_TRANSPORT", _TRANSPORT_MIRROR).strip().lower()
    if configured in {_TRANSPORT_JSONL, "json"}:
        return _TRANSPORT_JSONL
    if configured in {_TRANSPORT_CONTENT_LENGTH, "content_length", "framed"}:
        return _TRANSPORT_CONTENT_LENGTH
    if incoming_transport in {_TRANSPORT_JSONL, _TRANSPORT_CONTENT_LENGTH}:
        return incoming_transport
    return _TRANSPORT_JSONL


def _send_message(msg: dict[str, Any], *, transport: str = _TRANSPORT_CONTENT_LENGTH) -> None:
    """Write a JSON-RPC response using the requested transport."""
    body = json.dumps(msg)
    if transport == _TRANSPORT_JSONL:
        sys.stdout.write(body)
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        body_bytes = body.encode("utf-8")
        header = f"Content-Length: {len(body_bytes)}\r\n\r\n"
        out = getattr(sys.stdout, "buffer", None)
        if out is not None:
            out.write(header.encode("ascii"))
            out.write(body_bytes)
            out.flush()
        else:
            # StringIO in tests — no binary buffer available
            sys.stdout.write(header)
            sys.stdout.write(body)
            sys.stdout.flush()


def _make_response(request_id: Any, result: Any = None, error: str | None = None) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 response."""
    msg: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error:
        msg["error"] = {"code": -32603, "message": error}
    else:
        msg["result"] = result
    return msg


def _make_error_response(request_id: Any, message: str) -> dict[str, Any]:
    normalized = _truncate_text(str(message or ""), limit=_MAX_ERROR_MESSAGE_CHARS)
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32603, "message": normalized},
    }


def _make_notification(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 notification (no id field)."""
    return {"jsonrpc": "2.0", "method": method, "params": params}


def _truncate_text(value: str, *, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    notice = f"\n...[truncated; removed {omitted} chars]...\n"
    if limit <= len(notice):
        return notice[:limit]
    head = (limit - len(notice)) // 2
    tail = limit - len(notice) - head
    return f"{text[:head]}{notice}{text[-tail:]}"


def _serialize_tool_result(result: Any) -> str:
    serialized = json.dumps(result, default=str)
    if len(serialized) <= _MAX_TOOL_RESULT_TEXT_CHARS:
        return serialized
    summary_base = {
        "truncated": True,
        "reason": "mcp tool result exceeded PRAXIS_MCP_TOOL_RESULT_CHAR_LIMIT",
        "original_chars": len(serialized),
    }
    preview_limit = min(len(serialized), max(_MAX_TOOL_RESULT_TEXT_CHARS // 2, 64))
    while True:
        summary = {
            **summary_base,
            "preview": _truncate_text(serialized, limit=preview_limit),
        }
        summary_text = json.dumps(summary, default=str)
        if len(summary_text) <= _MAX_TOOL_RESULT_TEXT_CHARS:
            return summary_text
        if preview_limit <= 16:
            summary = {**summary_base, "preview": ""}
            return json.dumps(summary, default=str)
        overflow = len(summary_text) - _MAX_TOOL_RESULT_TEXT_CHARS
        preview_limit = max(16, preview_limit - max(overflow, 16))


def _format_tool_error(exc: Exception) -> str:
    """Normalize infra failures into concise tool errors."""
    if isinstance(exc, PostgresStorageError):
        return f"{type(exc).__name__}[{exc.reason_code}]: {exc}"
    if isinstance(exc, PermissionError):
        return f"PermissionError: {exc}"
    return f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"


# ---------------------------------------------------------------------------
# Progress emitter — passed to tool handlers for streaming updates
# ---------------------------------------------------------------------------

# Write lock so concurrent threads don't interleave stdout messages.
_stdout_lock = threading.Lock()


class ProgressEmitter:
    """Lets a tool handler push MCP notifications mid-call.

    Two emission modes:
    - emit(): progress bar updates (N of M) via notifications/progress
    - log(): streaming text feed via notifications/message (shows in
      client log panels, similar to sub-agent activity feeds)

    Usage inside a tool handler::

        emitter.emit(progress=1, total=5, message="Job 'build-api' succeeded")
        emitter.log("Queued 5 workflow jobs to the worker pool")
        emitter.log("Job 'build-api' failed: timeout", level="error")
    """

    def __init__(self, progress_token: str | None, transport: str) -> None:
        self.progress_token = progress_token
        self._transport = transport

    @property
    def enabled(self) -> bool:
        return self.progress_token is not None

    def emit(self, *, progress: int, total: int | None = None, message: str = "") -> None:
        """Push a progress bar update (notifications/progress)."""
        if not self.enabled:
            return
        params: dict[str, Any] = {
            "progressToken": self.progress_token,
            "progress": progress,
        }
        if total is not None:
            params["total"] = total
        if message:
            params["message"] = _truncate_text(message, limit=_MAX_LOG_MESSAGE_CHARS)
        notification = _make_notification("notifications/progress", params)
        with _stdout_lock:
            _send_message(notification, transport=self._transport)

    def log(self, message: str, *, level: str = "info", logger: str = "workflow") -> None:
        """Push a log line to the client (notifications/message).

        MCP logging levels: debug, info, notice, warning, error, critical,
        alert, emergency.  Clients typically render these in a streaming
        activity panel — closest analog to a sub-agent feed.
        """
        notification = _make_notification("notifications/message", {
            "level": level,
            "logger": logger,
            "data": _truncate_text(message, limit=_MAX_LOG_MESSAGE_CHARS),
        })
        with _stdout_lock:
            _send_message(notification, transport=self._transport)


# ---------------------------------------------------------------------------
# MCP protocol handlers
# ---------------------------------------------------------------------------

def handle_initialize(request_id: Any, params: dict) -> dict:
    return _make_response(request_id, {
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {},
            "logging": {},
        },
        "serverInfo": {"name": "praxis-mcp", "version": "2.2.0"},
    })


def _resolved_allowed_tool_names(value: object | None) -> set[str] | None:
    explicit = normalize_allowed_tool_names(value)
    if explicit is not None:
        return explicit
    return normalize_allowed_tool_names(
        os.environ.get("PRAXIS_ALLOWED_MCP_TOOLS")
        or os.environ.get("PRAXIS_ALLOWED_MCP_TOOLS")
    )


def handle_tools_list(request_id: Any, *, allowed_tool_names: object | None = None) -> dict:
    tools_dict = get_tool_catalog()
    allowed = _resolved_allowed_tool_names(allowed_tool_names)
    tools = [
        {
            "name": definition.name,
            "description": definition.description,
            "inputSchema": definition.input_schema,
        }
        for _, definition in tools_dict.items()
        if allowed is None or definition.name in allowed
    ]
    return _make_response(request_id, {"tools": tools})


def handle_tools_call(
    request_id: Any,
    params: dict,
    *,
    transport: str = _TRANSPORT_CONTENT_LENGTH,
    allowed_tool_names: object | None = None,
    workflow_token: str = "",
) -> dict:
    tool_name = params.get("name")
    canonical_name = canonical_tool_name(tool_name)
    allowed = _resolved_allowed_tool_names(allowed_tool_names)
    if allowed is not None and canonical_name not in allowed:
        return _make_error_response(request_id, f"Tool not allowed: {tool_name}")
    raw_tool_input = params.get("arguments", params.get("input", {}))
    if raw_tool_input is None:
        raw_tool_input = {}
    if not isinstance(raw_tool_input, dict):
        return _make_error_response(
            request_id,
            f"Tool arguments must be a JSON object: {tool_name}",
        )
    tool_input = dict(raw_tool_input)

    # Extract MCP progress token from _meta if the client sent one
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        meta = tool_input.pop("_meta", None)
    if not isinstance(meta, dict):
        meta = {}
    progress_token = meta.get("progressToken")
    emitter = ProgressEmitter(progress_token, transport)

    try:
        result = invoke_tool(
            canonical_name,
            tool_input,
            allowed_tool_names=allowed,
            workflow_token=workflow_token,
            progress_emitter=emitter,
        )
        return _make_response(request_id, {
            "content": [{"type": "text", "text": _serialize_tool_result(result)}],
        })
    except ToolInvocationError as exc:
        return _make_error_response(request_id, exc.message)
    except Exception as e:
        return _make_error_response(request_id, _format_tool_error(e))


def handle_request(
    msg: dict,
    *,
    transport: str = _TRANSPORT_CONTENT_LENGTH,
    allowed_tool_names: object | None = None,
    workflow_token: str = "",
) -> dict | None:
    """Route a JSON-RPC request to the appropriate handler."""
    request_id = msg.get("id")
    method = msg.get("method")
    params = msg.get("params", {})

    # Notifications (no id) — acknowledge silently
    if method == "notifications/initialized":
        return None

    if method == "initialize":
        return handle_initialize(request_id, params)
    elif method == "tools/list":
        return handle_tools_list(request_id, allowed_tool_names=allowed_tool_names)
    elif method == "tools/call":
        return handle_tools_call(
            request_id,
            params,
            transport=transport,
            allowed_tool_names=allowed_tool_names,
            workflow_token=workflow_token,
        )
    else:
        if request_id is not None:
            return _make_error_response(request_id, f"Unknown method: {method}")
        return None  # Ignore unknown notifications


# ---------------------------------------------------------------------------
# Content-Length framed reader
# ---------------------------------------------------------------------------

def _read_message_with_transport() -> tuple[dict | None, str | None]:
    """Read one JSON-RPC message from stdin and report the input transport.

    Supports both Content-Length framed messages (MCP spec) and bare
    JSON lines (for manual testing / backwards compat).
    """
    # Read the first line — could be a Content-Length header or bare JSON
    stdin = sys.stdin.buffer if hasattr(sys.stdin, 'buffer') else sys.stdin

    if hasattr(stdin, 'readline'):
        first_line = stdin.readline()
    else:
        first_line = sys.stdin.readline().encode()

    if not first_line:
        return None, None  # EOF

    first_str = first_line.decode('utf-8') if isinstance(first_line, bytes) else first_line

    # Check if this is a Content-Length header
    if first_str.strip().lower().startswith('content-length:'):
        transport = _TRANSPORT_CONTENT_LENGTH
        try:
            content_length = int(first_str.strip().split(':', 1)[1].strip())
        except ValueError as exc:
            raise _TransportDecodeError(transport, f"Invalid Content-Length header: {exc}") from exc

        # Read remaining headers until blank line
        while True:
            header_line = stdin.readline() if hasattr(stdin, 'readline') else sys.stdin.readline().encode()
            if not header_line:
                raise _TransportDecodeError(transport, "Unexpected EOF while reading MCP headers")
            header_str = header_line.decode('utf-8') if isinstance(header_line, bytes) else header_line
            if header_str.strip() == '':
                break

        # Read exactly content_length bytes
        body_bytes = b''
        while len(body_bytes) < content_length:
            chunk = stdin.read(content_length - len(body_bytes))
            if not chunk:
                raise _TransportDecodeError(transport, "Unexpected EOF while reading MCP body")
            if isinstance(chunk, str):
                chunk = chunk.encode('utf-8')
            body_bytes += chunk

        try:
            return json.loads(body_bytes.decode('utf-8')), transport
        except json.JSONDecodeError as exc:
            raise _TransportDecodeError(transport, f"JSON decode error: {exc}") from exc
    else:
        # Bare JSON line (backwards compat for testing)
        transport = _TRANSPORT_JSONL
        stripped = first_str.strip()
        if not stripped:
            return _read_message_with_transport()  # skip blank lines
        try:
            return json.loads(stripped), transport
        except json.JSONDecodeError as exc:
            raise _TransportDecodeError(transport, f"JSON decode error: {exc}") from exc


def _read_message() -> dict | None:
    """Backward-compatible wrapper that returns only the parsed message."""
    msg, _ = _read_message_with_transport()
    return msg


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    """Read JSON-RPC messages from stdin, process, write responses to stdout."""
    while True:
        try:
            msg, incoming_transport = _read_message_with_transport()
            if msg is None:
                break
            out_transport = _response_transport(incoming_transport)
            response = handle_request(msg, transport=out_transport)
            if response:
                with _stdout_lock:
                    _send_message(response, transport=out_transport)
        except _TransportDecodeError as e:
            _send_message(
                _make_error_response(-1, e.message),
                transport=_response_transport(e.transport),
            )
        except Exception as e:
            _send_message(
                _make_error_response(-1, f"Unexpected error: {e}"),
                transport=_response_transport(None),
            )
