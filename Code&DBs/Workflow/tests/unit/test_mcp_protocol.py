"""Transport compatibility tests for surfaces.mcp.protocol."""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path


WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from surfaces.mcp import protocol
from storage.postgres.validators import PostgresConfigurationError


class _FakeStdin:
    def __init__(self, payload: bytes) -> None:
        self.buffer = io.BytesIO(payload)


class _BinaryStdout:
    def __init__(self) -> None:
        self.buffer = io.BytesIO()


def _initialize_request_bytes(*, transport: str) -> bytes:
    message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"protocolVersion": "2024-11-05"},
    }
    body = json.dumps(message).encode("utf-8")
    if transport == "jsonl":
        return body + b"\n"
    return f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8") + body


def _parse_jsonl(output: str) -> dict:
    return json.loads(output.strip())


def _parse_framed(output: str) -> dict:
    header, body = output.split("\r\n\r\n", 1)
    assert header.startswith("Content-Length:")
    return json.loads(body)


def test_jsonl_request_mirrors_jsonl_response(monkeypatch):
    monkeypatch.delenv("PRAXIS_MCP_STDIO_TRANSPORT", raising=False)
    monkeypatch.setattr(sys, "stdin", _FakeStdin(_initialize_request_bytes(transport="jsonl")))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)

    protocol.main()

    response = _parse_jsonl(stdout.getvalue())
    assert response["result"]["protocolVersion"] == "2024-11-05"


def test_framed_request_mirrors_framed_response(monkeypatch):
    monkeypatch.delenv("PRAXIS_MCP_STDIO_TRANSPORT", raising=False)
    monkeypatch.setattr(sys, "stdin", _FakeStdin(_initialize_request_bytes(transport="content-length")))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)

    protocol.main()

    response = _parse_framed(stdout.getvalue())
    assert response["result"]["serverInfo"]["name"] == "praxis-mcp"


def test_framed_response_content_length_counts_utf8_bytes(monkeypatch):
    stdout = _BinaryStdout()
    monkeypatch.setattr(sys, "stdout", stdout)

    protocol._send_message(
        {"jsonrpc": "2.0", "id": 1, "result": {"message": "check \u2713"}},
        transport="content-length",
    )

    raw = stdout.buffer.getvalue()
    header, body = raw.split(b"\r\n\r\n", 1)
    length = int(header.decode("ascii").split(":", 1)[1].strip())
    assert length == len(body)
    assert json.loads(body.decode("utf-8"))["result"]["message"] == "check \u2713"


def test_jsonl_override_forces_jsonl_response(monkeypatch):
    monkeypatch.setenv("PRAXIS_MCP_STDIO_TRANSPORT", "jsonl")
    monkeypatch.setattr(sys, "stdin", _FakeStdin(_initialize_request_bytes(transport="content-length")))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)

    protocol.main()

    response = _parse_jsonl(stdout.getvalue())
    assert response["result"]["serverInfo"]["version"] == "2.2.0"


def test_tools_list_honors_allowed_tool_filter():
    response = protocol.handle_request(
        {"jsonrpc": "2.0", "id": 7, "method": "tools/list"},
        allowed_tool_names=["praxis_query", "praxis_status"],
    )

    assert response is not None
    names = [tool["name"] for tool in response["result"]["tools"]]
    assert set(names) == {"praxis_query", "praxis_status"}


def test_tools_call_rejects_unallowed_tool_before_execution():
    response = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {"name": "praxis_query", "arguments": {"question": "status"}},
        },
        allowed_tool_names=["praxis_status"],
    )

    assert response is not None
    assert response["error"]["message"] == "Tool not allowed: praxis_query"


def test_tools_call_truncates_oversized_result_text(monkeypatch):
    monkeypatch.setattr(protocol, "_MAX_TOOL_RESULT_TEXT_CHARS", 300)
    monkeypatch.setattr(
        protocol,
        "invoke_tool",
        lambda *_args, **_kwargs: {"payload": "x" * 2_000},
    )

    response = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {"name": "oversized_tool", "arguments": {}},
        },
        transport="jsonl",
    )

    assert response is not None
    text = response["result"]["content"][0]["text"]
    payload = json.loads(text)
    assert payload["truncated"] is True
    assert payload["reason"] == "mcp tool result exceeded PRAXIS_MCP_TOOL_RESULT_CHAR_LIMIT"
    assert payload["original_chars"] > 300
    assert len(text) <= 300


def test_progress_log_truncates_oversized_messages(monkeypatch):
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(protocol, "_MAX_LOG_MESSAGE_CHARS", 120)

    protocol.ProgressEmitter("progress-token", "jsonl").log("x" * 400)

    notification = _parse_jsonl(stdout.getvalue())
    message = notification["params"]["data"]
    assert len(message) <= 120
    assert "truncated" in message


def test_progress_emit_truncates_oversized_messages(monkeypatch):
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(protocol, "_MAX_LOG_MESSAGE_CHARS", 100)

    protocol.ProgressEmitter("progress-token", "jsonl").emit(
        progress=1,
        total=2,
        message="y" * 300,
    )

    notification = _parse_jsonl(stdout.getvalue())
    message = notification["params"]["message"]
    assert len(message) <= 100
    assert "truncated" in message


def test_error_response_truncates_oversized_message(monkeypatch):
    monkeypatch.setattr(protocol, "_MAX_ERROR_MESSAGE_CHARS", 90)

    response = protocol._make_error_response(10, "e" * 400)

    assert len(response["error"]["message"]) <= 90
    assert "truncated" in response["error"]["message"]


def test_tools_call_formats_postgres_authority_errors_without_traceback(monkeypatch):
    def _invoke(*_args, **_kwargs):
        raise PostgresConfigurationError(
            "postgres.authority_unavailable",
            "WORKFLOW_DATABASE_URL authority unavailable: PermissionError: [Errno 1] Operation not permitted",
        )

    monkeypatch.setattr(protocol, "invoke_tool", _invoke)

    response = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "db_tool", "arguments": {}},
        },
        transport="jsonl",
    )

    assert response is not None
    message = response["error"]["message"]
    assert "postgres.authority_unavailable" in message
    assert "Traceback" not in message


def test_tools_call_rejects_non_object_arguments_without_traceback(monkeypatch):
    monkeypatch.setattr(protocol, "get_tool_catalog", lambda: {"db_tool": object()})
    monkeypatch.setattr(protocol, "resolve_tool_entry", lambda _name: (lambda _params: {}, object()))

    response = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {"name": "db_tool", "arguments": ["bad-payload"]},
        },
        transport="jsonl",
    )

    assert response is not None
    assert response["error"]["message"] == "Tool arguments must be a JSON object: db_tool"


def test_tools_call_does_not_mutate_original_arguments_when_meta_embedded(monkeypatch):
    captured: list[dict[str, object]] = []

    def _invoke(_tool_name: str, params: dict, **_kwargs) -> dict[str, str]:
        captured.append(params)
        return {"ok": "yes"}

    monkeypatch.setattr(protocol, "invoke_tool", _invoke)

    arguments = {"question": "status", "_meta": {"progressToken": "tok-1"}}
    response = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 13,
            "method": "tools/call",
            "params": {"name": "meta_tool", "arguments": arguments},
        },
        transport="jsonl",
    )

    assert response is not None
    assert captured == [{"question": "status"}]
    assert arguments == {"question": "status", "_meta": {"progressToken": "tok-1"}}


def test_tools_call_preserves_progress_token_on_emitter(monkeypatch):
    captured: list[object] = []

    def _invoke(_tool_name: str, _params: dict, **kwargs) -> dict[str, str]:
        captured.append(kwargs.get("progress_emitter"))
        return {"ok": "yes"}

    monkeypatch.setattr(protocol, "invoke_tool", _invoke)

    without_token = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 14,
            "method": "tools/call",
            "params": {"name": "meta_tool", "arguments": {"question": "status"}},
        },
        transport="jsonl",
    )
    with_token = protocol.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 15,
            "method": "tools/call",
            "params": {
                "name": "meta_tool",
                "arguments": {"question": "status"},
                "_meta": {"progressToken": "tok-2"},
            },
        },
        transport="jsonl",
    )

    assert without_token is not None
    assert with_token is not None
    assert captured[0] is not None
    assert getattr(captured[0], "progress_token", None) is None
    assert captured[1] is not None
    assert getattr(captured[1], "progress_token", None) == "tok-2"
