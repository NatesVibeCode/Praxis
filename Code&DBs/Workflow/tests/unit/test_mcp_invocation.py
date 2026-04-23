from __future__ import annotations

from typing import Any

import pytest

from surfaces.mcp.catalog import McpToolDefinition
from surfaces.mcp import invocation


def _definition(*, name: str, session: bool = False) -> McpToolDefinition:
    cli = {
        "surface": "session" if session else "query",
        "tier": "session" if session else "stable",
        "recommended_alias": None,
        "when_to_use": "use it",
        "when_not_to_use": "do not use it",
        "risks": {"default": "session" if session else "read"},
        "examples": [{"title": "example", "input": {}}],
    }
    return McpToolDefinition(
        name=name,
        module_name="fake.module",
        handler_name="handler",
        metadata={
            "description": "fake tool",
            "inputSchema": {"type": "object", "properties": {}},
            "cli": cli,
        },
        selector_defaults={},
    )


def test_invoke_tool_supports_keyword_only_handlers_and_context(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _handler(
        *,
        _subsystems: Any = None,
        _session_token: str = "",
        channel: str = "build_state",
        **_kw: Any,
    ) -> dict[str, Any]:
        from surfaces.mcp.runtime_context import get_current_workflow_mcp_context

        context = get_current_workflow_mcp_context()
        captured["subsystems"] = _subsystems
        captured["session_token"] = _session_token
        captured["channel"] = channel
        captured["job_label"] = context.job_label if context else None
        return {"ok": True, "job_label": captured["job_label"]}

    definition = _definition(name="session_tool", session=True)
    monkeypatch.setattr(invocation, "get_tool_catalog", lambda: {"session_tool": definition})
    monkeypatch.setattr(invocation, "resolve_tool_entry", lambda _name: (_handler, {}))
    monkeypatch.setattr(
        invocation,
        "verify_workflow_mcp_session_token",
        lambda _token: {
            "run_id": "run-1",
            "workflow_id": "wf-1",
            "job_label": "job-1",
            "allowed_tools": ["session_tool"],
            "exp": 9999999999,
        },
    )

    result = invocation.invoke_tool(
        "session_tool",
        {"channel": "ops"},
        workflow_token="signed-token",
    )

    assert result == {"ok": True, "job_label": "job-1"}
    assert captured["session_token"] == "signed-token"
    assert captured["channel"] == "ops"
    assert captured["job_label"] == "job-1"
    assert captured["subsystems"] is not None


def test_invoke_tool_records_surface_usage_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[dict[str, Any]] = []

    monkeypatch.setattr(
        invocation,
        "_record_tool_usage",
        lambda **kwargs: recorded.append(kwargs),
    )
    monkeypatch.setattr(
        invocation,
        "get_tool_catalog",
        lambda: {"praxis_query": _definition(name="praxis_query")},
    )
    monkeypatch.setattr(
        invocation,
        "resolve_tool_entry",
        lambda _name: (lambda **_kw: {"ok": True}, {}),
    )

    result = invocation.invoke_tool("praxis_query", {"question": "status"})

    assert result == {"ok": True}
    assert recorded == [
        {
            "canonical_name": "praxis_query",
            "workflow_token": "",
            "status_code": 200,
            "tool_input": {"question": "status"},
            "result_payload": {"ok": True},
            "claims": None,
        }
    ]


def test_invoke_tool_requires_workflow_token_for_session_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition(name="session_tool", session=True)
    monkeypatch.setattr(invocation, "get_tool_catalog", lambda: {"session_tool": definition})
    monkeypatch.setattr(invocation, "resolve_tool_entry", lambda _name: (lambda **_kw: {"ok": True}, {}))

    with pytest.raises(invocation.ToolInvocationError) as exc_info:
        invocation.invoke_tool("session_tool", {})

    assert exc_info.value.reason_code == "workflow_mcp.token_required"
    assert "workflow token is required" in exc_info.value.message


def test_invoke_tool_enforces_allowed_tools_from_workflow_token(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition(name="session_tool", session=True)
    recorded: list[dict[str, Any]] = []

    monkeypatch.setattr(
        invocation,
        "_record_tool_usage",
        lambda **kwargs: recorded.append(kwargs),
    )
    monkeypatch.setattr(invocation, "get_tool_catalog", lambda: {"session_tool": definition})
    monkeypatch.setattr(invocation, "resolve_tool_entry", lambda _name: (lambda **_kw: {"ok": True}, {}))
    monkeypatch.setattr(
        invocation,
        "verify_workflow_mcp_session_token",
        lambda _token: {
            "run_id": "run-1",
            "workflow_id": "wf-1",
            "job_label": "job-1",
            "allowed_tools": ["different_tool"],
            "exp": 9999999999,
        },
    )

    with pytest.raises(invocation.ToolInvocationError) as exc_info:
        invocation.invoke_tool("session_tool", {}, workflow_token="signed-token")

    assert exc_info.value.reason_code == "workflow_mcp.tool_not_allowed"
    assert "Tool not allowed by workflow token" in exc_info.value.message
    assert recorded == [
        {
            "canonical_name": "session_tool",
            "workflow_token": "signed-token",
            "status_code": 400,
            "tool_input": {},
            "result_payload": {
                "error": "Tool not allowed by workflow token: session_tool",
                "reason_code": "workflow_mcp.tool_not_allowed",
            },
            "claims": {
                "run_id": "run-1",
                "workflow_id": "wf-1",
                "job_label": "job-1",
                "allowed_tools": ["different_tool"],
                "exp": 9999999999,
            },
        }
    ]
