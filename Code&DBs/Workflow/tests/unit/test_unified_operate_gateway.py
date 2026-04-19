from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

import surfaces.api.rest as rest
from surfaces.mcp.catalog import McpToolDefinition


def _tool_definition() -> McpToolDefinition:
    return McpToolDefinition(
        name="praxis_echo",
        module_name="surfaces.mcp.tools.echo",
        handler_name="tool_echo",
        metadata={
            "description": "Echo test tool",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["say", "list"],
                        "default": "say",
                    },
                    "text": {"type": "string"},
                },
                "required": ["action"],
            },
            "cli": {
                "surface": "stable",
                "tier": "general",
                "recommended_alias": "echo",
                "risks": {
                    "default": "read",
                    "actions": {
                        "say": "read",
                        "list": "read",
                    },
                },
            },
        },
        selector_defaults={},
    )


def test_operate_catalog_projects_tool_operations(monkeypatch) -> None:
    monkeypatch.setattr(rest, "get_tool_catalog", lambda: {"praxis_echo": _tool_definition()})

    payload = rest.build_operate_catalog_payload()

    assert payload["routed_to"] == "unified_operator_catalog"
    assert payload["call_path"] == "/api/operate"
    assert payload["catalog_path"] == "/api/operate/catalog"
    assert payload["tool_count"] == 1
    assert {operation["operation"] for operation in payload["operations"]} == {
        "praxis_echo.say",
        "praxis_echo.list",
    }
    assert payload["authority"] == "surfaces.mcp.catalog.get_tool_catalog"


def test_operate_endpoint_delegates_to_catalog_invocation(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any], str]] = []
    monkeypatch.setattr(rest, "get_tool_catalog", lambda: {"praxis_echo": _tool_definition()})
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)

    def fake_invoke_tool(tool_name: object, raw_arguments: object, *, workflow_token: str = "", **_: Any) -> dict[str, Any]:
        assert isinstance(raw_arguments, dict)
        calls.append((str(tool_name), raw_arguments, workflow_token))
        return {"echo": raw_arguments}

    monkeypatch.setattr(rest, "invoke_tool", fake_invoke_tool)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "praxis_echo.say",
                "input": {"text": "hello"},
                "trace": {"caller": "unit-test"},
            },
            headers={"X-Workflow-Token": "token-123"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["routed_to"] == "unified_operator_gateway"
    assert payload["operation"] == "praxis_echo.say"
    assert payload["tool"] == "praxis_echo"
    assert payload["result"] == {"echo": {"action": "say", "text": "hello"}}
    assert calls == [("praxis_echo", {"action": "say", "text": "hello"}, "token-123")]


def test_operate_endpoint_rejects_unknown_operation(monkeypatch) -> None:
    monkeypatch.setattr(rest, "get_tool_catalog", lambda: {"praxis_echo": _tool_definition()})
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)

    with TestClient(rest.app) as client:
        response = client.post("/api/operate", json={"operation": "praxis_missing", "input": {}})

    assert response.status_code == 404
    assert response.json()["detail"]["error_code"] == "operate.operation_not_found"


def test_operate_endpoint_selector_suffix_overrides_conflicting_input(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(rest, "get_tool_catalog", lambda: {"praxis_echo": _tool_definition()})
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)

    def fake_invoke_tool(tool_name: object, raw_arguments: object, *, workflow_token: str = "", **_: Any) -> dict[str, Any]:
        assert tool_name == "praxis_echo"
        assert isinstance(raw_arguments, dict)
        calls.append(raw_arguments)
        return {"ok": True}

    monkeypatch.setattr(rest, "invoke_tool", fake_invoke_tool)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "praxis_echo.say",
                "input": {"action": "list", "text": "hello"},
            },
        )

    assert response.status_code == 200
    assert calls == [{"action": "say", "text": "hello"}]
