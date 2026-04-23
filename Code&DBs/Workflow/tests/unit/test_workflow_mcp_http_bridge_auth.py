from __future__ import annotations

from io import BytesIO
import json
from typing import Any

from runtime.workflow.mcp_session import WorkflowMcpSessionError
from surfaces.api.handlers import workflow_mcp


class _Request:
    def __init__(
        self,
        *,
        body: dict[str, Any],
        headers: dict[str, str] | None = None,
        path: str = "/mcp",
    ) -> None:
        raw_body = json.dumps(body).encode("utf-8")
        self.path = path
        self.headers = {
            "Content-Length": str(len(raw_body)),
            **(headers or {}),
        }
        self.rfile = BytesIO(raw_body)
        self.responses: list[tuple[int, dict[str, Any]]] = []

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.responses.append((status, payload))


def test_mcp_http_bridge_accepts_public_api_token_for_local_operator_tools(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "local-operator-token")

    def _reject_as_workflow_token(_token: str) -> dict[str, Any]:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "not a workflow token")

    captured: dict[str, Any] = {}

    def _handle_request(body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        captured["body"] = body
        captured["kwargs"] = kwargs
        return {"jsonrpc": "2.0", "id": body["id"], "result": {"tools": []}}

    monkeypatch.setattr(workflow_mcp, "verify_workflow_mcp_session_token", _reject_as_workflow_token)
    monkeypatch.setattr(workflow_mcp, "handle_request", _handle_request)

    request = _Request(
        body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        headers={
            "Authorization": "Bearer local-operator-token",
            "X-Praxis-Allowed-MCP-Tools": "praxis_query,praxis_health",
        },
    )

    workflow_mcp._handle_mcp_post(request, "/mcp")

    assert request.responses == [(200, {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}})]
    assert captured["kwargs"] == {
        "transport": "jsonl",
        "allowed_tool_names": ["praxis_query", "praxis_health"],
        "workflow_token": "",
    }


def test_mcp_http_bridge_rejects_unknown_bearer_when_workflow_token_fails(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "local-operator-token")

    def _reject_as_workflow_token(_token: str) -> dict[str, Any]:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "bad token")

    monkeypatch.setattr(workflow_mcp, "verify_workflow_mcp_session_token", _reject_as_workflow_token)

    request = _Request(
        body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        headers={"Authorization": "Bearer wrong-token"},
    )

    workflow_mcp._handle_mcp_post(request, "/mcp")

    assert request.responses == [
        (401, {"error": "bad token", "reason_code": "workflow_mcp.token_invalid"})
    ]


def test_mcp_http_bridge_keeps_workflow_session_scope(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "local-operator-token")
    monkeypatch.setattr(
        workflow_mcp,
        "verify_workflow_mcp_session_token",
        lambda _token: {
            "run_id": "run-1",
            "workflow_id": "workflow-1",
            "job_label": "job-1",
            "allowed_tools": ["praxis_query", "praxis_health"],
            "exp": 9999999999,
        },
    )

    captured: dict[str, Any] = {}

    def _handle_request(body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        captured["kwargs"] = kwargs
        return {"jsonrpc": "2.0", "id": body["id"], "result": {"ok": True}}

    monkeypatch.setattr(workflow_mcp, "handle_request", _handle_request)

    request = _Request(
        body={"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}},
        headers={
            "Authorization": "Bearer signed-workflow-token",
            "X-Praxis-Allowed-MCP-Tools": "praxis_query",
        },
    )

    workflow_mcp._handle_mcp_post(request, "/mcp")

    assert request.responses == [(200, {"jsonrpc": "2.0", "id": 2, "result": {"ok": True}})]
    assert captured["kwargs"] == {
        "transport": "jsonl",
        "allowed_tool_names": ["praxis_query"],
        "workflow_token": "signed-workflow-token",
    }
