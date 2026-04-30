"""Unit tests for the praxis-agentd broker.

The broker is *transport*: every test asserts that auth is enforced at the
edge and dispatch is delegated to ``surfaces.mcp.protocol.handle_request``.
We monkeypatch ``handle_request`` so the test process never has to boot
real MCP subsystems.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime.workflow.mcp_session import WorkflowMcpSessionError
from surfaces.agent_broker import server as broker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_broker(monkeypatch, tmp_path, *, captured: dict[str, Any] | None = None):
    """Boot a broker bound to an ephemeral loopback port.

    Returns ``(server, base_url, token)``. Caller is responsible for shutting
    the server down via ``server.shutdown()``.
    """
    monkeypatch.setenv("PRAXIS_AGENT_BROKER_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("PRAXIS_AGENT_BROKER_HOST", "127.0.0.1")
    # Force a deterministic token so tests can compare-and-bypass without
    # racing the random generator.
    token_file = tmp_path / "token"
    token_file.write_text("test-broker-token\n", encoding="utf-8")

    # Module-level state is set up inside serve(); replicate the bits we need
    # without binding to the default port.
    broker._set_broker_token("test-broker-token")
    broker._write_state_env(tmp_path, host="127.0.0.1", port=0)

    if captured is not None:
        def _capture(body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            captured["body"] = body
            captured["kwargs"] = kwargs
            request_id = body.get("id")
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": []}}

        monkeypatch.setattr(broker, "handle_request", _capture)

    httpd = broker._BrokerHTTPServer(("127.0.0.1", 0), broker._BrokerHandler)
    host, port = httpd.server_address
    base_url = f"http://{host}:{port}"
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    return httpd, base_url, "test-broker-token"


def _http_request(
    method: str,
    url: str,
    *,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url=url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8") or "{}")
            return resp.status, payload
    except urllib.error.HTTPError as exc:
        payload = {}
        try:
            payload = json.loads(exc.read().decode("utf-8") or "{}")
        except Exception:
            payload = {}
        return exc.code, payload


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health_returns_ok(monkeypatch, tmp_path):
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path)
    try:
        status, body = _http_request("GET", base_url + "/health")
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 200
    assert body == {"ok": True, "service": "praxis-agentd"}


# ---------------------------------------------------------------------------
# /rpc — bearer broker token required, dispatches to handle_request
# ---------------------------------------------------------------------------

def test_rpc_dispatches_tools_list_with_valid_token(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}
    httpd, base_url, token = _start_broker(monkeypatch, tmp_path, captured=captured)
    try:
        status, body = _http_request(
            "POST",
            base_url + "/rpc",
            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 200
    assert body == {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}
    assert captured["body"]["method"] == "tools/list"
    # Critically: workflow_token is empty (host CLI lane is unscoped),
    # transport is jsonl, and no allowed_tool_names header was passed.
    assert captured["kwargs"]["workflow_token"] == ""
    assert captured["kwargs"]["transport"] == "jsonl"
    assert captured["kwargs"]["allowed_tool_names"] is None


def test_rpc_passes_allowed_tools_header_through(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}
    httpd, base_url, token = _start_broker(monkeypatch, tmp_path, captured=captured)
    try:
        status, _ = _http_request(
            "POST",
            base_url + "/rpc",
            body={"jsonrpc": "2.0", "id": 7, "method": "tools/list"},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "X-Praxis-Allowed-MCP-Tools": "praxis_query, praxis_health",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 200
    assert captured["kwargs"]["allowed_tool_names"] == ["praxis_health", "praxis_query"]


def test_rpc_rejects_missing_token(monkeypatch, tmp_path):
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path)
    try:
        status, body = _http_request(
            "POST",
            base_url + "/rpc",
            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={"Content-Type": "application/json"},
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 401
    assert body == {"error": "broker token required"}


def test_rpc_rejects_wrong_token(monkeypatch, tmp_path):
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path)
    try:
        status, body = _http_request(
            "POST",
            base_url + "/rpc",
            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Authorization": "Bearer not-the-broker-token",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 401
    assert body == {"error": "broker token required"}


def test_rpc_rejects_invalid_json(monkeypatch, tmp_path):
    httpd, base_url, token = _start_broker(monkeypatch, tmp_path)
    try:
        # Bypass urllib's JSON helper so we can send raw garbage.
        req = urllib.request.Request(
            url=base_url + "/rpc",
            data=b"{not-json",
            method="POST",
        )
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                status = resp.status
                body = json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = json.loads(exc.read().decode("utf-8") or "{}")
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 400
    assert "invalid JSON" in str(body.get("error", ""))


def test_rpc_rejects_oversized_body_before_dispatch(monkeypatch, tmp_path):
    monkeypatch.setenv("PRAXIS_AGENT_BROKER_MAX_BODY_BYTES", "32")
    captured: dict[str, Any] = {}
    httpd, base_url, token = _start_broker(monkeypatch, tmp_path, captured=captured)
    try:
        status, body = _http_request(
            "POST",
            base_url + "/rpc",
            body={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {"padding": "x" * 64},
            },
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 413
    assert "request body too large" in str(body.get("error", ""))
    assert captured == {}


# ---------------------------------------------------------------------------
# Broker must NOT consult host PRAXIS_API_TOKEN
# ---------------------------------------------------------------------------

def test_rpc_does_not_accept_praxis_api_token(monkeypatch, tmp_path):
    """The api-server's /mcp endpoint accepts PRAXIS_API_TOKEN as a fallback
    bearer for unscoped local calls. The broker must not — host env never
    grants broker access. Setting PRAXIS_API_TOKEN in the broker process and
    presenting it as a bearer must still fail 401."""
    monkeypatch.setenv("PRAXIS_API_TOKEN", "host-api-token")
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path)
    try:
        status, body = _http_request(
            "POST",
            base_url + "/rpc",
            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={
                "Authorization": "Bearer host-api-token",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 401


# ---------------------------------------------------------------------------
# /mcp — accepts signed workflow token OR broker token
# ---------------------------------------------------------------------------

def test_mcp_accepts_signed_workflow_token(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path, captured=captured)

    def _accept(token: str) -> dict[str, Any]:
        assert token == "signed-workflow-token"
        return {
            "run_id": "run_abc",
            "workflow_id": "wf_xyz",
            "job_label": "compose",
            "allowed_tools": ["praxis_query"],
            "exp": 0,
            "source_refs": [],
            "access_policy": {},
        }

    monkeypatch.setattr(broker, "verify_workflow_mcp_session_token", _accept)

    try:
        status, body = _http_request(
            "POST",
            base_url + "/mcp",
            body={"jsonrpc": "2.0", "id": 11, "method": "tools/call",
                  "params": {"name": "praxis_query", "arguments": {}}},
            headers={
                "Authorization": "Bearer signed-workflow-token",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 200
    # Workflow token + claims carried through to the protocol layer.
    assert captured["kwargs"]["workflow_token"] == "signed-workflow-token"
    assert captured["kwargs"]["allowed_tool_names"] == ["praxis_query"]


def test_mcp_falls_through_to_broker_token_when_workflow_token_invalid(
    monkeypatch, tmp_path
):
    captured: dict[str, Any] = {}
    httpd, base_url, token = _start_broker(monkeypatch, tmp_path, captured=captured)

    def _reject(_token: str) -> dict[str, Any]:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "not a workflow token")

    monkeypatch.setattr(broker, "verify_workflow_mcp_session_token", _reject)

    try:
        status, body = _http_request(
            "POST",
            base_url + "/mcp",
            body={"jsonrpc": "2.0", "id": 12, "method": "tools/list"},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 200
    # Broker-token fallback is unscoped: workflow_token is cleared.
    assert captured["kwargs"]["workflow_token"] == ""


def test_mcp_rejects_unknown_bearer(monkeypatch, tmp_path):
    httpd, base_url, _ = _start_broker(monkeypatch, tmp_path)

    def _reject(_token: str) -> dict[str, Any]:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "bad token")

    monkeypatch.setattr(broker, "verify_workflow_mcp_session_token", _reject)

    try:
        status, body = _http_request(
            "POST",
            base_url + "/mcp",
            body={"jsonrpc": "2.0", "id": 13, "method": "tools/list"},
            headers={
                "Authorization": "Bearer wrong-token",
                "Content-Type": "application/json",
            },
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 401
    assert body.get("reason_code") == "workflow_mcp.token_invalid"


# ---------------------------------------------------------------------------
# Token + state file management
# ---------------------------------------------------------------------------

def test_ensure_broker_token_creates_token_when_missing(tmp_path):
    state_dir = tmp_path / "fresh"
    token = broker._ensure_broker_token(state_dir)
    assert token
    persisted = (state_dir / "token").read_text(encoding="utf-8").strip()
    assert persisted == token


def test_ensure_broker_token_reuses_existing(tmp_path):
    state_dir = tmp_path / "reuse"
    state_dir.mkdir()
    (state_dir / "token").write_text("preserved-token\n", encoding="utf-8")
    assert broker._ensure_broker_token(state_dir) == "preserved-token"


def test_write_state_env_records_advertised_url(tmp_path):
    broker._write_state_env(
        tmp_path,
        host="0.0.0.0",
        port=8422,
        socket_path=tmp_path / "praxis.sock",
    )
    body = (tmp_path / "state.env").read_text(encoding="utf-8")
    assert "PRAXIS_AGENT_BROKER_URL=http://127.0.0.1:8422/rpc" in body
    assert "PRAXIS_AGENT_BROKER_PORT=8422" in body
    assert f"PRAXIS_AGENT_BROKER_SOCKET={tmp_path / 'praxis.sock'}" in body
    # 0.0.0.0 is rewritten to a client-reachable address; otherwise the
    # state file would advertise a bind address that no host curl can use.
    assert "PRAXIS_AGENT_BROKER_HOST=127.0.0.1" in body
