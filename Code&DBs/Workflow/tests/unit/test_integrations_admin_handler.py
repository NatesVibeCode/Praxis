"""Unit tests for integrations_admin HTTP handlers.

Covers: POST /api/integrations (create), GET /api/integrations (list),
GET /api/integrations/<id> (describe), PUT /api/integrations/<id>/secret,
POST /api/integrations/<id>/test, POST /api/integrations/reload.
"""
from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from surfaces.api.handlers import integrations_admin


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.rows: dict[str, dict[str, Any]] = {}

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((sql.strip().split()[0].upper(), args))
        if sql.strip().upper().startswith("INSERT INTO INTEGRATION_REGISTRY"):
            integration_id, name, description, provider, caps_json, auth_status, manifest_source, connector_slug, auth_shape_json = args
            self.rows[integration_id] = {
                "id": integration_id,
                "name": name,
                "description": description,
                "provider": provider,
                "capabilities": json.loads(caps_json),
                "auth_status": auth_status,
                "manifest_source": manifest_source,
                "connector_slug": connector_slug,
                "auth_shape": json.loads(auth_shape_json),
            }
        return []

    def execute_many(self, sql: str, rows: list[tuple]) -> None:
        self.calls.append(("EXECUTE_MANY", (len(rows),)))


class _RequestStub:
    def __init__(self, path: str = "/api/integrations", body: dict | None = None, conn: _FakeConn | None = None) -> None:
        raw = json.dumps(body or {}).encode()
        self.rfile = io.BytesIO(raw)
        self.headers = {"Content-Length": str(len(raw))}
        self.path = path
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: conn or _FakeConn())
        self.sent: tuple[int, dict] | None = None

    def _send_json(self, status: int, payload: dict) -> None:
        self.sent = (status, payload)


def test_create_validates_id() -> None:
    stub = _RequestStub(body={"id": "!bad!", "name": "Bad", "capabilities": [{"action": "x"}]})
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 400
    assert "id must be" in payload["error"]


def test_create_requires_name() -> None:
    stub = _RequestStub(body={"id": "test", "capabilities": [{"action": "x"}]})
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent == (400, {"error": "name is required"})


def test_create_requires_non_empty_capabilities() -> None:
    stub = _RequestStub(body={"id": "test", "name": "Test", "capabilities": []})
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent == (400, {"error": "capabilities must be a non-empty list"})


def test_create_rejects_non_http_path() -> None:
    stub = _RequestStub(body={
        "id": "test", "name": "Test",
        "capabilities": [{"action": "x", "path": "file:///etc/passwd"}],
    })
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 400
    assert "http(s)" in payload["error"]


def test_create_requires_env_var_for_api_key() -> None:
    stub = _RequestStub(body={
        "id": "test", "name": "Test",
        "capabilities": [{"action": "x"}],
        "auth": {"kind": "api_key"},
    })
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 400
    assert "env_var" in payload["error"]


def test_create_succeeds_and_upserts() -> None:
    conn = _FakeConn()
    stub = _RequestStub(
        body={
            "id": "ipify",
            "name": "IPify",
            "description": "IP lookup",
            "provider": "http",
            "capabilities": [
                {"action": "get_ip", "method": "GET", "path": "https://api.ipify.org/?format=json"},
            ],
            "auth": {"kind": "none"},
        },
        conn=conn,
    )
    integrations_admin._handle_create(stub, "/api/integrations")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 201
    assert payload["integration_id"] == "ipify"
    assert payload["capabilities"] == ["get_ip"]
    assert "ipify" in conn.rows
    assert conn.rows["ipify"]["auth_status"] == "connected"
    assert conn.rows["ipify"]["auth_shape"]["kind"] == "none"


def test_set_secret_requires_value() -> None:
    stub = _RequestStub(path="/api/integrations/foo/secret", body={})
    integrations_admin._handle_set_secret(stub, "/api/integrations/foo/secret")
    assert stub.sent == (400, {"error": "value is required"})


def test_set_secret_404_when_missing() -> None:
    conn = _FakeConn()
    # describe_integration will return None because row doesn't exist in _FakeConn
    stub = _RequestStub(path="/api/integrations/ghost/secret", body={"value": "x"}, conn=conn)
    integrations_admin._handle_set_secret(stub, "/api/integrations/ghost/secret")
    assert stub.sent is not None
    assert stub.sent[0] == 404


def test_list_returns_integrations(monkeypatch) -> None:
    stub = _RequestStub(path="/api/integrations", conn=_FakeConn())
    monkeypatch.setattr(
        "runtime.integrations.integration_registry.list_integrations",
        lambda conn: [{"id": "ipify", "name": "IPify"}],
    )
    integrations_admin._handle_list(stub, "/api/integrations")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["integrations"][0]["id"] == "ipify"


def test_reload_returns_synced_count(monkeypatch) -> None:
    stub = _RequestStub(path="/api/integrations/reload", conn=_FakeConn())
    monkeypatch.setattr(
        "registry.integration_registry_sync.sync_integration_registry",
        lambda conn: 7,
    )
    integrations_admin._handle_reload(stub, "/api/integrations/reload")
    assert stub.sent == (200, {"synced": 7})


def test_describe_404_when_missing(monkeypatch) -> None:
    stub = _RequestStub(path="/api/integrations/ghost", conn=_FakeConn())
    monkeypatch.setattr(
        "runtime.integrations.integration_registry.describe_integration",
        lambda conn, iid: None,
    )
    integrations_admin._handle_describe(stub, "/api/integrations/ghost")
    assert stub.sent is not None
    assert stub.sent[0] == 404


def test_describe_returns_payload(monkeypatch) -> None:
    stub = _RequestStub(path="/api/integrations/ipify", conn=_FakeConn())
    monkeypatch.setattr(
        "runtime.integrations.integration_registry.describe_integration",
        lambda conn, iid: {"id": "ipify", "name": "IPify"},
    )
    integrations_admin._handle_describe(stub, "/api/integrations/ipify")
    assert stub.sent is not None
    status, payload = stub.sent
    assert status == 200
    assert payload["id"] == "ipify"
