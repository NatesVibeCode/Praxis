from __future__ import annotations

import io
import json
from types import SimpleNamespace
import urllib.error

from runtime.integrations.webhook import execute_webhook


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200) -> None:
        self._payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_execute_webhook_uses_connector_endpoint_map_and_bearer_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_credential(ref: str):
        assert ref == "secret.demo.connector"
        return SimpleNamespace(api_key="test-token")

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data.decode("utf-8") if request.data else ""
        return _FakeResponse({"ok": True})

    import urllib.request
    import runtime.integrations.webhook as webhook_mod

    monkeypatch.setattr(webhook_mod, "resolve_credential", _fake_resolve_credential)
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "connector_spec": {
                "baseUrl": "https://api.example.com",
                "primaryEndpointId": "list_items",
            },
            "auth_strategy": {
                "mode": "bearer_token",
                "credentialRef": "secret.demo.connector",
                "headerName": "Authorization",
                "tokenPrefix": "Bearer",
            },
            "endpoint_map": [
                {
                    "id": "list_items",
                    "name": "List Items",
                    "method": "GET",
                    "path": "/v1/items",
                    "purpose": "Fetch items",
                    "requestBodyTemplate": "",
                }
            ],
        },
        pg=None,
    )

    assert result["status"] == "succeeded"
    assert captured["url"] == "https://api.example.com/v1/items"
    assert captured["method"] == "GET"
    assert captured["headers"]["Authorization"] == "Bearer test-token"


def test_execute_webhook_supports_api_key_query_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_credential(ref: str):
        assert ref == "secret.demo.query"
        return SimpleNamespace(api_key="query-token")

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["url"] = request.full_url
        return _FakeResponse({"ok": True})

    import urllib.request
    import runtime.integrations.webhook as webhook_mod

    monkeypatch.setattr(webhook_mod, "resolve_credential", _fake_resolve_credential)
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "endpoint": "https://api.example.com/v1/search?limit=10",
            "auth_strategy": {
                "mode": "api_key_query",
                "credentialRef": "secret.demo.query",
                "queryParam": "api_key",
            },
        },
        pg=None,
    )

    assert result["status"] == "succeeded"
    assert captured["url"] == "https://api.example.com/v1/search?limit=10&api_key=query-token"


def test_execute_webhook_fails_when_auth_cannot_be_resolved(monkeypatch) -> None:
    import runtime.integrations.webhook as webhook_mod

    def _fake_resolve_credential(ref: str):
        raise webhook_mod.CredentialResolutionError("missing", "no credential found")

    monkeypatch.setattr(webhook_mod, "resolve_credential", _fake_resolve_credential)

    result = execute_webhook(
        {
            "endpoint": "https://api.example.com/v1/items",
            "auth_strategy": {
                "mode": "api_key_header",
                "credentialRef": "secret.missing",
                "headerName": "X-API-Key",
            },
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "auth_resolution_failed"


def test_execute_webhook_supports_api_key_header_and_endpoint_body_template(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_credential(ref: str):
        assert ref == "secret.demo.header"
        return SimpleNamespace(api_key="header-token")

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["body"] = request.data.decode("utf-8") if request.data else ""
        return _FakeResponse({"created": True}, status=201)

    import urllib.request
    import runtime.integrations.webhook as webhook_mod

    monkeypatch.setattr(webhook_mod, "resolve_credential", _fake_resolve_credential)
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "connector_spec": {
                "baseUrl": "https://api.example.com",
                "primaryEndpointId": "create_item",
            },
            "auth_strategy": {
                "mode": "api_key_header",
                "credentialRef": "secret.demo.header",
                "headerName": "X-API-Key",
            },
            "endpoint_map": [
                {
                    "id": "list_items",
                    "name": "List Items",
                    "method": "GET",
                    "path": "/v1/items",
                    "purpose": "Fetch items",
                },
                {
                    "id": "create_item",
                    "name": "Create Item",
                    "method": "POST",
                    "path": "/v1/items",
                    "purpose": "Create item",
                    "requestBodyTemplate": {"name": "demo"},
                },
            ],
        },
        pg=None,
    )

    assert result["status"] == "succeeded"
    assert captured["url"] == "https://api.example.com/v1/items"
    assert captured["method"] == "POST"
    assert captured["headers"]["x-api-key"] == "header-token"
    assert captured["headers"]["content-type"] == "application/json"
    assert json.loads(str(captured["body"])) == {"name": "demo"}


def test_execute_webhook_supports_none_auth_with_first_endpoint_fallback(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_credential(ref: str):
        raise AssertionError(f"resolve_credential should not be called for none auth: {ref}")

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        return _FakeResponse({"ok": True})

    import urllib.request
    import runtime.integrations.webhook as webhook_mod

    monkeypatch.setattr(webhook_mod, "resolve_credential", _fake_resolve_credential)
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "connector_spec": {
                "baseUrl": "https://api.example.com",
            },
            "auth_strategy": {
                "mode": "none",
            },
            "endpoint_map": [
                {
                    "id": "health",
                    "name": "Health",
                    "method": "GET",
                    "path": "/health",
                    "purpose": "Health check",
                },
                {
                    "id": "secondary",
                    "name": "Secondary",
                    "method": "POST",
                    "path": "/v1/items",
                    "purpose": "Create item",
                },
            ],
        },
        pg=None,
    )

    assert result["status"] == "succeeded"
    assert captured["url"] == "https://api.example.com/health"
    assert captured["method"] == "GET"
    assert "authorization" not in captured["headers"]
    assert "x-api-key" not in captured["headers"]


def test_execute_webhook_prefers_explicit_endpoint_over_connector_map(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        return _FakeResponse({"ok": True})

    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "endpoint": "https://override.example.net/custom",
            "method": "PATCH",
            "connector_spec": {
                "baseUrl": "https://api.example.com",
                "primaryEndpointId": "ignored",
            },
            "endpoint_map": [
                {
                    "id": "ignored",
                    "name": "Ignored",
                    "method": "GET",
                    "path": "/v1/items",
                    "purpose": "Ignored because explicit endpoint wins",
                }
            ],
        },
        pg=None,
    )

    assert result["status"] == "succeeded"
    assert captured["url"] == "https://override.example.net/custom"
    assert captured["method"] == "PATCH"


def test_execute_webhook_rejects_relative_endpoint_without_base_url() -> None:
    result = execute_webhook(
        {
            "endpoint": "/v1/items",
            "method": "GET",
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "invalid_url"
    assert result["summary"] == "Invalid URL: /v1/items"


def test_execute_webhook_surfaces_http_error_with_reproducible_details(monkeypatch) -> None:
    def _fake_urlopen(request, timeout=0):
        del timeout
        raise urllib.error.HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"unauthorized"}'),
        )

    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "endpoint": "https://api.example.com/v1/items",
            "method": "GET",
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "http_401"
    assert result["data"]["http_status"] == 401
    assert result["data"]["response"] == '{"error":"unauthorized"}'


def test_execute_webhook_fails_when_auth_strategy_omits_credential_ref() -> None:
    result = execute_webhook(
        {
            "endpoint": "https://api.example.com/v1/items",
            "auth_strategy": {
                "mode": "api_key_header",
                "headerName": "X-API-Key",
            },
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "auth_resolution_failed"
    assert result["summary"] == "Failed to resolve connector authentication."


def test_execute_webhook_fails_when_no_url_can_be_resolved() -> None:
    result = execute_webhook(
        {
            "auth_strategy": {
                "mode": "none",
            },
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "missing_url"
    assert result["summary"] == "No URL provided for webhook."


def test_execute_webhook_surfaces_connection_errors(monkeypatch) -> None:
    def _fake_urlopen(request, timeout=0):
        del request, timeout
        raise urllib.error.URLError("connection refused")

    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    result = execute_webhook(
        {
            "endpoint": "https://api.example.com/v1/items",
            "method": "GET",
        },
        pg=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "connection_error"
    assert result["summary"] == "GET https://api.example.com/v1/items → connection refused"
