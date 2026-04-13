"""Tests for the connector execution backend."""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import runtime.integrations.connector_executor as connector_executor_mod
import runtime.integration_manifest as integration_manifest_mod
from runtime.integrations.connector_executor import (
    _call_async,
    _call_sync,
    _classify_error,
    _instantiate_client,
    execute_connector,
)
from runtime.integrations.connector_registrar import find_client_class


# ── Helpers ──────────────────────────────────────────────────────────


class FakeClient:
    def __init__(self, api_key: str = "", base_url: str = ""):
        self.api_key = api_key
        self.base_url = base_url

    async def list_items(self, limit: int = 10) -> dict:
        return {"items": [], "limit": limit}

    async def get_item(self, item_id: str) -> dict:
        return {"id": item_id, "name": "test"}

    def sync_method(self) -> str:
        return "sync_result"

    def _private(self):
        pass


class NoSuffixClientClass:
    pass


def _make_module(name: str, *classes: type) -> types.ModuleType:
    mod = types.ModuleType(name)
    for cls in classes:
        cls.__module__ = name
        setattr(mod, cls.__name__, cls)
    return mod


def _mock_pg(connector_row: dict | None = None):
    pg = MagicMock()
    if connector_row is None:
        pg.execute.return_value = []
    else:
        pg.execute.return_value = [connector_row]
    return pg


def _connector_row(**overrides):
    base = {"slug": "test", "module_path": "artifacts.connectors.test.client", "base_url": "", "auth_type": "", "timeout_s": 30}
    base.update(overrides)
    return base


def _definition(**overrides):
    base = {"id": "test", "connector_slug": "test", "auth_shape": {}}
    base.update(overrides)
    return base


# ── _find_client_class ───────────────────────────────────────────────


def test_find_client_class_by_suffix():
    mod = _make_module("test_mod", FakeClient)
    assert find_client_class(mod) is FakeClient


def test_find_client_class_fallback():
    mod = _make_module("test_mod", NoSuffixClientClass)
    assert find_client_class(mod) is NoSuffixClientClass


def test_find_client_class_empty_module():
    mod = _make_module("test_mod")
    assert find_client_class(mod) is None


# ── _instantiate_client ──────────────────────────────────────────────


def test_instantiate_with_api_key():
    client = _instantiate_client(FakeClient, token="sk-123", base_url="https://api.example.com")
    assert client.api_key == "sk-123"
    assert client.base_url == "https://api.example.com"


def test_instantiate_no_token():
    client = _instantiate_client(FakeClient, token=None, base_url=None)
    assert client.api_key == ""
    assert client.base_url == ""


# ── _classify_error ──────────────────────────────────────────────────


def test_classify_timeout():
    assert _classify_error(asyncio.TimeoutError()) == "connector_timeout"
    assert _classify_error(TimeoutError("read timed out")) == "connector_timeout"


def test_classify_connection_error():
    assert _classify_error(ConnectionError("refused")) == "connector_network_error"
    assert _classify_error(OSError("network unreachable")) == "connector_network_error"


def test_classify_rate_limit_by_status():
    exc = Exception("rate limited")
    exc.status_code = 429
    assert _classify_error(exc) == "connector_rate_limited"


def test_classify_auth_by_status():
    exc = Exception("unauthorized")
    exc.status_code = 401
    assert _classify_error(exc) == "connector_auth_error"


def test_classify_server_error_by_status():
    exc = Exception("internal server error")
    exc.status_code = 500
    assert _classify_error(exc) == "connector_server_error"


def test_classify_input_error_by_status():
    exc = Exception("bad request")
    exc.status_code = 400
    assert _classify_error(exc) == "connector_input_error"


def test_classify_rate_limit_by_keyword():
    assert _classify_error(Exception("Too Many Requests")) == "connector_rate_limited"


def test_classify_auth_by_keyword():
    assert _classify_error(Exception("Unauthorized access")) == "connector_auth_error"


def test_classify_generic_fallback():
    assert _classify_error(Exception("something weird")) == "connector_call_failed"


# ── execute_connector — basic paths ──────────────────────────────────


def test_missing_connector_slug():
    result = execute_connector(definition={"id": "test"}, action="x", args={}, pg=MagicMock())
    assert result["error"] == "connector_slug_missing"


def test_connector_not_in_registry():
    pg = _mock_pg(None)
    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        result = execute_connector(definition=_definition(), action="x", args={}, pg=pg)
    assert result["error"] == "connector_not_found"


def test_missing_module_path():
    pg = _mock_pg(_connector_row(module_path=""))
    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        result = execute_connector(definition=_definition(), action="x", args={}, pg=pg)
    assert result["error"] == "connector_module_path_missing"


# ── Module path validation ───────────────────────────────────────────


def test_module_path_rejected():
    pg = _mock_pg(_connector_row(module_path="os.path"))
    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        result = execute_connector(definition=_definition(), action="x", args={}, pg=pg)
    assert result["error"] == "connector_module_path_rejected"


def test_module_path_allowed():
    fake_mod = _make_module("artifacts.connectors.test.client", FakeClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value=None):
                with patch.object(connector_executor_mod, "_record_outcome"):
                    result = execute_connector(definition=_definition(), action="sync_method", args={}, pg=pg)
    assert result["status"] == "succeeded"


# ── Circuit breaker ──────────────────────────────────────────────────


def test_circuit_breaker_blocks():
    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=False):
        result = execute_connector(definition=_definition(), action="x", args={}, pg=MagicMock())
    assert result["error"] == "connector_circuit_open"


# ── Credential pre-flight ────────────────────────────────────────────


def test_credential_missing_blocks():
    fake_mod = _make_module("artifacts.connectors.test.client", FakeClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    defn = _definition(auth_shape={"kind": "env_var", "env_var": "MISSING_KEY"})

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value=None):
                result = execute_connector(definition=defn, action="list_items", args={}, pg=pg)
    assert result["error"] == "connector_credential_missing"


def test_credential_none_ok_when_no_auth():
    fake_mod = _make_module("artifacts.connectors.test.client", FakeClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    defn = _definition(auth_shape={"kind": "none"})

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value=None):
                with patch.object(connector_executor_mod, "_record_outcome"):
                    result = execute_connector(definition=defn, action="sync_method", args={}, pg=pg)
    assert result["status"] == "succeeded"


# ── Timeout ──────────────────────────────────────────────────────────


def test_async_timeout():
    async def slow_method():
        await asyncio.sleep(10)

    with pytest.raises(asyncio.TimeoutError):
        _call_async(slow_method, {}, timeout_s=0.1)


def test_sync_timeout():
    import time
    def slow_method():
        time.sleep(10)

    import concurrent.futures
    with pytest.raises(concurrent.futures.TimeoutError):
        _call_sync(slow_method, {}, timeout_s=0.1)


# ── Successful calls ─────────────────────────────────────────────────


def test_successful_async_call():
    fake_mod = _make_module("artifacts.connectors.test.client", FakeClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value="sk-test"):
                with patch.object(connector_executor_mod, "_record_outcome"):
                    result = execute_connector(
                        definition=_definition(), action="list_items", args={"limit": 5}, pg=pg,
                    )
    assert result["status"] == "succeeded"
    assert result["data"] == {"items": [], "limit": 5}


def test_successful_sync_call():
    fake_mod = _make_module("artifacts.connectors.test.client", FakeClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value=None):
                with patch.object(connector_executor_mod, "_record_outcome"):
                    result = execute_connector(
                        definition=_definition(auth_shape={"kind": "none"}),
                        action="sync_method", args={}, pg=pg,
                    )
    assert result["status"] == "succeeded"
    assert result["data"] == {"result": "sync_result"}


# ── Error recording ──────────────────────────────────────────────────


def test_call_failure_records_outcome():
    class FailingClient:
        def __init__(self, api_key: str = ""):
            pass
        async def fail_action(self):
            raise RuntimeError("boom")
    FailingClient.__name__ = "FailingClient"

    fake_mod = _make_module("artifacts.connectors.test.client", FailingClient)
    pg = MagicMock()
    pg.execute.side_effect = [[_connector_row()]]

    with patch.object(connector_executor_mod, "_circuit_breaker_allows", return_value=True):
        with patch.object(connector_executor_mod.importlib, "import_module", return_value=fake_mod):
            with patch.object(integration_manifest_mod, "resolve_token", return_value=None):
                with patch.object(connector_executor_mod, "_record_outcome") as mock_outcome:
                    result = execute_connector(
                        definition=_definition(auth_shape={"kind": "none"}),
                        action="fail_action", args={}, pg=pg,
                    )
    assert result["status"] == "failed"
    mock_outcome.assert_called_once_with(pg, "test", succeeded=False, error_code="connector_call_failed")
