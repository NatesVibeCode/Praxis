"""Tests for the connector registrar."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.integrations.connector_registrar import (
    _infer_auth_shape,
    _introspect_capabilities,
    register_built_connector,
)


# ── Helpers ──────────────────────────────────────────────────────────


class StripeClient:
    """A fake generated connector client."""

    def __init__(self, api_key: str = "", base_url: str = ""):
        self.api_key = api_key

    async def list_payments(self, limit: int = 10) -> dict:
        """List recent payments."""
        return {}

    async def get_payment(self, payment_id: str) -> dict:
        """Retrieve a single payment by ID."""
        return {}

    async def create_refund(self, payment_id: str, amount: int | None = None) -> dict:
        """Create a refund for a payment."""
        return {}

    def _internal_method(self):
        pass

    def __repr__(self):
        return "StripeClient"


class NoAuthClient:
    """A client with no auth parameters."""

    def __init__(self):
        pass

    async def health_check(self) -> dict:
        """Check API health."""
        return {}


def _make_module(name: str, *classes: type) -> types.ModuleType:
    mod = types.ModuleType(name)
    for cls in classes:
        cls.__module__ = name
        setattr(mod, cls.__name__, cls)
    return mod


# ── _introspect_capabilities ─────────────────────────────────────────


def test_introspect_capabilities():
    fake_mod = _make_module("artifacts.connectors.stripe.client", StripeClient)

    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        return_value=fake_mod,
    ):
        caps = _introspect_capabilities("artifacts.connectors.stripe.client")

    action_names = [c["action"] for c in caps]
    assert "list_payments" in action_names
    assert "get_payment" in action_names
    assert "create_refund" in action_names
    # Private and dunder methods excluded
    assert "_internal_method" not in action_names
    assert "__repr__" not in action_names


def test_introspect_capabilities_extracts_docstrings():
    fake_mod = _make_module("artifacts.connectors.stripe.client", StripeClient)

    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        return_value=fake_mod,
    ):
        caps = _introspect_capabilities("artifacts.connectors.stripe.client")

    list_cap = next(c for c in caps if c["action"] == "list_payments")
    assert list_cap["description"] == "List recent payments."


def test_introspect_capabilities_import_failure():
    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        side_effect=ImportError("no module"),
    ):
        caps = _introspect_capabilities("nonexistent.module")
    assert caps == []


def test_introspect_capabilities_no_classes():
    empty_mod = _make_module("empty_mod")
    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        return_value=empty_mod,
    ):
        caps = _introspect_capabilities("empty_mod")
    assert caps == []


# ── _infer_auth_shape ────────────────────────────────────────────────


def test_infer_auth_shape_api_key():
    fake_mod = _make_module("artifacts.connectors.stripe.client", StripeClient)

    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        return_value=fake_mod,
    ):
        shape = _infer_auth_shape("artifacts.connectors.stripe.client", "stripe")

    assert shape["kind"] == "env_var"
    assert shape["env_var"] == "STRIPE_API_KEY"


def test_infer_auth_shape_no_auth():
    fake_mod = _make_module("artifacts.connectors.noauth.client", NoAuthClient)

    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        return_value=fake_mod,
    ):
        shape = _infer_auth_shape("artifacts.connectors.noauth.client", "noauth")

    assert shape["kind"] == "none"


def test_infer_auth_shape_import_failure():
    with patch(
        "runtime.integrations.connector_registrar.importlib.import_module",
        side_effect=ImportError("no module"),
    ):
        shape = _infer_auth_shape("nonexistent.module", "test")

    # Falls back to env_var convention
    assert shape["kind"] == "env_var"
    assert shape["env_var"] == "TEST_API_KEY"


# ── register_built_connector ────────────────────────────────────────


def test_register_no_client_file():
    pg = MagicMock()

    with patch(
        "runtime.integrations.connector_registrar._CONNECTORS_DIR",
        Path("/nonexistent/path"),
    ):
        result = register_built_connector("stripe", "Stripe", pg)

    assert "error" in result
    assert "No client.py" in result["error"]


def test_register_success(tmp_path):
    # Create a fake client.py
    connector_dir = tmp_path / "stripe"
    connector_dir.mkdir()
    (connector_dir / "client.py").write_text("class StripeClient:\n    pass\n")

    fake_mod = _make_module("artifacts.connectors.stripe.client", StripeClient)
    pg = MagicMock()
    pg.execute.return_value = [{"connector_id": "conn_test"}]

    with patch("runtime.integrations.connector_registrar._CONNECTORS_DIR", tmp_path):
        with patch(
            "runtime.integrations.connector_registrar.importlib.import_module",
            return_value=fake_mod,
        ):
            result = register_built_connector("stripe", "Stripe", pg)

    assert result.get("registered") is True
    assert result["slug"] == "stripe"
    assert len(result["capabilities"]) > 0

    # Verify both registries were written
    assert pg.execute.call_count == 2
    calls = [str(c) for c in pg.execute.call_args_list]
    assert any("connector_registry" in c for c in calls)
    assert any("integration_registry" in c for c in calls)
