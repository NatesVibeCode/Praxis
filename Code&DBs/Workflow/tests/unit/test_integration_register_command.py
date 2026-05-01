"""Unit tests for the integration_register CQRS command handler.

Verifies the input model contract + the handler upserts integration_registry
through one call and returns the event_payload shape the gateway hoists onto
the integration.registered authority event.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime.operations.commands.integration_register import (
    IntegrationCapabilityInput,
    IntegrationRegisterCommand,
    handle_integration_register,
)


# ---------------------------------------------------------------------------
# Input model validation
# ---------------------------------------------------------------------------


def _capability(action: str, description: str = "") -> IntegrationCapabilityInput:
    return IntegrationCapabilityInput(action=action, description=description)


def test_input_model_validates_minimal_payload() -> None:
    cmd = IntegrationRegisterCommand(
        id="praxis_data",
        name="Praxis Data Plane",
        capabilities=[_capability("dedupe", "Deduplicate records.")],
    )
    assert cmd.id == "praxis_data"
    assert cmd.auth_status == "connected"
    assert cmd.manifest_source == "api"
    assert cmd.catalog_dispatch is False


def test_input_model_rejects_empty_id() -> None:
    with pytest.raises(ValueError, match="id is required"):
        IntegrationRegisterCommand(
            id="   ",
            name="x",
            capabilities=[_capability("a")],
        )


def test_input_model_rejects_empty_capabilities() -> None:
    with pytest.raises(ValueError, match="at least one action"):
        IntegrationRegisterCommand(id="xy", name="X", capabilities=[])


def test_input_model_rejects_duplicate_actions() -> None:
    with pytest.raises(ValueError, match="duplicate capability action"):
        IntegrationRegisterCommand(
            id="xy",
            name="X",
            capabilities=[_capability("dedupe"), _capability("dedupe")],
        )


def test_input_model_rejects_invalid_auth_status() -> None:
    with pytest.raises(ValueError, match="auth_status must be"):
        IntegrationRegisterCommand(
            id="xy",
            name="X",
            capabilities=[_capability("a")],
            auth_status="banana",
        )


def test_input_model_rejects_invalid_manifest_source() -> None:
    with pytest.raises(ValueError, match="manifest_source must be"):
        IntegrationRegisterCommand(
            id="xy",
            name="X",
            capabilities=[_capability("a")],
            manifest_source="banana",
        )


# ---------------------------------------------------------------------------
# Handler — upsert + event payload shape
# ---------------------------------------------------------------------------


class _ExecutingConn:
    """Records every conn.execute call so the test asserts the SQL shape."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args) -> None:
        self.calls.append((sql, args))


class _Subsystems:
    def __init__(self, conn: _ExecutingConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _ExecutingConn:
        return self._conn


def test_handler_upserts_through_gateway_friendly_helper() -> None:
    conn = _ExecutingConn()
    subsystems = _Subsystems(conn)
    cmd = IntegrationRegisterCommand(
        id="praxis_data",
        name="Praxis Data Plane",
        description="Deterministic record-level data operations.",
        provider="mcp",
        capabilities=[
            _capability("dedupe", "Deduplicate by key."),
            _capability("validate", "Validate against schema."),
        ],
        catalog_dispatch=True,
        manifest_source="mcp",
        mcp_server_id="praxis-workflow-mcp",
    )
    result = handle_integration_register(cmd, subsystems)

    assert len(conn.calls) == 1, "handler must do exactly one upsert"
    sql, args = conn.calls[0]
    assert "INSERT INTO integration_registry" in sql
    assert "ON CONFLICT (id) DO UPDATE" in sql
    # Positional args: id, name, description, provider, capabilities_json,
    # auth_status, icon, mcp_server_id, catalog_dispatch, manifest_source,
    # connector_slug, auth_shape_json
    assert args[0] == "praxis_data"
    assert args[1] == "Praxis Data Plane"
    assert args[3] == "mcp"
    capabilities_payload = json.loads(args[4])
    assert [c["action"] for c in capabilities_payload] == ["dedupe", "validate"]
    assert args[5] == "connected"
    assert args[7] == "praxis-workflow-mcp"
    assert args[8] is True  # catalog_dispatch
    assert args[9] == "mcp"

    assert result["ok"] is True
    assert result["integration_id"] == "praxis_data"
    assert result["catalog_dispatch"] is True
    assert result["actions"] == ["dedupe", "validate"]


def test_handler_emits_event_payload_with_canonical_fields() -> None:
    conn = _ExecutingConn()
    cmd = IntegrationRegisterCommand(
        id="praxis_data",
        name="Praxis Data Plane",
        capabilities=[_capability("dedupe")],
        catalog_dispatch=True,
        manifest_source="mcp",
        decision_ref="decision.architecture_policy.data_plane.deterministic_over_llm",
    )
    result = handle_integration_register(cmd, _Subsystems(conn))
    payload = result["event_payload"]
    # The payload must carry every field the migration's
    # event_contract.metadata.expected_payload_fields lists, so the
    # gateway-emitted authority_events row matches the contract.
    expected_keys = {
        "integration_id",
        "name",
        "provider",
        "auth_status",
        "catalog_dispatch",
        "manifest_source",
        "mcp_server_id",
        "actions",
        "action_count",
        "decision_ref",
    }
    assert expected_keys.issubset(payload.keys())
    assert payload["action_count"] == 1
    assert payload["decision_ref"].startswith("decision.architecture_policy")


def test_handler_default_icon_when_unset() -> None:
    conn = _ExecutingConn()
    cmd = IntegrationRegisterCommand(
        id="xy",
        name="X",
        capabilities=[_capability("a")],
    )
    handle_integration_register(cmd, _Subsystems(conn))
    icon_arg = conn.calls[0][1][6]
    assert icon_arg == "plug"
