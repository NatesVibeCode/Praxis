from __future__ import annotations

from surfaces.mcp.tools import integration_action_contracts


def test_integration_action_contract_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        integration_action_contracts,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "integration_action_contract_record"}

    monkeypatch.setattr(integration_action_contracts, "execute_operation_from_env", _execute)

    result = integration_action_contracts.tool_praxis_integration_action_contract_record(
        {
            "contracts": [{"action_id": "integration_action.hubspot.create_contact"}],
            "automation_snapshots": [],
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "integration_action_contract_record"}
    assert captured["operation_name"] == "integration_action_contract_record"
    assert captured["payload"] == {
        "contracts": [{"action_id": "integration_action.hubspot.create_contact"}],
        "automation_snapshots": [],
    }


def test_integration_action_contract_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        integration_action_contracts,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "integration_action_contract_read"}

    monkeypatch.setattr(integration_action_contracts, "execute_operation_from_env", _execute)

    result = integration_action_contracts.tool_praxis_integration_action_contract_read(
        {
            "action": "describe_contract",
            "action_contract_id": "integration_action.hubspot.create_contact",
            "status": None,
        }
    )

    assert result == {"ok": True, "operation": "integration_action_contract_read"}
    assert captured["operation_name"] == "integration_action_contract_read"
    assert captured["payload"] == {
        "action": "describe_contract",
        "action_contract_id": "integration_action.hubspot.create_contact",
    }
