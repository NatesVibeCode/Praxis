from __future__ import annotations

from surfaces.mcp.tools import task_environment_contracts


def test_task_environment_contract_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        task_environment_contracts,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "task_environment_contract_record"}

    monkeypatch.setattr(task_environment_contracts, "execute_operation_from_env", _execute)

    result = task_environment_contracts.tool_praxis_task_environment_contract_record(
        {
            "contract": {"contract_id": "task_contract.account_sync.1"},
            "evaluation_result": {"ok": True, "status": "valid"},
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "task_environment_contract_record"}
    assert captured["operation_name"] == "task_environment_contract_record"
    assert captured["payload"] == {
        "contract": {"contract_id": "task_contract.account_sync.1"},
        "evaluation_result": {"ok": True, "status": "valid"},
    }


def test_task_environment_contract_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        task_environment_contracts,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "task_environment_contract_read"}

    monkeypatch.setattr(task_environment_contracts, "execute_operation_from_env", _execute)

    result = task_environment_contracts.tool_praxis_task_environment_contract_read(
        {
            "action": "describe",
            "contract_id": "task_contract.account_sync.1",
            "task_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "task_environment_contract_read"}
    assert captured["operation_name"] == "task_environment_contract_read"
    assert captured["payload"] == {
        "action": "describe",
        "contract_id": "task_contract.account_sync.1",
    }
