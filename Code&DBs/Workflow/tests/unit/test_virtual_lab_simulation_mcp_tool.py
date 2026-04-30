from __future__ import annotations

from surfaces.mcp.tools import virtual_lab_simulation


def test_virtual_lab_simulation_run_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_simulation,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_simulation_run"}

    monkeypatch.setattr(virtual_lab_simulation, "execute_operation_from_env", _execute)

    result = virtual_lab_simulation.tool_praxis_virtual_lab_simulation_run(
        {
            "scenario": {"scenario_id": "scenario.demo"},
            "run_id": None,
            "source_ref": "phase_07_test",
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_simulation_run"}
    assert captured["operation_name"] == "virtual_lab_simulation_run"
    assert captured["payload"] == {
        "scenario": {"scenario_id": "scenario.demo"},
        "source_ref": "phase_07_test",
    }


def test_virtual_lab_simulation_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_simulation,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_simulation_read"}

    monkeypatch.setattr(virtual_lab_simulation, "execute_operation_from_env", _execute)

    result = virtual_lab_simulation.tool_praxis_virtual_lab_simulation_read(
        {
            "action": "describe_run",
            "run_id": "virtual_lab_simulation_run.demo",
            "status": None,
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_simulation_read"}
    assert captured["operation_name"] == "virtual_lab_simulation_read"
    assert captured["payload"] == {
        "action": "describe_run",
        "run_id": "virtual_lab_simulation_run.demo",
    }
