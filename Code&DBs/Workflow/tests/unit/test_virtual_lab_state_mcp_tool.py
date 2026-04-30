from __future__ import annotations

from surfaces.mcp.tools import virtual_lab_state


def test_virtual_lab_state_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_state,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_state_record"}

    monkeypatch.setattr(virtual_lab_state, "execute_operation_from_env", _execute)

    result = virtual_lab_state.tool_praxis_virtual_lab_state_record(
        {
            "environment_revision": {"environment_id": "virtual_lab.env.acme"},
            "object_states": [],
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_state_record"}
    assert captured["operation_name"] == "virtual_lab_state_record"
    assert captured["payload"] == {
        "environment_revision": {"environment_id": "virtual_lab.env.acme"},
        "object_states": [],
    }


def test_virtual_lab_state_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_state,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_state_read"}

    monkeypatch.setattr(virtual_lab_state, "execute_operation_from_env", _execute)

    result = virtual_lab_state.tool_praxis_virtual_lab_state_read(
        {
            "action": "describe_revision",
            "environment_id": "virtual_lab.env.acme",
            "revision_id": "virtual_lab_revision.demo",
            "status": None,
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_state_read"}
    assert captured["operation_name"] == "virtual_lab_state_read"
    assert captured["payload"] == {
        "action": "describe_revision",
        "environment_id": "virtual_lab.env.acme",
        "revision_id": "virtual_lab_revision.demo",
    }
