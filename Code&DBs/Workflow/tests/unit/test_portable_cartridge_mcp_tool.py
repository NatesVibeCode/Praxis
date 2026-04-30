from __future__ import annotations

from surfaces.mcp.tools import portable_cartridge


def test_portable_cartridge_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        portable_cartridge,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "authority.portable_cartridge.record"}

    monkeypatch.setattr(portable_cartridge, "execute_operation_from_env", _execute)

    result = portable_cartridge.tool_praxis_authority_portable_cartridge_record(
        {
            "manifest": {"cartridge_id": "phase9-portable-cartridge"},
            "deployment_mode": "staged_deployment",
            "source_ref": "phase_09_test",
            "observed_by_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "authority.portable_cartridge.record"}
    assert captured["operation_name"] == "authority.portable_cartridge.record"
    assert captured["payload"] == {
        "manifest": {"cartridge_id": "phase9-portable-cartridge"},
        "deployment_mode": "staged_deployment",
        "source_ref": "phase_09_test",
    }


def test_portable_cartridge_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        portable_cartridge,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "authority.portable_cartridge.read"}

    monkeypatch.setattr(portable_cartridge, "execute_operation_from_env", _execute)

    result = portable_cartridge.tool_praxis_authority_portable_cartridge_read(
        {
            "action": "describe_record",
            "cartridge_record_id": "portable_cartridge_record.demo",
            "cartridge_id": None,
        }
    )

    assert result == {"ok": True, "operation": "authority.portable_cartridge.read"}
    assert captured["operation_name"] == "authority.portable_cartridge.read"
    assert captured["payload"] == {
        "action": "describe_record",
        "cartridge_record_id": "portable_cartridge_record.demo",
    }
