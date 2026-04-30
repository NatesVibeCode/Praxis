from __future__ import annotations

from surfaces.mcp.tools import virtual_lab_sandbox_promotion


def test_virtual_lab_sandbox_promotion_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_sandbox_promotion,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_sandbox_promotion_record"}

    monkeypatch.setattr(virtual_lab_sandbox_promotion, "execute_operation_from_env", _execute)

    result = virtual_lab_sandbox_promotion.tool_praxis_virtual_lab_sandbox_promotion_record(
        {
            "manifest": {"manifest_id": "manifest.phase8"},
            "candidate_records": [],
            "source_ref": "phase_08_test",
            "observed_by_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_sandbox_promotion_record"}
    assert captured["operation_name"] == "virtual_lab_sandbox_promotion_record"
    assert captured["payload"] == {
        "manifest": {"manifest_id": "manifest.phase8"},
        "candidate_records": [],
        "source_ref": "phase_08_test",
    }


def test_virtual_lab_sandbox_promotion_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_lab_sandbox_promotion,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "virtual_lab_sandbox_promotion_read"}

    monkeypatch.setattr(virtual_lab_sandbox_promotion, "execute_operation_from_env", _execute)

    result = virtual_lab_sandbox_promotion.tool_praxis_virtual_lab_sandbox_promotion_read(
        {
            "action": "describe_record",
            "promotion_record_id": "sandbox_promotion_record.demo",
            "candidate_id": None,
        }
    )

    assert result == {"ok": True, "operation": "virtual_lab_sandbox_promotion_read"}
    assert captured["operation_name"] == "virtual_lab_sandbox_promotion_read"
    assert captured["payload"] == {
        "action": "describe_record",
        "promotion_record_id": "sandbox_promotion_record.demo",
    }
