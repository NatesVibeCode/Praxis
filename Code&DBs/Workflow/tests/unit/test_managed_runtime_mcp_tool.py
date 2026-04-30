from __future__ import annotations

from surfaces.mcp.tools import managed_runtime


def test_managed_runtime_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        managed_runtime,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "authority.managed_runtime.record"}

    monkeypatch.setattr(managed_runtime, "execute_operation_from_env", _execute)

    result = managed_runtime.tool_praxis_authority_managed_runtime_record(
        {
            "identity": {"run_id": "run.managed.phase10"},
            "policy": {"configured_mode": "managed"},
            "meter_events": [],
            "terminal_status": "succeeded",
            "generated_at": "2026-04-30T12:01:01Z",
            "runtime_version_ref": "runtime.managed.v1",
            "observed_by_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "authority.managed_runtime.record"}
    assert captured["operation_name"] == "authority.managed_runtime.record"
    assert captured["payload"] == {
        "identity": {"run_id": "run.managed.phase10"},
        "policy": {"configured_mode": "managed"},
        "meter_events": [],
        "terminal_status": "succeeded",
        "generated_at": "2026-04-30T12:01:01Z",
        "runtime_version_ref": "runtime.managed.v1",
    }


def test_managed_runtime_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        managed_runtime,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "authority.managed_runtime.read"}

    monkeypatch.setattr(managed_runtime, "execute_operation_from_env", _execute)

    result = managed_runtime.tool_praxis_authority_managed_runtime_read(
        {
            "action": "describe_record",
            "runtime_record_id": "managed_runtime_record.demo",
            "tenant_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "authority.managed_runtime.read"}
    assert captured["operation_name"] == "authority.managed_runtime.read"
    assert captured["payload"] == {
        "action": "describe_record",
        "runtime_record_id": "managed_runtime_record.demo",
    }
