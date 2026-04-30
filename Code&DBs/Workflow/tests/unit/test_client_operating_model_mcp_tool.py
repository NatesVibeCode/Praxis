from __future__ import annotations

from surfaces.mcp.tools import client_operating_model


def test_client_operating_model_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_operating_model,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "ok": True,
            "operation": "client_operating_model_operator_view",
            "state": "healthy",
        }

    monkeypatch.setattr(client_operating_model, "execute_operation_from_env", _execute)

    result = client_operating_model.tool_praxis_client_operating_model(
        {
            "view": "system_census",
            "inputs": {"system_records": []},
            "generated_at": "2026-04-30T12:00:00Z",
            "permission_scope": None,
            "evidence_refs": ["fixture.empty_census"],
        }
    )

    assert result == {
        "ok": True,
        "operation": "client_operating_model_operator_view",
        "state": "healthy",
    }
    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"}
    assert captured["operation_name"] == "client_operating_model_operator_view"
    assert captured["payload"] == {
        "view": "system_census",
        "inputs": {"system_records": []},
        "generated_at": "2026-04-30T12:00:00Z",
        "evidence_refs": ["fixture.empty_census"],
    }


def test_client_operating_model_snapshot_store_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_operating_model,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "ok": True,
            "operation": "client_operating_model_operator_view_snapshot_store",
        }

    monkeypatch.setattr(client_operating_model, "execute_operation_from_env", _execute)

    result = client_operating_model.tool_praxis_client_operating_model_snapshot_store(
        {
            "operator_view": {"view_id": "system_census.demo"},
            "observed_by_ref": "operator:nate",
            "source_ref": None,
        }
    )

    assert result == {
        "ok": True,
        "operation": "client_operating_model_operator_view_snapshot_store",
    }
    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"}
    assert captured["operation_name"] == "client_operating_model_operator_view_snapshot_store"
    assert captured["payload"] == {
        "operator_view": {"view_id": "system_census.demo"},
        "observed_by_ref": "operator:nate",
    }


def test_client_operating_model_snapshots_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_operating_model,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "ok": True,
            "operation": "client_operating_model_operator_view_snapshot_read",
            "count": 1,
        }

    monkeypatch.setattr(client_operating_model, "execute_operation_from_env", _execute)

    result = client_operating_model.tool_praxis_client_operating_model_snapshots(
        {
            "view": "system_census",
            "scope_ref": "tenant.acme",
            "limit": 5,
            "snapshot_ref": None,
        }
    )

    assert result == {
        "ok": True,
        "operation": "client_operating_model_operator_view_snapshot_read",
        "count": 1,
    }
    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"}
    assert captured["operation_name"] == "client_operating_model_operator_view_snapshot_read"
    assert captured["payload"] == {
        "view": "system_census",
        "scope_ref": "tenant.acme",
        "limit": 5,
    }
