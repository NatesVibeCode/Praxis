from __future__ import annotations

from surfaces.mcp.tools import catalog


def test_authority_domain_forge_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        catalog,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "view": "authority_domain_forge"}

    monkeypatch.setattr(catalog, "execute_operation_from_env", _execute)

    result = catalog.tool_praxis_authority_domain_forge(
        {
            "authority_domain_ref": "authority.object_truth",
            "decision_ref": "decision.object_truth",
            "current_projection_ref": None,
        }
    )

    assert result == {"ok": True, "view": "authority_domain_forge"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "authority_domain_forge"
    assert captured["payload"] == {
        "authority_domain_ref": "authority.object_truth",
        "decision_ref": "decision.object_truth",
    }


def test_register_authority_domain_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        catalog,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "action": "register"}

    monkeypatch.setattr(catalog, "execute_operation_from_env", _execute)

    result = catalog.tool_praxis_register_authority_domain(
        {
            "authority_domain_ref": "authority.object_truth",
            "decision_ref": "decision.object_truth",
            "enabled": True,
        }
    )

    assert result == {"ok": True, "action": "register"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "authority_domain_register"
    assert captured["payload"] == {
        "authority_domain_ref": "authority.object_truth",
        "decision_ref": "decision.object_truth",
        "enabled": True,
    }
