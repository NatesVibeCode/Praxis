from __future__ import annotations

from surfaces.mcp.catalog import get_tool_catalog, resolve_tool_entry


def test_credential_capture_is_discoverable_in_mcp_catalog() -> None:
    definition = get_tool_catalog()["praxis_credential_capture"]

    assert definition.cli_surface == "setup"
    assert definition.cli_tier == "stable"
    assert definition.risk_for_selector("capture") == "write"
    assert "raw API keys" in definition.cli_when_not_to_use
    assert "env_var_name" in definition.required_args
    assert "api_key" not in definition.input_properties
    assert "secret" not in definition.input_properties


def test_credential_capture_tool_dispatches_gateway(monkeypatch) -> None:
    handler, _metadata = resolve_tool_entry("praxis_credential_capture")
    calls = {}

    monkeypatch.setattr(
        "surfaces.mcp.tools.credential_capture.workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://example"},
    )

    def _fake_execute_operation_from_env(*, env, operation_name, payload):
        calls["env"] = env
        calls["operation_name"] = operation_name
        calls["payload"] = payload
        return {"ok": True, "credential_capture": {"kind": "secure_key_entry"}}

    monkeypatch.setattr(
        "surfaces.mcp.tools.credential_capture.execute_operation_from_env",
        _fake_execute_operation_from_env,
    )

    result = handler(
        {
            "action": "request",
            "env_var_name": "OPENAI_API_KEY",
            "provider_label": "OpenAI",
        }
    )

    assert result["ok"] is True
    assert calls["operation_name"] == "credential_capture_keychain"
    assert calls["payload"]["env_var_name"] == "OPENAI_API_KEY"
