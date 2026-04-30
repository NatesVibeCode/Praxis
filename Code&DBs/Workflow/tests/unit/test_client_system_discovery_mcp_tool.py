from __future__ import annotations

from surfaces.mcp.tools import client_system_discovery as tool


class _FakeConn:
    pass


def test_discover_delegates_to_census_record_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": operation_name, "census": {"connector_count": 1}}

    monkeypatch.setattr(tool, "execute_operation_from_env", _execute)

    result = tool.tool_praxis_client_system_discovery(
        {
            "action": "discover",
            "tenant_ref": "tenant.demo",
            "workspace_ref": "workspace.demo",
            "system_slug": "crm",
            "system_name": "CRM",
            "discovery_source": "fixture",
            "captured_at": "2026-04-30T00:00:00Z",
            "connectors": [
                {
                    "connector_slug": "slack",
                    "display_name": "Slack",
                    "provider": "slack",
                    "auth_kind": "oauth2",
                    "auth_status": "connected",
                    "capabilities": [{"action": "send_message"}],
                }
            ],
        }
    )

    assert result["ok"] is True
    assert captured["operation_name"] == "client_system_discovery_census_record"
    assert captured["payload"]["tenant_ref"] == "tenant.demo"


def test_search_delegates_to_census_read_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": operation_name, "action": "search", "count": 0, "items": []}

    monkeypatch.setattr(tool, "execute_operation_from_env", _execute)

    result = tool.tool_praxis_client_system_discovery({"action": "search", "query": "crm"})

    assert result["ok"] is True
    assert captured["operation_name"] == "client_system_discovery_census_read"
    assert captured["payload"]["query"] == "crm"


def test_record_gap_delegates_to_gap_record_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": operation_name, "gap_id": "typed_gap.client_system_discovery.1234"}

    monkeypatch.setattr(tool, "execute_operation_from_env", _execute)

    result = tool.tool_praxis_client_system_discovery(
        {
            "action": "record_gap",
            "gap_kind": "missing_connector",
            "reason_code": "connector.missing",
            "source_ref": "census:census.1",
            "detail": "Missing billing connector",
        }
    )

    assert result == {
        "ok": True,
        "operation": "client_system_discovery_gap_record",
        "gap_id": "typed_gap.client_system_discovery.1234",
    }
    assert captured["operation_name"] == "client_system_discovery_gap_record"


def test_specific_census_record_wrapper_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": operation_name}

    monkeypatch.setattr(tool, "execute_operation_from_env", _execute)

    result = tool.tool_praxis_client_system_discovery_census_record(
        {
            "tenant_ref": "tenant.demo",
            "workspace_ref": "workspace.demo",
            "system_slug": "crm",
            "captured_at": "2026-04-30T00:00:00Z",
        }
    )

    assert result["ok"] is True
    assert captured["operation_name"] == "client_system_discovery_census_record"
