from __future__ import annotations

from types import SimpleNamespace

from runtime.operations.commands import client_system_discovery as commands
from runtime.operations.queries import client_system_discovery as queries


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def test_census_record_command_persists_and_returns_event_payload(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(commands, "upsert_system_census", lambda conn, record: calls.append("upsert"))
    monkeypatch.setattr(commands, "replace_connector_census", lambda conn, record: calls.append("replace"))
    monkeypatch.setattr(
        commands,
        "load_system_census",
        lambda conn, census_id: {"census_id": census_id, "persisted": True},
    )

    command = commands.RecordClientSystemCensusCommand(
        tenant_ref="tenant.demo",
        workspace_ref="workspace.demo",
        system_slug="crm",
        system_name="CRM",
        discovery_source="fixture",
        captured_at="2026-04-30T00:00:00Z",
        connectors=[
            {
                "connector_slug": "hubspot",
                "display_name": "HubSpot",
                "provider": "hubspot",
                "auth_kind": "oauth2",
                "auth_status": "connected",
                "capabilities": [{"action": "list_contacts"}],
            }
        ],
    )

    result = commands.handle_client_system_discovery_census_record(command, _subsystems())

    assert calls == ["upsert", "replace"]
    assert result["ok"] is True
    assert result["operation"] == "client_system_discovery_census_record"
    assert result["event_payload"]["tenant_ref"] == "tenant.demo"
    assert result["event_payload"]["connector_count"] == 1


def test_gap_record_command_returns_gateway_event_payload() -> None:
    command = commands.RecordClientSystemDiscoveryGapCommand(
        gap_kind="credential_health_unknown",
        reason_code="credential.health.unknown",
        source_ref="census:census.1",
        detail="Credential check has not run.",
        legal_repair_actions=["run credential health probe"],
    )

    result = commands.handle_client_system_discovery_gap_record(command, _subsystems())

    assert result["ok"] is True
    assert result["operation"] == "client_system_discovery_gap_record"
    assert result["gap_id"].startswith("typed_gap.client_system_discovery.")
    assert result["event_payload"]["source_ref"] == "census:census.1"


def test_census_read_query_lists_by_tenant(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_system_census",
        lambda conn, tenant_ref=None: [{"census_id": "census.1", "tenant_ref": tenant_ref}],
    )

    query = queries.QueryClientSystemDiscoveryCensusRead(
        action="list",
        tenant_ref="tenant.demo",
    )

    result = queries.handle_client_system_discovery_census_read(query, _subsystems())

    assert result == {
        "ok": True,
        "operation": "client_system_discovery_census_read",
        "action": "list",
        "count": 1,
        "items": [{"census_id": "census.1", "tenant_ref": "tenant.demo"}],
    }


def test_census_read_query_requires_search_query() -> None:
    try:
        queries.QueryClientSystemDiscoveryCensusRead(action="search")
    except ValueError as exc:
        assert "query is required" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("search action should require query")
