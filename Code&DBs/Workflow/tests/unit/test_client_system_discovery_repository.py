from __future__ import annotations

import json

from runtime.client_system_discovery.models import DiscoveryGap, SystemCensusRecord, connector_record_from_payload
from storage.postgres import client_system_discovery_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, *args):
        self.calls.append((sql, args))
        return []

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _record() -> SystemCensusRecord:
    connector = connector_record_from_payload(
        census_id="census.1",
        payload={
            "integration_id": "hubspot",
            "connector_slug": "hubspot",
            "display_name": "HubSpot",
            "provider": "hubspot",
            "auth_kind": "oauth2",
            "auth_status": "connected",
            "capabilities": [{"action": "list_contacts"}],
            "surfaces": [
                {
                    "surface_kind": "api",
                    "surface_ref": "contacts.list",
                    "operation_name": "list_contacts",
                    "http_method": "GET",
                    "path_template": "/crm/v3/objects/contacts",
                    "evidence": {"source": "fixture"},
                }
            ],
            "credential_health_refs": [
                {
                    "credential_ref": "credential://hubspot/access",
                    "status": "valid",
                    "checked_at": "2026-04-30T00:00:00Z",
                    "detail": "Credential resolved via oauth2"
                }
            ],
        },
    )
    return SystemCensusRecord(
        census_id="census.1",
        tenant_ref="tenant.demo",
        workspace_ref="workspace.demo",
        system_slug="crm",
        system_name="CRM",
        discovery_source="fixture",
        captured_at="2026-04-30T00:00:00Z",
        connectors=[connector],
    )


def _record_with_integration() -> SystemCensusRecord:
    return SystemCensusRecord(
        census_id="census.2",
        tenant_ref="tenant.demo",
        workspace_ref="workspace.demo",
        system_slug="billing",
        system_name="Billing",
        discovery_source="fixture",
        captured_at="2026-04-30T00:00:00Z",
        integrations=[],
    )


def test_upsert_system_census_uses_typed_table() -> None:
    conn = _RecordingConn()

    repo.upsert_system_census(conn, _record())

    assert "INSERT INTO client_system_census" in conn.calls[0][0]
    assert conn.calls[0][1][0] == "census.1"


def test_replace_connector_census_writes_connector_and_child_rows() -> None:
    conn = _RecordingConn()

    repo.replace_connector_census(conn, _record())

    assert len(conn.calls) == 4
    assert len(conn.batch_calls) == 3
    assert "INSERT INTO client_connector_census" in conn.batch_calls[0][0]
    assert "INSERT INTO client_connector_surface_evidence" in conn.batch_calls[1][0]
    assert "INSERT INTO client_connector_credential_health_refs" in conn.batch_calls[2][0]


def test_emit_discovery_gap_writes_authority_event() -> None:
    conn = _RecordingConn()
    gap = DiscoveryGap(
        gap_kind="credential_health_unknown",
        reason_code="credential.health.unknown",
        source_ref="census:census.1",
        detail="Credential check has not run",
    )

    gap_id = repo.emit_discovery_gap(conn, gap)

    sql, args = conn.calls[0]
    assert "INSERT INTO authority_events" in sql
    assert args[1] == gap_id
    payload = json.loads(args[3])
    assert payload["reason_code"] == "credential.health.unknown"
