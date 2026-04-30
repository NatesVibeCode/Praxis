from __future__ import annotations

from runtime.client_system_discovery.models import (
    DiscoveryGap,
    SystemCensusRecord,
    classify_automation_bearing_tool,
    connector_record_from_payload,
)


def test_classify_automation_bearing_tool_detects_mutating_capability() -> None:
    record = connector_record_from_payload(
        census_id="census.1",
        payload={
            "connector_slug": "slack",
            "display_name": "Slack",
            "provider": "slack",
            "auth_kind": "oauth2",
            "auth_status": "connected",
            "capabilities": [{"action": "send_message", "description": "Send a message"}],
        },
    )

    assert record.automation_classification == "automation_bearing"


def test_classify_automation_bearing_tool_observe_only_when_read_shapes_only() -> None:
    automation_class = classify_automation_bearing_tool(
        [{"action": "list_contacts"}],
        [],
    )

    assert automation_class == "observe_only"


def test_system_census_evidence_hash_is_deterministic() -> None:
    connector = connector_record_from_payload(
        census_id="census.1",
        payload={
            "connector_slug": "hubspot",
            "display_name": "HubSpot",
            "provider": "hubspot",
            "auth_kind": "oauth2",
            "auth_status": "connected",
            "capabilities": [{"action": "list_contacts"}],
            "surfaces": [
                {
                    "surface_kind": "object",
                    "surface_ref": "crm.contact",
                    "object_name": "contact",
                    "evidence": {"source": "fixture"},
                }
            ],
        },
    )
    record = SystemCensusRecord(
        census_id="census.1",
        tenant_ref="tenant.demo",
        workspace_ref="workspace.demo",
        system_slug="crm",
        system_name="CRM",
        discovery_source="fixture",
        captured_at="2026-04-30T00:00:00Z",
        connectors=[connector],
    )

    assert record.evidence_hash() == record.evidence_hash()
    assert record.as_dict()["connector_count"] == 1


def test_discovery_gap_id_is_deterministic_for_same_payload() -> None:
    gap = DiscoveryGap(
        gap_kind="missing_event_surface",
        reason_code="connector.event_surface.missing",
        source_ref="census:census.1",
        detail="No event surface evidence for salesforce connector",
        legal_repair_actions=["inspect_webhook_docs"],
    )

    assert gap.resolved_gap_id() == gap.resolved_gap_id()
    assert gap.as_event_payload()["missing_type"] == "client_system_discovery"
