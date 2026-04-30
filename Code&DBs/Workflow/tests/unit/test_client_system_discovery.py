from __future__ import annotations

import pytest

from runtime.client_system_discovery.models import (
    ClientSystemDiscoveryError,
    connector_record_from_manifest,
    connector_record_from_payload,
    stable_digest,
    system_record_from_payload,
    validate_system_census,
)
from runtime.integration_manifest import ActionSpec, AuthShape, IntegrationManifest


def test_manifest_connector_becomes_capability_api_and_credential_evidence() -> None:
    manifest = IntegrationManifest(
        id="hubspot",
        name="HubSpot",
        description="CRM connector",
        provider="hubspot",
        icon="crm",
        auth_shape=AuthShape(
            kind="oauth2",
            credential_ref="credential://hubspot/access",
            env_var="HUBSPOT_TOKEN",
            scopes=("crm.objects.contacts.read",),
        ),
        capabilities=(
            ActionSpec(
                action="list_contacts",
                description="List contacts",
                method="GET",
                path="https://api.hubapi.com/crm/v3/objects/contacts",
            ),
            ActionSpec(
                action="create_contact",
                description="Create contact",
                method="POST",
                path="https://api.hubapi.com/crm/v3/objects/contacts",
            ),
        ),
    )

    record = connector_record_from_manifest(
        census_id="census.1",
        manifest=manifest,
        credential_status="valid",
    )

    assert record.connector_slug == "hubspot"
    assert record.automation_classification == "automation_bearing"
    assert record.counts() == {"capability": 2, "object": 0, "api": 2, "event": 0}
    assert record.credential_health_refs[0].as_dict() == {
        "credential_ref": "credential://hubspot/access",
        "env_var_ref": "HUBSPOT_TOKEN",
        "status": "valid",
        "checked_at": None,
        "expires_at": None,
        "detail": None,
        "metadata": {
            "auth_kind": "oauth2",
            "scope_summary": ["crm.objects.contacts.read"],
        },
    }


def test_secret_material_is_rejected_before_census_payload_is_built() -> None:
    with pytest.raises(ClientSystemDiscoveryError, match="secret_material"):
        connector_record_from_payload(
            census_id="census.1",
            payload={
                "connector_slug": "billing",
                "display_name": "Billing",
                "provider": "billing",
                "auth_kind": "api_key",
                "auth_status": "connected",
                "credential_health_refs": [
                    {
                        "credential_ref": "credential://billing/live",
                        "status": "valid",
                        "metadata": {"api_key": "sk-live-12345678901234567890"},
                    }
                ],
            },
        )


def test_validate_system_census_returns_deterministic_summary_and_typed_gaps() -> None:
    system = system_record_from_payload(
        {
            "census_id": "census.crm",
            "tenant_ref": "tenant.demo",
            "workspace_ref": "workspace.demo",
            "system_slug": "crm",
            "system_name": "CRM",
            "environment": "prod",
            "technical_owner": "owner:client-it",
            "captured_at": "2026-04-30T00:00:00Z",
            "connectors": [
                {
                    "connector_slug": "hubspot",
                    "display_name": "HubSpot",
                    "provider": "hubspot",
                    "auth_kind": "oauth2",
                    "auth_status": "connected",
                    "capabilities": [{"action": "list_contacts"}],
                    "surfaces": [
                        {
                            "surface_kind": "object",
                            "surface_ref": "hubspot.contact",
                            "object_name": "contact",
                            "evidence": {
                                "source": "fixture",
                                "read_capability": "verified",
                                "cursor_field": "updatedAt",
                            },
                        },
                        {
                            "surface_kind": "api",
                            "surface_ref": "hubspot.contacts.list",
                            "operation_name": "list_contacts",
                            "http_method": "GET",
                            "path_template": "/crm/v3/objects/contacts",
                            "evidence": {
                                "source": "fixture",
                                "pagination_model": "cursor",
                                "rate_limit_model": "burst_and_daily",
                            },
                        },
                    ],
                    "credential_health_refs": [
                        {
                            "credential_ref": "credential://hubspot/access",
                            "status": "unknown",
                        }
                    ],
                }
            ],
        }
    )

    report = validate_system_census(system)
    payload = report.as_dict()

    assert report.ok is False
    assert payload["summary"]["counts"]["connectors"] == 1
    assert payload["summary"]["counts"]["surfaces"] == {
        "capability": 0,
        "object": 1,
        "api": 1,
        "event": 0,
    }
    assert payload["gap_count"] == 1
    assert payload["blocker_count"] == 1
    assert payload["gaps"][0]["gap_kind"] == "credential_health_unknown"
    assert payload["gaps"][0]["owner"] == "owner:client-it"
    assert payload["gaps"][0]["gap_id"] == report.gaps[0].resolved_gap_id()
    assert stable_digest(payload["summary"]) == stable_digest(report.as_dict()["summary"])


def test_system_payload_keeps_integration_edges_in_the_evidence_hash() -> None:
    left = system_record_from_payload(
        {
            "census_id": "census.crm",
            "tenant_ref": "tenant.demo",
            "workspace_ref": "workspace.demo",
            "system_slug": "crm",
            "system_name": "CRM",
            "captured_at": "2026-04-30T00:00:00Z",
            "integrations": [
                {
                    "integration_id": "hubspot-to-warehouse",
                    "source_system_id": "crm",
                    "target_system_id": "warehouse",
                    "integration_type": "etl",
                    "transport": "https",
                    "directionality": "uni",
                    "trigger_mode": "schedule",
                    "observed_status": "declared",
                }
            ],
        }
    )
    right = system_record_from_payload(left.as_dict())

    assert left.as_dict()["integration_count"] == 1
    assert left.evidence_hash() == right.evidence_hash()
