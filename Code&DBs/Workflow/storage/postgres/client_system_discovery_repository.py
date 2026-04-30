"""DB authority for Phase 1 client system discovery."""
from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from runtime.client_system_discovery.models import DiscoveryGap, SystemCensusRecord

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"timestamp value must be an ISO string or datetime, got {type(value).__name__}")


def upsert_system_census(conn: "SyncPostgresConnection", record: SystemCensusRecord) -> None:
    payload = record.as_dict()
    conn.execute(
        """INSERT INTO client_system_census (
               census_id, tenant_ref, workspace_ref, system_slug, system_name,
               discovery_source, captured_at, status, connector_count,
               integration_count, category, vendor, deployment_model, environment,
               business_owner, technical_owner, criticality, declared_purpose,
               discovery_status, last_verified_at, evidence_hash, metadata_json
           ) VALUES (
               $1, $2, $3, $4, $5, $6, $7::timestamptz, $8, $9, $10,
               $11, $12, $13, $14, $15, $16, $17, $18, $19,
               $20::timestamptz, $21, $22::jsonb
           )
           ON CONFLICT (census_id) DO UPDATE SET
               tenant_ref = EXCLUDED.tenant_ref,
               workspace_ref = EXCLUDED.workspace_ref,
               system_slug = EXCLUDED.system_slug,
               system_name = EXCLUDED.system_name,
               discovery_source = EXCLUDED.discovery_source,
               captured_at = EXCLUDED.captured_at,
               status = EXCLUDED.status,
               connector_count = EXCLUDED.connector_count,
               integration_count = EXCLUDED.integration_count,
               category = EXCLUDED.category,
               vendor = EXCLUDED.vendor,
               deployment_model = EXCLUDED.deployment_model,
               environment = EXCLUDED.environment,
               business_owner = EXCLUDED.business_owner,
               technical_owner = EXCLUDED.technical_owner,
               criticality = EXCLUDED.criticality,
               declared_purpose = EXCLUDED.declared_purpose,
               discovery_status = EXCLUDED.discovery_status,
               last_verified_at = EXCLUDED.last_verified_at,
               evidence_hash = EXCLUDED.evidence_hash,
               metadata_json = EXCLUDED.metadata_json,
               updated_at = now()""",
        payload["census_id"],
        payload["tenant_ref"],
        payload["workspace_ref"],
        payload["system_slug"],
        payload["system_name"],
        payload["discovery_source"],
        _timestamp(payload["captured_at"]),
        payload["status"],
        payload["connector_count"],
        payload["integration_count"],
        payload["category"],
        payload["vendor"],
        payload["deployment_model"],
        payload["environment"],
        payload["business_owner"],
        payload["technical_owner"],
        payload["criticality"],
        payload["declared_purpose"],
        payload["discovery_status"],
        _timestamp(payload["last_verified_at"]),
        payload["evidence_hash"],
        _json(payload["metadata"]),
    )


def replace_connector_census(conn: "SyncPostgresConnection", record: SystemCensusRecord) -> None:
    conn.execute("DELETE FROM client_connector_credential_health_refs WHERE census_id = $1", record.census_id)
    conn.execute("DELETE FROM client_connector_surface_evidence WHERE census_id = $1", record.census_id)
    conn.execute("DELETE FROM client_connector_census WHERE census_id = $1", record.census_id)
    conn.execute("DELETE FROM client_system_integration_evidence WHERE census_id = $1", record.census_id)

    connector_rows: list[tuple[Any, ...]] = []
    surface_rows: list[tuple[Any, ...]] = []
    credential_rows: list[tuple[Any, ...]] = []
    integration_rows: list[tuple[Any, ...]] = []
    for index, integration in enumerate(record.integrations):
        payload = integration.as_dict()
        integration_rows.append(
            (
                f"{record.census_id}.integration.{index + 1}",
                record.census_id,
                payload["integration_id"],
                payload["source_system_id"],
                payload["target_system_id"],
                payload["integration_type"],
                payload["transport"],
                payload["directionality"],
                payload["trigger_mode"],
                payload["integration_owner"],
                payload["observed_status"],
                payload["evidence_ref"],
                _json(payload["metadata"]),
            )
        )
    for connector in record.connectors:
        payload = connector.as_dict()
        connector_rows.append(
            (
                payload["connector_census_id"],
                record.census_id,
                payload["integration_id"],
                payload["connector_slug"],
                payload["display_name"],
                payload["provider"],
                payload["auth_kind"],
                payload["auth_status"],
                payload["automation_classification"],
                payload["capability_count"],
                payload["object_surface_count"],
                payload["api_surface_count"],
                payload["event_surface_count"],
                _json(payload["capabilities"]),
                _json(payload["metadata"]),
            )
        )
        for index, surface in enumerate(payload["surfaces"]):
            surface_rows.append(
                (
                    f"{payload['connector_census_id']}.surface.{index + 1}",
                    record.census_id,
                    payload["connector_census_id"],
                    surface["surface_kind"],
                    surface["surface_ref"],
                    surface.get("operation_name"),
                    surface.get("object_name"),
                    surface.get("http_method"),
                    surface.get("path_template"),
                    surface.get("event_name"),
                    _json(surface["evidence"]),
                )
            )
        for index, credential in enumerate(payload["credential_health_refs"]):
            credential_rows.append(
                (
                    f"{payload['connector_census_id']}.credential.{index + 1}",
                    record.census_id,
                    payload["connector_census_id"],
                    payload["integration_id"],
                    credential.get("credential_ref"),
                    credential.get("env_var_ref"),
                    credential.get("status"),
                    _timestamp(credential.get("checked_at")),
                    _timestamp(credential.get("expires_at")),
                    credential.get("detail"),
                    _json(credential.get("metadata") or {}),
                )
            )

    if connector_rows:
        conn.execute_many(
            """INSERT INTO client_connector_census (
                   connector_census_id, census_id, integration_id, connector_slug,
                   display_name, provider, auth_kind, auth_status,
                   automation_classification, capability_count, object_surface_count,
                   api_surface_count, event_surface_count, capabilities_json, metadata_json
               ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15::jsonb)""",
            connector_rows,
        )
    if surface_rows:
        conn.execute_many(
            """INSERT INTO client_connector_surface_evidence (
                   evidence_id, census_id, connector_census_id, surface_kind, surface_ref,
                   operation_name, object_name, http_method, path_template, event_name, evidence_json
               ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)""",
            surface_rows,
        )
    if credential_rows:
        conn.execute_many(
            """INSERT INTO client_connector_credential_health_refs (
                   credential_health_ref_id, census_id, connector_census_id, integration_id,
                   credential_ref, env_var_ref, status, checked_at, expires_at, detail, metadata_json
               ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9::timestamptz, $10, $11::jsonb)""",
            credential_rows,
        )
    if integration_rows:
        conn.execute_many(
            """INSERT INTO client_system_integration_evidence (
                   integration_evidence_id, census_id, integration_id, source_system_id,
                   target_system_id, integration_type, transport, directionality,
                   trigger_mode, integration_owner, observed_status, evidence_ref, metadata_json
               ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)""",
            integration_rows,
        )


def list_system_census(conn: "SyncPostgresConnection", *, tenant_ref: str | None = None) -> list[dict[str, Any]]:
    if tenant_ref:
        rows = conn.execute(
            """SELECT census_id, tenant_ref, workspace_ref, system_slug, system_name,
                      discovery_source, captured_at, status, connector_count,
                      integration_count, category, vendor, deployment_model, environment,
                      business_owner, technical_owner, criticality, discovery_status,
                      last_verified_at, evidence_hash
                 FROM client_system_census
                WHERE tenant_ref = $1
                ORDER BY captured_at DESC, census_id DESC""",
            tenant_ref,
        )
    else:
        rows = conn.execute(
            """SELECT census_id, tenant_ref, workspace_ref, system_slug, system_name,
                      discovery_source, captured_at, status, connector_count,
                      integration_count, category, vendor, deployment_model, environment,
                      business_owner, technical_owner, criticality, discovery_status,
                      last_verified_at, evidence_hash
                 FROM client_system_census
                ORDER BY captured_at DESC, census_id DESC"""
        )
    return [dict(row) for row in rows or []]


def search_connector_census(
    conn: "SyncPostgresConnection",
    *,
    query: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT connector_census_id, census_id, integration_id, connector_slug, display_name,
                  provider, auth_kind, auth_status, automation_classification,
                  capability_count, object_surface_count, api_surface_count, event_surface_count
             FROM client_connector_census
            WHERE connector_slug ILIKE '%' || $1 || '%'
               OR display_name ILIKE '%' || $1 || '%'
               OR provider ILIKE '%' || $1 || '%'
            ORDER BY connector_slug
            LIMIT $2""",
        query,
        limit,
    )
    return [dict(row) for row in rows or []]


def load_system_census(conn: "SyncPostgresConnection", *, census_id: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """SELECT census_id, tenant_ref, workspace_ref, system_slug, system_name,
                  discovery_source, captured_at, status, connector_count,
                  integration_count, category, vendor, deployment_model, environment,
                  business_owner, technical_owner, criticality, declared_purpose,
                  discovery_status, last_verified_at, evidence_hash, metadata_json
             FROM client_system_census
            WHERE census_id = $1
            LIMIT 1""",
        census_id,
    )
    if not rows:
        return None
    summary = dict(rows[0])
    summary["connectors"] = [
        dict(row)
        for row in conn.execute(
            """SELECT connector_census_id, integration_id, connector_slug, display_name, provider,
                      auth_kind, auth_status, automation_classification, capability_count,
                      object_surface_count, api_surface_count, event_surface_count,
                      capabilities_json, metadata_json
                 FROM client_connector_census
                WHERE census_id = $1
                ORDER BY connector_slug""",
            census_id,
        )
        or []
    ]
    summary["integrations"] = [
        dict(row)
        for row in conn.execute(
            """SELECT integration_evidence_id, integration_id, source_system_id,
                      target_system_id, integration_type, transport, directionality,
                      trigger_mode, integration_owner, observed_status, evidence_ref,
                      metadata_json
                 FROM client_system_integration_evidence
                WHERE census_id = $1
                ORDER BY integration_id, integration_evidence_id""",
            census_id,
        )
        or []
    ]
    summary["typed_gaps"] = [
        dict(row)
        for row in conn.execute(
            """SELECT COALESCE(event_payload ->> 'gap_id', aggregate_ref) AS gap_id,
                      event_type,
                      event_payload,
                      operation_ref,
                      receipt_id
                 FROM authority_events
                WHERE authority_domain_ref = 'authority.client_system_discovery'
                  AND event_type IN ('typed_gap.created', 'client_system_discovery.typed_gap_recorded')
                  AND (
                      operation_ref = $1
                      OR event_payload ->> 'source_ref' = $1
                  )
                ORDER BY emitted_at DESC""",
            f"census:{census_id}",
        )
        or []
    ]
    return summary


def emit_discovery_gap(conn: "SyncPostgresConnection", gap: DiscoveryGap) -> str:
    gap_id = gap.resolved_gap_id()
    conn.execute(
        """INSERT INTO authority_events (
               authority_domain_ref, aggregate_ref, event_type, event_payload,
               operation_ref, emitted_by
           ) VALUES ($1, $2, $3, $4::jsonb, $5, $6)""",
        "authority.client_system_discovery",
        gap_id,
        "typed_gap.created",
        _json(gap.as_event_payload()),
        gap.source_ref,
        "runtime.client_system_discovery",
    )
    return gap_id
