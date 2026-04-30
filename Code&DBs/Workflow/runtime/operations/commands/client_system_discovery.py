"""CQRS commands for Client System Discovery authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.client_system_discovery.models import (
    DiscoveryGap,
    system_record_from_payload,
)
from storage.postgres.client_system_discovery_repository import (
    load_system_census,
    replace_connector_census,
    upsert_system_census,
)


class RecordClientSystemCensusCommand(BaseModel):
    """Persist one client-system census and its connector evidence."""

    tenant_ref: str
    workspace_ref: str
    system_slug: str
    system_name: str | None = None
    discovery_source: str | None = "repo_inspection"
    captured_at: str
    status: str | None = "captured"
    census_id: str | None = None
    category: str | None = "unknown"
    vendor: str | None = None
    deployment_model: str | None = "unknown"
    environment: str | None = "unknown"
    business_owner: str | None = None
    technical_owner: str | None = None
    criticality: str | None = "unknown"
    declared_purpose: str | None = None
    discovery_status: str | None = None
    last_verified_at: str | None = None
    integrations: list[dict[str, Any]] = Field(default_factory=list)
    connectors: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator(
        "tenant_ref",
        "workspace_ref",
        "system_slug",
        "captured_at",
        mode="before",
    )
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("tenant_ref, workspace_ref, system_slug, and captured_at are required")
        return value.strip()

    @field_validator(
        "system_name",
        "discovery_source",
        "status",
        "census_id",
        "category",
        "vendor",
        "deployment_model",
        "environment",
        "business_owner",
        "technical_owner",
        "criticality",
        "declared_purpose",
        "discovery_status",
        "last_verified_at",
        "observed_by_ref",
        "source_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional text fields must be non-empty strings when provided")
        return value.strip()

    @field_validator("integrations", "connectors", mode="before")
    @classmethod
    def _normalize_object_list(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("integrations and connectors must be lists")
        return [dict(item) for item in value if isinstance(item, dict)]

    @field_validator("metadata", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("metadata must be a JSON object")

    @model_validator(mode="after")
    def _apply_defaults(self) -> "RecordClientSystemCensusCommand":
        if self.system_name is None:
            self.system_name = self.system_slug
        if self.discovery_source is None:
            self.discovery_source = "repo_inspection"
        if self.status is None:
            self.status = "captured"
        if self.category is None:
            self.category = "unknown"
        if self.deployment_model is None:
            self.deployment_model = "unknown"
        if self.environment is None:
            self.environment = "unknown"
        if self.criticality is None:
            self.criticality = "unknown"
        return self


class RecordClientSystemDiscoveryGapCommand(BaseModel):
    """Record one typed client-system discovery gap through the gateway event ledger."""

    gap_kind: str | None = "missing_connector"
    reason_code: str
    source_ref: str
    detail: str
    severity: str | None = "medium"
    is_blocker: bool = False
    expected_evidence: str | None = None
    current_evidence: str | None = None
    next_action: str | None = None
    owner: str | None = None
    opened_at: str | None = None
    resolved_at: str | None = None
    legal_repair_actions: list[str] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)
    gap_id: str | None = None

    @field_validator("reason_code", "source_ref", "detail", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("reason_code, source_ref, and detail are required")
        return value.strip()

    @field_validator(
        "gap_kind",
        "severity",
        "expected_evidence",
        "current_evidence",
        "next_action",
        "owner",
        "opened_at",
        "resolved_at",
        "gap_id",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional gap fields must be non-empty strings when provided")
        return value.strip()

    @field_validator("legal_repair_actions", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("legal_repair_actions must be a list")
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("context", mode="before")
    @classmethod
    def _normalize_context(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("context must be a JSON object")

    @model_validator(mode="after")
    def _apply_defaults(self) -> "RecordClientSystemDiscoveryGapCommand":
        if self.gap_kind is None:
            self.gap_kind = "missing_connector"
        if self.severity is None:
            self.severity = "medium"
        return self


def handle_client_system_discovery_census_record(
    command: RecordClientSystemCensusCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Persist one census record and return receipt/event-ready proof."""

    conn = subsystems.get_pg_conn()
    payload = command.model_dump(exclude_none=True)
    record = system_record_from_payload(payload)
    upsert_system_census(conn, record)
    replace_connector_census(conn, record)
    persisted = load_system_census(conn, census_id=record.census_id) or record.as_dict()
    event_payload = {
        "census_id": record.census_id,
        "tenant_ref": record.tenant_ref,
        "workspace_ref": record.workspace_ref,
        "system_slug": record.system_slug,
        "system_name": record.system_name,
        "discovery_status": record.discovery_status,
        "connector_count": len(record.connectors),
        "integration_count": len(record.integrations),
        "evidence_hash": record.evidence_hash(),
        "observed_by_ref": command.observed_by_ref,
        "source_ref": command.source_ref,
    }
    return {
        "ok": True,
        "operation": "client_system_discovery_census_record",
        "census_id": record.census_id,
        "census": record.as_dict(),
        "persisted": persisted,
        "event_payload": event_payload,
    }


def handle_client_system_discovery_gap_record(
    command: RecordClientSystemDiscoveryGapCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Return a typed gap event payload for gateway persistence."""

    _ = subsystems
    gap = DiscoveryGap(
        gap_kind=command.gap_kind,
        reason_code=command.reason_code,
        source_ref=command.source_ref,
        detail=command.detail,
        severity=command.severity,
        is_blocker=command.is_blocker,
        expected_evidence=command.expected_evidence,
        current_evidence=command.current_evidence,
        next_action=command.next_action,
        owner=command.owner,
        opened_at=command.opened_at,
        resolved_at=command.resolved_at,
        legal_repair_actions=command.legal_repair_actions,
        context=command.context,
        gap_id=command.gap_id,
    )
    event_payload = gap.as_event_payload()
    return {
        "ok": True,
        "operation": "client_system_discovery_gap_record",
        "gap_id": event_payload["gap_id"],
        "gap": event_payload,
        "event_payload": event_payload,
    }


__all__ = [
    "RecordClientSystemCensusCommand",
    "RecordClientSystemDiscoveryGapCommand",
    "handle_client_system_discovery_census_record",
    "handle_client_system_discovery_gap_record",
]
