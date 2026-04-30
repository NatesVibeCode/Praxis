"""CQRS commands for integration action and automation contract authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.integrations.action_contracts import (
    automation_rule_snapshot_from_dict,
    integration_action_contract_from_dict,
)
from storage.postgres.integration_action_contract_repository import (
    persist_integration_action_contract_inventory,
)


class RecordIntegrationActionContractCommand(BaseModel):
    """Record integration action contracts and automation snapshots."""

    contracts: list[dict[str, Any]] = Field(default_factory=list)
    automation_snapshots: list[dict[str, Any]] = Field(default_factory=list)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("contracts", "automation_snapshots", mode="before")
    @classmethod
    def _normalize_record_list(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, dict):
            return [dict(value)]
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("contract record groups must be JSON objects or lists of JSON objects")
        return [dict(item) for item in value]

    @field_validator("observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @model_validator(mode="after")
    def _validate_non_empty(self) -> "RecordIntegrationActionContractCommand":
        if not self.contracts and not self.automation_snapshots:
            raise ValueError("at least one contract or automation snapshot is required")
        return self


def handle_integration_action_contract_record(
    command: RecordIntegrationActionContractCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    contracts = [_prepare_contract(item) for item in command.contracts]
    automation_snapshots = [_prepare_automation_snapshot(item) for item in command.automation_snapshots]
    persisted = persist_integration_action_contract_inventory(
        conn,
        contracts=contracts,
        automation_snapshots=automation_snapshots,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "contract_count": len(contracts),
        "automation_snapshot_count": len(automation_snapshots),
        "contract_typed_gap_count": persisted["contract_typed_gap_count"],
        "automation_snapshot_gap_count": persisted["automation_snapshot_gap_count"],
        "automation_action_link_count": persisted["automation_action_link_count"],
        "action_contract_ids": [item["action_contract_id"] for item in contracts],
        "automation_rule_ids": [item["rule_id"] for item in automation_snapshots],
    }
    return {
        "ok": True,
        "operation": "integration_action_contract_record",
        "contracts": contracts,
        "automation_snapshots": automation_snapshots,
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _prepare_contract(payload: dict[str, Any]) -> dict[str, Any]:
    source = dict(payload)
    contract = integration_action_contract_from_dict(source)
    prepared = contract.as_dict()
    contract_hash = contract.contract_hash()
    prepared["action_contract_id"] = str(source.get("action_contract_id") or prepared["action_id"])
    prepared["revision_id"] = str(source.get("revision_id") or f"rev.integration_action_contract.{contract_hash[:16]}")
    prepared["revision_no"] = int(source.get("revision_no") or 1)
    if source.get("parent_revision_id"):
        prepared["parent_revision_id"] = str(source["parent_revision_id"])
    prepared["contract_hash"] = contract_hash
    prepared["validation_gaps"] = [item.as_dict() for item in contract.validation_gaps()]
    return prepared


def _prepare_automation_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    source = dict(payload)
    snapshot = automation_rule_snapshot_from_dict(source)
    prepared = snapshot.as_dict()
    snapshot_hash = snapshot.snapshot_hash()
    prepared["snapshot_id"] = str(source.get("snapshot_id") or f"snapshot.integration_automation_rule.{snapshot_hash[:16]}")
    prepared["snapshot_hash"] = snapshot_hash
    prepared["validation_gaps"] = [item.as_dict() for item in snapshot.validation_gaps()]
    return prepared


__all__ = [
    "RecordIntegrationActionContractCommand",
    "handle_integration_action_contract_record",
]
