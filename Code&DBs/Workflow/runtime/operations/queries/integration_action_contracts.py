"""CQRS queries for integration action and automation contract authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.integration_action_contract_repository import (
    list_automation_rule_snapshots,
    list_integration_action_contracts,
    load_automation_rule_snapshot,
    load_integration_action_contract,
)


ReadAction = Literal[
    "list_contracts",
    "describe_contract",
    "list_automation_snapshots",
    "describe_automation_snapshot",
]


class QueryIntegrationActionContractRead(BaseModel):
    """Read integration action contracts and automation snapshots."""

    action: ReadAction = "list_contracts"
    action_contract_id: str | None = None
    automation_rule_id: str | None = None
    target_system_ref: str | None = None
    status: str | None = None
    owner_ref: str | None = None
    include_history: bool = True
    include_automation: bool = True
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator(
        "action_contract_id",
        "automation_rule_id",
        "target_system_ref",
        "status",
        "owner_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryIntegrationActionContractRead":
        if self.action == "describe_contract" and not self.action_contract_id:
            raise ValueError("action_contract_id is required for describe_contract")
        if self.action == "describe_automation_snapshot" and not self.automation_rule_id:
            raise ValueError("automation_rule_id is required for describe_automation_snapshot")
        return self


def handle_integration_action_contract_read(
    query: QueryIntegrationActionContractRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe_contract":
        contract = load_integration_action_contract(
            conn,
            action_contract_id=str(query.action_contract_id),
            include_history=query.include_history,
            include_automation=query.include_automation,
        )
        return {
            "ok": contract is not None,
            "operation": "integration_action_contract_read",
            "action": "describe_contract",
            "action_contract_id": query.action_contract_id,
            "contract": contract,
            "error_code": None if contract is not None else "integration_action_contract.not_found",
        }
    if query.action == "list_automation_snapshots":
        items = list_automation_rule_snapshots(
            conn,
            status=query.status,
            owner_ref=query.owner_ref,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "integration_action_contract_read",
            "action": "list_automation_snapshots",
            "count": len(items),
            "items": items,
        }
    if query.action == "describe_automation_snapshot":
        snapshot = load_automation_rule_snapshot(
            conn,
            automation_rule_id=str(query.automation_rule_id),
            include_history=query.include_history,
        )
        return {
            "ok": snapshot is not None,
            "operation": "integration_action_contract_read",
            "action": "describe_automation_snapshot",
            "automation_rule_id": query.automation_rule_id,
            "automation_snapshot": snapshot,
            "error_code": None if snapshot is not None else "integration_automation_snapshot.not_found",
        }

    items = list_integration_action_contracts(
        conn,
        target_system_ref=query.target_system_ref,
        status=query.status,
        owner_ref=query.owner_ref,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "integration_action_contract_read",
        "action": "list_contracts",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryIntegrationActionContractRead",
    "handle_integration_action_contract_read",
]
