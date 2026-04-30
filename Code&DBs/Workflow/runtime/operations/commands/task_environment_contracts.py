"""CQRS commands for task-environment contract authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.task_contracts import sha256_json
from storage.postgres.task_environment_contract_repository import (
    persist_task_environment_contract,
)


class RecordTaskEnvironmentContractCommand(BaseModel):
    """Record one task-environment contract head and revision."""

    contract: dict[str, Any]
    evaluation_result: dict[str, Any]
    hierarchy_nodes: list[dict[str, Any]] = Field(default_factory=list)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("contract", "evaluation_result", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("contract and evaluation_result must be JSON objects")

    @field_validator("hierarchy_nodes", mode="before")
    @classmethod
    def _normalize_node_list(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("hierarchy_nodes must be a list of JSON objects")
        return [dict(item) for item in value]

    @field_validator("observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when supplied")
        return value.strip()


def handle_task_environment_contract_record(
    command: RecordTaskEnvironmentContractCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    contract = dict(command.contract)
    if not contract.get("dependency_hash"):
        contract["dependency_hash"] = sha256_json(
            {
                "hierarchy_node_id": contract.get("hierarchy_node_id"),
                "sop_refs": contract.get("sop_refs") or [],
                "allowed_tools": contract.get("allowed_tools") or [],
                "model_policy": contract.get("model_policy"),
                "verifier_refs": contract.get("verifier_refs") or [],
                "object_truth_contract_refs": contract.get("object_truth_contract_refs") or [],
            }
        )
    if not contract.get("contract_hash"):
        hash_basis = dict(contract)
        hash_basis.pop("contract_hash", None)
        contract["contract_hash"] = sha256_json(hash_basis)
    persisted = persist_task_environment_contract(
        conn,
        contract=contract,
        evaluation_result=command.evaluation_result,
        hierarchy_nodes=command.hierarchy_nodes,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "contract_id": contract["contract_id"],
        "task_ref": contract["task_ref"],
        "revision_id": contract["revision_id"],
        "contract_hash": contract["contract_hash"],
        "dependency_hash": contract.get("dependency_hash"),
        "evaluation_status": command.evaluation_result.get("status"),
        "invalid_state_count": len(command.evaluation_result.get("invalid_states") or []),
        "warning_count": len(command.evaluation_result.get("warnings") or []),
        "hierarchy_node_count": len(command.hierarchy_nodes),
    }
    return {
        "ok": True,
        "operation": "task_environment_contract_record",
        "contract": contract,
        "evaluation_result": command.evaluation_result,
        "persisted": persisted,
        "event_payload": event_payload,
    }


__all__ = [
    "RecordTaskEnvironmentContractCommand",
    "handle_task_environment_contract_record",
]
