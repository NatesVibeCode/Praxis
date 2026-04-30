"""CQRS queries for task-environment contract authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.task_environment_contract_repository import (
    list_task_environment_contracts,
    load_task_environment_contract,
)


ReadAction = Literal["list", "describe"]


class QueryTaskEnvironmentContractRead(BaseModel):
    """Read task-environment contract heads and revisions."""

    action: ReadAction = "list"
    task_ref: str | None = None
    status: str | None = None
    contract_id: str | None = None
    include_history: bool = True
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("task_ref", "status", "contract_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryTaskEnvironmentContractRead":
        if self.action == "describe" and not self.contract_id:
            raise ValueError("contract_id is required for describe")
        return self


def handle_task_environment_contract_read(
    query: QueryTaskEnvironmentContractRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe":
        contract = load_task_environment_contract(
            conn,
            contract_id=str(query.contract_id),
            include_history=query.include_history,
        )
        return {
            "ok": contract is not None,
            "operation": "task_environment_contract_read",
            "action": "describe",
            "contract_id": query.contract_id,
            "contract": contract,
            "error_code": None if contract is not None else "task_environment_contract.not_found",
        }

    items = list_task_environment_contracts(
        conn,
        task_ref=query.task_ref,
        status=query.status,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "task_environment_contract_read",
        "action": "list",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryTaskEnvironmentContractRead",
    "handle_task_environment_contract_read",
]
