"""CQRS commands for durable Workflow repair queue authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


RepairQueueCommandAction = Literal["claim", "release", "complete"]
RepairScope = Literal["solution", "workflow", "job"]
RepairTerminalStatus = Literal["completed", "failed", "cancelled", "superseded"]


class WorkflowRepairQueueCommand(BaseModel):
    """Input for ``workflow_repair_queue.command``."""

    action: RepairQueueCommandAction
    repair_scope: RepairScope | None = Field(default=None)
    claimed_by: str | None = Field(default=None)
    claim_ttl_minutes: int = Field(default=30, ge=1, le=1440)
    repair_id: str | None = Field(default=None)
    queue_status: RepairTerminalStatus = Field(default="completed")
    result_ref: str | None = Field(default=None)
    repair_note: str | None = Field(default=None)

    @field_validator("action", mode="before")
    @classmethod
    def _normalize_action(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("action is required")
        return value.strip().lower()

    @field_validator(
        "repair_scope",
        "claimed_by",
        "repair_id",
        "queue_status",
        "result_ref",
        "repair_note",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional text fields must be non-empty strings when provided")
        return value.strip()

    @field_validator("claim_ttl_minutes", mode="before")
    @classmethod
    def _normalize_ttl(cls, value: object) -> int:
        if value in (None, ""):
            return 30
        if isinstance(value, bool):
            raise ValueError("claim_ttl_minutes must be an integer")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("claim_ttl_minutes must be an integer") from exc


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def _authority_payload() -> dict[str, str]:
    return {
        "authority": "workflow_repair_queue",
        "object_kind": "workflow_repair_queue",
        "storage_authority": "workflow_repair_queue",
    }


def _event_payload(command: WorkflowRepairQueueCommand, payload: dict[str, Any]) -> dict[str, Any]:
    raw_item = payload.get("item")
    item = raw_item if isinstance(raw_item, dict) else {}
    repair_id = payload.get("repair_id")
    if repair_id is None:
        repair_id = item.get("repair_id")
    queue_status = payload.get("queue_status")
    if queue_status is None:
        queue_status = item.get("queue_status")
    return {
        "action": command.action,
        "status": payload.get("status"),
        "repair_id": repair_id,
        "repair_scope": command.repair_scope or item.get("repair_scope"),
        "queue_status": queue_status,
        "claimed_by": command.claimed_by or item.get("claimed_by"),
        "result_ref": command.result_ref or payload.get("result_ref"),
    }


def handle_workflow_repair_queue_command(
    command: WorkflowRepairQueueCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Claim, release, or close repair queue work through gateway authority."""

    from runtime.workflow.repair_queue import claim_repair, complete_repair, release_repair

    conn = _pg_conn(subsystems)
    try:
        if command.action == "claim":
            payload = claim_repair(
                conn,
                claimed_by=command.claimed_by or "workflow_repair_queue.command",
                repair_scope=command.repair_scope,
                claim_ttl_minutes=command.claim_ttl_minutes,
            )
        elif command.action == "release":
            if not command.repair_id:
                raise ValueError("repair_id is required for release")
            payload = release_repair(
                conn,
                repair_id=command.repair_id,
                repair_note=command.repair_note,
            )
        elif command.action == "complete":
            if not command.repair_id:
                raise ValueError("repair_id is required for complete")
            payload = complete_repair(
                conn,
                repair_id=command.repair_id,
                queue_status=command.queue_status,
                result_ref=command.result_ref,
                repair_note=command.repair_note,
            )
        else:  # pragma: no cover - Literal/Pydantic rejects this first.
            raise ValueError(f"unknown repair queue action: {command.action}")
    except ValueError as exc:
        return {
            "ok": False,
            "status": "failed",
            "error_code": "workflow_repair_queue.invalid_command",
            "error": str(exc),
            **_authority_payload(),
        }

    ok_statuses = {"claimed", "empty", "released", "updated"}
    payload.update(
        {
            "ok": payload.get("status") in ok_statuses,
            **_authority_payload(),
        }
    )
    payload["event_payload"] = _event_payload(command, payload)
    return payload


__all__ = [
    "WorkflowRepairQueueCommand",
    "handle_workflow_repair_queue_command",
]
