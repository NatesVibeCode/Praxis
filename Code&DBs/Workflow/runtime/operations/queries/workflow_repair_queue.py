"""CQRS query handlers for durable Workflow repair queue authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


RepairQueueReadAction = Literal["list", "queue", "status", "summary"]
RepairScope = Literal["solution", "workflow", "job"]
RepairQueueStatus = Literal[
    "queued",
    "claimed",
    "repairing",
    "completed",
    "failed",
    "cancelled",
    "superseded",
]


class WorkflowRepairQueueStatusQuery(BaseModel):
    """Input for ``workflow_repair_queue.status``."""

    action: RepairQueueReadAction = Field(
        default="list",
        description="Read action: list/queue/status for rows, or summary for counts.",
    )
    queue_status: RepairQueueStatus | None = Field(
        default="queued",
        description="Queue status filter. Pass null to inspect every status.",
    )
    repair_scope: RepairScope | None = Field(
        default=None,
        description="Optional repair scope filter: solution, workflow, or job.",
    )
    run_id: str | None = Field(default=None, description="Optional workflow run filter.")
    solution_id: str | None = Field(default=None, description="Optional Solution id filter.")
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator("action", mode="before")
    @classmethod
    def _normalize_action(cls, value: object) -> str:
        action = str(value or "list").strip().lower()
        return "list" if action == "queue" else action

    @field_validator("queue_status", "repair_scope", "run_id", "solution_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional text fields must be non-empty strings when provided")
        return value.strip()

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 50
        if isinstance(value, bool):
            raise ValueError("limit must be an integer")
        try:
            return max(1, min(int(value), 500))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def _authority_payload() -> dict[str, str]:
    return {
        "authority": "workflow_repair_queue",
        "object_kind": "workflow_repair_queue",
        "storage_authority": "workflow_repair_queue",
    }


def handle_query_workflow_repair_queue_status(
    query: WorkflowRepairQueueStatusQuery,
    subsystems: Any,
) -> dict[str, Any]:
    """Read repair queue rows/counts through the CQRS gateway."""

    from runtime.workflow.repair_queue import list_repair_queue, repair_queue_summary

    conn = _pg_conn(subsystems)
    try:
        if query.action == "summary":
            payload = repair_queue_summary(conn)
            payload.update(
                {
                    "ok": payload.get("status") == "ok",
                    "view": "workflow_repair_queue_summary",
                    **_authority_payload(),
                }
            )
            return payload

        payload = list_repair_queue(
            conn,
            queue_status=query.queue_status,
            repair_scope=query.repair_scope,
            run_id=query.run_id,
            solution_id=query.solution_id,
            limit=query.limit,
        )
        payload.update(
            {
                "ok": payload.get("status") == "ok",
                "view": "workflow_repair_queue",
                **_authority_payload(),
            }
        )
        return payload
    except ValueError as exc:
        return {
            "ok": False,
            "view": "workflow_repair_queue",
            "error_code": "workflow_repair_queue.invalid_query",
            "error": str(exc),
            **_authority_payload(),
        }


__all__ = [
    "WorkflowRepairQueueStatusQuery",
    "handle_query_workflow_repair_queue_status",
]
