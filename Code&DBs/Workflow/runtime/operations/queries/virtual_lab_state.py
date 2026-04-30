"""CQRS queries for Virtual Lab state authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.virtual_lab_state_repository import (
    list_virtual_lab_command_receipts,
    list_virtual_lab_environments,
    list_virtual_lab_events,
    list_virtual_lab_revisions,
    load_virtual_lab_revision,
)


ReadAction = Literal[
    "list_environments",
    "list_revisions",
    "describe_revision",
    "list_events",
    "list_receipts",
]


class QueryVirtualLabStateRead(BaseModel):
    """Read Virtual Lab environment revisions, events, and receipts."""

    action: ReadAction = "list_environments"
    environment_id: str | None = None
    revision_id: str | None = None
    stream_id: str | None = None
    event_type: str | None = None
    status: str | None = None
    include_seed: bool = True
    include_objects: bool = True
    include_events: bool = True
    include_receipts: bool = True
    include_typed_gaps: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator("environment_id", "revision_id", "stream_id", "event_type", "status", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryVirtualLabStateRead":
        if self.action in {"describe_revision", "list_events", "list_receipts"}:
            if not self.environment_id or not self.revision_id:
                raise ValueError(f"environment_id and revision_id are required for {self.action}")
        return self


def handle_virtual_lab_state_read(
    query: QueryVirtualLabStateRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "list_revisions":
        items = list_virtual_lab_revisions(
            conn,
            environment_id=query.environment_id,
            status=query.status,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_state_read",
            "action": "list_revisions",
            "count": len(items),
            "items": items,
        }
    if query.action == "describe_revision":
        revision = load_virtual_lab_revision(
            conn,
            environment_id=str(query.environment_id),
            revision_id=str(query.revision_id),
            include_seed=query.include_seed,
            include_objects=query.include_objects,
            include_events=query.include_events,
            include_receipts=query.include_receipts,
            include_typed_gaps=query.include_typed_gaps,
        )
        return {
            "ok": revision is not None,
            "operation": "virtual_lab_state_read",
            "action": "describe_revision",
            "environment_id": query.environment_id,
            "revision_id": query.revision_id,
            "revision": revision,
            "error_code": None if revision is not None else "virtual_lab_state.revision_not_found",
        }
    if query.action == "list_events":
        items = list_virtual_lab_events(
            conn,
            environment_id=str(query.environment_id),
            revision_id=str(query.revision_id),
            stream_id=query.stream_id,
            event_type=query.event_type,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_state_read",
            "action": "list_events",
            "count": len(items),
            "items": items,
        }
    if query.action == "list_receipts":
        items = list_virtual_lab_command_receipts(
            conn,
            environment_id=str(query.environment_id),
            revision_id=str(query.revision_id),
            status=query.status,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_state_read",
            "action": "list_receipts",
            "count": len(items),
            "items": items,
        }

    items = list_virtual_lab_environments(conn, status=query.status, limit=query.limit)
    return {
        "ok": True,
        "operation": "virtual_lab_state_read",
        "action": "list_environments",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryVirtualLabStateRead",
    "handle_virtual_lab_state_read",
]
