"""CQRS queries for Object Truth MDM/source-authority evidence."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.object_truth_repository import (
    list_mdm_resolution_packets,
    load_mdm_resolution_packet,
)


ReadAction = Literal["list", "describe"]


class QueryObjectTruthMdmResolutionRead(BaseModel):
    """Read Object Truth MDM resolution evidence."""

    action: ReadAction = "list"
    client_ref: str | None = None
    entity_type: str | None = None
    packet_ref: str | None = None
    include_records: bool = True
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("client_ref", "entity_type", "packet_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryObjectTruthMdmResolutionRead":
        if self.action == "describe" and not self.packet_ref:
            raise ValueError("packet_ref is required for describe")
        return self


def handle_object_truth_mdm_resolution_read(
    query: QueryObjectTruthMdmResolutionRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe":
        packet = load_mdm_resolution_packet(
            conn,
            packet_ref=str(query.packet_ref),
            include_records=query.include_records,
        )
        return {
            "ok": packet is not None,
            "operation": "object_truth_mdm_resolution_read",
            "action": "describe",
            "packet_ref": query.packet_ref,
            "packet": packet,
            "error_code": None if packet is not None else "object_truth.mdm_resolution_not_found",
        }

    items = list_mdm_resolution_packets(
        conn,
        client_ref=query.client_ref,
        entity_type=query.entity_type,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "object_truth_mdm_resolution_read",
        "action": "list",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryObjectTruthMdmResolutionRead",
    "handle_object_truth_mdm_resolution_read",
]
