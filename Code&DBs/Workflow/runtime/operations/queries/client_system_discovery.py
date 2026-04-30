"""CQRS queries for Client System Discovery authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.client_system_discovery_repository import (
    list_system_census,
    load_system_census,
    search_connector_census,
)


ClientSystemDiscoveryReadAction = Literal["list", "search", "describe"]


class QueryClientSystemDiscoveryCensusRead(BaseModel):
    """Read client-system census records and connector evidence."""

    action: ClientSystemDiscoveryReadAction = Field(default="list")
    tenant_ref: str | None = None
    census_id: str | None = None
    query: str | None = None
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("tenant_ref", "census_id", "query", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("filters must be non-empty strings when provided")
        return value.strip()

    @model_validator(mode="after")
    def _require_action_inputs(self) -> "QueryClientSystemDiscoveryCensusRead":
        if self.action == "search" and not self.query:
            raise ValueError("query is required for action=search")
        if self.action == "describe" and not self.census_id:
            raise ValueError("census_id is required for action=describe")
        return self


def handle_client_system_discovery_census_read(
    query: QueryClientSystemDiscoveryCensusRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Read Client System Discovery census authority."""

    conn = subsystems.get_pg_conn()
    if query.action == "describe":
        item = load_system_census(conn, census_id=query.census_id or "")
        return {
            "ok": item is not None,
            "operation": "client_system_discovery_census_read",
            "action": query.action,
            "item": item,
            "error": None if item is not None else f"census_id '{query.census_id}' not found",
        }
    if query.action == "search":
        rows = search_connector_census(conn, query=query.query or "", limit=query.limit)
        return {
            "ok": True,
            "operation": "client_system_discovery_census_read",
            "action": query.action,
            "count": len(rows),
            "items": rows,
        }

    rows = list_system_census(conn, tenant_ref=query.tenant_ref)
    return {
        "ok": True,
        "operation": "client_system_discovery_census_read",
        "action": query.action,
        "count": len(rows),
        "items": rows,
    }


__all__ = [
    "QueryClientSystemDiscoveryCensusRead",
    "handle_client_system_discovery_census_read",
]
