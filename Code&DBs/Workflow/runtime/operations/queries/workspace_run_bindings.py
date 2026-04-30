"""CQRS queries for workspace/app-manifest run bindings and receipts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from storage.postgres.workspace_run_binding_repository import (
    list_manifest_receipts,
    list_manifest_run_bindings,
)


class QueryWorkspaceRunsList(BaseModel):
    """List workflow runs bound to one workspace manifest."""

    manifest_id: str
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("manifest_id", mode="before")
    @classmethod
    def _normalize_manifest_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("manifest_id is required")
        return value.strip()


class QueryWorkspaceReceiptsList(BaseModel):
    """List receipts scoped through workspace manifest run bindings."""

    manifest_id: str
    status: str | None = None
    limit: int = Field(default=100, ge=1, le=500)

    @field_validator("manifest_id", mode="before")
    @classmethod
    def _normalize_manifest_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("manifest_id is required")
        return value.strip()

    @field_validator("status", mode="before")
    @classmethod
    def _normalize_status(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("status must be a non-empty string when supplied")
        return value.strip()


def handle_workspace_runs_list(
    query: QueryWorkspaceRunsList,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_manifest_run_bindings(
        subsystems.get_pg_conn(),
        manifest_id=query.manifest_id,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "workspace.runs.list",
        "manifest_id": query.manifest_id,
        "count": len(rows),
        "items": rows,
    }


def handle_workspace_receipts_list(
    query: QueryWorkspaceReceiptsList,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_manifest_receipts(
        subsystems.get_pg_conn(),
        manifest_id=query.manifest_id,
        status=query.status,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "workspace.receipts.list",
        "manifest_id": query.manifest_id,
        "status": query.status,
        "count": len(rows),
        "items": rows,
    }


__all__ = [
    "QueryWorkspaceRunsList",
    "QueryWorkspaceReceiptsList",
    "handle_workspace_runs_list",
    "handle_workspace_receipts_list",
]
