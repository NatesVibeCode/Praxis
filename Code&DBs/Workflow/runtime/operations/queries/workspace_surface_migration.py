"""CQRS query for previewing workspace surface migrations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workspace_surface_migration import (
    BLANK_COMPOSE_MIGRATION_REF,
    preview_workspace_surface_migration,
)


class QueryWorkspaceSurfaceMigrationPreview(BaseModel):
    """Preview a workspace surface migration without changing app_manifests."""

    manifest_id: str = Field(min_length=1)
    migration_ref: str = BLANK_COMPOSE_MIGRATION_REF
    force: bool = False
    tab_id: str | None = None
    include_bundle: bool = False

    @field_validator("manifest_id", "migration_ref", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("manifest_id and migration_ref are required")
        return value.strip()

    @field_validator("tab_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("tab_id must be a non-empty string when supplied")
        return value.strip()


def handle_workspace_surface_migration_preview(
    query: QueryWorkspaceSurfaceMigrationPreview,
    subsystems: Any,
) -> dict[str, Any]:
    preview = preview_workspace_surface_migration(
        subsystems.get_pg_conn(),
        manifest_id=query.manifest_id,
        migration_ref=query.migration_ref,
        force=query.force,
        tab_id=query.tab_id,
        include_bundle=query.include_bundle,
    )
    return {
        "ok": True,
        "operation": "workspace.surface_migration.preview",
        **preview,
    }


__all__ = [
    "QueryWorkspaceSurfaceMigrationPreview",
    "handle_workspace_surface_migration_preview",
]
