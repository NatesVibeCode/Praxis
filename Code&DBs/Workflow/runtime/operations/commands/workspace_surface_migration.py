"""CQRS command for applying workspace surface migrations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.workspace_surface_migration import (
    BLANK_COMPOSE_MIGRATION_REF,
    apply_workspace_surface_migration,
)


class ApplyWorkspaceSurfaceMigrationCommand(BaseModel):
    """Apply a previewed workspace surface migration."""

    manifest_id: str = Field(min_length=1)
    migration_ref: str = BLANK_COMPOSE_MIGRATION_REF
    changed_by: str = "workspace.surface_migration"
    reason: str | None = None
    force: bool = False
    tab_id: str | None = None

    @field_validator("manifest_id", "migration_ref", "changed_by", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("manifest_id, migration_ref, and changed_by are required")
        return value.strip()

    @field_validator("reason", "tab_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional text fields must be non-empty strings when supplied")
        return value.strip()


def handle_workspace_surface_migration_apply(
    command: ApplyWorkspaceSurfaceMigrationCommand,
    subsystems: Any,
) -> dict[str, Any]:
    result = apply_workspace_surface_migration(
        subsystems.get_pg_conn(),
        manifest_id=command.manifest_id,
        migration_ref=command.migration_ref,
        changed_by=command.changed_by,
        reason=command.reason,
        force=command.force,
        tab_id=command.tab_id,
    )
    payload = {
        "ok": bool(result.get("ok")),
        "operation": "workspace.surface_migration.apply",
        **result,
    }
    if "event_payload" not in payload:
        payload["event_payload"] = {
            "manifest_id": command.manifest_id,
            "migration_ref": command.migration_ref,
            "surface_id": result.get("preview", {}).get("surface_id")
            if isinstance(result.get("preview"), dict)
            else None,
            "changed": False,
            "reject_reason": result.get("reject_reason"),
        }
    return payload


__all__ = [
    "ApplyWorkspaceSurfaceMigrationCommand",
    "handle_workspace_surface_migration_apply",
]
