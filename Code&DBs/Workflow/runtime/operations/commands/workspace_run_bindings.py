"""CQRS commands for workspace/app-manifest run bindings."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from storage.postgres.workspace_run_binding_repository import record_manifest_run_binding


class RecordWorkspaceRunBindingCommand(BaseModel):
    """Record that a workspace manifest dispatched a workflow run."""

    manifest_id: str
    workflow_id: str
    run_id: str
    operation_receipt_id: str | None = None
    dispatched_by: str | None = "workspace.compose"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("manifest_id", "workflow_id", "run_id", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("manifest_id, workflow_id, and run_id are required")
        return value.strip()

    @field_validator("operation_receipt_id", "dispatched_by", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when supplied")
        return value.strip()

    @field_validator("metadata", mode="before")
    @classmethod
    def _normalize_metadata(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("metadata must be a JSON object")
        return dict(value)


def handle_workspace_run_binding_record(
    command: RecordWorkspaceRunBindingCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    binding = record_manifest_run_binding(
        conn,
        manifest_id=command.manifest_id,
        workflow_id=command.workflow_id,
        run_id=command.run_id,
        operation_receipt_id=command.operation_receipt_id,
        dispatched_by=command.dispatched_by,
        metadata=command.metadata,
    )
    event_payload = {
        "manifest_id": binding["manifest_id"],
        "workflow_id": binding["workflow_id"],
        "run_id": binding["run_id"],
        "operation_receipt_id": binding.get("operation_receipt_id"),
        "dispatched_by": binding["dispatched_by"],
    }
    return {
        "ok": True,
        "operation": "workspace.run_binding.record",
        "binding": binding,
        "event_payload": event_payload,
    }


__all__ = [
    "RecordWorkspaceRunBindingCommand",
    "handle_workspace_run_binding_record",
]
