"""CQRS commands for durable Client Operating Model projections."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from storage.postgres.client_operating_model_repository import (
    persist_operator_view_snapshot,
)


class StoreOperatorViewSnapshotCommand(BaseModel):
    """Persist one Client Operating Model operator-view snapshot."""

    operator_view: dict[str, Any] = Field(
        description="JSON-ready operator_view payload produced by client_operating_model_operator_view.",
    )
    view: str | None = Field(
        default=None,
        description="Optional explicit view name; inferred from operator_view.kind when omitted.",
    )
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("operator_view", mode="before")
    @classmethod
    def _normalize_operator_view(cls, value: object) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("operator_view must be a JSON object")

    @field_validator("view", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()


def handle_store_operator_view_snapshot(
    command: StoreOperatorViewSnapshotCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Persist a read-model snapshot without rebuilding source evidence."""

    persisted = persist_operator_view_snapshot(
        subsystems.get_pg_conn(),
        operator_view=command.operator_view,
        view=command.view,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "snapshot_digest": persisted["snapshot_digest"],
        "snapshot_ref": persisted["snapshot_ref"],
        "view_name": persisted["view_name"],
        "view_id": persisted["view_id"],
        "scope_ref": persisted["scope_ref"],
        "state": persisted["state"],
        "observed_by_ref": command.observed_by_ref,
        "source_ref": command.source_ref,
    }
    return {
        "ok": True,
        "operation": "client_operating_model_operator_view_snapshot_store",
        "snapshot_digest": persisted["snapshot_digest"],
        "snapshot_ref": persisted["snapshot_ref"],
        "persisted": persisted,
        "event_payload": event_payload,
    }


__all__ = [
    "StoreOperatorViewSnapshotCommand",
    "handle_store_operator_view_snapshot",
]
