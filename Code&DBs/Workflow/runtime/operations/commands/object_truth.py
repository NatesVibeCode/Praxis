"""CQRS commands for durable object-truth evidence."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from core.object_truth_ops import (
    build_object_version,
    compare_object_versions,
    normalize_schema_snapshot,
)
from storage.postgres.object_truth_repository import (
    load_object_version,
    persist_comparison_run,
    persist_object_version,
    persist_schema_snapshot,
)


class StoreObservedRecordCommand(BaseModel):
    """Build and persist one deterministic object-version packet."""

    system_ref: str = Field(description="External system or integration reference.")
    object_ref: str = Field(description="External object reference inside the system.")
    record: dict[str, Any] = Field(description="Inline JSON object to observe and persist.")
    identity_fields: list[str] = Field(description="Required field paths that identify the object.")
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    schema_snapshot_digest: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("system_ref", "object_ref", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("system_ref and object_ref must be non-empty strings")
        return value.strip()

    @field_validator("record", "source_metadata", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("record and source_metadata must be JSON objects")

    @field_validator("identity_fields", mode="before")
    @classmethod
    def _normalize_identity_fields(cls, value: object) -> list[str]:
        if not isinstance(value, list):
            raise ValueError("identity_fields must be a list of strings")
        fields = [str(item).strip() for item in value if str(item).strip()]
        if not fields:
            raise ValueError("identity_fields must include at least one field")
        return fields

    @field_validator("schema_snapshot_digest", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional object-truth refs must be non-empty strings when provided")
        return value.strip()

    @model_validator(mode="after")
    def _guard_record_size(self) -> "StoreObservedRecordCommand":
        if len(self.record) > 500:
            raise ValueError("record has too many top-level fields for inline persistence")
        return self


class StoreSchemaSnapshotCommand(BaseModel):
    """Normalize and persist one external object schema snapshot."""

    system_ref: str = Field(description="External system or integration reference.")
    object_ref: str = Field(description="External object reference inside the system.")
    raw_schema: dict[str, Any] | list[dict[str, Any]] = Field(
        description="External schema payload or list of field descriptors.",
    )
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("system_ref", "object_ref", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("system_ref and object_ref must be non-empty strings")
        return value.strip()

    @field_validator("raw_schema", mode="before")
    @classmethod
    def _normalize_raw_schema(cls, value: object) -> dict[str, Any] | list[dict[str, Any]]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return [dict(item) for item in value]
        raise ValueError("raw_schema must be an object or a list of field objects")

    @field_validator("observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional object-truth refs must be non-empty strings when provided")
        return value.strip()


class RecordComparisonRunCommand(BaseModel):
    """Compare two stored object versions and persist the comparison output."""

    left_object_version_digest: str = Field(description="Digest of the left stored object version.")
    right_object_version_digest: str = Field(description="Digest of the right stored object version.")
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("left_object_version_digest", "right_object_version_digest", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("object version digests must be non-empty strings")
        return value.strip()

    @field_validator("observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional object-truth refs must be non-empty strings when provided")
        return value.strip()


def handle_store_observed_record(
    command: StoreObservedRecordCommand,
    subsystems: Any,
) -> dict[str, Any]:
    object_version = build_object_version(
        system_ref=command.system_ref,
        object_ref=command.object_ref,
        record=command.record,
        identity_fields=command.identity_fields,
        source_metadata=command.source_metadata,
        schema_snapshot_digest=command.schema_snapshot_digest,
    )
    persisted = persist_object_version(
        subsystems.get_pg_conn(),
        object_version=object_version,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "object_version_digest": object_version["object_version_digest"],
        "object_version_ref": persisted["object_version_ref"],
        "system_ref": command.system_ref,
        "object_ref": command.object_ref,
        "identity_digest": object_version["identity"]["identity_digest"],
        "payload_digest": object_version["payload_digest"],
        "field_observation_count": persisted["field_observation_count"],
        "schema_snapshot_digest": command.schema_snapshot_digest,
        "observed_by_ref": command.observed_by_ref,
        "source_ref": command.source_ref,
    }
    return {
        "ok": True,
        "operation": "object_truth_store_observed_record",
        "object_version_digest": object_version["object_version_digest"],
        "object_version_ref": persisted["object_version_ref"],
        "field_observation_count": persisted["field_observation_count"],
        "persisted": persisted,
        "event_payload": event_payload,
    }


def handle_record_comparison_run(
    command: RecordComparisonRunCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    left = load_object_version(
        conn,
        object_version_digest=command.left_object_version_digest,
    )
    right = load_object_version(
        conn,
        object_version_digest=command.right_object_version_digest,
    )
    missing = []
    if left is None:
        missing.append("left_object_version_digest")
    if right is None:
        missing.append("right_object_version_digest")
    if missing:
        return {
            "ok": False,
            "operation": "object_truth_record_comparison_run",
            "error_code": "object_truth.object_version_not_found",
            "missing": missing,
        }

    comparison = compare_object_versions(left, right)
    persisted = persist_comparison_run(
        conn,
        comparison=comparison,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "comparison_run_digest": persisted["comparison_run_digest"],
        "comparison_run_ref": persisted["comparison_run_ref"],
        "comparison_digest": comparison["comparison_digest"],
        "left_object_version_digest": command.left_object_version_digest,
        "right_object_version_digest": command.right_object_version_digest,
        "summary": comparison["summary"],
        "freshness": comparison["freshness"],
        "observed_by_ref": command.observed_by_ref,
        "source_ref": command.source_ref,
    }
    return {
        "ok": True,
        "operation": "object_truth_record_comparison_run",
        "comparison_run_digest": persisted["comparison_run_digest"],
        "comparison_run_ref": persisted["comparison_run_ref"],
        "comparison": comparison,
        "persisted": persisted,
        "event_payload": event_payload,
    }


def handle_store_schema_snapshot(
    command: StoreSchemaSnapshotCommand,
    subsystems: Any,
) -> dict[str, Any]:
    schema_snapshot = normalize_schema_snapshot(
        command.raw_schema,
        system_ref=command.system_ref,
        object_ref=command.object_ref,
    )
    persisted = persist_schema_snapshot(
        subsystems.get_pg_conn(),
        schema_snapshot=schema_snapshot,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "schema_snapshot_digest": schema_snapshot["schema_digest"],
        "schema_snapshot_ref": persisted["schema_snapshot_ref"],
        "system_ref": command.system_ref,
        "object_ref": command.object_ref,
        "field_count": persisted["field_count"],
        "observed_by_ref": command.observed_by_ref,
        "source_ref": command.source_ref,
    }
    return {
        "ok": True,
        "operation": "object_truth_store_schema_snapshot",
        "schema_snapshot_digest": schema_snapshot["schema_digest"],
        "schema_snapshot_ref": persisted["schema_snapshot_ref"],
        "field_count": persisted["field_count"],
        "schema_snapshot": schema_snapshot,
        "persisted": persisted,
        "event_payload": event_payload,
    }


__all__ = [
    "RecordComparisonRunCommand",
    "StoreObservedRecordCommand",
    "StoreSchemaSnapshotCommand",
    "handle_record_comparison_run",
    "handle_store_observed_record",
    "handle_store_schema_snapshot",
]
