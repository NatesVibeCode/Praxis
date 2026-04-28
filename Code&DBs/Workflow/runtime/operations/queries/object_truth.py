"""CQRS query handlers for deterministic object-truth evidence."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from core.object_truth_ops import build_object_version, compare_object_versions
from storage.postgres.object_truth_repository import load_object_version


class QueryObserveRecord(BaseModel):
    """Build one deterministic object-version packet from an inline record."""

    system_ref: str = Field(description="External system or integration reference.")
    object_ref: str = Field(description="External object reference inside the system.")
    record: dict[str, Any] = Field(description="Inline JSON object to observe.")
    identity_fields: list[str] = Field(description="Required field paths that identify the object.")
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    schema_snapshot_digest: str | None = None

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

    @field_validator("schema_snapshot_digest", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("schema_snapshot_digest must be a non-empty string when provided")
        return value.strip()

    @model_validator(mode="after")
    def _guard_record_size(self) -> "QueryObserveRecord":
        if len(self.record) > 500:
            raise ValueError("record has too many top-level fields for inline observation")
        return self


class QueryCompareVersions(BaseModel):
    """Compare two persisted object-version packets by digest."""

    left_object_version_digest: str = Field(description="Digest of the left stored object version.")
    right_object_version_digest: str = Field(description="Digest of the right stored object version.")

    @field_validator("left_object_version_digest", "right_object_version_digest", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("object version digests must be non-empty strings")
        return value.strip()


def handle_observe_record(query: QueryObserveRecord, subsystems: Any) -> dict[str, Any]:
    """Return deterministic evidence only; durable persistence comes later."""

    _ = subsystems
    object_version = build_object_version(
        system_ref=query.system_ref,
        object_ref=query.object_ref,
        record=query.record,
        identity_fields=query.identity_fields,
        source_metadata=query.source_metadata,
        schema_snapshot_digest=query.schema_snapshot_digest,
    )
    return {
        "ok": True,
        "operation": "object_truth_observe_record",
        "object_version": object_version,
        "stats": {
            "field_observation_count": len(object_version["field_observations"]),
            "has_nested_objects": object_version["hierarchy_signals"]["has_nested_objects"],
            "has_arrays": object_version["hierarchy_signals"]["has_arrays"],
        },
    }


def handle_compare_versions(query: QueryCompareVersions, subsystems: Any) -> dict[str, Any]:
    """Compare two persisted object-version packets without writing state."""

    conn = subsystems.get_pg_conn()
    left = load_object_version(
        conn,
        object_version_digest=query.left_object_version_digest,
    )
    right = load_object_version(
        conn,
        object_version_digest=query.right_object_version_digest,
    )
    missing = []
    if left is None:
        missing.append("left_object_version_digest")
    if right is None:
        missing.append("right_object_version_digest")
    if missing:
        return {
            "ok": False,
            "operation": "object_truth_compare_versions",
            "error_code": "object_truth.object_version_not_found",
            "missing": missing,
        }

    comparison = compare_object_versions(left, right)
    return {
        "ok": True,
        "operation": "object_truth_compare_versions",
        "comparison": comparison,
        "stats": comparison["summary"],
    }


__all__ = [
    "QueryCompareVersions",
    "QueryObserveRecord",
    "handle_compare_versions",
    "handle_observe_record",
]
