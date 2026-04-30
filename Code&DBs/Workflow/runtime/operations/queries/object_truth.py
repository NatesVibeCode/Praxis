"""CQRS query handlers for deterministic object-truth evidence."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from core.object_truth_ops import build_object_version, compare_object_versions
from storage.postgres.object_truth_repository import inspect_readiness, load_object_version


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


class QueryReadiness(BaseModel):
    """Inspect whether object-truth authority is safe to build on."""

    client_payload_mode: str = Field(
        default="redacted_hashes",
        description="Expected client payload mode: redacted_hashes or raw_client_payloads.",
    )
    privacy_policy_ref: str | None = None
    planned_fanout: int = Field(
        default=1,
        ge=1,
        description="Number of downstream jobs the caller expects to launch after this gate.",
    )
    include_counts: bool = True

    @field_validator("client_payload_mode", mode="before")
    @classmethod
    def _normalize_client_payload_mode(cls, value: object) -> str:
        text = str(value or "redacted_hashes").strip()
        if text not in {"redacted_hashes", "raw_client_payloads"}:
            raise ValueError("client_payload_mode must be redacted_hashes or raw_client_payloads")
        return text

    @field_validator("privacy_policy_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("privacy_policy_ref must be a non-empty string when provided")
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


def handle_readiness(query: QueryReadiness, subsystems: Any) -> dict[str, Any]:
    """Return the fail-closed Object Truth readiness gate."""

    readiness = inspect_readiness(
        subsystems.get_pg_conn(),
        client_payload_mode=query.client_payload_mode,
        privacy_policy_ref=query.privacy_policy_ref,
        planned_fanout=query.planned_fanout,
        include_counts=query.include_counts,
    )
    return {
        "ok": True,
        "operation": "object_truth_readiness",
        **readiness,
    }


__all__ = [
    "QueryCompareVersions",
    "QueryObserveRecord",
    "QueryReadiness",
    "handle_compare_versions",
    "handle_observe_record",
    "handle_readiness",
]
