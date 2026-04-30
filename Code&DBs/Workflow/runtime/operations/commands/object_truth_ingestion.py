"""CQRS commands for Object Truth ingestion evidence."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from core.object_truth_ops import build_object_version
from runtime.object_truth.ingestion import (
    PRIVACY_CLASSIFICATIONS,
    SAMPLE_STRATEGIES,
    build_ingestion_replay_fixture,
    build_raw_payload_reference,
    build_redacted_preview,
    build_sample_capture_record,
    build_source_query_evidence,
    build_system_snapshot_record,
    normalize_ingestion_source_metadata,
)
from storage.postgres.object_truth_repository import (
    persist_ingestion_sample,
    persist_object_version,
)


PrivacyClassification = Literal["public", "internal", "confidential", "restricted"]
SampleStrategy = Literal[
    "recent",
    "claimed_source_truth",
    "matching_ids",
    "random_window",
    "operator_supplied",
    "fixture",
]


class RecordObjectTruthIngestionSampleCommand(BaseModel):
    """Record one receipt-backed ingestion sample packet."""

    client_ref: str
    system_ref: str
    integration_id: str
    connector_ref: str
    environment_ref: str
    object_ref: str
    schema_snapshot_digest: str
    captured_at: str
    capture_receipt_id: str
    identity_fields: list[str] = Field(default_factory=list)
    sample_payloads: list[dict[str, Any]] = Field(default_factory=list)
    sample_payload_refs: list[str] = Field(default_factory=list)
    sample_strategy: SampleStrategy = "recent"
    source_query: dict[str, Any] = Field(default_factory=dict)
    cursor_ref: str | None = None
    cursor_value: Any | None = None
    window_kind: str = "source_updated_at"
    window_start: str | None = None
    window_end: str | None = None
    limit: int | None = None
    sample_size_requested: int | None = None
    sample_hash: str | None = None
    sample_size_returned: int | None = None
    status: str | None = None
    auth_context_hash: str | None = None
    auth_context: Any | None = None
    privacy_classification: PrivacyClassification = "internal"
    privacy_policy_ref: str | None = None
    retention_policy_ref: str | None = None
    preview_policy: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    snapshot_metadata: dict[str, Any] = Field(default_factory=dict)
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator(
        "client_ref",
        "system_ref",
        "integration_id",
        "connector_ref",
        "environment_ref",
        "object_ref",
        "schema_snapshot_digest",
        "captured_at",
        "capture_receipt_id",
        "window_kind",
        mode="before",
    )
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("required ingestion refs must be non-empty strings")
        return value.strip()

    @field_validator(
        "cursor_ref",
        "sample_hash",
        "auth_context_hash",
        "privacy_policy_ref",
        "retention_policy_ref",
        "observed_by_ref",
        "source_ref",
        "window_start",
        "window_end",
        "status",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional ingestion refs must be non-empty strings when provided")
        return value.strip()

    @field_validator(
        "source_query",
        "preview_policy",
        "metadata",
        "snapshot_metadata",
        "source_metadata",
        mode="before",
    )
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("ingestion mapping fields must be JSON objects")

    @field_validator("identity_fields", "sample_payload_refs", mode="before")
    @classmethod
    def _normalize_text_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("identity_fields and sample_payload_refs must be lists")
        normalized: list[str] = []
        for item in value:
            text = str(item).strip()
            if text and text not in normalized:
                normalized.append(text)
        return normalized

    @field_validator("sample_payloads", mode="before")
    @classmethod
    def _normalize_payloads(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("sample_payloads must be a list of JSON objects")
        return [dict(item) for item in value]

    @field_validator("privacy_classification", mode="before")
    @classmethod
    def _normalize_privacy_classification(cls, value: object) -> str:
        text = str(value or "internal").strip().lower()
        if text not in PRIVACY_CLASSIFICATIONS:
            raise ValueError("privacy_classification must be public, internal, confidential, or restricted")
        return text

    @field_validator("sample_strategy", mode="before")
    @classmethod
    def _normalize_sample_strategy(cls, value: object) -> str:
        text = str(value or "recent").strip()
        if text not in SAMPLE_STRATEGIES:
            raise ValueError("sample_strategy is invalid")
        return text

    @model_validator(mode="after")
    def _validate_payload_policy(self) -> "RecordObjectTruthIngestionSampleCommand":
        if self.sample_payloads and not self.identity_fields:
            raise ValueError("identity_fields are required when sample_payloads are supplied")
        if self.sample_payload_refs and len(self.sample_payload_refs) != len(self.sample_payloads):
            raise ValueError("sample_payload_refs must match sample_payloads length when supplied")
        if not self.sample_payloads and (self.sample_hash is None or self.sample_size_returned is None):
            raise ValueError("sample_hash and sample_size_returned are required without sample_payloads")
        return self


def handle_object_truth_ingestion_sample_record(
    command: RecordObjectTruthIngestionSampleCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    sample_payloads = [dict(item) for item in command.sample_payloads]
    sample_size_requested = command.sample_size_requested
    if sample_size_requested is None:
        sample_size_requested = len(sample_payloads)

    system_snapshot = build_system_snapshot_record(
        client_ref=command.client_ref,
        system_ref=command.system_ref,
        integration_id=command.integration_id,
        connector_ref=command.connector_ref,
        environment_ref=command.environment_ref,
        auth_context_hash=command.auth_context_hash,
        auth_context=command.auth_context,
        captured_at=command.captured_at,
        capture_receipt_id=command.capture_receipt_id,
        schema_snapshot_count=1,
        sample_count=1,
        metadata=command.snapshot_metadata,
    )
    source_evidence = build_source_query_evidence(
        system_ref=command.system_ref,
        object_ref=command.object_ref,
        source_query=command.source_query,
        cursor_ref=command.cursor_ref,
        cursor_value=command.cursor_value,
        window_kind=command.window_kind,
        window_start=command.window_start,
        window_end=command.window_end,
        limit=command.limit,
        metadata=command.source_metadata,
    )
    sample_capture = build_sample_capture_record(
        system_snapshot_id=str(system_snapshot["system_snapshot_id"]),
        schema_snapshot_digest=command.schema_snapshot_digest,
        system_ref=command.system_ref,
        object_ref=command.object_ref,
        sample_strategy=command.sample_strategy,
        source_evidence=source_evidence,
        sample_size_requested=sample_size_requested,
        sample_payloads=sample_payloads or None,
        sample_size_returned=command.sample_size_returned,
        sample_hash=command.sample_hash,
        status=command.status,
        receipt_id=command.capture_receipt_id,
        metadata=command.metadata,
    )

    payload_references: list[dict[str, Any]] = []
    object_versions: list[dict[str, Any]] = []
    object_version_refs: list[dict[str, Any]] = []
    for index, payload in enumerate(sample_payloads):
        raw_payload_reference = build_raw_payload_reference(
            raw_payload=payload,
            raw_payload_ref=command.sample_payload_refs[index] if command.sample_payload_refs else None,
            privacy_classification=command.privacy_classification,
            retention_policy_ref=command.retention_policy_ref,
            privacy_policy_ref=command.privacy_policy_ref,
            inline_payload_approved=False,
        )
        redacted_preview = build_redacted_preview(
            payload,
            policy=command.preview_policy,
            privacy_policy_ref=command.privacy_policy_ref,
        )
        source_metadata = normalize_ingestion_source_metadata(
            payload,
            raw_payload_reference=raw_payload_reference,
            redacted_preview=redacted_preview,
        )
        object_version = build_object_version(
            system_ref=command.system_ref,
            object_ref=command.object_ref,
            record=payload,
            identity_fields=command.identity_fields,
            source_metadata=source_metadata,
            schema_snapshot_digest=command.schema_snapshot_digest,
        )
        persisted_version = persist_object_version(
            conn,
            object_version=object_version,
            observed_by_ref=command.observed_by_ref,
            source_ref=str(sample_capture["sample_id"]),
        )
        object_versions.append(object_version)
        object_version_refs.append(
            {
                "object_version_digest": object_version["object_version_digest"],
                "object_version_ref": persisted_version["object_version_ref"],
                "identity_digest": object_version["identity"]["identity_digest"],
                "field_observation_count": persisted_version["field_observation_count"],
            }
        )
        raw_payload_reference_json = {
            key: value
            for key, value in raw_payload_reference.items()
            if key != "raw_payload_json"
        }
        payload_references.append(
            {
                "payload_index": index,
                "external_record_id": source_metadata.get("external_record_id"),
                "source_metadata_digest": source_metadata["source_metadata_digest"],
                "raw_payload_ref": raw_payload_reference.get("raw_payload_ref"),
                "raw_payload_hash": raw_payload_reference.get("raw_payload_hash"),
                "normalized_payload_hash": raw_payload_reference.get("normalized_payload_hash"),
                "privacy_classification": raw_payload_reference["privacy_classification"],
                "retention_policy_ref": raw_payload_reference.get("retention_policy_ref"),
                "privacy_policy_ref": raw_payload_reference.get("privacy_policy_ref"),
                "inline_payload_stored": False,
                "reference_digest": raw_payload_reference["reference_digest"],
                "redacted_preview_digest": redacted_preview["preview_digest"],
                "source_metadata_json": source_metadata,
                "redacted_preview_json": redacted_preview,
                "raw_payload_reference_json": raw_payload_reference_json,
            }
        )

    replay_fixture = build_ingestion_replay_fixture(
        system_snapshot=system_snapshot,
        samples=[sample_capture],
        object_versions=object_versions,
        metadata={
            "source_evidence_digest": source_evidence["source_evidence_digest"],
            "payload_reference_count": len(payload_references),
        },
    )
    persisted = persist_ingestion_sample(
        conn,
        system_snapshot=system_snapshot,
        sample_capture=sample_capture,
        payload_references=payload_references,
        object_version_refs=object_version_refs,
        replay_fixture=replay_fixture,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "system_snapshot_id": system_snapshot["system_snapshot_id"],
        "system_snapshot_digest": system_snapshot["system_snapshot_digest"],
        "sample_id": sample_capture["sample_id"],
        "sample_capture_digest": sample_capture["sample_capture_digest"],
        "client_ref": command.client_ref,
        "system_ref": command.system_ref,
        "object_ref": command.object_ref,
        "sample_strategy": command.sample_strategy,
        "sample_size_returned": sample_capture["sample_size_returned"],
        "payload_reference_count": len(payload_references),
        "object_version_count": len(object_version_refs),
        "fixture_digest": replay_fixture["fixture_digest"],
    }
    return {
        "ok": True,
        "operation": "object_truth_ingestion_sample_record",
        "system_snapshot": system_snapshot,
        "source_evidence": source_evidence,
        "sample_capture": sample_capture,
        "payload_reference_count": len(payload_references),
        "object_version_refs": object_version_refs,
        "replay_fixture": replay_fixture,
        "persisted": persisted,
        "event_payload": event_payload,
    }


__all__ = [
    "RecordObjectTruthIngestionSampleCommand",
    "handle_object_truth_ingestion_sample_record",
]
