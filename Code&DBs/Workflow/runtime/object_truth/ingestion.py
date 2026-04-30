"""Pure Object Truth ingestion primitives.

This module builds deterministic ingestion evidence for client-system snapshots,
sample captures, payload references, redacted previews, replay fixtures, and
readiness inputs. It performs no IO, does not call live connectors, and does not
decide business truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from fnmatch import fnmatchcase
import re
from typing import Any, Literal

from core.object_truth_ops import (
    ObjectTruthOperationError,
    canonical_digest,
    canonical_value,
)


OBJECT_TRUTH_INGESTION_SCHEMA_VERSION = 1

PrivacyClassification = Literal["public", "internal", "confidential", "restricted"]
SampleStrategy = Literal[
    "recent",
    "claimed_source_truth",
    "matching_ids",
    "random_window",
    "operator_supplied",
    "fixture",
]
SampleStatus = Literal["planned", "captured", "partial", "empty", "failed", "rejected"]

PRIVACY_CLASSIFICATIONS = {"public", "internal", "confidential", "restricted"}
SAMPLE_STRATEGIES = {
    "recent",
    "claimed_source_truth",
    "matching_ids",
    "random_window",
    "operator_supplied",
    "fixture",
}
SAMPLE_STATUSES = {"planned", "captured", "partial", "empty", "failed", "rejected"}
FAIL_CLOSED_READINESS_STATES = ["blocked", "unknown", "revoked"]

_UNKNOWN_MARKERS = {"", "unknown", "none", "null", "n/a", "na"}
_RESTRICTED_FIELD_PATTERN = re.compile(
    r"(^|[_.\-\s])("
    r"password|passwd|secret|token|api[_\-.]?key|private[_\-.]?key|"
    r"client[_\-.]?secret|credential|auth|session|refresh[_\-.]?token|"
    r"access[_\-.]?token|ssn|social[_\-.]?security"
    r")([_.\-\s]|$)",
    re.IGNORECASE,
)
_CONFIDENTIAL_FIELD_PATTERN = re.compile(
    r"(^|[_.\-\s])("
    r"email|e[_\-.]?mail|phone|mobile|address|dob|birth|name|"
    r"first[_\-.]?name|last[_\-.]?name|full[_\-.]?name|contact|"
    r"owner|actor|user|customer|external[_\-.]?id|record[_\-.]?id|id"
    r")([_.\-\s]|$)",
    re.IGNORECASE,
)
_FREE_TEXT_FIELD_PATTERN = re.compile(
    r"(^|[_.\-\s])(note|notes|comment|comments|description|message|body|summary|subject|content)([_.\-\s]|$)",
    re.IGNORECASE,
)

_SOURCE_METADATA_ALIASES = {
    "external_record_id": ("external_record_id", "record_id", "external_id", "id", "source_id"),
    "source_created_at": ("source_created_at", "created_at", "createdAt", "created"),
    "source_updated_at": (
        "source_updated_at",
        "updated_at",
        "updatedAt",
        "modified_at",
        "last_modified_at",
        "lastModifiedAt",
    ),
    "source_actor_ref": ("source_actor_ref", "actor_ref", "actor", "updated_by", "owner_id", "user_id"),
    "source_version_ref": ("source_version_ref", "version", "etag", "revision", "rev"),
    "observed_at": ("observed_at", "captured_at", "seen_at"),
    "extracted_at": ("extracted_at", "pulled_at", "fetched_at"),
}


@dataclass(frozen=True)
class SystemSnapshotRecord:
    """One observed state of a client system."""

    system_snapshot_id: str
    client_ref: str
    system_ref: str
    integration_id: str
    connector_ref: str
    environment_ref: str
    auth_context_hash: str
    captured_at: str
    capture_receipt_id: str
    schema_snapshot_count: int = 0
    sample_count: int = 0
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "kind": "object_truth.system_snapshot.v1",
            "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
            "system_snapshot_id": self.system_snapshot_id,
            "client_ref": self.client_ref,
            "system_ref": self.system_ref,
            "integration_id": self.integration_id,
            "connector_ref": self.connector_ref,
            "environment_ref": self.environment_ref,
            "auth_context_hash": self.auth_context_hash,
            "captured_at": self.captured_at,
            "capture_receipt_id": self.capture_receipt_id,
            "schema_snapshot_count": self.schema_snapshot_count,
            "sample_count": self.sample_count,
            "metadata_json": canonical_value(self.metadata_json),
        }
        payload["system_snapshot_digest"] = canonical_digest(
            payload,
            purpose="object_truth.system_snapshot.v1",
        )
        return payload


@dataclass(frozen=True)
class SampleCaptureRecord:
    """One deterministic sample capture from an external object."""

    sample_id: str
    system_snapshot_id: str
    schema_snapshot_digest: str
    system_ref: str
    object_ref: str
    sample_strategy: SampleStrategy
    source_query_json: dict[str, Any]
    cursor_ref: str | None
    sample_size_requested: int
    sample_size_returned: int
    sample_hash: str
    status: SampleStatus
    receipt_id: str | None
    source_window_json: dict[str, Any] = field(default_factory=dict)
    source_evidence_digest: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "kind": "object_truth.sample_capture.v1",
            "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
            "sample_id": self.sample_id,
            "system_snapshot_id": self.system_snapshot_id,
            "schema_snapshot_digest": self.schema_snapshot_digest,
            "system_ref": self.system_ref,
            "object_ref": self.object_ref,
            "sample_strategy": self.sample_strategy,
            "source_query_json": canonical_value(self.source_query_json),
            "cursor_ref": self.cursor_ref,
            "sample_size_requested": self.sample_size_requested,
            "sample_size_returned": self.sample_size_returned,
            "sample_hash": self.sample_hash,
            "status": self.status,
            "receipt_id": self.receipt_id,
            "source_window_json": canonical_value(self.source_window_json),
            "source_evidence_digest": self.source_evidence_digest,
            "metadata_json": canonical_value(self.metadata_json),
        }
        payload["sample_capture_digest"] = canonical_digest(
            payload,
            purpose="object_truth.sample_capture.v1",
        )
        return payload


def build_system_snapshot_record(
    *,
    client_ref: str,
    system_ref: str,
    integration_id: str,
    connector_ref: str,
    environment_ref: str,
    captured_at: Any,
    capture_receipt_id: str,
    auth_context_hash: str | None = None,
    auth_context: Any | None = None,
    schema_snapshot_count: int = 0,
    sample_count: int = 0,
    metadata: dict[str, Any] | None = None,
    system_snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic system snapshot record without storing credentials."""

    resolved_auth_hash = _optional_text(auth_context_hash)
    if resolved_auth_hash is None:
        if auth_context is None:
            raise ObjectTruthOperationError(
                "object_truth.auth_context_missing",
                "auth_context_hash or auth_context is required for system snapshots",
            )
        resolved_auth_hash = canonical_digest(
            auth_context,
            purpose="object_truth.auth_context.v1",
        )

    basis = {
        "client_ref": _required_text(client_ref, "client_ref"),
        "system_ref": _required_text(system_ref, "system_ref"),
        "integration_id": _required_text(integration_id, "integration_id"),
        "connector_ref": _required_text(connector_ref, "connector_ref"),
        "environment_ref": _required_text(environment_ref, "environment_ref"),
        "auth_context_hash": resolved_auth_hash,
        "captured_at": _normalize_required_datetime(captured_at, "captured_at"),
        "capture_receipt_id": _required_text(capture_receipt_id, "capture_receipt_id"),
        "schema_snapshot_count": _nonnegative_int(schema_snapshot_count, "schema_snapshot_count"),
        "sample_count": _nonnegative_int(sample_count, "sample_count"),
        "metadata_json": canonical_value(metadata or {}),
    }
    snapshot_id = _optional_text(system_snapshot_id) or f"object_truth_system_snapshot.{canonical_digest(basis, purpose='object_truth.system_snapshot_id.v1')[:16]}"
    return SystemSnapshotRecord(system_snapshot_id=snapshot_id, **basis).as_dict()


def build_source_query_evidence(
    *,
    system_ref: str,
    object_ref: str,
    source_query: dict[str, Any] | None = None,
    cursor_ref: str | None = None,
    cursor_value: Any | None = None,
    window_kind: str = "source_updated_at",
    window_start: Any | None = None,
    window_end: Any | None = None,
    limit: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build source query, cursor, and window evidence without cursor contents."""

    query = _optional_mapping(source_query, "source_query")
    cursor_hash = (
        canonical_digest(cursor_value, purpose="object_truth.source_cursor.v1")
        if cursor_value is not None
        else None
    )
    resolved_cursor_ref = _optional_text(cursor_ref)
    if resolved_cursor_ref is None and cursor_hash is not None:
        resolved_cursor_ref = f"object_truth_cursor.{cursor_hash[:16]}"
    requested_limit = _positive_int(limit, "limit") if limit is not None else None
    window = {
        "window_kind": _required_text(window_kind, "window_kind"),
        "window_start": _normalize_optional_datetime(window_start),
        "window_end": _normalize_optional_datetime(window_end),
    }
    source_query_json = {
        "query": canonical_value(query),
        "limit": requested_limit,
        "window": window,
        "cursor_ref": resolved_cursor_ref,
        "cursor_hash": cursor_hash,
    }
    payload = {
        "kind": "object_truth.source_query_evidence.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
        "system_ref": _required_text(system_ref, "system_ref"),
        "object_ref": _required_text(object_ref, "object_ref"),
        "source_query_json": canonical_value(source_query_json),
        "cursor_ref": resolved_cursor_ref,
        "cursor_hash": cursor_hash,
        "source_window_json": canonical_value(window),
        "metadata_json": canonical_value(metadata or {}),
    }
    payload["source_evidence_digest"] = canonical_digest(
        payload,
        purpose="object_truth.source_query_evidence.v1",
    )
    return payload


def build_sample_capture_record(
    *,
    system_snapshot_id: str,
    schema_snapshot_digest: str,
    system_ref: str,
    object_ref: str,
    sample_strategy: SampleStrategy,
    source_evidence: dict[str, Any],
    sample_size_requested: int,
    sample_payloads: list[dict[str, Any]] | None = None,
    sample_size_returned: int | None = None,
    sample_hash: str | None = None,
    status: SampleStatus | None = None,
    receipt_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    sample_id: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic sample capture record without embedding payloads."""

    strategy = _validate_member(sample_strategy, SAMPLE_STRATEGIES, "sample_strategy")
    evidence = _require_mapping(source_evidence, "source_evidence")
    payloads = _optional_payload_list(sample_payloads)
    returned = (
        len(payloads)
        if payloads is not None
        else _nonnegative_int(sample_size_returned, "sample_size_returned")
        if sample_size_returned is not None
        else None
    )
    if returned is None:
        raise ObjectTruthOperationError(
            "object_truth.sample_size_returned_missing",
            "sample_size_returned is required when sample_payloads are not supplied",
        )
    requested = _nonnegative_int(sample_size_requested, "sample_size_requested")
    if returned > requested and requested != 0:
        raise ObjectTruthOperationError(
            "object_truth.sample_returned_exceeds_requested",
            "sample_size_returned cannot exceed sample_size_requested",
            details={"sample_size_requested": requested, "sample_size_returned": returned},
        )
    resolved_sample_hash = _optional_text(sample_hash)
    if payloads is not None:
        resolved_sample_hash = canonical_digest(
            payloads,
            purpose="object_truth.sample_payloads.v1",
        )
    if resolved_sample_hash is None:
        raise ObjectTruthOperationError(
            "object_truth.sample_hash_missing",
            "sample_hash is required when sample_payloads are not supplied",
        )
    resolved_status = _validate_member(
        status or ("empty" if returned == 0 else "captured"),
        SAMPLE_STATUSES,
        "status",
    )

    basis = {
        "system_snapshot_id": _required_text(system_snapshot_id, "system_snapshot_id"),
        "schema_snapshot_digest": _required_text(schema_snapshot_digest, "schema_snapshot_digest"),
        "system_ref": _required_text(system_ref, "system_ref"),
        "object_ref": _required_text(object_ref, "object_ref"),
        "sample_strategy": strategy,
        "source_query_json": canonical_value(evidence.get("source_query_json") or {}),
        "cursor_ref": _optional_text(evidence.get("cursor_ref")),
        "sample_size_requested": requested,
        "sample_size_returned": returned,
        "sample_hash": resolved_sample_hash,
        "status": resolved_status,
        "receipt_id": _optional_text(receipt_id),
        "source_window_json": canonical_value(evidence.get("source_window_json") or {}),
        "source_evidence_digest": _optional_text(evidence.get("source_evidence_digest")),
        "metadata_json": canonical_value(metadata or {}),
    }
    resolved_sample_id = _optional_text(sample_id) or f"object_truth_sample.{canonical_digest(basis, purpose='object_truth.sample_id.v1')[:16]}"
    return SampleCaptureRecord(sample_id=resolved_sample_id, **basis).as_dict()


def normalize_ingestion_source_metadata(
    metadata: dict[str, Any] | None = None,
    *,
    raw_payload_reference: dict[str, Any] | None = None,
    redacted_preview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize source-aware object version metadata for ingestion evidence."""

    raw = _optional_mapping(metadata, "metadata")
    used_keys: set[str] = set()
    normalized: dict[str, Any] = {
        "kind": "object_truth.source_metadata.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
    }
    for canonical_name, aliases in _SOURCE_METADATA_ALIASES.items():
        used_keys.update(alias for alias in aliases if alias in raw)
        value, used_key = _first_present(raw, aliases)
        if used_key is not None:
            used_keys.add(used_key)
        if canonical_name.endswith("_at"):
            normalized[canonical_name] = _normalize_optional_datetime(value)
        else:
            normalized[canonical_name] = _optional_text(value)

    raw_ref = _optional_mapping(raw_payload_reference, "raw_payload_reference")
    preview = _optional_mapping(redacted_preview, "redacted_preview")
    normalized.update(
        {
            "raw_payload_hash": _optional_text(raw_ref.get("raw_payload_hash") or raw.get("raw_payload_hash")),
            "normalized_payload_hash": _optional_text(
                raw_ref.get("normalized_payload_hash") or raw.get("normalized_payload_hash")
            ),
            "raw_payload_ref": _optional_text(raw_ref.get("raw_payload_ref") or raw.get("raw_payload_ref")),
            "privacy_classification": _normalize_privacy_classification(
                raw_ref.get("privacy_classification") or raw.get("privacy_classification") or "internal"
            ),
            "retention_policy_ref": _optional_text(
                raw_ref.get("retention_policy_ref") or raw.get("retention_policy_ref")
            ),
            "redacted_preview_digest": _optional_text(
                preview.get("preview_digest") or raw.get("redacted_preview_digest")
            ),
            "metadata_json": canonical_value(
                {
                    str(key): raw[key]
                    for key in sorted(raw, key=str)
                    if key not in used_keys
                    and key
                    not in {
                        "raw_payload_hash",
                        "normalized_payload_hash",
                        "raw_payload_ref",
                        "privacy_classification",
                        "retention_policy_ref",
                        "redacted_preview_digest",
                    }
                }
            ),
        }
    )
    normalized["source_metadata_digest"] = canonical_digest(
        normalized,
        purpose="object_truth.source_metadata.v1",
    )
    return normalized


def build_raw_payload_reference(
    *,
    raw_payload: Any | None = None,
    raw_payload_ref: str | None = None,
    privacy_classification: PrivacyClassification = "internal",
    retention_policy_ref: str | None = None,
    privacy_policy_ref: str | None = None,
    inline_payload_approved: bool = False,
) -> dict[str, Any]:
    """Build a reference-first raw payload policy record.

    Raw payload content is omitted unless an explicit privacy policy and
    retention policy approve inline handling.
    """

    classification = _normalize_privacy_classification(privacy_classification)
    retention_ref = _optional_text(retention_policy_ref)
    policy_ref = _optional_text(privacy_policy_ref)
    if inline_payload_approved and (not policy_ref or not retention_ref):
        raise ObjectTruthOperationError(
            "object_truth.raw_payload_policy_missing",
            "inline raw payload handling requires privacy_policy_ref and retention_policy_ref",
            details={
                "privacy_policy_ref_present": bool(policy_ref),
                "retention_policy_ref_present": bool(retention_ref),
            },
        )

    payload = {
        "kind": "object_truth.raw_payload_reference.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
        "raw_payload_ref": _optional_text(raw_payload_ref),
        "raw_payload_hash": (
            canonical_digest(raw_payload, purpose="object_truth.raw_payload.v1")
            if raw_payload is not None
            else None
        ),
        "normalized_payload_hash": (
            canonical_digest(canonical_value(raw_payload), purpose="object_truth.normalized_payload.v1")
            if raw_payload is not None
            else None
        ),
        "privacy_classification": classification,
        "retention_policy_ref": retention_ref,
        "privacy_policy_ref": policy_ref,
        "inline_payload_stored": bool(inline_payload_approved and raw_payload is not None),
        "policy": {
            "default_storage_posture": "reference_only",
            "raw_payload_content": "included_by_explicit_policy" if inline_payload_approved else "omitted",
        },
    }
    if inline_payload_approved and raw_payload is not None:
        payload["raw_payload_json"] = canonical_value(raw_payload)
    payload["reference_digest"] = canonical_digest(
        payload,
        purpose="object_truth.raw_payload_reference.v1",
    )
    return payload


def build_redacted_preview(
    payload: Any,
    *,
    policy: dict[str, Any] | None = None,
    privacy_policy_ref: str | None = None,
    max_string_preview: int = 64,
) -> dict[str, Any]:
    """Build a structure-preserving redacted preview for client payload evidence."""

    if max_string_preview < 0:
        raise ObjectTruthOperationError(
            "object_truth.invalid_preview_limit",
            "max_string_preview must be non-negative",
            details={"max_string_preview": max_string_preview},
        )
    redaction_count = 0
    classifications: list[dict[str, Any]] = []

    def walk(value: Any, path: str, inherited: PrivacyClassification | None = None) -> Any:
        nonlocal redaction_count
        classification = inherited or classify_preview_field(path, value, policy=policy)
        redacted = classification in {"confidential", "restricted"}
        if path:
            classifications.append(
                {
                    "field_path": path,
                    "classification": classification,
                    "redacted": redacted,
                    "value_kind": _value_kind(value),
                }
            )
        child_inherited = classification if redacted else None
        if isinstance(value, dict):
            return {
                str(key): walk(value[key], _join_path(path, str(key)), child_inherited)
                for key in sorted(value, key=str)
            }
        if isinstance(value, (list, tuple)):
            return [
                walk(item, f"{path}[{index}]" if path else f"[{index}]", child_inherited)
                for index, item in enumerate(value)
            ]
        if redacted:
            redaction_count += 1
            return _redaction_marker(value, classification)
        return _safe_preview_value(value, max_string_preview=max_string_preview)

    preview_json = walk(payload, "")
    result = {
        "kind": "object_truth.redacted_preview.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
        "privacy_policy_ref": _optional_text(privacy_policy_ref),
        "preview_json": canonical_value(preview_json),
        "field_classifications": sorted(classifications, key=lambda item: item["field_path"]),
        "redaction_count": redaction_count,
    }
    result["preview_digest"] = canonical_digest(
        result,
        purpose="object_truth.redacted_preview.v1",
    )
    return result


def classify_preview_field(
    field_path: str,
    value: Any,
    *,
    policy: dict[str, Any] | None = None,
) -> PrivacyClassification:
    """Classify one preview field using policy first, then deterministic patterns."""

    path = str(field_path or "").strip()
    policy_classification = _policy_classification(path, policy or {})
    if policy_classification:
        return policy_classification
    normalized_path = path.replace("[", ".").replace("]", "")
    if _RESTRICTED_FIELD_PATTERN.search(normalized_path):
        return "restricted"
    if isinstance(value, str) and _FREE_TEXT_FIELD_PATTERN.search(normalized_path):
        return "confidential"
    if _CONFIDENTIAL_FIELD_PATTERN.search(normalized_path):
        return "confidential"
    return "internal"


def build_ingestion_replay_fixture(
    *,
    system_snapshot: dict[str, Any],
    samples: list[dict[str, Any]],
    object_versions: list[dict[str, Any]] | None = None,
    schema_snapshots: list[dict[str, Any]] | None = None,
    fixture_ref: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a replayable ingestion fixture without requiring filesystem IO."""

    snapshot = _require_mapping(system_snapshot, "system_snapshot")
    sample_records = [_require_mapping(item, "sample") for item in samples]
    object_version_records = [_require_mapping(item, "object_version") for item in (object_versions or [])]
    schema_records = [_require_mapping(item, "schema_snapshot") for item in (schema_snapshots or [])]
    sorted_samples = sorted(
        (canonical_value(item) for item in sample_records),
        key=lambda item: str(item.get("sample_id") or item.get("sample_capture_digest") or ""),
    )
    sorted_versions = sorted(
        (canonical_value(item) for item in object_version_records),
        key=lambda item: str(item.get("object_version_digest") or ""),
    )
    sorted_schemas = sorted(
        (canonical_value(item) for item in schema_records),
        key=lambda item: str(item.get("schema_digest") or ""),
    )
    basis = {
        "system_snapshot_id": snapshot.get("system_snapshot_id"),
        "samples": sorted_samples,
        "object_versions": sorted_versions,
        "schema_snapshots": sorted_schemas,
    }
    resolved_fixture_ref = _optional_text(fixture_ref) or f"object_truth_replay_fixture.{canonical_digest(basis, purpose='object_truth.replay_fixture_ref.v1')[:16]}"
    payload = {
        "kind": "object_truth.ingestion_replay_fixture.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
        "fixture_ref": resolved_fixture_ref,
        "system_ref": _optional_text(snapshot.get("system_ref")),
        "system_snapshot": canonical_value(snapshot),
        "samples": sorted_samples,
        "schema_snapshots": sorted_schemas,
        "object_versions": sorted_versions,
        "sample_strategies": sorted(
            {
                str(item.get("sample_strategy"))
                for item in sorted_samples
                if item.get("sample_strategy")
            }
        ),
        "object_refs": sorted(
            {
                str(item.get("object_ref"))
                for item in [*sorted_samples, *sorted_versions, *sorted_schemas]
                if item.get("object_ref")
            }
        ),
        "metadata_json": canonical_value(metadata or {}),
    }
    payload["fixture_digest"] = canonical_digest(
        payload,
        purpose="object_truth.ingestion_replay_fixture.v1",
    )
    return payload


def build_readiness_inputs(
    *,
    client_payload_mode: str = "redacted_hashes",
    privacy_policy_ref: str | None = None,
    planned_fanout: int | None = None,
    include_counts: bool = True,
    system_snapshots: list[dict[str, Any]] | None = None,
    sample_records: list[dict[str, Any]] | None = None,
    required_connector_refs: list[str] | None = None,
    required_source_refs: list[str] | None = None,
    max_evidence_age_hours: int | None = None,
) -> dict[str, Any]:
    """Build the fail-closed input packet for the Object Truth readiness query."""

    if client_payload_mode not in {"redacted_hashes", "raw_client_payloads"}:
        raise ObjectTruthOperationError(
            "object_truth.invalid_client_payload_mode",
            "client_payload_mode must be redacted_hashes or raw_client_payloads",
            details={"client_payload_mode": client_payload_mode},
        )
    snapshots = [_require_mapping(item, "system_snapshot") for item in (system_snapshots or [])]
    samples = [_require_mapping(item, "sample") for item in (sample_records or [])]
    fanout = _positive_int(planned_fanout, "planned_fanout") if planned_fanout is not None else max(1, len(samples))
    payload: dict[str, Any] = {
        "client_payload_mode": client_payload_mode,
        "planned_fanout": fanout,
        "include_counts": bool(include_counts),
    }
    policy_ref = _optional_text(privacy_policy_ref)
    if policy_ref:
        payload["privacy_policy_ref"] = policy_ref

    connector_refs = set(_normalized_text_list(required_connector_refs or [], "required_connector_refs"))
    connector_refs.update(
        str(item.get("connector_ref"))
        for item in snapshots
        if item.get("connector_ref")
    )
    source_refs = set(_normalized_text_list(required_source_refs or [], "required_source_refs"))
    source_refs.update(
        str(item.get("source_evidence_digest"))
        for item in samples
        if item.get("source_evidence_digest")
    )
    requirements = {
        "system_snapshot_count": len(snapshots),
        "sample_count": len(samples),
        "system_refs": sorted(
            {
                str(item.get("system_ref"))
                for item in [*snapshots, *samples]
                if item.get("system_ref")
            }
        ),
        "object_refs": sorted(
            {
                str(item.get("object_ref"))
                for item in samples
                if item.get("object_ref")
            }
        ),
        "sample_strategies": sorted(
            {
                str(item.get("sample_strategy"))
                for item in samples
                if item.get("sample_strategy")
            }
        ),
        "connector_refs": sorted(connector_refs),
        "source_refs": sorted(source_refs),
        "max_evidence_age_hours": (
            _positive_int(max_evidence_age_hours, "max_evidence_age_hours")
            if max_evidence_age_hours is not None
            else None
        ),
    }
    result = {
        "kind": "object_truth.readiness_inputs.v1",
        "schema_version": OBJECT_TRUTH_INGESTION_SCHEMA_VERSION,
        "operation_name": "object_truth_readiness",
        "tool_ref": "praxis_object_truth_readiness",
        "payload": payload,
        "fail_closed_states": list(FAIL_CLOSED_READINESS_STATES),
        "ingestion_requirements": requirements,
    }
    result["readiness_input_digest"] = canonical_digest(
        result,
        purpose="object_truth.readiness_inputs.v1",
    )
    return result


def _policy_classification(
    field_path: str,
    policy: dict[str, Any],
) -> PrivacyClassification | None:
    field_classifications = policy.get("field_classifications")
    if isinstance(field_classifications, dict):
        for pattern, classification in sorted(field_classifications.items(), key=lambda item: str(item[0])):
            if _matches_path(str(pattern), field_path):
                return _normalize_privacy_classification(classification)
    for classification, key in (
        ("restricted", "restricted_fields"),
        ("confidential", "confidential_fields"),
        ("internal", "internal_fields"),
        ("public", "public_fields"),
    ):
        values = policy.get(key)
        if isinstance(values, list):
            for pattern in values:
                if _matches_path(str(pattern), field_path):
                    return classification  # type: ignore[return-value]
    return None


def _matches_path(pattern: str, field_path: str) -> bool:
    if not pattern:
        return False
    if fnmatchcase(field_path, pattern):
        return True
    return field_path == pattern or field_path.endswith(f".{pattern}")


def _redaction_marker(value: Any, classification: PrivacyClassification) -> dict[str, Any]:
    return {
        "redacted": True,
        "classification": classification,
        "value_kind": _value_kind(value),
        "value_digest": canonical_digest(value, purpose="object_truth.redacted_value.v1"),
    }


def _safe_preview_value(value: Any, *, max_string_preview: int) -> Any:
    if isinstance(value, str) and len(value) > max_string_preview:
        return {
            "preview": value[:max_string_preview],
            "truncated": True,
            "value_digest": canonical_digest(value, purpose="object_truth.preview_string.v1"),
        }
    return canonical_value(value)


def _value_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    return "text"


def _join_path(parent: str, key: str) -> str:
    return f"{parent}.{key}" if parent else key


def _first_present(
    payload: dict[str, Any],
    aliases: tuple[str, ...],
) -> tuple[Any | None, str | None]:
    for alias in aliases:
        value = payload.get(alias)
        if alias in payload and not _is_unknown_marker(value):
            return value, alias
    return None, None


def _is_unknown_marker(value: Any) -> bool:
    if value is None:
        return True
    return isinstance(value, str) and value.strip().lower() in _UNKNOWN_MARKERS


def _normalize_privacy_classification(value: Any) -> PrivacyClassification:
    text = str(value or "internal").strip().lower()
    if text not in PRIVACY_CLASSIFICATIONS:
        raise ObjectTruthOperationError(
            "object_truth.invalid_privacy_classification",
            "privacy_classification must be public, internal, confidential, or restricted",
            details={"privacy_classification": value},
        )
    return text  # type: ignore[return-value]


def _validate_member(value: Any, allowed: set[str], field_name: str) -> Any:
    text = _required_text(value, field_name)
    if text not in allowed:
        raise ObjectTruthOperationError(
            "object_truth.invalid_enum",
            f"{field_name} is not valid",
            details={"field_name": field_name, "value": text, "allowed": sorted(allowed)},
        )
    return text


def _optional_payload_list(value: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ObjectTruthOperationError(
            "object_truth.sample_payloads_not_objects",
            "sample_payloads must be a list of JSON objects",
        )
    return [dict(item) for item in value]


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ObjectTruthOperationError(
            "object_truth.mapping_required",
            f"{field_name} must be a JSON object",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _optional_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return _require_mapping(value, field_name)


def _normalized_text_list(values: list[str], field_name: str) -> list[str]:
    if not isinstance(values, list):
        raise ObjectTruthOperationError(
            "object_truth.invalid_string_list",
            f"{field_name} must be a list of strings",
            details={"field_name": field_name, "value_type": type(values).__name__},
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            normalized.append(text)
            seen.add(text)
    return normalized


def _required_text(value: Any, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ObjectTruthOperationError(
            "object_truth.required_text_missing",
            f"{field_name} is required",
            details={"field_name": field_name},
        )
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ObjectTruthOperationError(
            "object_truth.invalid_integer",
            f"{field_name} must be an integer",
            details={"field_name": field_name, "value": value},
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ObjectTruthOperationError(
            "object_truth.invalid_integer",
            f"{field_name} must be an integer",
            details={"field_name": field_name, "value": value},
        ) from exc
    if parsed < 0:
        raise ObjectTruthOperationError(
            "object_truth.invalid_integer",
            f"{field_name} must be non-negative",
            details={"field_name": field_name, "value": value},
        )
    return parsed


def _positive_int(value: Any, field_name: str) -> int:
    parsed = _nonnegative_int(value, field_name)
    if parsed < 1:
        raise ObjectTruthOperationError(
            "object_truth.invalid_integer",
            f"{field_name} must be greater than zero",
            details={"field_name": field_name, "value": value},
        )
    return parsed


def _normalize_required_datetime(value: Any, field_name: str) -> str:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ObjectTruthOperationError(
            "object_truth.invalid_datetime",
            f"{field_name} must be an ISO datetime",
            details={"field_name": field_name, "value": value},
        )
    return _iso_datetime(parsed)


def _normalize_optional_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in _UNKNOWN_MARKERS:
        return None
    parsed = _parse_datetime(value)
    return _iso_datetime(parsed) if parsed is not None else _optional_text(value)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso_datetime(value: datetime) -> str:
    parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "FAIL_CLOSED_READINESS_STATES",
    "OBJECT_TRUTH_INGESTION_SCHEMA_VERSION",
    "PRIVACY_CLASSIFICATIONS",
    "SAMPLE_STATUSES",
    "SAMPLE_STRATEGIES",
    "SampleCaptureRecord",
    "SampleStatus",
    "SampleStrategy",
    "SystemSnapshotRecord",
    "PrivacyClassification",
    "build_ingestion_replay_fixture",
    "build_raw_payload_reference",
    "build_readiness_inputs",
    "build_redacted_preview",
    "build_sample_capture_record",
    "build_source_query_evidence",
    "build_system_snapshot_record",
    "classify_preview_field",
    "normalize_ingestion_source_metadata",
]
