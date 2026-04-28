"""Deterministic object-truth primitives.

This module owns pure parsing, hashing, identity, field observation, and
comparison logic for cross-system object truth work. It does not perform IO,
call models, or decide business truth.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import re
from typing import Any


OBJECT_TRUTH_SCHEMA_VERSION = 1

SENSITIVE_FIELD_PATTERN = re.compile(
    r"(^|[_\-.])(password|passwd|secret|token|api[_\-.]?key|private[_\-.]?key|ssn|social[_\-.]?security)([_\-.]|$)",
    re.IGNORECASE,
)


class ObjectTruthOperationError(RuntimeError):
    """Raised when object-truth evidence cannot be represented safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


def canonical_value(value: Any) -> Any:
    """Return a JSON-safe value with stable key ordering."""

    if isinstance(value, dict):
        return {str(key): canonical_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, datetime):
        return _iso_datetime(value)
    return value


def canonical_digest(value: Any, *, purpose: str = "object_truth") -> str:
    """Return a purpose-scoped sha256 digest for deterministic evidence."""

    payload = {
        "purpose": purpose,
        "value": canonical_value(value),
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def normalize_schema_snapshot(
    raw_schema: dict[str, Any] | list[dict[str, Any]],
    *,
    system_ref: str,
    object_ref: str,
) -> dict[str, Any]:
    """Normalize an external schema into sorted, hashable field metadata."""

    system = _required_text(system_ref, "system_ref")
    obj = _required_text(object_ref, "object_ref")
    fields = _extract_schema_fields(raw_schema)
    normalized_fields = sorted(
        (_normalize_schema_field(field) for field in fields),
        key=lambda item: item["field_path"],
    )
    payload = {
        "schema_version": OBJECT_TRUTH_SCHEMA_VERSION,
        "system_ref": system,
        "object_ref": obj,
        "fields": normalized_fields,
    }
    payload["schema_digest"] = canonical_digest(payload, purpose="object_truth.schema_snapshot.v1")
    return payload


def build_object_version(
    *,
    system_ref: str,
    object_ref: str,
    record: dict[str, Any],
    identity_fields: list[str],
    source_metadata: dict[str, Any] | None = None,
    schema_snapshot_digest: str | None = None,
) -> dict[str, Any]:
    """Build one deterministic object-version evidence packet from a source record."""

    if not isinstance(record, dict):
        raise ObjectTruthOperationError(
            "object_truth.record_not_object",
            "object truth records must be JSON objects",
            details={"record_type": type(record).__name__},
        )
    system = _required_text(system_ref, "system_ref")
    obj = _required_text(object_ref, "object_ref")
    identity = build_identity(record, identity_fields)
    observations = extract_field_observations(record)
    metadata = normalize_source_metadata(source_metadata or {})
    payload = {
        "kind": "object_truth.object_version.v1",
        "schema_version": OBJECT_TRUTH_SCHEMA_VERSION,
        "system_ref": system,
        "object_ref": obj,
        "identity": identity,
        "payload_digest": canonical_digest(record, purpose="object_truth.record_payload.v1"),
        "schema_snapshot_digest": _optional_text(schema_snapshot_digest),
        "source_metadata": metadata,
        "field_observations": observations,
        "hierarchy_signals": detect_hierarchy_signals(record),
    }
    payload["object_version_digest"] = canonical_digest(payload, purpose="object_truth.object_version.v1")
    return payload


def build_identity(record: dict[str, Any], identity_fields: list[str]) -> dict[str, Any]:
    """Build a stable identity key from required field paths."""

    fields = _normalized_field_list(identity_fields, field_name="identity_fields")
    values: dict[str, Any] = {}
    missing: list[str] = []
    for field_path in fields:
        found, value = _get_path(record, field_path)
        if not found or _is_empty(value):
            missing.append(field_path)
        else:
            values[field_path] = canonical_value(value)
    if missing:
        raise ObjectTruthOperationError(
            "object_truth.identity_missing_fields",
            "identity fields must all be present and non-empty",
            details={"missing_fields": missing},
        )
    return {
        "identity_fields": fields,
        "identity_values": values,
        "identity_digest": canonical_digest(values, purpose="object_truth.identity.v1"),
    }


def normalize_source_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Normalize source metadata that will later support freshness and lineage scoring."""

    if not isinstance(metadata, dict):
        raise ObjectTruthOperationError(
            "object_truth.metadata_not_object",
            "source metadata must be an object",
            details={"metadata_type": type(metadata).__name__},
        )
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        text_key = str(key).strip()
        if not text_key:
            continue
        if text_key.endswith("_at") or text_key in {"created_at", "updated_at", "observed_at", "extracted_at"}:
            normalized[text_key] = _normalize_datetime_or_text(value)
        else:
            normalized[text_key] = canonical_value(value)
    return {key: normalized[key] for key in sorted(normalized)}


def extract_field_observations(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract deterministic field-level evidence from one JSON object."""

    if not isinstance(record, dict):
        raise ObjectTruthOperationError(
            "object_truth.record_not_object",
            "field observations require a JSON object record",
            details={"record_type": type(record).__name__},
        )
    observations: list[dict[str, Any]] = []
    _walk_record(record, parent_path="", observations=observations)
    return sorted(observations, key=lambda item: item["field_path"])


def detect_hierarchy_signals(record: dict[str, Any]) -> dict[str, Any]:
    """Summarize hierarchy/flattening clues without making business decisions."""

    observations = extract_field_observations(record)
    object_paths = [item["field_path"] for item in observations if item["field_kind"] == "object"]
    array_paths = [item["field_path"] for item in observations if item["field_kind"] == "array"]
    dotted_leaf_paths = [
        item["field_path"]
        for item in observations
        if "." in item["field_path"] and item["field_kind"] not in {"object", "array"}
    ]
    literal_flattened_keys = [
        key
        for key in sorted(record, key=str)
        if isinstance(key, str) and ("." in key or "__" in key)
    ]
    return {
        "has_nested_objects": bool(object_paths),
        "has_arrays": bool(array_paths),
        "nested_object_paths": object_paths,
        "array_paths": array_paths,
        "dotted_leaf_paths": dotted_leaf_paths,
        "literal_flattened_keys": literal_flattened_keys,
    }


def compare_object_versions(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Compare two object-version packets by field observations and freshness hints."""

    left_fields = _observation_map(left)
    right_fields = _observation_map(right)
    comparisons: list[dict[str, Any]] = []
    summary = {
        "matching_fields": 0,
        "different_fields": 0,
        "missing_left_fields": 0,
        "missing_right_fields": 0,
    }
    for field_path in sorted(set(left_fields) | set(right_fields)):
        left_obs = left_fields.get(field_path)
        right_obs = right_fields.get(field_path)
        if left_obs is None:
            status = "missing_left"
            summary["missing_left_fields"] += 1
        elif right_obs is None:
            status = "missing_right"
            summary["missing_right_fields"] += 1
        elif left_obs["normalized_value_digest"] == right_obs["normalized_value_digest"]:
            status = "match"
            summary["matching_fields"] += 1
        else:
            status = "different"
            summary["different_fields"] += 1
        comparisons.append(
            {
                "field_path": field_path,
                "status": status,
                "left": _comparison_projection(left_obs),
                "right": _comparison_projection(right_obs),
            }
        )

    freshness = compare_freshness(left.get("source_metadata", {}), right.get("source_metadata", {}))
    payload = {
        "kind": "object_truth.object_version_comparison.v1",
        "schema_version": OBJECT_TRUTH_SCHEMA_VERSION,
        "left_identity_digest": left.get("identity", {}).get("identity_digest"),
        "right_identity_digest": right.get("identity", {}).get("identity_digest"),
        "left_object_version_digest": left.get("object_version_digest"),
        "right_object_version_digest": right.get("object_version_digest"),
        "summary": summary,
        "freshness": freshness,
        "field_comparisons": comparisons,
    }
    payload["comparison_digest"] = canonical_digest(payload, purpose="object_truth.comparison.v1")
    return payload


def compare_freshness(left_metadata: dict[str, Any], right_metadata: dict[str, Any]) -> dict[str, Any]:
    """Compare source updated_at metadata without assigning business truth."""

    left_dt = _parse_datetime(left_metadata.get("updated_at") if isinstance(left_metadata, dict) else None)
    right_dt = _parse_datetime(right_metadata.get("updated_at") if isinstance(right_metadata, dict) else None)
    if left_dt is None or right_dt is None:
        state = "unknown"
    elif left_dt > right_dt:
        state = "left_newer"
    elif right_dt > left_dt:
        state = "right_newer"
    else:
        state = "same"
    return {
        "state": state,
        "left_updated_at": _iso_datetime(left_dt) if left_dt else None,
        "right_updated_at": _iso_datetime(right_dt) if right_dt else None,
    }


def build_task_environment_contract(
    *,
    task_type: str,
    authority_inputs: dict[str, Any],
    allowed_model_routes: list[str],
    tool_refs: list[str],
    object_version_refs: list[str] | None = None,
    sop_refs: list[str] | None = None,
    policy_refs: list[str] | None = None,
    previous_contract_digest: str | None = None,
    failure_pattern_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Build an append-only task environment contract candidate."""

    task = _required_text(task_type, "task_type")
    if not isinstance(authority_inputs, dict):
        raise ObjectTruthOperationError(
            "object_truth.contract_inputs_not_object",
            "authority_inputs must be an object",
            details={"authority_inputs_type": type(authority_inputs).__name__},
        )
    contract = {
        "kind": "object_truth.task_environment_contract.v1",
        "schema_version": OBJECT_TRUTH_SCHEMA_VERSION,
        "task_type": task,
        "authority_inputs": canonical_value(authority_inputs),
        "allowed_model_routes": _normalized_field_list(allowed_model_routes, field_name="allowed_model_routes"),
        "tool_refs": _normalized_field_list(tool_refs, field_name="tool_refs"),
        "object_version_refs": _normalized_field_list(object_version_refs or [], field_name="object_version_refs"),
        "sop_refs": _normalized_field_list(sop_refs or [], field_name="sop_refs"),
        "policy_refs": _normalized_field_list(policy_refs or [], field_name="policy_refs"),
        "previous_contract_digest": _optional_text(previous_contract_digest),
        "failure_pattern_refs": _normalized_field_list(failure_pattern_refs or [], field_name="failure_pattern_refs"),
    }
    contract["contract_digest"] = canonical_digest(contract, purpose="object_truth.task_environment_contract.v1")
    return contract


def _walk_record(value: Any, *, parent_path: str, observations: list[dict[str, Any]]) -> None:
    if parent_path:
        observations.append(_field_observation(parent_path, value))
    if isinstance(value, dict):
        for key in sorted(value, key=str):
            child_path = f"{parent_path}.{key}" if parent_path else str(key)
            _walk_record(value[key], parent_path=child_path, observations=observations)


def _field_observation(field_path: str, value: Any) -> dict[str, Any]:
    return {
        "field_path": field_path,
        "field_kind": infer_field_kind(value),
        "presence": "empty" if _is_empty(value) else "present",
        "cardinality_kind": _cardinality_kind(value),
        "cardinality_count": len(value) if isinstance(value, (dict, list, tuple)) else None,
        "sensitive": _is_sensitive_field(field_path),
        "normalized_value_digest": canonical_digest(value, purpose="object_truth.field_value.v1"),
        "redacted_value_preview": _redacted_preview(field_path, value),
    }


def infer_field_kind(value: Any) -> str:
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
    if isinstance(value, str):
        if _parse_datetime(value) is not None:
            return "datetime"
        return "text"
    return "text"


def _extract_schema_fields(raw_schema: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(raw_schema, list):
        return [dict(item) for item in raw_schema if isinstance(item, dict)]
    if not isinstance(raw_schema, dict):
        raise ObjectTruthOperationError(
            "object_truth.schema_not_object",
            "schema snapshots must be objects or lists of field objects",
            details={"schema_type": type(raw_schema).__name__},
        )
    if isinstance(raw_schema.get("fields"), list):
        return [dict(item) for item in raw_schema["fields"] if isinstance(item, dict)]
    properties = raw_schema.get("properties")
    if isinstance(properties, dict):
        required = raw_schema.get("required") if isinstance(raw_schema.get("required"), list) else []
        return [
            {
                "field_path": str(name),
                "field_kind": spec.get("type") if isinstance(spec, dict) else None,
                "required": str(name) in required,
                "metadata": spec if isinstance(spec, dict) else {},
            }
            for name, spec in properties.items()
        ]
    return [
        {
            "field_path": str(name),
            "field_kind": spec.get("type") if isinstance(spec, dict) else None,
            "metadata": spec if isinstance(spec, dict) else {},
        }
        for name, spec in raw_schema.items()
    ]


def _normalize_schema_field(field: dict[str, Any]) -> dict[str, Any]:
    path = _required_text(
        field.get("field_path") or field.get("path") or field.get("name"),
        "field_path",
    )
    kind = str(field.get("field_kind") or field.get("type") or "unknown").strip().lower() or "unknown"
    normalized = {
        "field_path": path,
        "field_kind": kind,
        "required": bool(field.get("required", False)),
        "metadata": canonical_value(field.get("metadata") if isinstance(field.get("metadata"), dict) else {}),
    }
    normalized["field_digest"] = canonical_digest(normalized, purpose="object_truth.schema_field.v1")
    return normalized


def _observation_map(version: dict[str, Any]) -> dict[str, dict[str, Any]]:
    observations = version.get("field_observations")
    if not isinstance(observations, list):
        raise ObjectTruthOperationError(
            "object_truth.version_missing_observations",
            "object version comparisons require field_observations",
        )
    result: dict[str, dict[str, Any]] = {}
    for item in observations:
        if isinstance(item, dict) and item.get("field_path"):
            result[str(item["field_path"])] = item
    return result


def _comparison_projection(observation: dict[str, Any] | None) -> dict[str, Any] | None:
    if observation is None:
        return None
    return {
        "field_kind": observation.get("field_kind"),
        "presence": observation.get("presence"),
        "cardinality_kind": observation.get("cardinality_kind"),
        "normalized_value_digest": observation.get("normalized_value_digest"),
        "redacted_value_preview": observation.get("redacted_value_preview"),
    }


def _normalized_field_list(values: list[str], *, field_name: str) -> list[str]:
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
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    if not normalized and field_name in {"identity_fields", "allowed_model_routes", "tool_refs"}:
        raise ObjectTruthOperationError(
            "object_truth.required_list_empty",
            f"{field_name} must contain at least one value",
            details={"field_name": field_name},
        )
    return normalized


def _get_path(record: dict[str, Any], field_path: str) -> tuple[bool, Any]:
    current: Any = record
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def _cardinality_kind(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return "many"
    if isinstance(value, dict):
        return "object"
    if _is_empty(value):
        return "empty"
    return "one"


def _is_sensitive_field(field_path: str) -> bool:
    return SENSITIVE_FIELD_PATTERN.search(field_path) is not None


def _redacted_preview(field_path: str, value: Any) -> Any:
    if _is_sensitive_field(field_path):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {"keys": sorted(str(key) for key in value.keys())[:10]}
    if isinstance(value, (list, tuple)):
        return {"count": len(value)}
    if isinstance(value, str):
        return value[:64]
    return value


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_datetime_or_text(value: Any) -> Any:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return _iso_datetime(parsed)
    return canonical_value(value)


def _iso_datetime(value: datetime) -> str:
    parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
