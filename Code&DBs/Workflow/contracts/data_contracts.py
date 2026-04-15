"""Deterministic data-job contracts.

This module defines the machine-facing payload truth for deterministic data
cleanup and reconciliation work. It owns normalization and validation of the
job shape, not file IO or execution.
"""

from __future__ import annotations

from hashlib import sha256
import json
from typing import Any


DATA_JOB_SCHEMA_VERSION = 1
SUPPORTED_DATA_OPERATIONS = frozenset(
    {
        "parse",
        "profile",
        "filter",
        "sort",
        "normalize",
        "repair",
        "backfill",
        "redact",
        "checkpoint",
        "replay",
        "validate",
        "transform",
        "join",
        "merge",
        "aggregate",
        "split",
        "export",
        "dead_letter",
        "dedupe",
        "reconcile",
        "sync",
    }
)


class DataContractError(RuntimeError):
    """Raised when a deterministic data job cannot be represented safely."""


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _mapping_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        values = [str(part).strip() for part in value]
    else:
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for item in values:
        if not item or item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def _boolean(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return default


def _normalize_source(
    payload: dict[str, Any],
    *,
    source_field: str,
    path_field: str,
    records_field: str,
    format_field: str,
) -> dict[str, Any]:
    source = _mapping(payload.get(source_field))
    if not source:
        path_value = _text(payload.get(path_field))
        records_value = payload.get(records_field)
        format_value = _text(payload.get(format_field))
        if path_value:
            source = {"path": path_value}
        elif records_value is not None:
            source = {"records": records_value}
        if format_value:
            source["format"] = format_value
    if not source:
        return {}

    normalized: dict[str, Any] = {}
    path_value = _text(source.get("path"))
    if path_value:
        normalized["path"] = path_value
    if "records" in source:
        normalized["records"] = source.get("records")
    format_value = _text(source.get("format"))
    if format_value:
        normalized["format"] = format_value
    return normalized


def normalize_data_job(
    payload: dict[str, Any],
    *,
    default_operation: str | None = None,
) -> dict[str, Any]:
    """Normalize one deterministic data job and fail closed on ambiguity."""

    raw = dict(payload)
    operation = _text(raw.get("operation") or default_operation).lower()
    if not operation:
        raise DataContractError("data job requires an operation")
    if operation not in SUPPORTED_DATA_OPERATIONS:
        raise DataContractError(f"unsupported data operation: {operation}")

    input_ref = _normalize_source(
        raw,
        source_field="input",
        path_field="input_path",
        records_field="records",
        format_field="input_format",
    )
    if not input_ref:
        raise DataContractError("data job requires an input source")

    secondary_input = _normalize_source(
        raw,
        source_field="secondary_input",
        path_field="secondary_input_path",
        records_field="secondary_records",
        format_field="secondary_input_format",
    )
    if operation == "reconcile" and not secondary_input:
        raise DataContractError("reconcile jobs require secondary_input or secondary_input_path")

    output = _mapping(raw.get("output"))
    if not output:
        output_path = _text(raw.get("output_path"))
        output_format = _text(raw.get("output_format"))
        receipt_path = _text(raw.get("receipt_path"))
        if output_path:
            output["path"] = output_path
        if output_format:
            output["format"] = output_format
        if receipt_path:
            output["receipt_path"] = receipt_path
    normalized_output: dict[str, Any] = {}
    for key in ("path", "format", "receipt_path"):
        value = _text(output.get(key))
        if value:
            normalized_output[key] = value

    rules = _mapping(raw.get("rules"))
    repairs = _mapping(raw.get("repairs"))
    backfill = _mapping(raw.get("backfill"))
    redactions = _mapping(raw.get("redactions"))
    schema = _mapping(raw.get("schema"))
    checks = _mapping_list(raw.get("checks"))
    mapping = _mapping(raw.get("mapping"))
    checkpoint = _mapping(raw.get("checkpoint"))
    checkpoint_path = _text(raw.get("checkpoint_path"))
    if not checkpoint and checkpoint_path:
        checkpoint = {"path": checkpoint_path}
    field_map = _mapping(raw.get("field_map"))
    predicates = _mapping_list(raw.get("predicates"))
    sort_spec = _mapping_list(raw.get("sort"))
    aggregations = _mapping_list(raw.get("aggregations"))
    partitions = _mapping_list(raw.get("partitions"))
    fields = _string_list(raw.get("fields"))
    drop_fields = _string_list(raw.get("drop_fields"))
    keys = _string_list(raw.get("keys"))
    left_keys = _string_list(raw.get("left_keys"))
    right_keys = _string_list(raw.get("right_keys"))
    compare_fields = _string_list(raw.get("compare_fields"))
    group_by = _string_list(raw.get("group_by"))
    strategy = _text(raw.get("strategy") or "first").lower() or "first"
    order_field = _text(raw.get("order_field")) or None
    split_by_field = _text(raw.get("split_by_field")) or None
    cursor_field = _text(raw.get("cursor_field")) or None
    after = raw.get("after")
    before = raw.get("before")
    predicate_mode = _text(raw.get("predicate_mode") or "all").lower() or "all"
    join_kind = _text(raw.get("join_kind") or "inner").lower() or "inner"
    merge_mode = _text(raw.get("merge_mode") or "full").lower() or "full"
    precedence = _text(raw.get("precedence") or "right").lower() or "right"
    split_mode = _text(raw.get("split_mode") or "first_match").lower() or "first_match"
    left_prefix = _text(raw.get("left_prefix")) or None
    right_prefix = _text(raw.get("right_prefix")) or None
    sync_mode = _text(raw.get("sync_mode") or "upsert").lower() or "upsert"
    include_unmatched = _boolean(raw.get("include_unmatched"), default=True)

    if operation == "filter" and not predicates:
        raise DataContractError("filter jobs require predicates")
    if operation == "sort" and not sort_spec:
        raise DataContractError("sort jobs require sort")
    if operation == "normalize" and not rules:
        raise DataContractError("normalize jobs require rules")
    if operation == "repair" and not repairs and not drop_fields:
        raise DataContractError("repair jobs require repairs or drop_fields")
    if operation == "backfill" and not backfill:
        raise DataContractError("backfill jobs require backfill")
    if operation == "redact" and not redactions:
        raise DataContractError("redact jobs require redactions")
    if operation == "checkpoint" and not keys and not cursor_field:
        raise DataContractError("checkpoint jobs require keys or cursor_field")
    if operation == "replay" and not cursor_field:
        raise DataContractError("replay jobs require cursor_field")
    if operation == "replay" and after is None and before is None and not checkpoint:
        raise DataContractError("replay jobs require after, before, checkpoint, or checkpoint_path")
    if operation == "validate" and not schema and not checks:
        raise DataContractError("validate jobs require schema or checks")
    if operation == "transform" and not mapping:
        raise DataContractError("transform jobs require mapping")
    if operation == "join" and not secondary_input:
        raise DataContractError("join jobs require secondary_input or secondary_input_path")
    if operation == "join" and not (keys or (left_keys and right_keys)):
        raise DataContractError("join jobs require keys or both left_keys and right_keys")
    if operation == "merge" and not secondary_input:
        raise DataContractError("merge jobs require secondary_input or secondary_input_path")
    if operation == "merge" and not keys:
        raise DataContractError("merge jobs require keys")
    if operation == "aggregate" and not aggregations:
        raise DataContractError("aggregate jobs require aggregations")
    if operation == "split" and not split_by_field and not partitions:
        raise DataContractError("split jobs require split_by_field or partitions")
    if operation == "export" and not fields and not field_map:
        raise DataContractError("export jobs require fields or field_map")
    if operation == "dead_letter" and not schema and not checks and not predicates:
        raise DataContractError("dead_letter jobs require schema, checks, or predicates")
    if operation == "dedupe" and not keys:
        raise DataContractError("dedupe jobs require keys")
    if operation == "reconcile" and not keys:
        raise DataContractError("reconcile jobs require keys")
    if operation == "sync" and not secondary_input:
        raise DataContractError("sync jobs require secondary_input or secondary_input_path")
    if operation == "sync" and not keys:
        raise DataContractError("sync jobs require keys")

    return {
        "schema_version": DATA_JOB_SCHEMA_VERSION,
        "operation": operation,
        "job_name": _text(raw.get("job_name")) or f"{operation}-data-job",
        "workspace_root": _text(raw.get("workspace_root")) or None,
        "input": input_ref,
        "secondary_input": secondary_input,
        "predicates": predicates,
        "predicate_mode": predicate_mode,
        "sort": sort_spec,
        "rules": rules,
        "repairs": repairs,
        "backfill": backfill,
        "redactions": redactions,
        "schema": schema,
        "checks": checks,
        "mapping": mapping,
        "checkpoint": checkpoint,
        "field_map": field_map,
        "fields": fields,
        "drop_fields": drop_fields,
        "keys": keys,
        "left_keys": left_keys,
        "right_keys": right_keys,
        "compare_fields": compare_fields,
        "group_by": group_by,
        "aggregations": aggregations,
        "partitions": partitions,
        "strategy": strategy,
        "order_field": order_field,
        "split_by_field": split_by_field,
        "cursor_field": cursor_field,
        "after": after,
        "before": before,
        "join_kind": join_kind,
        "merge_mode": merge_mode,
        "precedence": precedence,
        "split_mode": split_mode,
        "include_unmatched": include_unmatched,
        "left_prefix": left_prefix,
        "right_prefix": right_prefix,
        "sync_mode": sync_mode,
        "output": normalized_output,
    }


def data_job_digest(job: dict[str, Any]) -> str:
    canonical = json.dumps(job, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(canonical.encode("utf-8")).hexdigest()
