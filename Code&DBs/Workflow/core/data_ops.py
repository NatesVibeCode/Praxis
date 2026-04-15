"""Pure deterministic data operations.

This module owns value transforms and row-set comparisons only.
It does not read files, write files, or resolve workspace paths.
"""

from __future__ import annotations

from hashlib import sha256
import re
from collections import defaultdict
from datetime import datetime
import json
from typing import Any


class DataOperationError(RuntimeError):
    """Raised when a deterministic data operation cannot be executed safely."""

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


def _json_clone(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_clone(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_clone(item) for item in value]
    if isinstance(value, tuple):
        return [_json_clone(item) for item in value]
    return value


def _non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) > 0
    return True


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _parse_datetime(value: Any, input_formats: list[str] | None = None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = _stringify(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in input_formats or ():
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _apply_value_operation(value: Any, operation: Any) -> Any:
    if isinstance(operation, str):
        op_name = operation.strip().lower()
        options: dict[str, Any] = {}
    elif isinstance(operation, dict):
        op_name = str(operation.get("op") or operation.get("type") or "").strip().lower()
        options = dict(operation)
    else:
        raise DataOperationError(
            "data.normalize.invalid_operation",
            "normalize operation entries must be strings or objects",
            details={"operation": operation},
        )

    if not op_name:
        raise DataOperationError(
            "data.normalize.invalid_operation",
            "normalize operation must declare an op/type",
            details={"operation": operation},
        )

    if op_name == "trim":
        return _stringify(value).strip()
    if op_name == "lower":
        return _stringify(value).lower()
    if op_name == "upper":
        return _stringify(value).upper()
    if op_name == "title":
        return _stringify(value).title()
    if op_name == "collapse_whitespace":
        return _normalize_whitespace(_stringify(value))
    if op_name == "null_if_empty":
        text = _stringify(value).strip()
        return None if text == "" else value
    if op_name == "empty_if_null":
        return "" if value is None else value
    if op_name == "strip_non_digits":
        return re.sub(r"\D+", "", _stringify(value))
    if op_name == "default":
        return value if _non_empty(value) else _json_clone(options.get("value"))
    if op_name == "map_values":
        mapping = options.get("mapping")
        if not isinstance(mapping, dict):
            raise DataOperationError(
                "data.normalize.invalid_map_values",
                "map_values requires a mapping object",
            )
        key = _stringify(value)
        if key in mapping:
            return _json_clone(mapping[key])
        return _json_clone(options.get("default", value))
    if op_name == "boolean":
        parsed = _parse_bool(value)
        if parsed is None:
            raise DataOperationError(
                "data.normalize.boolean_parse_failed",
                f"cannot coerce value {value!r} to boolean",
            )
        return parsed
    if op_name == "integer":
        text = _stringify(value).strip()
        if not text:
            return None
        return int(float(text))
    if op_name == "float":
        text = _stringify(value).strip()
        if not text:
            return None
        return float(text)
    if op_name == "date_iso":
        parsed = _parse_datetime(value, options.get("input_formats"))
        if parsed is None:
            raise DataOperationError(
                "data.normalize.date_parse_failed",
                f"cannot coerce value {value!r} to ISO date",
            )
        return parsed.date().isoformat()
    if op_name == "datetime_iso":
        parsed = _parse_datetime(value, options.get("input_formats"))
        if parsed is None:
            raise DataOperationError(
                "data.normalize.datetime_parse_failed",
                f"cannot coerce value {value!r} to ISO datetime",
            )
        return parsed.isoformat()

    raise DataOperationError(
        "data.normalize.unsupported_operation",
        f"unsupported normalize operation: {op_name}",
        details={"operation": operation},
    )


def _normalize_operations(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    return [raw]


def _field_names(records: list[dict[str, Any]]) -> list[str]:
    names: set[str] = set()
    for record in records:
        names.update(record.keys())
    return sorted(names)


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value


def _key_tuple(record: dict[str, Any], fields: list[str]) -> tuple[Any, ...]:
    return tuple(_canonical_value(record.get(field)) for field in fields)


def _record_completeness_score(record: dict[str, Any]) -> int:
    return sum(1 for value in record.values() if _non_empty(value))


def _sort_key_for_strategy(
    record: dict[str, Any],
    *,
    strategy: str,
    order_field: str | None,
    row_index: int,
) -> tuple[Any, ...]:
    if strategy == "latest_by_field":
        if not order_field:
            raise DataOperationError(
                "data.dedupe.order_field_required",
                "latest_by_field strategy requires order_field",
            )
        parsed = _parse_datetime(record.get(order_field))
        return (parsed or datetime.min, row_index)
    if strategy == "most_complete":
        return (_record_completeness_score(record), row_index)
    return (row_index,)


def _matches_type(value: Any, expected_type: str) -> bool:
    normalized = expected_type.strip().lower()
    if normalized == "string":
        return isinstance(value, str)
    if normalized == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if normalized in {"float", "number"}:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if normalized == "boolean":
        return isinstance(value, bool)
    if normalized == "object":
        return isinstance(value, dict)
    if normalized == "array":
        return isinstance(value, list)
    raise DataOperationError(
        "data.validate.unsupported_type",
        f"unsupported validation type: {expected_type}",
    )


def _append_violation(
    violations: list[dict[str, Any]],
    *,
    row_index: int,
    field: str | None,
    code: str,
    message: str,
    record_key: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "row_index": row_index,
        "code": code,
        "message": message,
    }
    if field is not None:
        payload["field"] = field
    if record_key:
        payload["record_key"] = record_key
    violations.append(payload)


def _compare_values(left: Any, right: Any, op: str) -> bool:
    normalized = op.strip().lower()
    if normalized in {"eq", "equals"}:
        return left == right
    if normalized in {"neq", "not_equals"}:
        return left != right
    if normalized == "gt":
        return left > right
    if normalized == "gte":
        return left >= right
    if normalized == "lt":
        return left < right
    if normalized == "lte":
        return left <= right
    raise DataOperationError(
        "data.validate.unsupported_compare",
        f"unsupported comparison operator: {op}",
    )


def _coerce_comparable(value: Any) -> Any:
    parsed_datetime = _parse_datetime(value)
    if parsed_datetime is not None:
        return parsed_datetime
    text = _stringify(value).strip()
    if text:
        try:
            return float(text)
        except ValueError:
            pass
    return value


def _predicate_matches(record: dict[str, Any], predicate: dict[str, Any]) -> bool:
    field = str(predicate.get("field") or "").strip()
    op = str(predicate.get("op") or predicate.get("type") or "").strip().lower()
    value = record.get(field)
    expected = predicate.get("value")
    if not field or not op:
        raise DataOperationError(
            "data.filter.invalid_predicate",
            "predicates require field and op",
            details={"predicate": predicate},
        )

    if op == "exists":
        return field in record and _non_empty(value)
    if op in {"in", "not_in"}:
        if not isinstance(expected, list):
            raise DataOperationError(
                "data.filter.invalid_predicate_value",
                f"{op} predicates require a list value",
            )
        result = value in expected
        return result if op == "in" else not result
    if op == "contains":
        if isinstance(value, list):
            return expected in value
        return _stringify(expected) in _stringify(value)
    if op == "starts_with":
        return _stringify(value).startswith(_stringify(expected))
    if op == "ends_with":
        return _stringify(value).endswith(_stringify(expected))
    if op == "regex":
        return re.search(_stringify(expected), _stringify(value)) is not None

    left = _coerce_comparable(value)
    right = _coerce_comparable(expected)
    return _compare_values(left, right, op)


def filter_records(
    records: list[dict[str, Any]],
    *,
    predicates: list[dict[str, Any]],
    predicate_mode: str = "all",
) -> dict[str, Any]:
    normalized_mode = predicate_mode.strip().lower() or "all"
    if normalized_mode not in {"all", "any"}:
        raise DataOperationError(
            "data.filter.unsupported_mode",
            f"unsupported predicate mode: {predicate_mode}",
        )
    output: list[dict[str, Any]] = []
    dropped_rows = 0
    for record in records:
        matches = [_predicate_matches(record, predicate) for predicate in predicates]
        keep = all(matches) if normalized_mode == "all" else any(matches)
        if keep:
            output.append(dict(record))
        else:
            dropped_rows += 1
    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "dropped_rows": dropped_rows,
            "predicate_count": len(predicates),
            "predicate_mode": normalized_mode,
        },
    }


def _sortable_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (1, "")
    comparable = _coerce_comparable(value)
    if isinstance(comparable, (datetime, int, float)):
        return (0, comparable)
    return (0, _stringify(comparable))


def sort_records(records: list[dict[str, Any]], *, sort_spec: list[dict[str, Any]]) -> dict[str, Any]:
    output = [dict(record) for record in records]
    for raw_spec in reversed(sort_spec):
        spec = dict(raw_spec)
        field = str(spec.get("field") or "").strip()
        if not field:
            raise DataOperationError(
                "data.sort.field_required",
                "sort entries require field",
            )
        direction = str(spec.get("direction") or "asc").strip().lower()
        nulls = str(spec.get("nulls") or "last").strip().lower()
        if direction not in {"asc", "desc"}:
            raise DataOperationError(
                "data.sort.unsupported_direction",
                f"unsupported sort direction: {direction}",
            )
        if nulls not in {"first", "last"}:
            raise DataOperationError(
                "data.sort.unsupported_nulls",
                f"unsupported null sort placement: {nulls}",
            )
        non_null = [record for record in output if record.get(field) is not None]
        null_rows = [record for record in output if record.get(field) is None]
        non_null.sort(key=lambda record: _sortable_value(record.get(field))[1], reverse=direction == "desc")
        output = [*null_rows, *non_null] if nulls == "first" else [*non_null, *null_rows]
    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "sort_keys": [str(item.get("field") or "") for item in sort_spec],
        },
    }


def profile_records(records: list[dict[str, Any]], *, sample_limit: int = 5) -> dict[str, Any]:
    fields = _field_names(records)
    field_profiles: dict[str, Any] = {}
    for field in fields:
        non_empty_count = 0
        null_count = 0
        distinct_values: set[str] = set()
        inferred_types: dict[str, int] = defaultdict(int)
        sample_values: list[Any] = []
        for record in records:
            value = record.get(field)
            if value is None:
                null_count += 1
            if _non_empty(value):
                non_empty_count += 1
            inferred_types[_infer_type(value)] += 1
            distinct_values.add(repr(_canonical_value(value)))
            if len(sample_values) < sample_limit and _non_empty(value):
                sample_values.append(_json_clone(value))
        field_profiles[field] = {
            "non_empty_count": non_empty_count,
            "null_count": null_count,
            "distinct_count": len(distinct_values),
            "inferred_types": dict(sorted(inferred_types.items())),
            "sample_values": sample_values,
        }

    return {
        "row_count": len(records),
        "field_count": len(fields),
        "fields": fields,
        "field_profiles": field_profiles,
        "sample_rows": [_json_clone(record) for record in records[:sample_limit]],
    }


def normalize_records(records: list[dict[str, Any]], rules: dict[str, Any]) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    changed_rows = 0
    changed_cells = 0
    for record in records:
        updated = dict(record)
        row_changed = False
        for field, operations in rules.items():
            current = updated.get(field)
            new_value = current
            for operation in _normalize_operations(operations):
                new_value = _apply_value_operation(new_value, operation)
            if new_value != current:
                updated[field] = new_value
                row_changed = True
                changed_cells += 1
        if row_changed:
            changed_rows += 1
        normalized.append(updated)

    return {
        "records": normalized,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(normalized),
            "changed_rows": changed_rows,
            "changed_cells": changed_cells,
        },
    }


def _row_matches_predicates(
    record: dict[str, Any],
    *,
    predicates: list[dict[str, Any]] | None = None,
    predicate_mode: str = "all",
) -> bool:
    normalized_predicates = list(predicates or [])
    if not normalized_predicates:
        return True
    normalized_mode = predicate_mode.strip().lower() or "all"
    if normalized_mode not in {"all", "any"}:
        raise DataOperationError(
            "data.filter.unsupported_mode",
            f"unsupported predicate mode: {predicate_mode}",
        )
    matches = [_predicate_matches(record, predicate) for predicate in normalized_predicates]
    return all(matches) if normalized_mode == "all" else any(matches)


def repair_records(
    records: list[dict[str, Any]],
    *,
    repairs: dict[str, Any],
    predicates: list[dict[str, Any]] | None = None,
    predicate_mode: str = "all",
    drop_fields: list[str] | None = None,
) -> dict[str, Any]:
    if not repairs and not drop_fields:
        raise DataOperationError(
            "data.repair.patch_required",
            "repair requires repairs or drop_fields",
        )

    output: list[dict[str, Any]] = []
    matched_rows = 0
    changed_rows = 0
    changed_cells = 0
    dropped_fields_count = 0
    for record in records:
        updated = dict(record)
        row_changed = False
        if _row_matches_predicates(updated, predicates=predicates, predicate_mode=predicate_mode):
            matched_rows += 1
            for field, expression in repairs.items():
                new_value = _evaluate_expression(updated, expression)
                if field not in updated or updated.get(field) != new_value:
                    updated[str(field)] = new_value
                    changed_cells += 1
                    row_changed = True
            for field in drop_fields or []:
                if field in updated:
                    del updated[field]
                    dropped_fields_count += 1
                    row_changed = True
        if row_changed:
            changed_rows += 1
        output.append(updated)

    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "matched_rows": matched_rows,
            "changed_rows": changed_rows,
            "changed_cells": changed_cells,
            "dropped_fields": dropped_fields_count,
        },
    }


def backfill_records(
    records: list[dict[str, Any]],
    *,
    backfill: dict[str, Any],
    predicates: list[dict[str, Any]] | None = None,
    predicate_mode: str = "all",
) -> dict[str, Any]:
    if not backfill:
        raise DataOperationError(
            "data.backfill.mapping_required",
            "backfill requires backfill rules",
        )

    output: list[dict[str, Any]] = []
    matched_rows = 0
    filled_rows = 0
    filled_cells = 0
    skipped_populated_cells = 0
    for record in records:
        updated = dict(record)
        row_filled = False
        if _row_matches_predicates(updated, predicates=predicates, predicate_mode=predicate_mode):
            matched_rows += 1
            for field, expression in backfill.items():
                if _non_empty(updated.get(field)):
                    skipped_populated_cells += 1
                    continue
                new_value = _evaluate_expression(updated, expression)
                if new_value is None:
                    continue
                if field not in updated or updated.get(field) != new_value:
                    updated[str(field)] = new_value
                    filled_cells += 1
                    row_filled = True
        if row_filled:
            filled_rows += 1
        output.append(updated)

    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "matched_rows": matched_rows,
            "filled_rows": filled_rows,
            "filled_cells": filled_cells,
            "skipped_populated_cells": skipped_populated_cells,
        },
    }


def checkpoint_records(
    records: list[dict[str, Any]],
    *,
    keys: list[str] | None = None,
    cursor_field: str | None = None,
    sample_limit: int = 5,
) -> dict[str, Any]:
    canonical_rows = [_canonical_value(record) for record in records]
    content_hash = sha256(
        json.dumps(canonical_rows, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()

    checkpoint: dict[str, Any] = {
        "row_count": len(records),
        "field_count": len(_field_names(records)),
        "content_hash": content_hash,
    }
    if keys:
        canonical_keys = [_key_tuple(record, list(keys)) for record in records]
        checkpoint["key_fields"] = list(keys)
        checkpoint["key_hash"] = sha256(
            json.dumps(canonical_keys, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        checkpoint["sample_keys"] = [
            {field: _json_clone(record.get(field)) for field in keys}
            for record in records[:sample_limit]
        ]
    if cursor_field:
        pairs = [
            (_coerce_comparable(record.get(cursor_field)), _json_clone(record.get(cursor_field)))
            for record in records
            if _non_empty(record.get(cursor_field))
        ]
        if pairs:
            sorted_pairs = sorted(pairs, key=lambda item: item[0])
            checkpoint["cursor_field"] = cursor_field
            checkpoint["cursor_min"] = sorted_pairs[0][1]
            checkpoint["cursor_max"] = sorted_pairs[-1][1]
            checkpoint["watermark"] = sorted_pairs[-1][1]

    return {
        "checkpoint": checkpoint,
        "stats": {
            "input_rows": len(records),
            "row_count": len(records),
            "field_count": len(_field_names(records)),
        },
    }


def _canonical_digest(value: Any) -> str:
    return sha256(
        json.dumps(_canonical_value(value), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def replay_records(
    records: list[dict[str, Any]],
    *,
    cursor_field: str,
    after: Any = None,
    before: Any = None,
) -> dict[str, Any]:
    normalized_cursor = cursor_field.strip()
    if not normalized_cursor:
        raise DataOperationError(
            "data.replay.cursor_field_required",
            "replay requires cursor_field",
        )

    after_value = _coerce_comparable(after) if after is not None else None
    before_value = _coerce_comparable(before) if before is not None else None
    output: list[dict[str, Any]] = []
    missing_cursor_rows = 0
    skipped_before = 0
    skipped_after = 0
    for record in records:
        raw_value = record.get(normalized_cursor)
        if not _non_empty(raw_value):
            missing_cursor_rows += 1
            continue
        comparable = _coerce_comparable(raw_value)
        try:
            if after_value is not None and not (comparable > after_value):
                skipped_before += 1
                continue
            if before_value is not None and not (comparable <= before_value):
                skipped_after += 1
                continue
        except TypeError as exc:
            raise DataOperationError(
                "data.replay.cursor_compare_failed",
                f"cannot compare replay cursor values for field {normalized_cursor}",
                details={"cursor_field": normalized_cursor, "after": after, "before": before},
            ) from exc
        output.append(dict(record))

    return {
        "records": output,
        "replay_window": {
            "cursor_field": normalized_cursor,
            "after": _json_clone(after),
            "before": _json_clone(before),
        },
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "missing_cursor_rows": missing_cursor_rows,
            "skipped_before": skipped_before,
            "skipped_after": skipped_after,
        },
    }


def repair_loop_records(
    records: list[dict[str, Any]],
    *,
    repairs: dict[str, Any] | None = None,
    backfill: dict[str, Any] | None = None,
    rules: dict[str, Any] | None = None,
    schema: dict[str, Any] | None = None,
    checks: list[dict[str, Any]] | None = None,
    predicates: list[dict[str, Any]] | None = None,
    predicate_mode: str = "all",
    drop_fields: list[str] | None = None,
    keys: list[str] | None = None,
    max_passes: int = 3,
) -> dict[str, Any]:
    normalized_repairs = dict(repairs or {})
    normalized_backfill = dict(backfill or {})
    normalized_rules = dict(rules or {})
    normalized_schema = dict(schema or {})
    normalized_checks = [dict(item) for item in checks or []]
    normalized_drop_fields = list(drop_fields or [])
    if max_passes <= 0:
        raise DataOperationError(
            "data.repair_loop.max_passes_invalid",
            "repair_loop max_passes must be positive",
        )
    if not (normalized_repairs or normalized_backfill or normalized_rules or normalized_drop_fields):
        raise DataOperationError(
            "data.repair_loop.steps_required",
            "repair_loop requires repairs, backfill, rules, or drop_fields",
        )

    current = [dict(record) for record in records]
    passes: list[dict[str, Any]] = []
    converged = False
    for pass_index in range(1, max_passes + 1):
        pass_changed = False
        pass_payload: dict[str, Any] = {"pass_index": pass_index}

        if normalized_repairs or normalized_drop_fields:
            repair = repair_records(
                current,
                repairs=normalized_repairs,
                predicates=predicates,
                predicate_mode=predicate_mode,
                drop_fields=normalized_drop_fields,
            )
            current = [dict(record) for record in repair["records"]]
            pass_payload["repair"] = dict(repair.get("stats") or {})
            pass_changed = pass_changed or bool((repair.get("stats") or {}).get("changed_rows"))

        if normalized_backfill:
            backfilled = backfill_records(
                current,
                backfill=normalized_backfill,
                predicates=predicates,
                predicate_mode=predicate_mode,
            )
            current = [dict(record) for record in backfilled["records"]]
            pass_payload["backfill"] = dict(backfilled.get("stats") or {})
            pass_changed = pass_changed or bool((backfilled.get("stats") or {}).get("filled_rows"))

        if normalized_rules:
            normalized = normalize_records(current, normalized_rules)
            current = [dict(record) for record in normalized["records"]]
            pass_payload["normalize"] = dict(normalized.get("stats") or {})
            pass_changed = pass_changed or bool((normalized.get("stats") or {}).get("changed_rows"))

        validation = validate_records(
            current,
            normalized_schema,
            checks=normalized_checks,
            keys=keys or [],
        ) if (normalized_schema or normalized_checks) else {"violations": [], "stats": {"violation_count": 0}}
        pass_payload["validate"] = dict(validation.get("stats") or {})
        pass_payload["violation_count"] = int((validation.get("stats") or {}).get("violation_count", 0))
        passes.append(pass_payload)

        if pass_payload["violation_count"] == 0 or not pass_changed:
            converged = pass_payload["violation_count"] == 0
            break

    dead_letter = dead_letter_records(
        current,
        schema=normalized_schema,
        checks=normalized_checks,
        predicates=[],
        predicate_mode="any",
        keys=keys or [],
    ) if (normalized_schema or normalized_checks) else {
        "partitions": {"accepted": [dict(record) for record in current], "dead_letter": []},
        "partition_counts": {"accepted": len(current), "dead_letter": 0},
        "violations": [],
        "stats": {"accepted_rows": len(current), "dead_letter_rows": 0, "violation_count": 0},
    }

    accepted = list((dead_letter.get("partitions") or {}).get("accepted") or [])
    return {
        "records": accepted,
        "partitions": _json_clone(dead_letter.get("partitions") or {}),
        "partition_counts": dict(dead_letter.get("partition_counts") or {}),
        "violations": _json_clone(dead_letter.get("violations") or []),
        "passes": passes,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(accepted),
            "pass_count": len(passes),
            "converged": converged,
            "accepted_rows": int((dead_letter.get("stats") or {}).get("accepted_rows", len(accepted))),
            "dead_letter_rows": int((dead_letter.get("stats") or {}).get("dead_letter_rows", 0)),
            "final_violation_count": int((dead_letter.get("stats") or {}).get("violation_count", 0)),
        },
    }


def _mask_text(value: Any) -> str:
    text = _stringify(value)
    return "".join("*" if not char.isspace() else char for char in text)


def _mask_email(value: Any) -> str:
    text = _stringify(value)
    if "@" not in text:
        return _mask_text(text)
    local, _, domain = text.partition("@")
    if not local:
        return f"***@{domain}"
    return f"{local[:1]}***@{domain}"


def _apply_redaction(value: Any, spec: Any) -> tuple[bool, Any]:
    if isinstance(spec, str):
        op = spec.strip().lower()
        options: dict[str, Any] = {}
    elif isinstance(spec, dict):
        op = str(spec.get("op") or spec.get("type") or "").strip().lower()
        options = dict(spec)
    else:
        raise DataOperationError(
            "data.redact.invalid_rule",
            "redaction rules must be strings or objects",
            details={"rule": spec},
        )

    if not op:
        raise DataOperationError(
            "data.redact.invalid_rule",
            "redaction rules must declare an op/type",
            details={"rule": spec},
        )
    if op == "remove":
        return False, None
    if op == "mask":
        return True, _mask_text(value)
    if op == "mask_email":
        return True, _mask_email(value)
    if op == "hash_sha256":
        return True, sha256(_stringify(value).encode("utf-8")).hexdigest()
    if op == "replace":
        return True, _json_clone(options.get("value"))

    raise DataOperationError(
        "data.redact.unsupported_operation",
        f"unsupported redaction operation: {op}",
        details={"rule": spec},
    )


def redact_records(records: list[dict[str, Any]], redactions: dict[str, Any]) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    changed_rows = 0
    redacted_cells = 0
    removed_fields = 0
    for record in records:
        updated = dict(record)
        row_changed = False
        for field, spec in redactions.items():
            if field not in updated:
                continue
            keep_field, new_value = _apply_redaction(updated.get(field), spec)
            if keep_field:
                if updated.get(field) != new_value:
                    updated[field] = new_value
                    redacted_cells += 1
                    row_changed = True
            else:
                del updated[field]
                removed_fields += 1
                row_changed = True
        if row_changed:
            changed_rows += 1
        output.append(updated)
    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "changed_rows": changed_rows,
            "redacted_cells": redacted_cells,
            "removed_fields": removed_fields,
        },
    }


def _evaluate_expression(record: dict[str, Any], expression: Any) -> Any:
    if isinstance(expression, str):
        return _json_clone(record.get(expression))
    if not isinstance(expression, dict):
        return _json_clone(expression)
    if "value" in expression:
        return _json_clone(expression.get("value"))
    if "from" in expression:
        value = _json_clone(record.get(str(expression.get("from"))))
        for operation in _normalize_operations(expression.get("ops")):
            value = _apply_value_operation(value, operation)
        return value
    if "template" in expression:
        template = str(expression.get("template") or "")
        values = {key: _stringify(value) for key, value in record.items()}
        return template.format_map(defaultdict(str, values))
    if "concat" in expression:
        parts = expression.get("concat")
        if not isinstance(parts, list):
            raise DataOperationError(
                "data.transform.invalid_concat",
                "concat expressions require a list",
            )
        separator = str(expression.get("separator") or "")
        evaluated = [_stringify(_evaluate_expression(record, part)) for part in parts]
        return separator.join(evaluated)
    if "coalesce" in expression:
        items = expression.get("coalesce")
        if not isinstance(items, list):
            raise DataOperationError(
                "data.transform.invalid_coalesce",
                "coalesce expressions require a list",
            )
        for item in items:
            candidate = _evaluate_expression(record, item)
            if _non_empty(candidate):
                return candidate
        return _json_clone(expression.get("default"))

    raise DataOperationError(
        "data.transform.invalid_expression",
        "transform expressions must use one of: from, value, template, concat, coalesce",
        details={"expression": expression},
    )


def transform_records(records: list[dict[str, Any]], mapping: dict[str, Any]) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    for record in records:
        transformed: dict[str, Any] = {}
        for target_field, expression in mapping.items():
            transformed[str(target_field)] = _evaluate_expression(record, expression)
        output.append(transformed)
    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "target_fields": sorted(str(field) for field in mapping),
        },
    }


def export_records(
    records: list[dict[str, Any]],
    *,
    fields: list[str],
    field_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not fields and not field_map:
        raise DataOperationError(
            "data.export.fields_required",
            "export requires fields or field_map",
        )
    if not fields and field_map:
        fields = [str(key) for key in field_map]
    field_map = dict(field_map or {})
    output: list[dict[str, Any]] = []
    target_fields: list[str] = []
    for field in fields:
        target_fields.append(str(field_map.get(field) or field))
    for record in records:
        exported: dict[str, Any] = {}
        for field in fields:
            target_field = str(field_map.get(field) or field)
            exported[target_field] = _json_clone(record.get(field))
        output.append(exported)
    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "export_fields": list(fields),
            "target_fields": target_fields,
        },
    }


def _merge_join_row(
    left_record: dict[str, Any] | None,
    right_record: dict[str, Any] | None,
    *,
    left_prefix: str | None,
    right_prefix: str | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if left_record is not None:
        for field, value in left_record.items():
            output_field = f"{left_prefix}{field}" if left_prefix else field
            merged[output_field] = _json_clone(value)
    if right_record is not None:
        for field, value in right_record.items():
            output_field = f"{right_prefix}{field}" if right_prefix else field
            if output_field in merged:
                output_field = f"right_{field}"
            merged[output_field] = _json_clone(value)
    return merged


def join_records(
    left_records: list[dict[str, Any]],
    right_records: list[dict[str, Any]],
    *,
    left_keys: list[str],
    right_keys: list[str],
    join_kind: str = "inner",
    left_prefix: str | None = None,
    right_prefix: str | None = None,
) -> dict[str, Any]:
    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        raise DataOperationError(
            "data.join.keys_invalid",
            "join requires equal-length left_keys and right_keys",
        )
    normalized_kind = join_kind.strip().lower() or "inner"
    if normalized_kind not in {"inner", "left", "right", "full"}:
        raise DataOperationError(
            "data.join.unsupported_kind",
            f"unsupported join kind: {join_kind}",
        )

    right_index: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for row_index, record in enumerate(right_records):
        key = tuple(_canonical_value(record.get(field)) for field in right_keys)
        right_index[key].append((row_index, record))

    output: list[dict[str, Any]] = []
    matched_right_rows: set[int] = set()
    match_count = 0
    left_only_count = 0
    for left_record in left_records:
        key = tuple(_canonical_value(left_record.get(field)) for field in left_keys)
        matches = right_index.get(key, [])
        if matches:
            for row_index, right_record in matches:
                matched_right_rows.add(row_index)
                match_count += 1
                output.append(
                    _merge_join_row(
                        left_record,
                        right_record,
                        left_prefix=left_prefix,
                        right_prefix=right_prefix,
                    )
                )
        elif normalized_kind in {"left", "full"}:
            left_only_count += 1
            output.append(
                _merge_join_row(
                    left_record,
                    None,
                    left_prefix=left_prefix,
                    right_prefix=right_prefix,
                )
            )

    right_only_count = 0
    if normalized_kind in {"right", "full"}:
        for row_index, right_record in enumerate(right_records):
            if row_index in matched_right_rows:
                continue
            right_only_count += 1
            output.append(
                _merge_join_row(
                    None,
                    right_record,
                    left_prefix=left_prefix,
                    right_prefix=right_prefix,
                )
            )

    return {
        "records": output,
        "stats": {
            "left_rows": len(left_records),
            "right_rows": len(right_records),
            "output_rows": len(output),
            "match_count": match_count,
            "left_only_count": left_only_count,
            "right_only_count": right_only_count,
            "join_kind": normalized_kind,
        },
    }


def merge_records(
    left_records: list[dict[str, Any]],
    right_records: list[dict[str, Any]],
    *,
    keys: list[str],
    merge_mode: str = "full",
    precedence: str = "right",
) -> dict[str, Any]:
    if not keys:
        raise DataOperationError(
            "data.merge.keys_required",
            "merge requires key fields",
        )
    normalized_mode = merge_mode.strip().lower() or "full"
    if normalized_mode not in {"inner", "left", "right", "full"}:
        raise DataOperationError(
            "data.merge.unsupported_mode",
            f"unsupported merge mode: {merge_mode}",
        )
    normalized_precedence = precedence.strip().lower() or "right"
    if normalized_precedence not in {"left", "right"}:
        raise DataOperationError(
            "data.merge.unsupported_precedence",
            f"unsupported merge precedence: {precedence}",
        )

    def _index(records: list[dict[str, Any]], side: str) -> tuple[dict[tuple[Any, ...], tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
        index: dict[tuple[Any, ...], tuple[int, dict[str, Any]]] = {}
        conflicts: list[dict[str, Any]] = []
        for row_index, record in enumerate(records):
            key = _key_tuple(record, keys)
            if key in index:
                conflicts.append(
                    {
                        "side": side,
                        "key": {keys[idx]: _json_clone(value) for idx, value in enumerate(key)},
                        "row_indices": [index[key][0], row_index],
                    }
                )
                continue
            index[key] = (row_index, record)
        return index, conflicts

    left_index, left_conflicts = _index(left_records, "left")
    right_index, right_conflicts = _index(right_records, "right")
    all_keys = sorted(set(left_index) | set(right_index), key=repr)
    output: list[dict[str, Any]] = []
    match_count = 0
    left_only_count = 0
    right_only_count = 0
    for key in all_keys:
        left_entry = left_index.get(key)
        right_entry = right_index.get(key)
        if left_entry and right_entry:
            match_count += 1
            left_record = dict(left_entry[1])
            right_record = dict(right_entry[1])
            merged = {**left_record, **right_record} if normalized_precedence == "right" else {**right_record, **left_record}
            output.append(merged)
            continue
        if left_entry and normalized_mode in {"left", "full"}:
            left_only_count += 1
            output.append(dict(left_entry[1]))
            continue
        if right_entry and normalized_mode in {"right", "full"}:
            right_only_count += 1
            output.append(dict(right_entry[1]))

    return {
        "records": output,
        "conflicts": [*left_conflicts, *right_conflicts],
        "stats": {
            "left_rows": len(left_records),
            "right_rows": len(right_records),
            "output_rows": len(output),
            "match_count": match_count,
            "left_only_count": left_only_count,
            "right_only_count": right_only_count,
            "conflict_count": len(left_conflicts) + len(right_conflicts),
            "merge_mode": normalized_mode,
            "precedence": normalized_precedence,
        },
    }


def _aggregate_key(record: dict[str, Any], group_by: list[str]) -> tuple[Any, ...]:
    return tuple(_canonical_value(record.get(field)) for field in group_by)


def _aggregate_alias(spec: dict[str, Any]) -> str:
    alias = str(spec.get("as") or "").strip()
    if alias:
        return alias
    op = str(spec.get("op") or "").strip().lower()
    field = str(spec.get("field") or "rows").strip() or "rows"
    return f"{op}_{field}"


def aggregate_records(
    records: list[dict[str, Any]],
    *,
    group_by: list[str],
    aggregations: list[dict[str, Any]],
) -> dict[str, Any]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    if group_by:
        for record in records:
            groups[_aggregate_key(record, group_by)].append(record)
    else:
        groups[()] = list(records)

    output: list[dict[str, Any]] = []
    for key, rows in groups.items():
        result_row = {
            field: _json_clone(value)
            for field, value in zip(group_by, key)
        }
        for raw_spec in aggregations:
            spec = dict(raw_spec)
            op = str(spec.get("op") or "").strip().lower()
            field = str(spec.get("field") or "").strip()
            alias = _aggregate_alias(spec)
            values = [row.get(field) for row in rows] if field else [None for _ in rows]
            if op == "count":
                result_row[alias] = len(rows)
            elif op == "count_non_empty":
                result_row[alias] = sum(1 for value in values if _non_empty(value))
            elif op == "count_distinct":
                result_row[alias] = len({repr(_canonical_value(value)) for value in values if _non_empty(value)})
            elif op in {"sum", "avg"}:
                numeric = [float(_coerce_comparable(value)) for value in values if _non_empty(value)]
                result_row[alias] = sum(numeric) if op == "sum" else (sum(numeric) / len(numeric) if numeric else None)
            elif op == "min":
                comparable = [_coerce_comparable(value) for value in values if _non_empty(value)]
                result_row[alias] = min(comparable) if comparable else None
            elif op == "max":
                comparable = [_coerce_comparable(value) for value in values if _non_empty(value)]
                result_row[alias] = max(comparable) if comparable else None
            elif op == "first":
                result_row[alias] = _json_clone(next((value for value in values if value is not None), None))
            elif op == "last":
                result_row[alias] = _json_clone(next((value for value in reversed(values) if value is not None), None))
            else:
                raise DataOperationError(
                    "data.aggregate.unsupported_operation",
                    f"unsupported aggregate operation: {op}",
                )
        output.append(result_row)

    return {
        "records": output,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(output),
            "group_count": len(output),
            "group_by": list(group_by),
            "aggregation_count": len(aggregations),
        },
    }


def split_records(
    records: list[dict[str, Any]],
    *,
    split_by_field: str | None = None,
    partitions: list[dict[str, Any]] | None = None,
    split_mode: str = "first_match",
    include_unmatched: bool = True,
) -> dict[str, Any]:
    normalized_mode = split_mode.strip().lower() or "first_match"
    if normalized_mode not in {"first_match", "all_matches"}:
        raise DataOperationError(
            "data.split.unsupported_mode",
            f"unsupported split mode: {split_mode}",
        )
    if not split_by_field and not partitions:
        raise DataOperationError(
            "data.split.strategy_required",
            "split requires split_by_field or partitions",
        )

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched_count = 0
    if split_by_field:
        field = split_by_field.strip()
        if not field:
            raise DataOperationError(
                "data.split.field_required",
                "split_by_field must be non-empty",
            )
        for record in records:
            bucket = _stringify(record.get(field)).strip() or "null"
            buckets[bucket].append(dict(record))
    else:
        normalized_partitions = [dict(item) for item in partitions or []]
        for record in records:
            matched_names: list[str] = []
            for partition in normalized_partitions:
                name = str(partition.get("name") or "").strip()
                predicates = [dict(item) for item in partition.get("predicates") or [] if isinstance(item, dict)]
                if not name or not predicates:
                    raise DataOperationError(
                        "data.split.invalid_partition",
                        "partitions require name and predicates",
                        details={"partition": partition},
                    )
                mode = str(partition.get("predicate_mode") or "all").strip().lower() or "all"
                matches = [_predicate_matches(record, predicate) for predicate in predicates]
                keep = all(matches) if mode == "all" else any(matches)
                if keep:
                    matched_names.append(name)
                    buckets[name].append(dict(record))
                    if normalized_mode == "first_match":
                        break
            if not matched_names and include_unmatched:
                buckets["unmatched"].append(dict(record))
                unmatched_count += 1

    partition_rows = {
        name: rows
        for name, rows in sorted(buckets.items(), key=lambda item: item[0])
    }
    partition_counts = {name: len(rows) for name, rows in partition_rows.items()}
    return {
        "partitions": partition_rows,
        "partition_counts": partition_counts,
        "stats": {
            "input_rows": len(records),
            "partition_count": len(partition_rows),
            "unmatched_count": unmatched_count,
            "split_mode": normalized_mode,
            **({"split_by_field": split_by_field} if split_by_field else {}),
        },
    }


def dead_letter_records(
    records: list[dict[str, Any]],
    *,
    schema: dict[str, Any] | None = None,
    checks: list[dict[str, Any]] | None = None,
    predicates: list[dict[str, Any]] | None = None,
    predicate_mode: str = "any",
    keys: list[str] | None = None,
) -> dict[str, Any]:
    if not schema and not checks and not predicates:
        raise DataOperationError(
            "data.dead_letter.rules_required",
            "dead_letter requires schema, checks, or predicates",
        )

    validation = validate_records(records, schema or {}, checks=checks or [], keys=keys or [])
    violations = list(validation.get("violations") or [])
    reasons_by_row: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for violation in violations:
        reasons_by_row[int(violation["row_index"])].append(
            {
                "code": violation.get("code"),
                "field": violation.get("field"),
                "message": violation.get("message"),
            }
        )

    predicate_dead_letter_count = 0
    for row_index, record in enumerate(records):
        if not predicates:
            continue
        if _row_matches_predicates(record, predicates=predicates, predicate_mode=predicate_mode):
            predicate_dead_letter_count += 1
            reasons_by_row[row_index].append(
                {
                    "code": "predicate_match",
                    "message": "row matched dead-letter predicate",
                }
            )

    accepted: list[dict[str, Any]] = []
    dead_letter: list[dict[str, Any]] = []
    for row_index, record in enumerate(records):
        row_reasons = reasons_by_row.get(row_index, [])
        if row_reasons:
            payload = dict(record)
            payload["_dead_letter_reasons"] = _json_clone(row_reasons)
            dead_letter.append(payload)
        else:
            accepted.append(dict(record))

    return {
        "partitions": {
            "accepted": accepted,
            "dead_letter": dead_letter,
        },
        "partition_counts": {
            "accepted": len(accepted),
            "dead_letter": len(dead_letter),
        },
        "violations": violations,
        "stats": {
            "input_rows": len(records),
            "accepted_rows": len(accepted),
            "dead_letter_rows": len(dead_letter),
            "violation_count": len(violations),
            "predicate_dead_letter_count": predicate_dead_letter_count,
        },
    }


def validate_records(
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    *,
    checks: list[dict[str, Any]] | None = None,
    keys: list[str] | None = None,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    for row_index, record in enumerate(records):
        record_key = (
            {field: _json_clone(record.get(field)) for field in keys or []}
            if keys
            else None
        )
        for field, raw_rule in schema.items():
            rule = dict(raw_rule) if isinstance(raw_rule, dict) else {}
            value = record.get(field)
            if rule.get("required") and not _non_empty(value):
                _append_violation(
                    violations,
                    row_index=row_index,
                    field=field,
                    code="required",
                    message=f"{field} is required",
                    record_key=record_key,
                )
                continue
            if value is None:
                if rule.get("allow_null", True):
                    continue
                _append_violation(
                    violations,
                    row_index=row_index,
                    field=field,
                    code="null_not_allowed",
                    message=f"{field} cannot be null",
                    record_key=record_key,
                )
                continue

            expected_type = rule.get("type")
            if expected_type and not _matches_type(value, str(expected_type)):
                _append_violation(
                    violations,
                    row_index=row_index,
                    field=field,
                    code="type_mismatch",
                    message=f"{field} must be {expected_type}",
                    record_key=record_key,
                )
                continue

            if "enum" in rule and value not in list(rule.get("enum") or []):
                _append_violation(
                    violations,
                    row_index=row_index,
                    field=field,
                    code="enum_mismatch",
                    message=f"{field} must be one of {list(rule.get('enum') or [])}",
                    record_key=record_key,
                )

            pattern = rule.get("regex")
            if pattern and not re.search(str(pattern), _stringify(value)):
                _append_violation(
                    violations,
                    row_index=row_index,
                    field=field,
                    code="regex_mismatch",
                    message=f"{field} does not match {pattern}",
                    record_key=record_key,
                )

            minimum = rule.get("min")
            maximum = rule.get("max")
            if minimum is not None or maximum is not None:
                comparable: float | int
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    comparable = value
                else:
                    comparable = len(_stringify(value))
                if minimum is not None and comparable < minimum:
                    _append_violation(
                        violations,
                        row_index=row_index,
                        field=field,
                        code="below_minimum",
                        message=f"{field} is below minimum {minimum}",
                        record_key=record_key,
                    )
                if maximum is not None and comparable > maximum:
                    _append_violation(
                        violations,
                        row_index=row_index,
                        field=field,
                        code="above_maximum",
                        message=f"{field} is above maximum {maximum}",
                        record_key=record_key,
                    )

    checks = checks or []
    for raw_check in checks:
        check = dict(raw_check)
        kind = str(check.get("kind") or "").strip().lower()
        if kind == "unique":
            unique_fields = [str(field).strip() for field in check.get("fields") or [] if str(field).strip()]
            if not unique_fields:
                raise DataOperationError(
                    "data.validate.unique_fields_required",
                    "unique checks require fields",
                )
            seen: dict[tuple[Any, ...], int] = {}
            for row_index, record in enumerate(records):
                key = _key_tuple(record, unique_fields)
                if key in seen:
                    _append_violation(
                        violations,
                        row_index=row_index,
                        field=",".join(unique_fields),
                        code="duplicate_key",
                        message=f"duplicate key for fields {unique_fields}",
                        record_key={field: _json_clone(record.get(field)) for field in unique_fields},
                    )
                else:
                    seen[key] = row_index
            continue
        if kind == "compare":
            left_field = str(check.get("left_field") or "").strip()
            right_field = str(check.get("right_field") or "").strip()
            op = str(check.get("op") or "eq").strip()
            if not left_field or not right_field:
                raise DataOperationError(
                    "data.validate.compare_fields_required",
                    "compare checks require left_field and right_field",
                )
            for row_index, record in enumerate(records):
                if not _compare_values(record.get(left_field), record.get(right_field), op):
                    _append_violation(
                        violations,
                        row_index=row_index,
                        field=f"{left_field},{right_field}",
                        code="compare_failed",
                        message=f"{left_field} must satisfy {op} against {right_field}",
                    )
            continue
        raise DataOperationError(
            "data.validate.unsupported_check",
            f"unsupported validation check kind: {kind}",
        )

    return {
        "violations": violations,
        "stats": {
            "input_rows": len(records),
            "violation_count": len(violations),
            "valid_row_count": max(0, len(records) - len({item['row_index'] for item in violations})),
        },
    }


def dedupe_records(
    records: list[dict[str, Any]],
    *,
    keys: list[str],
    strategy: str,
    order_field: str | None = None,
) -> dict[str, Any]:
    if not keys:
        raise DataOperationError(
            "data.dedupe.keys_required",
            "dedupe requires at least one key field",
        )

    normalized_strategy = strategy.strip().lower() or "first"
    if normalized_strategy not in {"first", "last", "most_complete", "latest_by_field"}:
        raise DataOperationError(
            "data.dedupe.unsupported_strategy",
            f"unsupported dedupe strategy: {strategy}",
        )

    groups: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for row_index, record in enumerate(records):
        groups[_key_tuple(record, keys)].append((row_index, record))

    selected_rows: list[tuple[int, dict[str, Any]]] = []
    duplicate_groups: list[dict[str, Any]] = []
    dropped_rows = 0
    for key, entries in groups.items():
        if normalized_strategy == "first":
            selected = min(entries, key=lambda item: item[0])
        elif normalized_strategy == "last":
            selected = max(entries, key=lambda item: item[0])
        else:
            selected = max(
                entries,
                key=lambda item: _sort_key_for_strategy(
                    item[1],
                    strategy=normalized_strategy,
                    order_field=order_field,
                    row_index=item[0],
                ),
            )
        selected_rows.append((selected[0], dict(selected[1])))
        if len(entries) > 1:
            duplicate_groups.append(
                {
                    "key": {keys[index]: _json_clone(value) for index, value in enumerate(key)},
                    "survivor_row_index": selected[0],
                    "row_indices": [row_index for row_index, _ in entries],
                    "dropped_row_indices": [row_index for row_index, _ in entries if row_index != selected[0]],
                }
            )
            dropped_rows += len(entries) - 1

    selected_rows.sort(key=lambda item: item[0])
    survivors = [record for _, record in selected_rows]

    return {
        "records": survivors,
        "duplicate_groups": duplicate_groups,
        "stats": {
            "input_rows": len(records),
            "output_rows": len(survivors),
            "duplicate_group_count": len(duplicate_groups),
            "dropped_rows": dropped_rows,
        },
    }


def reconcile_records(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    *,
    keys: list[str],
    compare_fields: list[str] | None = None,
) -> dict[str, Any]:
    if not keys:
        raise DataOperationError(
            "data.reconcile.keys_required",
            "reconcile requires at least one key field",
        )

    def _index_records(records: list[dict[str, Any]], side: str) -> tuple[dict[tuple[Any, ...], tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
        index: dict[tuple[Any, ...], tuple[int, dict[str, Any]]] = {}
        conflicts: list[dict[str, Any]] = []
        for row_index, record in enumerate(records):
            key = _key_tuple(record, keys)
            if key in index:
                conflicts.append(
                    {
                        "side": side,
                        "key": {keys[idx]: _json_clone(value) for idx, value in enumerate(key)},
                        "row_indices": [index[key][0], row_index],
                    }
                )
                continue
            index[key] = (row_index, record)
        return index, conflicts

    source_index, source_conflicts = _index_records(source_records, "source")
    target_index, target_conflicts = _index_records(target_records, "target")
    conflicts = [*source_conflicts, *target_conflicts]

    compare_fields = [
        field for field in (compare_fields or sorted((set(_field_names(source_records)) | set(_field_names(target_records))) - set(keys)))
        if field not in keys
    ]

    creates: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    deletes: list[dict[str, Any]] = []
    noops: list[dict[str, Any]] = []

    all_keys = sorted(set(source_index) | set(target_index), key=lambda item: repr(item))
    for key in all_keys:
        source_entry = source_index.get(key)
        target_entry = target_index.get(key)
        key_payload = {keys[idx]: _json_clone(value) for idx, value in enumerate(key)}

        if source_entry and not target_entry:
            creates.append({"key": key_payload, "record": _json_clone(source_entry[1])})
            continue
        if target_entry and not source_entry:
            deletes.append({"key": key_payload, "record": _json_clone(target_entry[1])})
            continue
        if not source_entry or not target_entry:
            continue

        source_record = source_entry[1]
        target_record = target_entry[1]
        field_diffs: dict[str, Any] = {}
        for field in compare_fields:
            left = source_record.get(field)
            right = target_record.get(field)
            if left != right:
                field_diffs[field] = {
                    "source": _json_clone(left),
                    "target": _json_clone(right),
                }
        if field_diffs:
            updates.append(
                {
                    "key": key_payload,
                    "diff": field_diffs,
                    "source_record": _json_clone(source_record),
                    "target_record": _json_clone(target_record),
                }
            )
        else:
            noops.append({"key": key_payload})

    plan = {
        "create": creates,
        "update": updates,
        "delete": deletes,
        "noop": noops,
        "conflicts": conflicts,
    }
    return {
        "plan": plan,
        "plan_digest": _canonical_digest(plan),
        "stats": {
            "source_rows": len(source_records),
            "target_rows": len(target_records),
            "create_count": len(creates),
            "update_count": len(updates),
            "delete_count": len(deletes),
            "noop_count": len(noops),
            "conflict_count": len(conflicts),
        },
        "compare_fields": compare_fields,
    }


def plan_summary(plan: dict[str, Any]) -> dict[str, int]:
    normalized = dict(plan or {})
    return {
        "create_count": len(list(normalized.get("create") or [])),
        "update_count": len(list(normalized.get("update") or [])),
        "delete_count": len(list(normalized.get("delete") or [])),
        "noop_count": len(list(normalized.get("noop") or [])),
        "conflict_count": len(list(normalized.get("conflicts") or [])),
    }


def plan_digest(plan: dict[str, Any]) -> str:
    return _canonical_digest(plan or {})


def _plan_entry_key(entry: dict[str, Any], keys: list[str]) -> tuple[Any, ...]:
    key_payload = dict(entry.get("key") or {})
    return tuple(_canonical_value(key_payload.get(field)) for field in keys)


def apply_plan_records(
    target_records: list[dict[str, Any]],
    *,
    plan: dict[str, Any],
    keys: list[str],
    approval: dict[str, Any],
) -> dict[str, Any]:
    if not keys:
        raise DataOperationError(
            "data.apply.keys_required",
            "apply requires key fields",
        )

    normalized_plan = _json_clone(plan)
    normalized_approval = _json_clone(approval)
    digest = plan_digest(normalized_plan)
    if str(normalized_approval.get("plan_digest") or "") != digest:
        raise DataOperationError(
            "data.apply.approval_mismatch",
            "approval does not match plan digest",
            details={
                "expected_plan_digest": digest,
                "approval_plan_digest": normalized_approval.get("plan_digest"),
            },
        )

    plan_conflicts = list(normalized_plan.get("conflicts") or [])
    if plan_conflicts:
        raise DataOperationError(
            "data.apply.plan_conflicts",
            "apply refuses plans that contain conflicts",
            details={"conflict_count": len(plan_conflicts), "conflicts_preview": _json_clone(plan_conflicts[:20])},
        )

    state: dict[tuple[Any, ...], dict[str, Any]] = {}
    order: list[tuple[Any, ...]] = []
    duplicate_targets: list[dict[str, Any]] = []
    for row_index, record in enumerate(target_records):
        key = _key_tuple(record, keys)
        if key in state:
            duplicate_targets.append(
                {
                    "key": {keys[idx]: _json_clone(value) for idx, value in enumerate(key)},
                    "row_index": row_index,
                }
            )
            continue
        state[key] = dict(record)
        order.append(key)
    if duplicate_targets:
        raise DataOperationError(
            "data.apply.target_conflicts",
            "apply requires a uniquely keyed target dataset",
            details={"conflict_count": len(duplicate_targets), "conflicts_preview": duplicate_targets[:20]},
        )

    stale_operations: list[dict[str, Any]] = []
    applied_create_count = 0
    applied_update_count = 0
    applied_delete_count = 0
    noop_count = 0

    for entry in list(normalized_plan.get("create") or []):
        key = _plan_entry_key(entry, keys)
        record = dict(entry.get("record") or {})
        current = state.get(key)
        if current is None:
            state[key] = record
            order.append(key)
            applied_create_count += 1
        elif current == record:
            noop_count += 1
        else:
            stale_operations.append({"type": "create", "key": dict(entry.get("key") or {})})

    for entry in list(normalized_plan.get("update") or []):
        key = _plan_entry_key(entry, keys)
        source_record = dict(entry.get("source_record") or {})
        target_record = dict(entry.get("target_record") or {})
        current = state.get(key)
        if current is None:
            stale_operations.append({"type": "update_missing_target", "key": dict(entry.get("key") or {})})
            continue
        if current == source_record:
            noop_count += 1
            continue
        if current != target_record:
            stale_operations.append({"type": "update_drift", "key": dict(entry.get("key") or {})})
            continue
        state[key] = source_record
        applied_update_count += 1

    for entry in list(normalized_plan.get("delete") or []):
        key = _plan_entry_key(entry, keys)
        target_record = dict(entry.get("record") or {})
        current = state.get(key)
        if current is None:
            noop_count += 1
            continue
        if current != target_record:
            stale_operations.append({"type": "delete_drift", "key": dict(entry.get("key") or {})})
            continue
        del state[key]
        applied_delete_count += 1

    if stale_operations:
        raise DataOperationError(
            "data.apply.target_drift",
            "target state no longer matches the approved plan",
            details={"stale_count": len(stale_operations), "stale_operations": _json_clone(stale_operations[:20])},
        )

    output_records = [dict(state[key]) for key in order if key in state]
    summary = plan_summary(normalized_plan)
    return {
        "records": output_records,
        "plan": normalized_plan,
        "plan_digest": digest,
        "approval": normalized_approval,
        "stats": {
            "target_rows": len(target_records),
            "output_rows": len(output_records),
            "planned_create_count": summary["create_count"],
            "planned_update_count": summary["update_count"],
            "planned_delete_count": summary["delete_count"],
            "applied_create_count": applied_create_count,
            "applied_update_count": applied_update_count,
            "applied_delete_count": applied_delete_count,
            "noop_count": noop_count,
        },
    }


def _chunked_records(records: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]


def sync_records(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    *,
    keys: list[str],
    compare_fields: list[str] | None = None,
    mode: str = "upsert",
    batch_size: int | None = None,
    cursor_field: str | None = None,
    checkpoint: dict[str, Any] | None = None,
    before: Any = None,
) -> dict[str, Any]:
    normalized_mode = mode.strip().lower() or "upsert"
    if normalized_mode not in {"upsert", "mirror"}:
        raise DataOperationError(
            "data.sync.unsupported_mode",
            f"unsupported sync mode: {mode}",
        )
    if batch_size is not None and batch_size <= 0:
        raise DataOperationError(
            "data.sync.batch_size_invalid",
            "sync batch_size must be positive",
        )
    if (batch_size is not None or checkpoint or before is not None) and normalized_mode != "upsert":
        raise DataOperationError(
            "data.sync.partial_mirror_unsupported",
            "checkpointed or batched sync is only supported for upsert mode",
        )

    replay_window: dict[str, Any] | None = None
    effective_source = [dict(record) for record in source_records]
    normalized_checkpoint = dict(checkpoint or {})
    if normalized_checkpoint or before is not None:
        effective_cursor_field = str(cursor_field or normalized_checkpoint.get("cursor_field") or "").strip()
        if not effective_cursor_field:
            raise DataOperationError(
                "data.sync.cursor_field_required",
                "checkpointed sync requires cursor_field",
            )
        replay = replay_records(
            effective_source,
            cursor_field=effective_cursor_field,
            after=normalized_checkpoint.get("watermark", normalized_checkpoint.get("cursor_max")),
            before=before,
        )
        effective_source = [dict(record) for record in replay["records"]]
        replay_window = dict(replay["replay_window"])
        cursor_field = effective_cursor_field

    if batch_size is not None:
        current_target = [dict(record) for record in target_records]
        combined_plan = {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []}
        batch_manifest: list[dict[str, Any]] = []
        compare_result_fields = list(compare_fields or [])
        for batch_index, batch_records in enumerate(_chunked_records(effective_source, batch_size), start=1):
            batch_result = sync_records(
                batch_records,
                current_target,
                keys=keys,
                compare_fields=compare_fields,
                mode="upsert",
            )
            current_target = [dict(record) for record in batch_result["records"]]
            compare_result_fields = list(batch_result.get("compare_fields") or compare_result_fields)
            batch_plan = dict(batch_result.get("plan") or {})
            for key_name in combined_plan:
                combined_plan[key_name].extend(_json_clone(list(batch_plan.get(key_name) or [])))
            batch_entry: dict[str, Any] = {
                "batch_index": batch_index,
                "input_rows": len(batch_records),
                "create_count": len(list(batch_plan.get("create") or [])),
                "update_count": len(list(batch_plan.get("update") or [])),
                "noop_count": len(list(batch_plan.get("noop") or [])),
                "output_rows": len(current_target),
            }
            if cursor_field:
                pairs = [
                    (_coerce_comparable(record.get(cursor_field)), _json_clone(record.get(cursor_field)))
                    for record in batch_records
                    if _non_empty(record.get(cursor_field))
                ]
                if pairs:
                    batch_entry["watermark"] = sorted(pairs, key=lambda item: item[0])[-1][1]
            batch_manifest.append(batch_entry)

        checkpoint_payload = None
        if cursor_field and effective_source:
            checkpoint_payload = checkpoint_records(
                effective_source,
                keys=keys,
                cursor_field=cursor_field,
            )["checkpoint"]
        summary = plan_summary(combined_plan)
        stats = {
            "source_rows": len(effective_source),
            "target_rows": len(target_records),
            "create_count": summary["create_count"],
            "update_count": summary["update_count"],
            "delete_count": 0,
            "noop_count": summary["noop_count"],
            "conflict_count": summary["conflict_count"],
            "output_rows": len(current_target),
            "sync_mode": normalized_mode,
            "applied_create_count": summary["create_count"],
            "applied_update_count": summary["update_count"],
            "applied_delete_count": 0,
            "batch_count": len(batch_manifest),
            "batched_input_rows": len(effective_source),
        }
        result: dict[str, Any] = {
            "records": current_target,
            "plan": combined_plan,
            "plan_digest": plan_digest(combined_plan),
            "compare_fields": compare_result_fields,
            "batch_manifest": batch_manifest,
            "stats": stats,
        }
        if checkpoint_payload is not None:
            result["checkpoint"] = checkpoint_payload
        if replay_window is not None:
            result["replay_window"] = replay_window
        return result

    reconcile = reconcile_records(
        effective_source,
        target_records,
        keys=keys,
        compare_fields=compare_fields,
    )
    plan = _json_clone(reconcile["plan"])
    if normalized_mode == "upsert":
        plan["delete"] = []

    if normalized_mode == "mirror":
        output_records = [dict(record) for record in effective_source]
    else:
        source_index = {
            _key_tuple(record, keys): dict(record)
            for record in effective_source
        }
        output_records = []
        seen: set[tuple[Any, ...]] = set()
        for target_record in target_records:
            key = _key_tuple(target_record, keys)
            if key in source_index:
                output_records.append(dict(source_index[key]))
                seen.add(key)
            else:
                output_records.append(dict(target_record))
        for source_record in effective_source:
            key = _key_tuple(source_record, keys)
            if key in seen:
                continue
            output_records.append(dict(source_record))
            seen.add(key)

    checkpoint_payload = None
    if cursor_field and effective_source:
        checkpoint_payload = checkpoint_records(
            effective_source,
            keys=keys,
            cursor_field=cursor_field,
        )["checkpoint"]

    summary = plan_summary(plan)
    result = {
        "records": output_records,
        "plan": plan,
        "plan_digest": plan_digest(plan),
        "compare_fields": list(reconcile.get("compare_fields") or []),
        "stats": {
            "source_rows": len(effective_source),
            "target_rows": len(target_records),
            "create_count": summary["create_count"],
            "update_count": summary["update_count"],
            "delete_count": summary["delete_count"] if normalized_mode == "mirror" else 0,
            "noop_count": summary["noop_count"],
            "conflict_count": summary["conflict_count"],
            "output_rows": len(output_records),
            "sync_mode": normalized_mode,
            "applied_create_count": summary["create_count"],
            "applied_update_count": summary["update_count"],
            "applied_delete_count": summary["delete_count"] if normalized_mode == "mirror" else 0,
        },
    }
    if checkpoint_payload is not None:
        result["checkpoint"] = checkpoint_payload
    if replay_window is not None:
        result["replay_window"] = replay_window
    return result
