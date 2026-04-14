"""Shared condition evaluator and path extraction.

Used by:
- Workflow graph conditional edges (dependency.py)
- Webhook trigger filters (trigger_filter.py)
- Trigger payload matching (triggers.py)
- Data mapping field extraction (data_mapper.py)

Supports:
- Dot-notation path extraction: 'a.b.c', 'items[0]', 'items[*].name'
- Comparison ops: eq, neq, gt, gte, lt, lte, contains, starts_with, ends_with, exists, in, regex
- Logical composition: and, or, not (recursive condition trees)
- Flat legacy format: {"field": "x", "op": "equals", "value": 1}
"""

from __future__ import annotations

import re
from typing import Any, Optional

_ARRAY_INDEX_RE = re.compile(r"^(.+?)\[(-?\d+|\*)\]$")

_COMPARISON_OPS = frozenset({
    "eq", "neq", "gt", "gte", "lt", "lte", "contains",
    "starts_with", "ends_with", "exists", "in", "regex",
})
_LOGICAL_OPS = frozenset({"and", "or", "not"})

# Legacy op names from dependency.py / contracts
_LEGACY_OP_MAP = {
    "equals": "eq",
    "not_equals": "neq",
}


# ---------------------------------------------------------------------------
# Path extraction
# ---------------------------------------------------------------------------

def extract_path(data: Any, path: str) -> Any:
    """Extract a value from nested data using dot notation.

    Supports: 'a.b.c', 'items[0]', 'items[-1]', 'items[*].name'
    """
    if data is None:
        return None
    parts = path.split(".")
    current = data
    for i, part in enumerate(parts):
        if current is None:
            return None
        match = _ARRAY_INDEX_RE.match(part)
        if match:
            key, index_str = match.group(1), match.group(2)
            current = _get_key(current, key)
            if not isinstance(current, (list, tuple)):
                return None
            if index_str == "*":
                remaining = ".".join(parts[i + 1:])
                if remaining:
                    return [extract_path(item, remaining) for item in current]
                return list(current)
            try:
                idx = int(index_str)
                current = current[idx] if -len(current) <= idx < len(current) else None
            except (IndexError, ValueError):
                return None
        else:
            current = _get_key(current, part)
    return current


def _get_key(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        return data.get(key)
    return None


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def _compare(actual: Any, op: str, expected: Any) -> bool:
    """Evaluate a single comparison."""
    try:
        if op == "exists":
            return actual is not None
        if actual is None:
            return False
        if op == "eq":
            return actual == expected
        if op == "neq":
            return actual != expected
        if op == "gt":
            return float(actual) > float(expected)
        if op == "gte":
            return float(actual) >= float(expected)
        if op == "lt":
            return float(actual) < float(expected)
        if op == "lte":
            return float(actual) <= float(expected)
        if op == "contains":
            if isinstance(actual, str):
                return str(expected) in actual
            if isinstance(actual, (list, tuple)):
                return expected in actual
            return False
        if op == "starts_with":
            return str(actual).startswith(str(expected))
        if op == "ends_with":
            return str(actual).endswith(str(expected))
        if op == "in":
            if isinstance(expected, (list, tuple)):
                return actual in expected
            return False
        if op == "regex":
            return bool(re.search(str(expected), str(actual)))
    except (ValueError, TypeError):
        pass
    return False


# ---------------------------------------------------------------------------
# Filter evaluation (tree and flat formats)
# ---------------------------------------------------------------------------

def evaluate_filter(payload: dict, expression: dict) -> bool:
    """Recursively evaluate a condition tree against a payload."""
    if not expression:
        return True

    op = expression.get("op", "").lower()
    op = _LEGACY_OP_MAP.get(op, op)

    if op in _LOGICAL_OPS:
        conditions = expression.get("conditions", [])
        if op == "and":
            return all(evaluate_filter(payload, c) for c in conditions)
        if op == "or":
            return any(evaluate_filter(payload, c) for c in conditions)
        if op == "not":
            return not evaluate_filter(payload, conditions[0]) if conditions else True

    if op in _COMPARISON_OPS:
        field_path = expression.get("field", "")
        expected = expression.get("value")
        actual = extract_path(payload, field_path)
        return _compare(actual, op, expected)

    return True


def evaluate_condition_tree(data: dict, expression: dict) -> bool:
    """Auto-detect flat or tree format and evaluate.

    Flat format (legacy):  {"field": "x", "op": "equals", "value": 1}
    Tree format:           {"op": "and", "conditions": [...]}
    """
    if not expression:
        return True

    # Flat format: has "field" key, no "conditions" key
    if "field" in expression and "conditions" not in expression:
        op = expression.get("op", "")
        # Legacy compat: {"field": "x", "equals": value}
        if not op:
            for legacy_op in ("equals", "not_equals", "in", "not_in"):
                if legacy_op in expression:
                    op = legacy_op
                    expression = {"field": expression["field"], "op": op, "value": expression[legacy_op]}
                    break
        normalized_op = _LEGACY_OP_MAP.get(op, op)
        if op == "not_in":
            return not _compare(
                extract_path(data, expression.get("field", "")),
                "in",
                expression.get("value"),
            )
        return _compare(
            extract_path(data, expression.get("field", "")),
            normalized_op,
            expression.get("value"),
        )

    return evaluate_filter(data, expression)


def validate_filter(expression: dict) -> list[str]:
    """Validate a filter expression. Returns list of errors (empty = valid)."""
    errors: list[str] = []
    if not isinstance(expression, dict):
        errors.append("expression must be a dict")
        return errors
    op = expression.get("op", "")
    if not op:
        errors.append("missing 'op'")
        return errors
    normalized = _LEGACY_OP_MAP.get(op, op)
    if normalized in _LOGICAL_OPS:
        conditions = expression.get("conditions")
        if not isinstance(conditions, list):
            errors.append(f"{op} requires 'conditions' list")
        else:
            for i, c in enumerate(conditions):
                sub_errors = validate_filter(c)
                errors.extend(f"conditions[{i}].{e}" for e in sub_errors)
    elif normalized in _COMPARISON_OPS:
        if "field" not in expression:
            errors.append(f"comparison op '{op}' requires 'field'")
    else:
        errors.append(f"unknown op: {op}")
    return errors
