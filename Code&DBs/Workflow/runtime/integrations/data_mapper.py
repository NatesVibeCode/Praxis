"""Lightweight data mapping / transformation engine.

Normalizes API responses across services with dot-notation extraction,
array indexing, wildcards, and type coercion.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from runtime.condition_evaluator import extract_path  # noqa: F401 — re-exported

logger = logging.getLogger(__name__)


@dataclass
class FieldMapping:
    source: str
    target: str
    coerce: Optional[str] = None
    fallback: Any = None


@dataclass
class TransformSpec:
    mappings: list[FieldMapping] = field(default_factory=list)
    defaults: dict[str, Any] = field(default_factory=dict)
    drop_unmapped: bool = False


def coerce_value(value: Any, target_type: str) -> Any:
    """Coerce a value to the target type. Returns unchanged on failure."""
    if value is None:
        return value
    try:
        if target_type == "str":
            return str(value)
        if target_type == "int":
            return int(float(value))
        if target_type == "float":
            return float(value)
        if target_type == "bool":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
        if target_type == "iso_date":
            if isinstance(value, (int, float)):
                return datetime.utcfromtimestamp(value).isoformat() + "Z"
            if isinstance(value, str):
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                    try:
                        return datetime.strptime(value[:len(fmt.replace("%", "x"))], fmt).isoformat() + "Z"
                    except ValueError:
                        continue
                return value
    except (ValueError, TypeError, OverflowError):
        logger.debug("coerce_value failed: %r -> %s", value, target_type)
    return value


def apply_transform(data: dict, spec: TransformSpec) -> dict:
    """Apply a TransformSpec to input data, returning a flat output dict."""
    output: dict[str, Any] = {}
    if not spec.drop_unmapped:
        output.update(data)
    for mapping in spec.mappings:
        value = extract_path(data, mapping.source)
        if value is None:
            value = mapping.fallback
        if value is not None and mapping.coerce:
            value = coerce_value(value, mapping.coerce)
        if value is not None:
            output[mapping.target] = value
    for key, default in spec.defaults.items():
        if key not in output:
            output[key] = default
    return output


def transform_from_dict(spec_dict: dict) -> TransformSpec:
    """Parse a JSON/dict representation into TransformSpec."""
    mappings = [
        FieldMapping(
            source=m["source"],
            target=m["target"],
            coerce=m.get("coerce"),
            fallback=m.get("fallback"),
        )
        for m in spec_dict.get("mappings", [])
    ]
    return TransformSpec(
        mappings=mappings,
        defaults=spec_dict.get("defaults", {}),
        drop_unmapped=spec_dict.get("drop_unmapped", False),
    )
