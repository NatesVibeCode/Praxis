"""Shared payload normalization helpers for API front doors.

These helpers intentionally stay small and opinionated: normalize text-heavy
inputs with explicit failure modes so both query and write surfaces share one
contract without duplicating boilerplate checks.
"""

from __future__ import annotations

import re
from typing import Any, Iterable


def require_text(value: object, *, field_name: str) -> str:
    """Require a non-empty, non-whitespace string and return stripped text."""

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def optional_text(value: object, *, field_name: str = "field") -> str | None:
    """Return normalized text or ``None`` when the value is omitted."""

    if value is None:
        return None
    return require_text(value, field_name=field_name)


def coerce_optional_text(
    value: object,
    *,
    field_name: str = "field",
) -> str | None:
    """Parse optional text, accepting non-string values via text coercion."""

    if value is None:
        return None
    if isinstance(value, str):
        return require_text(value, field_name=field_name)
    return require_text(str(value), field_name=field_name)


def _coerce_query_text_value(
    value: Any,
    *,
    field_name: str,
) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    return normalized or None


def coerce_query_text(
    value: Any,
    *,
    field_name: str,
    default: str | None = None,
) -> str | None:
    normalized = _coerce_query_text_value(value, field_name=field_name)
    if normalized is None:
        return default
    return normalized


def coerce_query_int(
    value: Any,
    *,
    field_name: str,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
    min_value: int | None = None,
    max_value: int | None = None,
    strict: bool = False,
) -> int:
    if minimum is None:
        minimum = min_value
    if maximum is None:
        maximum = max_value

    normalized = _coerce_query_text_value(value, field_name=field_name)
    if normalized is None:
        return default

    try:
        parsed = int(normalized)
    except (TypeError, ValueError) as exc:
        if strict:
            raise ValueError(f"{field_name} must be an integer") from exc
        return default

    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


_QUERY_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled", "y"})
_QUERY_FALSE_VALUES = frozenset({"0", "false", "no", "off", "disabled", "n"})


def coerce_query_bool(
    value: Any,
    *,
    field_name: str,
    default: bool = False,
    strict: bool = False,
) -> bool:
    normalized = _coerce_query_text_value(value, field_name=field_name)
    if normalized is None:
        return default

    lowered = normalized.lower()
    if lowered in _QUERY_TRUE_VALUES:
        return True
    if lowered in _QUERY_FALSE_VALUES:
        return False
    if strict:
        raise ValueError(f"{field_name} must be a boolean")
    return default


def require_choice(
    value: object,
    *,
    field_name: str,
    choices: Iterable[str],
) -> str:
    """Validate normalized text against an explicit allow-list."""

    allowed = tuple(choices)
    normalized = require_text(value, field_name=field_name).strip()
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} must be one of {allowed_values}")
    return normalized


def coerce_choice(
    value: object,
    *,
    field_name: str,
    choices: Iterable[str],
    normalize: bool = True,
) -> str:
    normalized = require_text(value, field_name=field_name)
    if normalize:
        normalized = normalized.lower()

    allowed = tuple(choices)
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} must be one of {allowed_values}")
    return normalized


def coerce_slug(
    value: object,
    *,
    field_name: str,
    separator: str,
) -> str:
    normalized = require_text(value, field_name=field_name).lower()
    tokens = re.findall(r"[a-z0-9]+", normalized)
    if not tokens:
        raise ValueError(f"{field_name} must contain at least one alphanumeric character")
    return separator.join(tokens)


def coerce_text_sequence(value: Any, *, field_name: str) -> tuple[str, ...]:
    """Normalize text list-like inputs into deduplicated tuple tokens.

    Behavior is intentionally stable across operators:
    - ``None`` becomes an empty tuple
    - a single string becomes a single-item tuple
    - iterables of strings are trimmed and deduplicated in order
    """

    if value is None:
        return ()
    if isinstance(value, str):
        return (require_text(value, field_name=field_name),)
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be a list of non-empty strings")
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(require_text(item, field_name=f"{field_name}[{index}]"))
    # preserve first occurrence order while removing duplicates
    return tuple(dict.fromkeys(normalized))


__all__ = [
    "coerce_choice",
    "coerce_optional_text",
    "coerce_query_bool",
    "coerce_query_int",
    "coerce_query_text",
    "coerce_slug",
    "coerce_text_sequence",
    "optional_text",
    "require_choice",
    "require_text",
]
