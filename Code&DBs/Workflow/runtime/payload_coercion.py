"""Shared payload coercion utilities.

All functions return safe defaults on failure — no exceptions raised.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def json_object(value: Any) -> dict[str, Any]:
    """Coerce value to dict; returns {} on failure."""
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


def json_list(value: Any) -> list[Any]:
    """Coerce value to list; returns [] on failure."""
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def coerce_datetime(value: Any) -> datetime | None:
    """Coerce value to datetime; returns None on failure."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def coerce_text(value: Any, default: str = "") -> str:
    """Strip string value; returns default if not a string."""
    return value.strip() if isinstance(value, str) else default


def coerce_int(
    value: Any,
    *,
    default: int = 0,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Coerce value to int with optional bounds; returns default on failure."""
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def coerce_isoformat(value: Any) -> str | None:
    """Render datetime-like value as ISO string; returns None for None input."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def parse_json_field(value: Any) -> Any:
    """Parse JSON string; returns original value if not a JSON string."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value
