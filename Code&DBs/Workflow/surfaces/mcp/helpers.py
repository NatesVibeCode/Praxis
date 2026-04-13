"""Shared utility functions for MCP tool handlers."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from surfaces.api.handlers._shared import _bug_to_dict as _shared_bug_to_dict


def _serialize(obj: Any, *, strip_empty: bool = False) -> Any:
    """Convert dataclass / datetime / enum / tuple to JSON-safe form.

    When *strip_empty* is True, recursively drop None, empty strings,
    empty collections, ``False``, and ``0`` — then prune any container
    that becomes empty after stripping.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        out = {k: _serialize(v, strip_empty=strip_empty) for k, v in obj.items()}
        if strip_empty:
            out = {k: v for k, v in out.items() if not _is_empty(v)}
        return out
    if isinstance(obj, (list, tuple)):
        out = [_serialize(v, strip_empty=strip_empty) for v in obj]
        if strip_empty:
            out = [v for v in out if not _is_empty(v)]
        return out
    if hasattr(obj, "__dataclass_fields__"):
        out = {k: _serialize(v, strip_empty=strip_empty) for k, v in obj.__dict__.items()}
        if strip_empty:
            out = {k: v for k, v in out.items() if not _is_empty(v)}
        return out
    if hasattr(obj, "value"):  # enum
        return obj.value
    return str(obj)


def _is_empty(v: Any) -> bool:
    """True when *v* carries no meaningful signal."""
    if v is None:
        return True
    if v is False or v == 0:
        return True
    if isinstance(v, str) and v == "":
        return True
    if isinstance(v, (dict, list)) and len(v) == 0:
        return True
    return False


def _bug_to_dict(bug) -> dict:
    """Convert a Bug dataclass to a plain dict."""
    return _shared_bug_to_dict(bug)


def _matches(text: str, keywords: list[str]) -> bool:
    """True if any keyword appears in text."""
    return any(kw in text for kw in keywords)
