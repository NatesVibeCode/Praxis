"""Shared handling for documented example identifiers.

Example IDs are useful in docs, but they must never select live resources.
Keep this module dependency-light because API startup imports it early.
"""
from __future__ import annotations


_DEMO_PLACEHOLDER_IDS_BY_FIELD: dict[str, frozenset[str]] = {
    "entity_id": frozenset({"entity_abc123"}),
    "sandbox_id": frozenset({"sandbox_abc123"}),
    "wave_id": frozenset({"wave_abc123"}),
}


def is_demo_placeholder(field_name: str, value: object) -> bool:
    """Return True when *value* is a known non-live example ID."""
    raw = str(value or "").strip()
    return raw in _DEMO_PLACEHOLDER_IDS_BY_FIELD.get(field_name, frozenset())


def placeholder_error(field_name: str, value: object) -> dict[str, str]:
    """Machine-readable error payload for rejected example IDs."""
    raw = str(value or "").strip()
    return {
        "error": (
            f"{field_name} '{raw}' is an example placeholder and cannot be used "
            "as a live resource selector"
        ),
        "reason_code": f"{field_name}.placeholder_not_allowed",
        field_name: raw,
    }


def placeholder_error_message(field_name: str, value: object) -> str:
    """Human-readable error text for surfaces that raise client errors."""
    return placeholder_error(field_name, value)["error"]
