"""Shared workflow submission normalization helpers.

The submission frontdoor and submission capture path both validate the same
shapes. Keeping those rules in one place prevents the surface and the runtime
from drifting into slightly different versions of the same contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SubmissionContractError(ValueError):
    """Raised when a submission field cannot be normalized safely."""

    message: str
    field_name: str
    details: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        ValueError.__init__(self, self.message)


def _raise(field_name: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
    raise SubmissionContractError(
        message=message,
        field_name=field_name,
        details={"field": field_name, **dict(details or {})},
    )


def normalize_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _raise(
            field_name,
            f"{field_name} must be a non-empty string",
            details={"value_type": type(value).__name__},
        )
    return text


def optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return normalize_text(value, field_name=field_name)


def normalize_text_list(value: object | None, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [normalize_text(value, field_name=field_name)]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        _raise(
            field_name,
            f"{field_name} must be a list of strings",
            details={"value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(normalize_text(item, field_name=f"{field_name}[{index}]"))
    return normalized


def normalize_path(value: object, *, field_name: str) -> str:
    text = normalize_text(value, field_name=field_name)
    if text.startswith("file:"):
        text = text[5:]
    return Path(text).as_posix().lstrip("./")


def normalize_declared_operations(value: object | None) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        _raise(
            "declared_operations",
            "declared_operations must be a list of objects",
            details={"value_type": type(value).__name__},
        )

    result: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            _raise(
                f"declared_operations[{index}]",
                f"declared_operations[{index}] must be an object",
                details={"value_type": type(item).__name__},
            )
        action = normalize_text(item.get("action"), field_name=f"declared_operations[{index}].action").lower()
        if action not in {"create", "update", "delete", "rename"}:
            _raise(
                f"declared_operations[{index}].action",
                "declared_operations action must be one of create, update, delete, rename",
                details={"action": action},
            )
        entry = {
            "path": normalize_path(item.get("path"), field_name=f"declared_operations[{index}].path"),
            "action": action,
        }
        if item.get("from_path") is not None:
            entry["from_path"] = normalize_path(
                item["from_path"],
                field_name=f"declared_operations[{index}].from_path",
            )
        result.append(entry)
    return result


def normalize_scope_paths(value: object | None) -> list[str]:
    normalized = [
        normalize_path(item, field_name="write_scope")
        for item in normalize_text_list(value, field_name="write_scope")
    ]
    return list(dict.fromkeys(normalized))


def optional_datetime(value: object | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            _raise(
                field_name,
                f"{field_name} must be timezone-aware",
                details={"value_type": type(value).__name__},
            )
        return value.astimezone(timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        _raise(
            field_name,
            f"{field_name} must be an ISO-8601 timestamp when provided",
            details={"value": text, "error": str(exc)},
        )
    if normalized.tzinfo is None or normalized.utcoffset() is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc)


def normalize_timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value or "").strip()


def strip_str(value: str | None) -> str | None:
    return (value.strip() or None) if isinstance(value, str) else None
