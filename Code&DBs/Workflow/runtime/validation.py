"""Shared validation helpers for runtime and surface modules.

This is the single canonical home for text, integer, boolean, datetime, and
mapping validation. Callers pass their own ``error_factory`` + ``reason_code``
so the error shape matches each surface's existing contract (no swallowing of
callers' exception types).

Roadmap: ``roadmap_item.extract.shared.validation.layer.from.surfaces.and.runtime``.
Historical path ``policy/_authority_validation.py`` was the earlier location
for these helpers; callers now import from ``runtime.validation`` directly.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, NoReturn, Protocol, TypeVar

_ErrorT = TypeVar("_ErrorT", bound=BaseException)


class ErrorFactory(Protocol[_ErrorT]):
    def __call__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> _ErrorT: ...


def _raise_validation_error(
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> NoReturn:
    raise error_factory(reason_code, message, details=details)


def normalize_as_of(
    value: datetime,
    *,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    coerce_utc: bool = False,
) -> datetime:
    if not isinstance(value, datetime):
        _raise_validation_error(
            error_factory,
            reason_code,
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        _raise_validation_error(
            error_factory,
            reason_code,
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc) if coerce_utc else value


def require_text(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
) -> str:
    if not isinstance(value, str) or not value.strip():
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be a non-empty string",
            details=payload,
        )
    return value.strip()


def optional_text(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
) -> str | None:
    if value is None:
        return None
    return require_text(
        value,
        field_name=field_name,
        error_factory=error_factory,
        reason_code=reason_code,
        details=details,
        include_value_type=include_value_type,
    )


def require_int(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be an integer",
            details=payload,
        )
    return value


def require_bool(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
) -> bool:
    if not isinstance(value, bool):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be a boolean",
            details=payload,
        )
    return value


def require_datetime(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
    require_timezone: bool = False,
    coerce_utc: bool = False,
) -> datetime:
    if not isinstance(value, datetime):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be a datetime",
            details=payload,
        )
    if require_timezone and (value.tzinfo is None or value.utcoffset() is None):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be timezone-aware",
            details=payload,
        )
    return value.astimezone(timezone.utc) if coerce_utc else value


def require_mapping(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
    parse_json_strings: bool = False,
    normalize_keys: bool = False,
    mapping_label: str = "mapping",
) -> Mapping[str, Any] | dict[str, Any]:
    normalized_value = json.loads(value) if parse_json_strings and isinstance(value, str) else value
    if not isinstance(normalized_value, Mapping):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(normalized_value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be a {mapping_label}",
            details=payload,
        )
    if normalize_keys:
        return {str(key): normalized_value[key] for key in normalized_value}
    return normalized_value


def coerce_text_sequence(
    value: object,
    *,
    field_name: str,
    error_factory: ErrorFactory[_ErrorT],
    reason_code: str,
    details: Mapping[str, Any] | None = None,
    include_value_type: bool = True,
) -> tuple[str, ...]:
    """Return a tuple of non-empty stripped strings from a list/tuple of values."""
    if not isinstance(value, (list, tuple)):
        payload = dict(details or {})
        payload["field"] = field_name
        if include_value_type:
            payload["value_type"] = type(value).__name__
        _raise_validation_error(
            error_factory,
            reason_code,
            f"{field_name} must be a list of strings",
            details=payload,
        )
    items: list[str] = []
    for index, raw in enumerate(value):
        items.append(
            require_text(
                raw,
                field_name=f"{field_name}[{index}]",
                error_factory=error_factory,
                reason_code=reason_code,
                include_value_type=include_value_type,
            )
        )
    return tuple(items)
