"""Shared validation helpers for policy authority modules."""

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
