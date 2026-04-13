"""Validation functions and exception classes for Postgres storage."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any
import json


class PostgresStorageError(RuntimeError):
    """Base class for explicit Postgres storage failures."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


class PostgresConfigurationError(PostgresStorageError):
    """Raised when the Postgres configuration is missing or malformed."""


class PostgresSchemaError(PostgresStorageError):
    """Raised when the control-plane schema cannot be bootstrapped."""


class PostgresWriteError(PostgresStorageError):
    """Raised when a control-plane write cannot be completed safely."""


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a mapping",
            details={"field": field_name},
        )
    return value


def _require_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a positive integer",
            details={"field": field_name},
        )
    return value


def _require_nonnegative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a non-negative integer",
            details={"field": field_name},
        )
    return value


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_utc(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be UTC-backed",
            details={"field": field_name},
        )
    return value


def _encode_jsonb(value: object, *, field_name: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be JSON serializable",
            details={"field": field_name},
        ) from exc


def _require_child_row_payloads(
    value: object,
    *,
    field_name: str,
    required_keys: tuple[str, ...],
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a list when present",
            details={"field": field_name},
        )

    payloads: list[Mapping[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise PostgresWriteError(
                "postgres.invalid_submission",
                f"{field_name}[{index}] must be a mapping",
                details={"field": f"{field_name}[{index}]"},
            )
        missing_keys = [key for key in required_keys if key not in item]
        if missing_keys:
            raise PostgresWriteError(
                "postgres.invalid_submission",
                f"{field_name}[{index}] is missing required keys",
                details={
                    "field": f"{field_name}[{index}]",
                    "missing_keys": missing_keys,
                },
            )
        payloads.append(item)

    return tuple(payloads)
