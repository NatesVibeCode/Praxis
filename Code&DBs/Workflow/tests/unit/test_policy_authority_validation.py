from __future__ import annotations

from datetime import datetime, timezone

import pytest

from policy._authority_validation import (
    normalize_as_of,
    require_datetime,
    require_mapping,
    require_text,
)


class _TestPolicyValidationError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, details=None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _error(
    reason_code: str,
    message: str,
    *,
    details=None,
) -> _TestPolicyValidationError:
    return _TestPolicyValidationError(reason_code, message, details=details)


def test_require_text_reports_field_and_value_type() -> None:
    with pytest.raises(_TestPolicyValidationError) as exc_info:
        require_text(
            42,
            field_name="decision_key",
            error_factory=_error,
            reason_code="policy.invalid_row",
        )

    assert exc_info.value.reason_code == "policy.invalid_row"
    assert exc_info.value.details == {
        "field": "decision_key",
        "value_type": "int",
    }


def test_require_mapping_parses_json_strings_and_normalizes_keys() -> None:
    value = require_mapping(
        '{"attempts":3,"backoff":"fast"}',
        field_name="retry_policy",
        error_factory=_error,
        reason_code="policy.invalid_row",
        parse_json_strings=True,
        normalize_keys=True,
        mapping_label="object",
    )

    assert value == {
        "attempts": 3,
        "backoff": "fast",
    }


def test_require_datetime_can_enforce_timezone_and_coerce_utc() -> None:
    pacific_time = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc).astimezone()

    normalized = require_datetime(
        pacific_time,
        field_name="effective_from",
        error_factory=_error,
        reason_code="policy.invalid_row",
        require_timezone=True,
        coerce_utc=True,
    )

    assert normalized.tzinfo == timezone.utc


def test_normalize_as_of_rejects_naive_datetimes() -> None:
    with pytest.raises(_TestPolicyValidationError) as exc_info:
        normalize_as_of(
            datetime(2026, 4, 7, 12, 0),
            error_factory=_error,
            reason_code="policy.invalid_as_of",
        )

    assert exc_info.value.reason_code == "policy.invalid_as_of"
    assert exc_info.value.details == {"value_type": "datetime"}
