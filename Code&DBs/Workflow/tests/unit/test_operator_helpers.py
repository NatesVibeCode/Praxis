from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

import pytest

from surfaces.api._operator_helpers import _json_compatible, _normalize_as_of, _run_async


class _SurfaceError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, details=None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


class _Status(Enum):
    ACTIVE = "active"


@dataclass
class _ContractModel:
    created_at: datetime
    status: _Status


class _ToJsonModel:
    def to_json(self) -> dict[str, object]:
        return {
            "emitted_at": datetime(2026, 4, 7, 18, 0, tzinfo=timezone.utc),
            "items": ("alpha", "beta"),
        }


def test_run_async_executes_without_running_loop() -> None:
    async def _sample() -> str:
        await asyncio.sleep(0)
        return "ok"

    assert _run_async(_sample()) == "ok"


def test_run_async_raises_custom_error_inside_running_loop() -> None:
    async def _invoke() -> None:
        coro = asyncio.sleep(0)
        with pytest.raises(_SurfaceError) as exc_info:
            _run_async(
                coro,
                error_type=_SurfaceError,
                reason_code="surface.async_boundary_required",
                message="sync surface requires a non-async call boundary",
            )
        coro.close()
        assert exc_info.value.reason_code == "surface.async_boundary_required"
        assert str(exc_info.value) == "sync surface requires a non-async call boundary"

    asyncio.run(_invoke())


def test_normalize_as_of_converts_timezone_to_utc() -> None:
    local_time = datetime(
        2026,
        4,
        7,
        11,
        30,
        tzinfo=timezone(timedelta(hours=-7)),
    )

    normalized = _normalize_as_of(
        local_time,
        error_type=_SurfaceError,
        reason_code="surface.invalid_as_of",
    )

    assert normalized == datetime(2026, 4, 7, 18, 30, tzinfo=timezone.utc)


def test_normalize_as_of_rejects_naive_datetimes() -> None:
    with pytest.raises(_SurfaceError) as exc_info:
        _normalize_as_of(
            datetime(2026, 4, 7, 18, 30),
            error_type=_SurfaceError,
            reason_code="surface.invalid_as_of",
        )

    assert exc_info.value.reason_code == "surface.invalid_as_of"
    assert exc_info.value.details == {"value_type": "datetime"}


def test_json_compatible_serializes_dataclasses_to_json_safe_payloads() -> None:
    payload = {
        "contract": _ContractModel(
            created_at=datetime(2026, 4, 7, 18, 0, tzinfo=timezone.utc),
            status=_Status.ACTIVE,
        ),
        "nested": _ToJsonModel(),
    }

    assert _json_compatible(payload) == {
        "contract": {
            "created_at": "2026-04-07T18:00:00+00:00",
            "status": "active",
        },
        "nested": {
            "emitted_at": "2026-04-07T18:00:00+00:00",
            "items": ["alpha", "beta"],
        },
    }
