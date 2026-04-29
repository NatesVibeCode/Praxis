"""Shared helpers for repo-local API surfaces."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _json_compatible
from runtime.async_bridge import run_sync_safe


def _build_error(
    error_type: type[Exception],
    *,
    message: str,
    reason_code: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> Exception:
    if reason_code is None:
        return error_type(message)
    try:
        return error_type(reason_code, message, details=details)  # type: ignore[call-arg,misc]
    except TypeError:
        return error_type(f"{reason_code}: {message}")


def _run_async(
    awaitable: Awaitable[Any],
    *,
    error_type: type[Exception] = RuntimeError,
    reason_code: str | None = None,
    message: str = "sync entrypoints require a non-async call boundary",
) -> Any:
    # Cache-bypass-style fix (operator decision 2026-04-29): the previous
    # implementation refused to run when a parent event loop was active.
    # That broke every operator-query wizard reachable from MCP HTTP
    # (api-server runs an asyncio loop). run_sync_safe handles both lanes
    # — direct asyncio.run when there's no parent loop, worker-thread when
    # there is — so the proactive guard is dead. Kept the kwargs for API
    # compatibility with callers that still pass error_type / reason_code /
    # message; if run_sync_safe itself raises (e.g. coroutine error), the
    # exception bubbles unchanged.
    return run_sync_safe(awaitable)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_as_of(
    value: datetime,
    *,
    error_type: type[Exception] = RuntimeError,
    reason_code: str | None = None,
) -> datetime:
    if not isinstance(value, datetime):
        raise _build_error(
            error_type,
            message="as_of must be a datetime",
            reason_code=reason_code,
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _build_error(
            error_type,
            message="as_of must be timezone-aware",
            reason_code=reason_code,
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


__all__ = [
    "_json_compatible",
    "_normalize_as_of",
    "_now",
    "_run_async",
]
