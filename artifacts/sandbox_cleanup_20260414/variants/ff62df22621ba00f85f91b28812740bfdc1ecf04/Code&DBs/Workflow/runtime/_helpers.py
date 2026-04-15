"""Shared private helper utilities for runtime-adjacent modules."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, MutableSequence, Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

if TYPE_CHECKING:
    from adapters.deterministic import DeterministicTaskRequest, DeterministicTaskResult

__all__ = [
    "_append_indexed_lines",
    "_dedupe",
    "_fail",
    "_format_bool",
    "_json_compatible",
]

_ItemT = TypeVar("_ItemT")
_ErrorT = TypeVar("_ErrorT")


class _ErrorFactory(Protocol[_ErrorT]):
    def __call__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> _ErrorT: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _merged_details(
    details: Mapping[str, Any] | None,
    extra_details: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(details or {})
    merged.update(extra_details)
    return merged


def _fail(
    code: str | None = None,
    message: str | None = None,
    /,
    **details: Any,
) -> dict[str, Any] | DeterministicTaskResult | _ErrorT:
    """Build a standardized failure payload or a typed failure object."""

    error_type = details.pop("error_type", None)
    request = details.pop("request", None)
    failure_code = details.pop("failure_code", None)
    started_at = details.pop("started_at", None)
    inputs = details.pop("inputs", None)
    outputs = details.pop("outputs", None)
    executor_type = details.pop("executor_type", None)
    finished_at = details.pop("finished_at", None)
    explicit_details = details.pop("details", None)

    if explicit_details is not None and not isinstance(explicit_details, Mapping):
        raise TypeError("details must be a mapping when provided")

    payload_details = _merged_details(explicit_details, details)

    if request is not None:
        from adapters.deterministic import DeterministicTaskRequest, DeterministicTaskResult

        if not isinstance(request, DeterministicTaskRequest):
            raise TypeError("request must be a DeterministicTaskRequest")
        if not isinstance(started_at, datetime):
            raise TypeError("started_at must be a datetime")
        if not isinstance(inputs, Mapping):
            raise TypeError("inputs must be a mapping")
        if outputs is not None and not isinstance(outputs, Mapping):
            raise TypeError("outputs must be a mapping when provided")
        if not isinstance(executor_type, str) or not executor_type:
            raise TypeError("executor_type must be a non-empty string")
        if finished_at is None:
            finished_at = _utc_now()
        elif not isinstance(finished_at, datetime):
            raise TypeError("finished_at must be a datetime when provided")

        reason_code = code or payload_details.pop("reason_code", None)
        if not isinstance(reason_code, str) or not reason_code:
            raise TypeError("reason_code must be a non-empty string")

        resolved_failure_code = failure_code if failure_code is not None else reason_code
        if not isinstance(resolved_failure_code, str) or not resolved_failure_code:
            raise TypeError("failure_code must be a non-empty string")

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="failed",
            reason_code=reason_code,
            executor_type=executor_type,
            inputs=dict(inputs),
            outputs=dict(outputs or {}),
            started_at=started_at,
            finished_at=finished_at,
            failure_code=resolved_failure_code,
        )

    reason_code = code or payload_details.pop("reason_code", None)
    if not isinstance(reason_code, str) or not reason_code:
        raise TypeError("code must be a non-empty string")
    if not isinstance(message, str) or not message:
        raise TypeError("message must be a non-empty string")

    if error_type is not None:
        typed_error_type = error_type
        return typed_error_type(reason_code, message, details=payload_details or None)

    error: dict[str, Any] = {
        "ok": False,
        "code": reason_code,
        "message": message,
    }
    if payload_details:
        error["details"] = payload_details
    return error


def _dedupe(
    items: Iterable[_ItemT],
    key: Callable[[_ItemT], object] | None = None,
) -> tuple[_ItemT, ...]:
    """Preserve input order while removing duplicates."""

    seen: set[object] = set()
    deduped: list[_ItemT] = []
    for item in items:
        marker = item if key is None else key(item)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return tuple(deduped)


def _format_bool(value: object) -> str:
    return "true" if value else "false"


def _json_compatible(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "to_json"):
        return _json_compatible(value.to_json())
    if hasattr(value, "to_contract"):
        return _json_compatible(value.to_contract())
    if is_dataclass(value):
        return {
            field.name: _json_compatible(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_compatible(item) for item in value]
    if hasattr(value, "value") and type(getattr(value, "value")).__name__ == "str":
        return getattr(value, "value")
    return value


def _append_indexed_lines(
    lines: MutableSequence[str],
    label: str,
    items: Iterable[object],
) -> None:
    values = tuple(items)
    lines.append(f"{label}_count: {len(values)}")
    for index, value in enumerate(values):
        lines.append(f"{label}[{index}]: {value}")
