"""Native scheduler authority over stored workflow classes and schedules.

This module keeps recurring workflow scheduling deterministic and fail-closed:

- schedule meaning comes from stored `schedule_definitions` rows
- workflow breadth comes from stored `workflow_classes` rows
- overlapping active rows are treated as ambiguity, not as a hint to guess
- no wrapper memory or hosted scheduler service is consulted
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Protocol, cast

import asyncpg

from runtime._helpers import _fail as _shared_fail, _json_compatible


class NativeSchedulerError(RuntimeError):
    """Raised when native scheduler authority cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=NativeSchedulerError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "native_scheduler.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise _fail(
            "native_scheduler.invalid_row",
            f"{field_name} must be a boolean",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_utc(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "native_scheduler.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise _fail(
            "native_scheduler.invalid_row",
            f"{field_name} must be UTC-backed",
            details={"field": field_name},
        )
    return value


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail(
            "native_scheduler.invalid_row",
            f"{field_name} must be an object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_as_of(value: datetime) -> datetime:
    normalized = _require_utc(value, field_name="as_of")
    return normalized.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class NativeWorkflowClassRecord:
    """One canonical native workflow-class row."""

    workflow_class_id: str
    class_name: str
    class_kind: str
    workflow_lane_id: str
    status: str
    queue_shape: Mapping[str, Any]
    throttle_policy: Mapping[str, Any]
    review_required: bool
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str | None
    created_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "workflow_class_id": self.workflow_class_id,
            "class_name": self.class_name,
            "class_kind": self.class_kind,
            "workflow_lane_id": self.workflow_lane_id,
            "status": self.status,
            "queue_shape": _json_compatible(self.queue_shape),
            "throttle_policy": _json_compatible(self.throttle_policy),
            "review_required": self.review_required,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": None if self.effective_to is None else self.effective_to.isoformat(),
            "decision_ref": self.decision_ref,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class NativeScheduleDefinitionRecord:
    """One canonical recurring schedule-definition row."""

    schedule_definition_id: str
    workflow_class_id: str
    schedule_name: str
    schedule_kind: str
    status: str
    cadence_policy: Mapping[str, Any]
    throttle_policy: Mapping[str, Any]
    target_ref: str
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str | None
    created_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "schedule_definition_id": self.schedule_definition_id,
            "workflow_class_id": self.workflow_class_id,
            "schedule_name": self.schedule_name,
            "schedule_kind": self.schedule_kind,
            "status": self.status,
            "cadence_policy": _json_compatible(self.cadence_policy),
            "throttle_policy": _json_compatible(self.throttle_policy),
            "target_ref": self.target_ref,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": None if self.effective_to is None else self.effective_to.isoformat(),
            "decision_ref": self.decision_ref,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class NativeScheduledWorkflow:
    """Deterministic scheduler read model for one recurring workflow path."""

    as_of: datetime
    schedule_definition: NativeScheduleDefinitionRecord
    workflow_class: NativeWorkflowClassRecord
    schedule_authority: str = "runtime.schedule_definitions"
    workflow_class_authority: str = "policy.workflow_classes"

    def to_json(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "schedule_authority": self.schedule_authority,
            "workflow_class_authority": self.workflow_class_authority,
            "schedule_definition": self.schedule_definition.to_json(),
            "workflow_class": self.workflow_class.to_json(),
        }


class NativeSchedulerRepository(Protocol):
    """Minimal repository contract for native scheduler authority."""

    async def load_schedule_definition(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
        as_of: datetime,
    ) -> NativeScheduleDefinitionRecord:
        ...

    async def load_workflow_class(
        self,
        *,
        workflow_class_id: str,
        as_of: datetime,
    ) -> NativeWorkflowClassRecord:
        ...


_SCHEDULE_QUERY = """
SELECT
    schedule_definition_id,
    workflow_class_id,
    schedule_name,
    schedule_kind,
    status,
    cadence_policy,
    throttle_policy,
    target_ref,
    effective_from,
    effective_to,
    decision_ref,
    created_at
FROM schedule_definitions
WHERE status = 'active'
  AND target_ref = $1
  AND schedule_kind = $2
  AND effective_from <= $3
  AND (effective_to IS NULL OR effective_to > $3)
ORDER BY effective_from DESC, created_at DESC, schedule_definition_id
"""

_WORKFLOW_CLASS_QUERY = """
SELECT
    workflow_class_id,
    class_name,
    class_kind,
    workflow_lane_id,
    status,
    queue_shape,
    throttle_policy,
    review_required,
    effective_from,
    effective_to,
    decision_ref,
    created_at
FROM workflow_classes
WHERE status = 'active'
  AND workflow_class_id = $1
  AND effective_from <= $2
  AND (effective_to IS NULL OR effective_to > $2)
ORDER BY effective_from DESC, created_at DESC, workflow_class_id
"""


def _schedule_record_from_row(row: Mapping[str, Any]) -> NativeScheduleDefinitionRecord:
    return NativeScheduleDefinitionRecord(
        schedule_definition_id=_require_text(
            row.get("schedule_definition_id"),
            field_name="schedule_definition_id",
        ),
        workflow_class_id=_require_text(row.get("workflow_class_id"), field_name="workflow_class_id"),
        schedule_name=_require_text(row.get("schedule_name"), field_name="schedule_name"),
        schedule_kind=_require_text(row.get("schedule_kind"), field_name="schedule_kind"),
        status=_require_text(row.get("status"), field_name="status"),
        cadence_policy=_require_mapping(row.get("cadence_policy"), field_name="cadence_policy"),
        throttle_policy=_require_mapping(row.get("throttle_policy"), field_name="throttle_policy"),
        target_ref=_require_text(row.get("target_ref"), field_name="target_ref"),
        effective_from=_require_utc(row.get("effective_from"), field_name="effective_from"),
        effective_to=(
            None
            if row.get("effective_to") is None
            else _require_utc(row.get("effective_to"), field_name="effective_to")
        ),
        decision_ref=cast(str | None, row.get("decision_ref")),
        created_at=_require_utc(row.get("created_at"), field_name="created_at"),
    )


def _workflow_class_record_from_row(row: Mapping[str, Any]) -> NativeWorkflowClassRecord:
    return NativeWorkflowClassRecord(
        workflow_class_id=_require_text(row.get("workflow_class_id"), field_name="workflow_class_id"),
        class_name=_require_text(row.get("class_name"), field_name="class_name"),
        class_kind=_require_text(row.get("class_kind"), field_name="class_kind"),
        workflow_lane_id=_require_text(row.get("workflow_lane_id"), field_name="workflow_lane_id"),
        status=_require_text(row.get("status"), field_name="status"),
        queue_shape=_require_mapping(row.get("queue_shape"), field_name="queue_shape"),
        throttle_policy=_require_mapping(row.get("throttle_policy"), field_name="throttle_policy"),
        review_required=_require_bool(row.get("review_required"), field_name="review_required"),
        effective_from=_require_utc(row.get("effective_from"), field_name="effective_from"),
        effective_to=(
            None
            if row.get("effective_to") is None
            else _require_utc(row.get("effective_to"), field_name="effective_to")
        ),
        decision_ref=cast(str | None, row.get("decision_ref")),
        created_at=_require_utc(row.get("created_at"), field_name="created_at"),
    )


class PostgresNativeSchedulerRepository:
    """Explicit Postgres repository for native recurring scheduler authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def load_schedule_definition(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
        as_of: datetime,
    ) -> NativeScheduleDefinitionRecord:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                _SCHEDULE_QUERY,
                target_ref,
                schedule_kind,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise _fail(
                "native_scheduler.schedule_read_failed",
                "failed to read active schedule-definition rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        if not rows:
            raise _fail(
                "native_scheduler.schedule_missing",
                "no active schedule-definition row matched the requested target and kind",
                details={
                    "target_ref": target_ref,
                    "schedule_kind": schedule_kind,
                    "as_of": normalized_as_of.isoformat(),
                },
            )
        if len(rows) > 1:
            raise _fail(
                "native_scheduler.schedule_ambiguous",
                "more than one active schedule-definition row matched the requested target and kind",
                details={
                    "target_ref": target_ref,
                    "schedule_kind": schedule_kind,
                    "as_of": normalized_as_of.isoformat(),
                    "row_count": len(rows),
                },
            )
        return _schedule_record_from_row(cast(Mapping[str, Any], rows[0]))

    async def load_workflow_class(
        self,
        *,
        workflow_class_id: str,
        as_of: datetime,
    ) -> NativeWorkflowClassRecord:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                _WORKFLOW_CLASS_QUERY,
                workflow_class_id,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise _fail(
                "native_scheduler.workflow_class_read_failed",
                "failed to read active workflow-class rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        if not rows:
            raise _fail(
                "native_scheduler.workflow_class_missing",
                "no active workflow-class row matched the schedule definition",
                details={
                    "workflow_class_id": workflow_class_id,
                    "as_of": normalized_as_of.isoformat(),
                },
            )
        if len(rows) > 1:
            raise _fail(
                "native_scheduler.workflow_class_ambiguous",
                "more than one active workflow-class row matched the schedule definition",
                details={
                    "workflow_class_id": workflow_class_id,
                    "as_of": normalized_as_of.isoformat(),
                    "row_count": len(rows),
                },
            )
        return _workflow_class_record_from_row(cast(Mapping[str, Any], rows[0]))


@dataclass(frozen=True, slots=True)
class NativeSchedulerRuntime:
    """Deterministic native scheduler seam over stored authority rows."""

    repository: NativeSchedulerRepository

    async def inspect_schedule(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
        as_of: datetime,
    ) -> NativeScheduledWorkflow:
        schedule_definition = await self.repository.load_schedule_definition(
            target_ref=target_ref,
            schedule_kind=schedule_kind,
            as_of=as_of,
        )
        workflow_class = await self.repository.load_workflow_class(
            workflow_class_id=schedule_definition.workflow_class_id,
            as_of=as_of,
        )
        return NativeScheduledWorkflow(
            as_of=_normalize_as_of(as_of),
            schedule_definition=schedule_definition,
            workflow_class=workflow_class,
        )


async def load_native_schedule_plan(
    conn: asyncpg.Connection,
    *,
    target_ref: str,
    schedule_kind: str,
    as_of: datetime,
) -> NativeScheduledWorkflow:
    """Load one deterministic native recurring schedule path from Postgres."""

    repository = PostgresNativeSchedulerRepository(conn)
    return await NativeSchedulerRuntime(repository=repository).inspect_schedule(
        target_ref=target_ref,
        schedule_kind=schedule_kind,
        as_of=as_of,
    )


__all__ = [
    "NativeWorkflowClassRecord",
    "NativeScheduleDefinitionRecord",
    "NativeScheduledWorkflow",
    "NativeSchedulerError",
    "NativeSchedulerRepository",
    "NativeSchedulerRuntime",
    "PostgresNativeSchedulerRepository",
    "load_native_schedule_plan",
]
