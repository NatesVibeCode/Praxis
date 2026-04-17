"""Native scheduler authority over stored recurring schedules."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol
from datetime import datetime

import asyncpg

from authority.workflow_schedule import (
    NativeWorkflowScheduleCatalog,
    NativeWorkflowScheduleResolution,
    ScheduleRepositoryError,
    _normalize_as_of,
    load_workflow_schedule_catalog,
)
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


def _map_schedule_reason(reason_code: str) -> str:
    if reason_code.startswith("schedule."):
        return f"native_scheduler.{reason_code[len('schedule.'):]}"
    return f"native_scheduler.{reason_code}"


def _raise_native_schedule_error(exc: ScheduleRepositoryError) -> None:
    raise _fail(
        _map_schedule_reason(exc.reason_code),
        str(exc),
        details=exc.details,
    ) from exc


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
    recurring_run_window_id: str
    recurring_run_window_status: str
    capacity_limit: int | None
    capacity_used: int
    schedule_authority: str = "authority.workflow_schedule"
    workflow_class_authority: str = "policy.workflow_classes"

    def to_json(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "schedule_authority": self.schedule_authority,
            "workflow_class_authority": self.workflow_class_authority,
            "schedule_definition": self.schedule_definition.to_json(),
            "workflow_class": self.workflow_class.to_json(),
            "recurring_run_window_id": self.recurring_run_window_id,
            "recurring_run_window_status": self.recurring_run_window_status,
            "capacity_limit": self.capacity_limit,
            "capacity_used": self.capacity_used,
        }


class NativeSchedulerRepository(Protocol):
    """Minimal repository contract for native scheduler authority."""

    async def load_catalog(
        self,
        *,
        as_of: datetime,
    ) -> NativeWorkflowScheduleCatalog:
        ...


def _native_workflow_class_record_from_resolution(
    resolution: NativeWorkflowScheduleResolution,
) -> NativeWorkflowClassRecord:
    return NativeWorkflowClassRecord(
        workflow_class_id=resolution.workflow_class_id,
        class_name=resolution.class_name,
        class_kind=resolution.class_kind,
        workflow_lane_id=resolution.workflow_lane_id,
        status=resolution.workflow_class_resolution.workflow_class.status,
        queue_shape=resolution.queue_shape,
        throttle_policy=resolution.workflow_class_throttle_policy,
        review_required=resolution.workflow_class_resolution.workflow_class.review_required,
        effective_from=resolution.workflow_class_resolution.workflow_class.effective_from,
        effective_to=resolution.workflow_class_resolution.workflow_class.effective_to,
        decision_ref=resolution.workflow_class_resolution.workflow_class.decision_ref,
        created_at=resolution.workflow_class_resolution.workflow_class.created_at,
    )


def _native_schedule_definition_record_from_resolution(
    resolution: NativeWorkflowScheduleResolution,
) -> NativeScheduleDefinitionRecord:
    return NativeScheduleDefinitionRecord(
        schedule_definition_id=resolution.schedule_definition.schedule_definition_id,
        workflow_class_id=resolution.workflow_class_id,
        schedule_name=resolution.schedule_name,
        schedule_kind=resolution.schedule_kind,
        status=resolution.schedule_definition.status,
        cadence_policy=resolution.schedule_cadence_policy,
        throttle_policy=resolution.schedule_throttle_policy,
        target_ref=resolution.target_ref,
        effective_from=resolution.schedule_definition.effective_from,
        effective_to=resolution.schedule_definition.effective_to,
        decision_ref=resolution.schedule_definition.decision_ref,
        created_at=resolution.schedule_definition.created_at,
    )


class PostgresNativeSchedulerRepository:
    """Postgres-native repository for native scheduler authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def load_catalog(
        self,
        *,
        as_of: datetime,
    ) -> NativeWorkflowScheduleCatalog:
        return await load_workflow_schedule_catalog(self._conn, as_of=as_of)


@dataclass(frozen=True, slots=True)
class NativeSchedulerRuntime:
    """Deterministic native scheduler seam over canonical authority."""

    repository: NativeSchedulerRepository

    async def inspect_schedule(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
        as_of: datetime,
    ) -> NativeScheduledWorkflow:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            catalog = await self.repository.load_catalog(as_of=normalized_as_of)
        except ScheduleRepositoryError as exc:
            _raise_native_schedule_error(exc)
        resolution = catalog.resolve(
            target_ref=target_ref,
            schedule_kind=schedule_kind,
        )
        return NativeScheduledWorkflow(
            as_of=normalized_as_of,
            schedule_definition=_native_schedule_definition_record_from_resolution(
                resolution,
            ),
            workflow_class=_native_workflow_class_record_from_resolution(resolution),
            recurring_run_window_id=resolution.recurring_run_window.recurring_run_window_id,
            recurring_run_window_status=resolution.window_status,
            capacity_limit=resolution.capacity_limit,
            capacity_used=resolution.capacity_used,
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
