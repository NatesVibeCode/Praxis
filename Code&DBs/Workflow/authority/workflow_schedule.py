"""Canonical recurring-workflow schedule authority.

This module composes stored workflow-class authority with recurring schedule
definitions and run windows. It resolves native scheduling from explicit
authority rows only and does not invent a second orchestration brain.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from policy.workflow_classes import (
    WorkflowClassCatalog,
    WorkflowClassCatalogError,
    WorkflowClassResolution,
)


class ScheduleRepositoryError(RuntimeError):
    """Raised when recurring schedule authority cannot be resolved safely."""

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


@dataclass(frozen=True, slots=True)
class ScheduleDefinitionAuthorityRecord:
    """Canonical recurring schedule-definition row."""

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
    decision_ref: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RecurringRunWindowAuthorityRecord:
    """Canonical recurring run-window row."""

    recurring_run_window_id: str
    schedule_definition_id: str
    window_started_at: datetime
    window_ended_at: datetime
    window_status: str
    capacity_limit: int | None
    capacity_used: int
    last_workflow_at: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class NativeWorkflowScheduleResolution:
    """Resolved schedule, class, and active window for one recurring target."""

    schedule_definition: ScheduleDefinitionAuthorityRecord
    workflow_class_resolution: WorkflowClassResolution
    recurring_run_window: RecurringRunWindowAuthorityRecord
    as_of: datetime

    @property
    def schedule_definition_id(self) -> str:
        return self.schedule_definition.schedule_definition_id

    @property
    def schedule_name(self) -> str:
        return self.schedule_definition.schedule_name

    @property
    def schedule_kind(self) -> str:
        return self.schedule_definition.schedule_kind

    @property
    def target_ref(self) -> str:
        return self.schedule_definition.target_ref

    @property
    def workflow_class_id(self) -> str:
        return self.workflow_class_resolution.workflow_class_id

    @property
    def class_name(self) -> str:
        return self.workflow_class_resolution.class_name

    @property
    def class_kind(self) -> str:
        return self.workflow_class_resolution.class_kind

    @property
    def workflow_lane_id(self) -> str:
        return self.workflow_class_resolution.workflow_lane_id

    @property
    def queue_shape(self) -> Mapping[str, Any]:
        return self.workflow_class_resolution.queue_shape

    @property
    def workflow_class_throttle_policy(self) -> Mapping[str, Any]:
        return self.workflow_class_resolution.throttle_policy

    @property
    def schedule_cadence_policy(self) -> Mapping[str, Any]:
        return self.schedule_definition.cadence_policy

    @property
    def schedule_throttle_policy(self) -> Mapping[str, Any]:
        return self.schedule_definition.throttle_policy

    @property
    def window_status(self) -> str:
        return self.recurring_run_window.window_status

    @property
    def capacity_limit(self) -> int | None:
        return self.recurring_run_window.capacity_limit

    @property
    def capacity_used(self) -> int:
        return self.recurring_run_window.capacity_used

    @property
    def decision_ref(self) -> str:
        return self.schedule_definition.decision_ref


@dataclass(frozen=True, slots=True)
class NativeWorkflowScheduleCatalog:
    """Inspectable snapshot of workflow classes and recurring schedule authority."""

    workflow_class_catalog: WorkflowClassCatalog
    schedule_definition_records: tuple[ScheduleDefinitionAuthorityRecord, ...]
    recurring_run_window_records: tuple[RecurringRunWindowAuthorityRecord, ...]
    as_of: datetime

    @property
    def schedule_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (record.target_ref, record.schedule_kind)
            for record in self.schedule_definition_records
        )

    @property
    def schedule_names(self) -> tuple[str, ...]:
        return tuple(record.schedule_name for record in self.schedule_definition_records)

    def resolve(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
    ) -> NativeWorkflowScheduleResolution:
        normalized_target_ref = _require_text(target_ref, field_name="target_ref")
        normalized_schedule_kind = _require_text(
            schedule_kind,
            field_name="schedule_kind",
        )
        matching_schedules = [
            record
            for record in self.schedule_definition_records
            if record.target_ref == normalized_target_ref
            and record.schedule_kind == normalized_schedule_kind
        ]
        if not matching_schedules:
            raise ScheduleRepositoryError(
                "schedule.schedule_missing",
                (
                    "missing authoritative recurring schedule for "
                    f"target_ref={normalized_target_ref!r} "
                    f"schedule_kind={normalized_schedule_kind!r}"
                ),
                details={
                    "target_ref": normalized_target_ref,
                    "schedule_kind": normalized_schedule_kind,
                },
            )
        if len(matching_schedules) > 1:
            raise ScheduleRepositoryError(
                "schedule.schedule_ambiguous",
                (
                    "ambiguous authoritative recurring schedule for "
                    f"target_ref={normalized_target_ref!r} "
                    f"schedule_kind={normalized_schedule_kind!r}"
                ),
                details={
                    "target_ref": normalized_target_ref,
                    "schedule_kind": normalized_schedule_kind,
                    "schedule_definition_ids": ",".join(
                        record.schedule_definition_id for record in matching_schedules
                    ),
                },
            )

        schedule_definition = matching_schedules[0]
        workflow_class_resolution = self.workflow_class_catalog.resolve_by_id(
            workflow_class_id=schedule_definition.workflow_class_id,
        )
        matching_windows = [
            record
            for record in self.recurring_run_window_records
            if record.schedule_definition_id == schedule_definition.schedule_definition_id
            and record.window_started_at <= self.as_of < record.window_ended_at
        ]
        if not matching_windows:
            raise ScheduleRepositoryError(
                "schedule.window_missing",
                (
                    "missing active recurring run window for "
                    f"schedule_definition_id={schedule_definition.schedule_definition_id!r}"
                ),
                details={
                    "schedule_definition_id": schedule_definition.schedule_definition_id,
                    "target_ref": normalized_target_ref,
                    "schedule_kind": normalized_schedule_kind,
                    "as_of": self.as_of.isoformat(),
                },
            )
        if len(matching_windows) > 1:
            raise ScheduleRepositoryError(
                "schedule.window_ambiguous",
                (
                    "ambiguous active recurring run windows for "
                    f"schedule_definition_id={schedule_definition.schedule_definition_id!r}"
                ),
                details={
                    "schedule_definition_id": schedule_definition.schedule_definition_id,
                    "recurring_run_window_ids": ",".join(
                        record.recurring_run_window_id for record in matching_windows
                    ),
                    "as_of": self.as_of.isoformat(),
                },
            )

        return NativeWorkflowScheduleResolution(
            schedule_definition=schedule_definition,
            workflow_class_resolution=workflow_class_resolution,
            recurring_run_window=matching_windows[0],
            as_of=self.as_of,
        )

    @classmethod
    def from_records(
        cls,
        *,
        workflow_class_catalog: WorkflowClassCatalog,
        schedule_definition_records: Sequence[ScheduleDefinitionAuthorityRecord],
        recurring_run_window_records: Sequence[RecurringRunWindowAuthorityRecord],
        as_of: datetime,
    ) -> "NativeWorkflowScheduleCatalog":
        normalized_as_of = _normalize_as_of(as_of)
        ordered_schedule_definitions = tuple(
            sorted(
                schedule_definition_records,
                key=lambda record: (
                    record.target_ref,
                    record.schedule_kind,
                    record.effective_from,
                    record.created_at,
                    record.schedule_definition_id,
                ),
            )
        )
        ordered_windows = tuple(
            sorted(
                recurring_run_window_records,
                key=lambda record: (
                    record.schedule_definition_id,
                    record.window_started_at,
                    record.created_at,
                    record.recurring_run_window_id,
                ),
            )
        )
        if not ordered_schedule_definitions:
            raise ScheduleRepositoryError(
                "schedule.catalog_empty",
                "no active schedule-definition rows were available for the requested snapshot",
                details={"as_of": normalized_as_of.isoformat()},
            )

        _validate_unique_schedule_keys(
            ordered_schedule_definitions,
            as_of=normalized_as_of,
        )
        _validate_windows_reference_known_schedule_definitions(
            ordered_windows,
            schedule_definition_records=ordered_schedule_definitions,
            as_of=normalized_as_of,
        )
        _validate_schedule_class_references(
            workflow_class_catalog=workflow_class_catalog,
            schedule_definition_records=ordered_schedule_definitions,
            as_of=normalized_as_of,
        )
        return cls(
            workflow_class_catalog=workflow_class_catalog,
            schedule_definition_records=ordered_schedule_definitions,
            recurring_run_window_records=ordered_windows,
            as_of=normalized_as_of,
        )


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ScheduleRepositoryError(
            "schedule.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ScheduleRepositoryError(
            "schedule.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_non_negative_int(value: object, *, field_name: str) -> int:
    normalized_value = _require_int(value, field_name=field_name)
    if normalized_value < 0:
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be non-negative",
            details={"field": field_name, "value": normalized_value},
        )
    return normalized_value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be timezone-aware",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _json_value(value: object) -> object:
    if isinstance(value, str):
        import json

        return json.loads(value)
    return value


def _require_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    normalized_value = _json_value(value)
    if not isinstance(normalized_value, Mapping):
        raise ScheduleRepositoryError(
            "schedule.invalid_record",
            f"{field_name} must be an object",
            details={
                "field": field_name,
                "value_type": type(normalized_value).__name__,
            },
        )
    return {
        str(key): normalized_value[key]
        for key in normalized_value
    }


def _schedule_definition_record_from_row(
    row: asyncpg.Record,
) -> ScheduleDefinitionAuthorityRecord:
    return ScheduleDefinitionAuthorityRecord(
        schedule_definition_id=_require_text(
            row["schedule_definition_id"],
            field_name="schedule_definition_id",
        ),
        workflow_class_id=_require_text(
            row["workflow_class_id"],
            field_name="workflow_class_id",
        ),
        schedule_name=_require_text(row["schedule_name"], field_name="schedule_name"),
        schedule_kind=_require_text(row["schedule_kind"], field_name="schedule_kind"),
        status=_require_text(row["status"], field_name="status"),
        cadence_policy=_require_mapping(
            row["cadence_policy"],
            field_name="cadence_policy",
        ),
        throttle_policy=_require_mapping(
            row["throttle_policy"],
            field_name="throttle_policy",
        ),
        target_ref=_require_text(row["target_ref"], field_name="target_ref"),
        effective_from=_require_datetime(
            row["effective_from"],
            field_name="effective_from",
        ),
        effective_to=(
            _require_datetime(row["effective_to"], field_name="effective_to")
            if row["effective_to"] is not None
            else None
        ),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _recurring_run_window_record_from_row(
    row: asyncpg.Record,
) -> RecurringRunWindowAuthorityRecord:
    return RecurringRunWindowAuthorityRecord(
        recurring_run_window_id=_require_text(
            row["recurring_run_window_id"],
            field_name="recurring_run_window_id",
        ),
        schedule_definition_id=_require_text(
            row["schedule_definition_id"],
            field_name="schedule_definition_id",
        ),
        window_started_at=_require_datetime(
            row["window_started_at"],
            field_name="window_started_at",
        ),
        window_ended_at=_require_datetime(
            row["window_ended_at"],
            field_name="window_ended_at",
        ),
        window_status=_require_text(row["window_status"], field_name="window_status"),
        capacity_limit=(
            _require_non_negative_int(row["capacity_limit"], field_name="capacity_limit")
            if row["capacity_limit"] is not None
            else None
        ),
        capacity_used=_require_non_negative_int(
            row["capacity_used"],
            field_name="capacity_used",
        ),
        last_workflow_at=(
            _require_datetime(row["last_workflow_at"], field_name="last_workflow_at")
            if row["last_workflow_at"] is not None
            else None
        ),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _validate_unique_schedule_keys(
    schedule_definition_records: Sequence[ScheduleDefinitionAuthorityRecord],
    *,
    as_of: datetime,
) -> None:
    grouped: dict[tuple[str, str], list[ScheduleDefinitionAuthorityRecord]] = {}
    for record in schedule_definition_records:
        grouped.setdefault((record.target_ref, record.schedule_kind), []).append(record)
    duplicates = {
        schedule_key: tuple(
            record.schedule_definition_id for record in records
        )
        for schedule_key, records in grouped.items()
        if len(records) > 1
    }
    if duplicates:
        (target_ref, schedule_kind), schedule_ids = next(iter(duplicates.items()))
        raise ScheduleRepositoryError(
            "schedule.schedule_ambiguous",
            (
                "ambiguous active schedule rows for "
                f"target_ref={target_ref!r} schedule_kind={schedule_kind!r}"
            ),
            details={
                "as_of": as_of.isoformat(),
                "target_ref": target_ref,
                "schedule_kind": schedule_kind,
                "schedule_definition_ids": ",".join(schedule_ids),
            },
        )


def _validate_windows_reference_known_schedule_definitions(
    recurring_run_window_records: Sequence[RecurringRunWindowAuthorityRecord],
    *,
    schedule_definition_records: Sequence[ScheduleDefinitionAuthorityRecord],
    as_of: datetime,
) -> None:
    schedule_definition_ids = {
        record.schedule_definition_id for record in schedule_definition_records
    }
    missing_schedule_definition_ids = tuple(
        record.schedule_definition_id
        for record in recurring_run_window_records
        if record.schedule_definition_id not in schedule_definition_ids
    )
    if missing_schedule_definition_ids:
        raise ScheduleRepositoryError(
            "schedule.window_orphaned",
            "one or more recurring run windows referenced a missing schedule definition",
            details={
                "as_of": as_of.isoformat(),
                "missing_schedule_definition_ids": ",".join(
                    dict.fromkeys(missing_schedule_definition_ids)
                ),
            },
        )


def _validate_schedule_class_references(
    *,
    workflow_class_catalog: WorkflowClassCatalog,
    schedule_definition_records: Sequence[ScheduleDefinitionAuthorityRecord],
    as_of: datetime,
) -> None:
    missing_workflow_class_ids: list[str] = []
    for record in schedule_definition_records:
        try:
            workflow_class_catalog.resolve_by_id(
                workflow_class_id=record.workflow_class_id,
            )
        except WorkflowClassCatalogError as exc:
            if exc.reason_code != "workflow_class.class_missing":
                raise ScheduleRepositoryError(
                    "schedule.workflow_class_resolution_failed",
                    "failed to resolve workflow-class authority for a schedule row",
                    details={
                        "as_of": as_of.isoformat(),
                        "schedule_definition_id": record.schedule_definition_id,
                        "workflow_class_id": record.workflow_class_id,
                        "workflow_class_reason_code": exc.reason_code,
                    },
                ) from exc
            missing_workflow_class_ids.append(record.workflow_class_id)
    if missing_workflow_class_ids:
        raise ScheduleRepositoryError(
            "schedule.workflow_class_missing",
            "one or more schedule definitions referenced a missing workflow class",
            details={
                "as_of": as_of.isoformat(),
                "workflow_class_ids": ",".join(dict.fromkeys(missing_workflow_class_ids)),
            },
        )


async def load_workflow_schedule_catalog(
    conn,
    *,
    as_of: datetime,
) -> NativeWorkflowScheduleCatalog:
    """Load canonical active native-workflow schedule authority from Postgres."""

    from storage.postgres.workflow_schedule_repository import PostgresWorkflowScheduleRepository

    repository = PostgresWorkflowScheduleRepository(conn)
    return await repository.load_catalog(as_of=as_of)


__all__ = [
    "NativeWorkflowScheduleCatalog",
    "NativeWorkflowScheduleResolution",
    "RecurringRunWindowAuthorityRecord",
    "ScheduleDefinitionAuthorityRecord",
    "ScheduleRepositoryError",
    "load_workflow_schedule_catalog",
]
