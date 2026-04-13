"""Postgres-backed scheduler-window authority.

This module reads canonical recurring schedule definitions and run windows
from Postgres-backed authority rows. It does not resolve workflow classes or
consult wrapper memory.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

import asyncpg

from runtime._helpers import _fail as _shared_fail
from authority.workflow_schedule import (
    RecurringRunWindowAuthorityRecord,
    ScheduleDefinitionAuthorityRecord,
)


class SchedulerWindowRepositoryError(RuntimeError):
    """Raised when scheduler-window authority cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=SchedulerWindowRepositoryError)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "scheduler_window.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "scheduler_window.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "scheduler_window.invalid_record",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(
            "scheduler_window.invalid_record",
            f"{field_name} must be an integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_non_negative_int(value: object, *, field_name: str) -> int:
    normalized_value = _require_int(value, field_name=field_name)
    if normalized_value < 0:
        raise _fail(
            "scheduler_window.invalid_record",
            f"{field_name} must be non-negative",
            details={"field": field_name, "value": normalized_value},
        )
    return normalized_value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "scheduler_window.invalid_record",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "scheduler_window.invalid_record",
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
        raise _fail(
            "scheduler_window.invalid_record",
            f"{field_name} must be an object",
            details={
                "field": field_name,
                "value_type": type(normalized_value).__name__,
            },
        )
    return {str(key): normalized_value[key] for key in normalized_value}


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


@dataclass(frozen=True, slots=True)
class SchedulerWindowAuthorityResolution:
    """Resolved schedule-definition and active window for one recurring target."""

    schedule_definition: ScheduleDefinitionAuthorityRecord
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
    def recurring_run_window_id(self) -> str:
        return self.recurring_run_window.recurring_run_window_id

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
class SchedulerWindowAuthorityCatalog:
    """Inspectable snapshot of recurring schedule-definition and window rows."""

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

    @property
    def window_keys(self) -> tuple[tuple[str, datetime], ...]:
        return tuple(
            (record.schedule_definition_id, record.window_started_at)
            for record in self.recurring_run_window_records
        )

    def resolve(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
    ) -> SchedulerWindowAuthorityResolution:
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
            raise _fail(
                "scheduler_window.schedule_missing",
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
            raise _fail(
                "scheduler_window.schedule_ambiguous",
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
        matching_windows = [
            record
            for record in self.recurring_run_window_records
            if record.schedule_definition_id == schedule_definition.schedule_definition_id
            and record.window_started_at <= self.as_of < record.window_ended_at
        ]
        if not matching_windows:
            raise _fail(
                "scheduler_window.window_missing",
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
            raise _fail(
                "scheduler_window.window_ambiguous",
                (
                    "ambiguous active recurring run windows for "
                    f"schedule_definition_id={schedule_definition.schedule_definition_id!r}"
                ),
                details={
                    "schedule_definition_id": schedule_definition.schedule_definition_id,
                    "target_ref": normalized_target_ref,
                    "schedule_kind": normalized_schedule_kind,
                    "as_of": self.as_of.isoformat(),
                    "recurring_run_window_ids": ",".join(
                        record.recurring_run_window_id for record in matching_windows
                    ),
                },
            )

        return SchedulerWindowAuthorityResolution(
            schedule_definition=schedule_definition,
            recurring_run_window=matching_windows[0],
            as_of=self.as_of,
        )

    @classmethod
    def from_records(
        cls,
        *,
        schedule_definition_records: Sequence[ScheduleDefinitionAuthorityRecord],
        recurring_run_window_records: Sequence[RecurringRunWindowAuthorityRecord],
        as_of: datetime,
    ) -> "SchedulerWindowAuthorityCatalog":
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
            raise _fail(
                "scheduler_window.catalog_empty",
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
        return cls(
            schedule_definition_records=ordered_schedule_definitions,
            recurring_run_window_records=ordered_windows,
            as_of=normalized_as_of,
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
        schedule_key: tuple(record.schedule_definition_id for record in records)
        for schedule_key, records in grouped.items()
        if len(records) > 1
    }
    if duplicates:
        (target_ref, schedule_kind), schedule_ids = next(iter(duplicates.items()))
        raise _fail(
            "scheduler_window.schedule_ambiguous",
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
        raise _fail(
            "scheduler_window.window_orphaned",
            "one or more recurring run windows referenced a missing schedule definition",
            details={
                "as_of": as_of.isoformat(),
                "missing_schedule_definition_ids": ",".join(
                    dict.fromkeys(missing_schedule_definition_ids)
                ),
            },
        )


class PostgresSchedulerWindowAuthorityRepository:
    """Explicit Postgres repository for recurring schedule definitions and windows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_schedule_definition_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[ScheduleDefinitionAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
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
                  AND effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY target_ref, schedule_kind, effective_from DESC, created_at DESC, schedule_definition_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise _fail(
                "scheduler_window.read_failed",
                "failed to read active schedule-definition rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_schedule_definition_record_from_row(row) for row in rows)

    async def fetch_recurring_run_window_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[RecurringRunWindowAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    recurring_run_window_id,
                    schedule_definition_id,
                    window_started_at,
                    window_ended_at,
                    window_status,
                    capacity_limit,
                    capacity_used,
                    last_workflow_at,
                    created_at
                FROM recurring_run_windows
                WHERE window_status = 'active'
                  AND window_started_at <= $1
                  AND window_ended_at > $1
                ORDER BY schedule_definition_id, window_started_at DESC, created_at DESC, recurring_run_window_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise _fail(
                "scheduler_window.read_failed",
                "failed to read active recurring run-window rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_recurring_run_window_record_from_row(row) for row in rows)

    async def load_authority(
        self,
        *,
        as_of: datetime,
    ) -> SchedulerWindowAuthorityCatalog:
        normalized_as_of = _normalize_as_of(as_of)
        async with self._conn.transaction():
            schedule_definition_records = await self.fetch_schedule_definition_records(
                as_of=normalized_as_of,
            )
            recurring_run_window_records = await self.fetch_recurring_run_window_records(
                as_of=normalized_as_of,
            )
            return SchedulerWindowAuthorityCatalog.from_records(
                schedule_definition_records=schedule_definition_records,
                recurring_run_window_records=recurring_run_window_records,
                as_of=normalized_as_of,
            )


async def load_scheduler_window_authority(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> SchedulerWindowAuthorityCatalog:
    """Load canonical recurring schedule-definition and window authority from Postgres."""

    repository = PostgresSchedulerWindowAuthorityRepository(conn)
    return await repository.load_authority(as_of=as_of)


__all__ = [
    "PostgresSchedulerWindowAuthorityRepository",
    "RecurringRunWindowAuthorityRecord",
    "ScheduleDefinitionAuthorityRecord",
    "SchedulerWindowAuthorityCatalog",
    "SchedulerWindowAuthorityResolution",
    "SchedulerWindowRepositoryError",
    "load_scheduler_window_authority",
]
