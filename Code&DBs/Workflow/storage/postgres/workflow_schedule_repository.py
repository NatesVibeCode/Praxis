"""Raw Postgres repository for recurring workflow schedule authority."""

from __future__ import annotations

import asyncpg

from authority.workflow_schedule import (
    NativeWorkflowScheduleCatalog,
    RecurringRunWindowAuthorityRecord,
    ScheduleDefinitionAuthorityRecord,
    ScheduleRepositoryError,
    _normalize_as_of,
    _recurring_run_window_record_from_row,
    _schedule_definition_record_from_row,
)
from policy.workflow_classes import PostgresWorkflowClassRepository, WorkflowClassCatalog


class PostgresWorkflowScheduleRepository:
    """Explicit Postgres repository for schedule definitions and run windows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_schedule_definition_records(
        self,
        *,
        as_of,
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
            raise ScheduleRepositoryError(
                "schedule.read_failed",
                "failed to read active schedule-definition rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_schedule_definition_record_from_row(row) for row in rows)

    async def fetch_recurring_run_window_records(
        self,
        *,
        as_of,
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
            raise ScheduleRepositoryError(
                "schedule.read_failed",
                "failed to read active recurring run-window rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_recurring_run_window_record_from_row(row) for row in rows)

    async def load_catalog(
        self,
        *,
        as_of,
    ) -> NativeWorkflowScheduleCatalog:
        normalized_as_of = _normalize_as_of(as_of)
        async with self._conn.transaction():
            workflow_class_repository = PostgresWorkflowClassRepository(self._conn)
            class_records = await workflow_class_repository.fetch_workflow_class_records(
                as_of=normalized_as_of,
            )
            schedule_definition_records = await self.fetch_schedule_definition_records(
                as_of=normalized_as_of,
            )
            recurring_run_window_records = await self.fetch_recurring_run_window_records(
                as_of=normalized_as_of,
            )
            workflow_class_catalog = WorkflowClassCatalog.from_records(
                class_records=class_records,
                as_of=normalized_as_of,
            )
            return NativeWorkflowScheduleCatalog.from_records(
                workflow_class_catalog=workflow_class_catalog,
                schedule_definition_records=schedule_definition_records,
                recurring_run_window_records=recurring_run_window_records,
                as_of=normalized_as_of,
            )


__all__ = ["PostgresWorkflowScheduleRepository"]
