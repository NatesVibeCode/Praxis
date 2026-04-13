"""Explicit Postgres repository for task-route eligibility authority."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, cast

import asyncpg

from storage.postgres.validators import (
    PostgresWriteError,
    _optional_text,
    _require_text,
)


class PostgresTaskRouteEligibilityRepository:
    """Write task-route eligibility windows through explicit Postgres authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def record_task_route_eligibility_window(
        self,
        *,
        task_route_eligibility_id: str,
        task_type: str | None,
        provider_slug: str,
        model_slug: str | None,
        eligibility_status: str,
        reason_code: str,
        rationale: str,
        effective_from: datetime,
        effective_to: datetime | None,
        decision_ref: str,
    ) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
        """Persist one canonical task-route eligibility window."""

        normalized_task_route_eligibility_id = _require_text(
            task_route_eligibility_id,
            field_name="task_route_eligibility_id",
        )
        normalized_task_type = _optional_text(task_type, field_name="task_type")
        normalized_provider_slug = _require_text(provider_slug, field_name="provider_slug")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        normalized_eligibility_status = _require_text(
            eligibility_status,
            field_name="eligibility_status",
        )
        normalized_reason_code = _require_text(reason_code, field_name="reason_code")
        normalized_rationale = _require_text(rationale, field_name="rationale")
        normalized_decision_ref = _require_text(decision_ref, field_name="decision_ref")
        if not isinstance(effective_from, datetime):
            raise PostgresWriteError(
                "task_route_eligibility.invalid_submission",
                "effective_from must be a datetime",
                details={"field": "effective_from"},
            )
        if effective_from.tzinfo is None or effective_from.utcoffset() is None:
            raise PostgresWriteError(
                "task_route_eligibility.invalid_submission",
                "effective_from must be timezone-aware",
                details={"field": "effective_from"},
            )
        effective_from = effective_from.astimezone(timezone.utc)
        if effective_to is not None and not isinstance(effective_to, datetime):
            raise PostgresWriteError(
                "task_route_eligibility.invalid_submission",
                "effective_to must be a datetime when provided",
                details={"field": "effective_to"},
            )
        if effective_to is not None and (effective_to.tzinfo is None or effective_to.utcoffset() is None):
            raise PostgresWriteError(
                "task_route_eligibility.invalid_submission",
                "effective_to must be timezone-aware when provided",
                details={"field": "effective_to"},
            )
        if effective_to is not None:
            effective_to = effective_to.astimezone(timezone.utc)
        if effective_to is not None and effective_to <= effective_from:
            raise PostgresWriteError(
                "task_route_eligibility.invalid_submission",
                "effective_to must be later than effective_from",
                details={
                    "effective_from": effective_from.isoformat(),
                    "effective_to": effective_to.isoformat(),
                },
            )

        try:
            async with self._conn.transaction():
                superseded_rows = await self._conn.fetch(
                    """
                    UPDATE task_type_route_eligibility
                    SET effective_to = $1
                    WHERE provider_slug = $2
                      AND task_type IS NOT DISTINCT FROM $3
                      AND model_slug IS NOT DISTINCT FROM $4
                      AND effective_from < $1
                      AND (effective_to IS NULL OR effective_to > $1)
                    RETURNING task_route_eligibility_id
                    """,
                    effective_from,
                    normalized_provider_slug,
                    normalized_task_type,
                    normalized_model_slug,
                )
                await self._conn.execute(
                    """
                    INSERT INTO task_type_route_eligibility (
                        task_route_eligibility_id,
                        task_type,
                        provider_slug,
                        model_slug,
                        eligibility_status,
                        reason_code,
                        rationale,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                    )
                    """,
                    normalized_task_route_eligibility_id,
                    normalized_task_type,
                    normalized_provider_slug,
                    normalized_model_slug,
                    normalized_eligibility_status,
                    normalized_reason_code,
                    normalized_rationale,
                    effective_from,
                    effective_to,
                    normalized_decision_ref,
                    effective_from,
                )
                inserted_row = await self._conn.fetchrow(
                    """
                    SELECT
                        task_route_eligibility_id,
                        task_type,
                        provider_slug,
                        model_slug,
                        eligibility_status,
                        reason_code,
                        rationale,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    FROM task_type_route_eligibility
                    WHERE task_route_eligibility_id = $1
                    """,
                    normalized_task_route_eligibility_id,
                )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "task_route_eligibility.write_failed",
                "failed to write task-route eligibility window",
                details={
                    "task_route_eligibility_id": normalized_task_route_eligibility_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc

        if inserted_row is None:
            raise PostgresWriteError(
                "task_route_eligibility.write_failed",
                "writing task-route eligibility window returned no row",
                details={"task_route_eligibility_id": normalized_task_route_eligibility_id},
            )

        return (
            cast(Mapping[str, Any], inserted_row),
            tuple(cast(Mapping[str, Any], row) for row in superseded_rows),
        )


__all__ = ["PostgresTaskRouteEligibilityRepository"]
