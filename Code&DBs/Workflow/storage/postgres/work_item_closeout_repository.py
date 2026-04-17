"""Explicit async Postgres repository for proof-backed work-item closeout."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import asyncpg

from .validators import PostgresWriteError, _require_text, _require_utc


def _normalize_text_sequence(
    values: Sequence[str],
    *,
    field_name: str,
) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        text = _require_text(value, field_name=f"{field_name}[{index}]")
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _normalize_resolution_summaries(
    bug_ids: Sequence[str],
    summaries_by_bug_id: Mapping[str, str],
) -> tuple[str, ...]:
    normalized: list[str] = []
    for bug_id in bug_ids:
        if bug_id not in summaries_by_bug_id:
            raise PostgresWriteError(
                "work_item_closeout.invalid_submission",
                "resolution summary missing for bug closeout candidate",
                details={"bug_id": bug_id},
            )
        normalized.append(
            _require_text(
                summaries_by_bug_id[bug_id],
                field_name=f"resolution_summaries_by_bug_id[{bug_id}]",
            )
        )
    return tuple(normalized)


class PostgresWorkItemCloseoutRepository:
    """Owns explicit bug and roadmap closeout mutations."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def mark_bugs_fixed(
        self,
        *,
        bug_ids: Sequence[str],
        resolution_summaries_by_bug_id: Mapping[str, str],
        resolved_at: datetime,
    ) -> tuple[dict[str, Any], ...]:
        normalized_bug_ids = _normalize_text_sequence(bug_ids, field_name="bug_ids")
        if not normalized_bug_ids:
            return ()
        normalized_resolved_at = _require_utc(
            resolved_at,
            field_name="resolved_at",
        )
        normalized_summaries = _normalize_resolution_summaries(
            normalized_bug_ids,
            resolution_summaries_by_bug_id,
        )
        try:
            rows = await self._conn.fetch(
                """
                UPDATE bugs AS bug
                SET
                    status = 'FIXED',
                    resolved_at = COALESCE(bug.resolved_at, $1),
                    updated_at = $1,
                    resolution_summary = COALESCE(
                        NULLIF(bug.resolution_summary, ''),
                        candidate.resolution_summary
                    )
                FROM UNNEST($2::text[], $3::text[]) AS candidate(
                    bug_id,
                    resolution_summary
                )
                WHERE bug.bug_id = candidate.bug_id
                  AND bug.resolved_at IS NULL
                RETURNING
                    bug.bug_id,
                    bug.status,
                    bug.resolved_at,
                    bug.resolution_summary
                """,
                normalized_resolved_at,
                list(normalized_bug_ids),
                list(normalized_summaries),
            )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "work_item_closeout.write_failed",
                "failed to mark bugs fixed",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(dict(row) for row in rows)

    async def mark_roadmap_items_completed(
        self,
        *,
        roadmap_item_ids: Sequence[str],
        completed_status: str,
        completed_at: datetime,
    ) -> tuple[dict[str, Any], ...]:
        normalized_roadmap_item_ids = _normalize_text_sequence(
            roadmap_item_ids,
            field_name="roadmap_item_ids",
        )
        if not normalized_roadmap_item_ids:
            return ()
        normalized_completed_status = _require_text(
            completed_status,
            field_name="completed_status",
        )
        normalized_completed_at = _require_utc(
            completed_at,
            field_name="completed_at",
        )
        try:
            rows = await self._conn.fetch(
                """
                UPDATE roadmap_items
                SET
                    status = $1,
                    lifecycle = 'completed',
                    completed_at = COALESCE(completed_at, $2),
                    updated_at = $2
                WHERE roadmap_item_id = ANY($3::text[])
                  AND completed_at IS NULL
                RETURNING roadmap_item_id, status, lifecycle, completed_at, source_bug_id
                """,
                normalized_completed_status,
                normalized_completed_at,
                list(normalized_roadmap_item_ids),
            )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "work_item_closeout.write_failed",
                "failed to mark roadmap items completed",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(dict(row) for row in rows)

    async def mark_issues_resolved_by_bug_ids(
        self,
        *,
        bug_ids: Sequence[str],
        resolved_at: datetime,
    ) -> tuple[dict[str, Any], ...]:
        normalized_bug_ids = _normalize_text_sequence(bug_ids, field_name="bug_ids")
        if not normalized_bug_ids:
            return ()
        normalized_resolved_at = _require_utc(
            resolved_at,
            field_name="resolved_at",
        )
        try:
            rows = await self._conn.fetch(
                """
                UPDATE issues AS issue
                SET
                    status = 'resolved',
                    resolved_at = COALESCE(issue.resolved_at, $1),
                    updated_at = $1,
                    resolution_summary = COALESCE(
                        NULLIF(issue.resolution_summary, ''),
                        candidate.resolution_summary
                    )
                FROM (
                    SELECT
                        bug.source_issue_id AS issue_id,
                        CONCAT('Resolved through ', bug.bug_id) AS resolution_summary
                    FROM bugs AS bug
                    WHERE bug.bug_id = ANY($2::text[])
                      AND bug.source_issue_id IS NOT NULL
                ) AS candidate
                WHERE issue.issue_id = candidate.issue_id
                  AND issue.resolved_at IS NULL
                RETURNING issue.issue_id, issue.status, issue.resolved_at, issue.resolution_summary
                """,
                normalized_resolved_at,
                list(normalized_bug_ids),
            )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "work_item_closeout.write_failed",
                "failed to mark issues resolved",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(dict(row) for row in rows)


__all__ = ["PostgresWorkItemCloseoutRepository"]
