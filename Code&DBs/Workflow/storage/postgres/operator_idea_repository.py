"""Postgres authority for pre-commitment operator ideas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import asyncpg

from storage.postgres.validators import PostgresWriteError, _require_text, _require_utc

IDEA_OPEN_STATUS = "open"
IDEA_PROMOTED_STATUS = "promoted"
IDEA_TERMINAL_STATUSES = frozenset(
    {IDEA_PROMOTED_STATUS, "rejected", "superseded", "archived"}
)
IDEA_STATUSES = frozenset({IDEA_OPEN_STATUS, *IDEA_TERMINAL_STATUSES})


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        raise PostgresWriteError(
            "operator_ideas.invalid_submission",
            f"{field_name} must not be empty",
            details={"field": field_name},
        )
    return normalized


def _normalize_status(value: object, *, allow_open: bool = True) -> str:
    normalized = _require_text(value, field_name="status").lower()
    allowed = IDEA_STATUSES if allow_open else IDEA_TERMINAL_STATUSES
    if normalized not in allowed:
        raise PostgresWriteError(
            "operator_ideas.invalid_status",
            f"status must be one of {', '.join(sorted(allowed))}",
            details={"status": normalized, "allowed": sorted(allowed)},
        )
    return normalized


def _row_to_json(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in dict(row).items()
    }


class PostgresOperatorIdeaRepository:
    """Owns durable operator-idea writes and promotion linkage."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def list_ideas(
        self,
        *,
        idea_ids: Sequence[str] | None = None,
        status: str | None = None,
        open_only: bool = True,
        limit: int = 50,
    ) -> tuple[dict[str, Any], ...]:
        clauses: list[str] = []
        args: list[object] = []
        if idea_ids:
            normalized_ids = [
                _require_text(value, field_name=f"idea_ids[{index}]")
                for index, value in enumerate(idea_ids)
            ]
            args.append(normalized_ids)
            clauses.append(f"idea_id = ANY(${len(args)}::text[])")
        if status is not None:
            args.append(_normalize_status(status))
            clauses.append(f"status = ${len(args)}")
        elif open_only:
            clauses.append("status = 'open'")
        args.append(max(1, int(limit)))
        query = """
            SELECT
                idea_id,
                idea_key,
                title,
                status,
                summary,
                source_kind,
                source_ref,
                owner_ref,
                decision_ref,
                resolution_summary,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM operator_ideas
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += f" ORDER BY opened_at DESC, created_at DESC, idea_id LIMIT ${len(args)}"
        try:
            rows = await self._conn.fetch(query, *args)
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "operator_ideas.read_failed",
                "failed to list operator ideas",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_row_to_json(row) for row in rows)

    async def record_idea(
        self,
        *,
        idea_id: str,
        idea_key: str,
        title: str,
        summary: str,
        source_kind: str,
        source_ref: str | None,
        owner_ref: str | None,
        decision_ref: str,
        opened_at: datetime,
        created_at: datetime,
        updated_at: datetime,
    ) -> dict[str, Any]:
        normalized_opened_at = _require_utc(opened_at, field_name="opened_at")
        normalized_created_at = _require_utc(created_at, field_name="created_at")
        normalized_updated_at = _require_utc(updated_at, field_name="updated_at")
        try:
            row = await self._conn.fetchrow(
                """
                INSERT INTO operator_ideas (
                    idea_id,
                    idea_key,
                    title,
                    status,
                    summary,
                    source_kind,
                    source_ref,
                    owner_ref,
                    decision_ref,
                    resolution_summary,
                    opened_at,
                    resolved_at,
                    created_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, 'open', $4, $5, $6, $7, $8, NULL, $9, NULL, $10, $11
                )
                ON CONFLICT (idea_id) DO UPDATE SET
                    idea_key = EXCLUDED.idea_key,
                    title = EXCLUDED.title,
                    summary = EXCLUDED.summary,
                    source_kind = EXCLUDED.source_kind,
                    source_ref = EXCLUDED.source_ref,
                    owner_ref = EXCLUDED.owner_ref,
                    decision_ref = EXCLUDED.decision_ref,
                    updated_at = EXCLUDED.updated_at
                WHERE operator_ideas.status = 'open'
                RETURNING
                    idea_id,
                    idea_key,
                    title,
                    status,
                    summary,
                    source_kind,
                    source_ref,
                    owner_ref,
                    decision_ref,
                    resolution_summary,
                    opened_at,
                    resolved_at,
                    created_at,
                    updated_at
                """,
                _require_text(idea_id, field_name="idea_id"),
                _require_text(idea_key, field_name="idea_key"),
                _require_text(title, field_name="title"),
                _require_text(summary, field_name="summary"),
                _require_text(source_kind, field_name="source_kind"),
                _optional_text(source_ref, field_name="source_ref"),
                _optional_text(owner_ref, field_name="owner_ref"),
                _require_text(decision_ref, field_name="decision_ref"),
                normalized_opened_at,
                normalized_created_at,
                normalized_updated_at,
            )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "operator_ideas.write_failed",
                "failed to record operator idea",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        if row is None:
            raise PostgresWriteError(
                "operator_ideas.write_conflict",
                "existing idea is no longer open and cannot be overwritten",
                details={"idea_id": idea_id},
            )
        return _row_to_json(row)

    async def resolve_idea(
        self,
        *,
        idea_id: str,
        status: str,
        resolution_summary: str,
        decision_ref: str,
        resolved_at: datetime,
    ) -> dict[str, Any]:
        normalized_status = _normalize_status(status, allow_open=False)
        if normalized_status == IDEA_PROMOTED_STATUS:
            raise PostgresWriteError(
                "operator_ideas.invalid_status",
                "use promote_idea to set status promoted",
                details={"status": normalized_status},
            )
        normalized_resolved_at = _require_utc(resolved_at, field_name="resolved_at")
        try:
            row = await self._conn.fetchrow(
                """
                UPDATE operator_ideas
                SET
                    status = $2,
                    resolution_summary = $3,
                    decision_ref = $4,
                    resolved_at = $5,
                    updated_at = $5
                WHERE idea_id = $1
                  AND status = 'open'
                RETURNING
                    idea_id,
                    idea_key,
                    title,
                    status,
                    summary,
                    source_kind,
                    source_ref,
                    owner_ref,
                    decision_ref,
                    resolution_summary,
                    opened_at,
                    resolved_at,
                    created_at,
                    updated_at
                """,
                _require_text(idea_id, field_name="idea_id"),
                normalized_status,
                _require_text(resolution_summary, field_name="resolution_summary"),
                _require_text(decision_ref, field_name="decision_ref"),
                normalized_resolved_at,
            )
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "operator_ideas.write_failed",
                "failed to resolve operator idea",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        if row is None:
            raise PostgresWriteError(
                "operator_ideas.not_open",
                "idea was not found or is not open",
                details={"idea_id": idea_id},
            )
        return _row_to_json(row)

    async def promote_idea(
        self,
        *,
        idea_promotion_id: str,
        idea_id: str,
        roadmap_item_id: str,
        decision_ref: str,
        promoted_by: str,
        promoted_at: datetime,
    ) -> dict[str, Any]:
        normalized_promoted_at = _require_utc(promoted_at, field_name="promoted_at")
        try:
            async with self._conn.transaction():
                idea = await self._conn.fetchrow(
                    """
                    SELECT idea_id, status
                    FROM operator_ideas
                    WHERE idea_id = $1
                    FOR UPDATE
                    """,
                    _require_text(idea_id, field_name="idea_id"),
                )
                if idea is None:
                    raise PostgresWriteError(
                        "operator_ideas.not_found",
                        "idea not found",
                        details={"idea_id": idea_id},
                    )
                if str(idea["status"]) not in {"open", "promoted"}:
                    raise PostgresWriteError(
                        "operator_ideas.not_promotable",
                        "only open or already-promoted ideas can be promoted to roadmap",
                        details={"idea_id": idea_id, "status": str(idea["status"])},
                    )
                roadmap_row = await self._conn.fetchrow(
                    """
                    UPDATE roadmap_items
                    SET
                        source_idea_id = $1,
                        updated_at = $3
                    WHERE roadmap_item_id = $2
                    RETURNING roadmap_item_id, source_idea_id, status, lifecycle, updated_at
                    """,
                    idea_id,
                    _require_text(roadmap_item_id, field_name="roadmap_item_id"),
                    normalized_promoted_at,
                )
                if roadmap_row is None:
                    raise PostgresWriteError(
                        "operator_ideas.roadmap_not_found",
                        "roadmap item not found for idea promotion",
                        details={"roadmap_item_id": roadmap_item_id},
                    )
                promotion_row = await self._conn.fetchrow(
                    """
                    INSERT INTO operator_idea_promotions (
                        idea_promotion_id,
                        idea_id,
                        roadmap_item_id,
                        decision_ref,
                        promoted_by,
                        promoted_at,
                        created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $6)
                    ON CONFLICT (idea_id, roadmap_item_id) DO UPDATE SET
                        decision_ref = EXCLUDED.decision_ref,
                        promoted_by = EXCLUDED.promoted_by,
                        promoted_at = EXCLUDED.promoted_at
                    RETURNING
                        idea_promotion_id,
                        idea_id,
                        roadmap_item_id,
                        decision_ref,
                        promoted_by,
                        promoted_at,
                        created_at
                    """,
                    _require_text(idea_promotion_id, field_name="idea_promotion_id"),
                    idea_id,
                    roadmap_item_id,
                    _require_text(decision_ref, field_name="decision_ref"),
                    _require_text(promoted_by, field_name="promoted_by"),
                    normalized_promoted_at,
                )
                idea_row = await self._conn.fetchrow(
                    """
                    UPDATE operator_ideas
                    SET
                        status = 'promoted',
                        resolution_summary = COALESCE(
                            resolution_summary,
                            'Promoted into roadmap item ' || $2
                        ),
                        decision_ref = $3,
                        resolved_at = COALESCE(resolved_at, $4),
                        updated_at = $4
                    WHERE idea_id = $1
                    RETURNING
                        idea_id,
                        idea_key,
                        title,
                        status,
                        summary,
                        source_kind,
                        source_ref,
                        owner_ref,
                        decision_ref,
                        resolution_summary,
                        opened_at,
                        resolved_at,
                        created_at,
                        updated_at
                    """,
                    idea_id,
                    roadmap_item_id,
                    decision_ref,
                    normalized_promoted_at,
                )
        except PostgresWriteError:
            raise
        except asyncpg.PostgresError as exc:
            raise PostgresWriteError(
                "operator_ideas.write_failed",
                "failed to promote operator idea",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return {
            "idea": _row_to_json(idea_row),
            "promotion": _row_to_json(promotion_row),
            "roadmap_item": _row_to_json(roadmap_row),
        }


__all__ = [
    "IDEA_OPEN_STATUS",
    "IDEA_PROMOTED_STATUS",
    "IDEA_STATUSES",
    "IDEA_TERMINAL_STATUSES",
    "PostgresOperatorIdeaRepository",
]
