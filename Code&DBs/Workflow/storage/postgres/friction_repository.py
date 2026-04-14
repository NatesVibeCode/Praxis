"""Explicit sync Postgres repository for friction-ledger queries and mutations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .validators import _require_text, _require_utc, PostgresWriteError


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a boolean",
            details={"field": field_name},
        )
    return value


class PostgresFrictionRepository:
    """Owns canonical friction-event persistence for runtime guardrail flows."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def record_friction_event(
        self,
        *,
        event_id: str,
        friction_type: str,
        source: str,
        job_label: str,
        message: str,
        timestamp: datetime,
        is_test: bool = False,
    ) -> str:
        normalized_event_id = _require_text(event_id, field_name="event_id")
        self._conn.execute(
            """
            INSERT INTO friction_events
            (
                event_id,
                friction_type,
                source,
                job_label,
                message,
                timestamp,
                is_test
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            normalized_event_id,
            _require_text(friction_type, field_name="friction_type"),
            _require_text(source, field_name="source"),
            _require_text(job_label, field_name="job_label"),
            _require_text(message, field_name="message"),
            _require_utc(timestamp, field_name="timestamp"),
            _require_bool(is_test, field_name="is_test"),
        )
        return normalized_event_id

    def list_embedded_events_since(
        self,
        *,
        since: datetime,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT event_id, friction_type, source, message, embedding
              FROM friction_events
             WHERE embedding IS NOT NULL
               AND timestamp >= $1
            """,
            _require_utc(since, field_name="since"),
        )
        return [dict(row) for row in rows or []]

    def list_friction_events(
        self,
        *,
        friction_type: str | None = None,
        source: str | None = None,
        since: datetime | None = None,
        limit: int = 50,
        include_test: bool = False,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        index = 1
        if not include_test:
            clauses.append(f"is_test = ${index}")
            params.append(False)
            index += 1
        if friction_type is not None:
            clauses.append(f"friction_type = ${index}")
            params.append(_require_text(friction_type, field_name="friction_type"))
            index += 1
        if source is not None:
            clauses.append(f"source = ${index}")
            params.append(_require_text(source, field_name="source"))
            index += 1
        if since is not None:
            clauses.append(f"timestamp >= ${index}")
            params.append(_require_utc(since, field_name="since"))
            index += 1

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"""
            SELECT event_id, friction_type, source, job_label, message, timestamp, is_test
              FROM friction_events
            {where}
             ORDER BY timestamp DESC
             LIMIT ${index}
            """,
            *params,
        )
        return [dict(row) for row in rows or []]

    def list_type_source_rows(
        self,
        *,
        include_test: bool = False,
    ) -> list[dict[str, Any]]:
        if include_test:
            rows = self._conn.execute(
                """
                SELECT friction_type, source
                  FROM friction_events
                """,
            )
        else:
            rows = self._conn.execute(
                """
                SELECT friction_type, source
                  FROM friction_events
                 WHERE is_test = false
                """,
            )
        return [dict(row) for row in rows or []]

    def list_type_rows_since(
        self,
        *,
        since: datetime,
        include_test: bool = False,
    ) -> list[dict[str, Any]]:
        normalized_since = _require_utc(since, field_name="since")
        if include_test:
            rows = self._conn.execute(
                """
                SELECT friction_type
                  FROM friction_events
                 WHERE timestamp >= $1
                """,
                normalized_since,
            )
        else:
            rows = self._conn.execute(
                """
                SELECT friction_type
                  FROM friction_events
                 WHERE timestamp >= $1
                   AND is_test = false
                """,
                normalized_since,
            )
        return [dict(row) for row in rows or []]


__all__ = ["PostgresFrictionRepository"]
