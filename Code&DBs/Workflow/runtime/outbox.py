"""Workflow outbox subscriber seam.

This module exposes one narrow read contract over committed workflow evidence.
It does not mutate lifecycle state, and it does not treat notifications as
truth. Subscribers advance only by explicit run-scoped `evidence_seq`
watermarks copied from the authority rows.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import asyncpg

from storage.migrations import workflow_migrations_root
from storage.postgres import resolve_workflow_database_url

from .domain import RuntimeBoundaryError

_OUTBOX_SCHEMA_FILENAME = "005_workflow_outbox.sql"


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _require_text(value: str | None, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeBoundaryError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_int(value: int, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeBoundaryError(f"{field_name} must be a positive integer")
    return value


def _optional_non_negative_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeBoundaryError(f"{field_name} must be a non-negative integer")
    return value


def _schema_path() -> Path:
    root = workflow_migrations_root()
    path = root / _OUTBOX_SCHEMA_FILENAME
    if not path.is_file():
        raise RuntimeBoundaryError(
            "workflow outbox schema file could not be read from the canonical workflow migration root"
        )
    return path


@lru_cache(maxsize=1)
def _schema_sql_text() -> str:
    path = _schema_path()
    try:
        sql_text = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - defensive failure path
        raise RuntimeBoundaryError(
            "workflow outbox schema file could not be read from the canonical workflow migration root"
        ) from exc
    if not sql_text.strip():
        raise RuntimeBoundaryError("workflow outbox schema file did not contain executable statements")
    return sql_text


@dataclass(frozen=True, slots=True)
class WorkflowOutboxCursor:
    """Explicit subscriber watermark for one run."""

    run_id: str
    last_evidence_seq: int | None = None


@dataclass(frozen=True, slots=True)
class WorkflowOutboxRecord:
    """One committed authority row mirrored into the outbox."""

    authority_table: str
    authority_id: str
    envelope_kind: str
    workflow_id: str
    run_id: str
    request_id: str
    evidence_seq: int
    transition_seq: int
    authority_recorded_at: object
    captured_at: object
    envelope: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class WorkflowOutboxBatch:
    """One explicit read batch for a subscriber."""

    cursor: WorkflowOutboxCursor
    rows: tuple[WorkflowOutboxRecord, ...]
    has_more: bool


def _record_from_row(row: asyncpg.Record) -> WorkflowOutboxRecord:
    envelope = _json_value(row["envelope"])
    if not isinstance(envelope, Mapping):
        raise RuntimeBoundaryError("workflow outbox envelope must be a mapping")
    return WorkflowOutboxRecord(
        authority_table=str(row["authority_table"]),
        authority_id=str(row["authority_id"]),
        envelope_kind=str(row["envelope_kind"]),
        workflow_id=str(row["workflow_id"]),
        run_id=str(row["run_id"]),
        request_id=str(row["request_id"]),
        evidence_seq=int(row["evidence_seq"]),
        transition_seq=int(row["transition_seq"]),
        authority_recorded_at=row["authority_recorded_at"],
        captured_at=row["captured_at"],
        envelope=envelope,
    )


async def bootstrap_workflow_outbox_schema(conn: asyncpg.Connection) -> None:
    """Apply the workflow outbox schema in one explicit step."""

    async with conn.transaction():
        await conn.execute(_schema_sql_text())


async def fetch_workflow_outbox_batch(
    conn: asyncpg.Connection,
    *,
    run_id: str,
    after_evidence_seq: int | None = None,
    limit: int = 100,
) -> WorkflowOutboxBatch:
    """Read committed outbox rows for one run in canonical replay order."""

    normalized_run_id = _require_text(run_id, field_name="run_id")
    normalized_after = _optional_non_negative_int(
        after_evidence_seq,
        field_name="after_evidence_seq",
    )
    normalized_limit = _require_positive_int(limit, field_name="limit")
    rows = await conn.fetch(
        """
        SELECT
            authority_table,
            authority_id,
            envelope_kind,
            workflow_id,
            run_id,
            request_id,
            evidence_seq,
            transition_seq,
            authority_recorded_at,
            captured_at,
            envelope
        FROM workflow_outbox
        WHERE run_id = $1
          AND ($2::bigint IS NULL OR evidence_seq > $2)
        ORDER BY evidence_seq
        LIMIT $3
        """,
        normalized_run_id,
        normalized_after,
        normalized_limit + 1,
    )
    visible_rows = tuple(_record_from_row(row) for row in rows[:normalized_limit])
    cursor = WorkflowOutboxCursor(
        run_id=normalized_run_id,
        last_evidence_seq=(
            visible_rows[-1].evidence_seq if visible_rows else normalized_after
        ),
    )
    return WorkflowOutboxBatch(
        cursor=cursor,
        rows=visible_rows,
        has_more=len(rows) > normalized_limit,
    )


@dataclass(frozen=True, slots=True)
class PostgresWorkflowOutboxSubscriber:
    """Explicit Postgres-backed subscriber seam over committed outbox rows."""

    database_url: str | None = None
    env: Mapping[str, str] | None = None

    def read_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.load_batch(
                    run_id=run_id,
                    after_evidence_seq=after_evidence_seq,
                    limit=limit,
                )
            )
        raise RuntimeBoundaryError(
            "sync read_batch() requires an explicit non-async call boundary"
        )

    async def load_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        database_url = (
            resolve_workflow_database_url(env={"WORKFLOW_DATABASE_URL": self.database_url})
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await fetch_workflow_outbox_batch(
                conn,
                run_id=run_id,
                after_evidence_seq=after_evidence_seq,
                limit=limit,
            )
        finally:
            await conn.close()


def clear_workflow_outbox_schema_cache() -> None:
    """Reset cached schema reads for tests and patched call sites."""

    _schema_sql_text.cache_clear()


__all__ = [
    "PostgresWorkflowOutboxSubscriber",
    "WorkflowOutboxBatch",
    "WorkflowOutboxCursor",
    "WorkflowOutboxRecord",
    "bootstrap_workflow_outbox_schema",
    "clear_workflow_outbox_schema_cache",
    "fetch_workflow_outbox_batch",
]
