"""Postgres authority for runtime control-operator frames."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from typing import Any

from runtime.control_operator_frames import RunOperatorFrame

from .connection import SyncPostgresConnection
from .validators import _encode_jsonb


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _frame_from_row(row: Mapping[str, Any]) -> RunOperatorFrame:
    source_snapshot = row["source_snapshot"]
    aggregate_outputs = row["aggregate_outputs"]
    if isinstance(source_snapshot, str):
        source_snapshot = json.loads(source_snapshot)
    if isinstance(aggregate_outputs, str):
        aggregate_outputs = json.loads(aggregate_outputs)
    return RunOperatorFrame(
        operator_frame_id=str(row["operator_frame_id"]),
        run_id=str(row["run_id"]),
        node_id=str(row["node_id"]),
        operator_kind=str(row["operator_kind"]),
        frame_state=str(row["frame_state"]),
        item_index=int(row["item_index"]) if row["item_index"] is not None else None,
        iteration_index=(
            int(row["iteration_index"]) if row["iteration_index"] is not None else None
        ),
        source_snapshot=dict(source_snapshot or {}),
        aggregate_outputs=dict(aggregate_outputs or {}),
        active_count=int(row["active_count"]),
        stop_reason=str(row["stop_reason"]) if row["stop_reason"] is not None else None,
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


class PostgresOperatorFrameRepository:
    """Canonical Postgres-backed authority for dynamic operator frames."""

    def __init__(self, conn: SyncPostgresConnection) -> None:
        self._conn = conn

    def create_frame(
        self,
        *,
        operator_frame_id: str,
        run_id: str,
        node_id: str,
        operator_kind: str,
        item_index: int | None,
        iteration_index: int | None,
        source_snapshot: dict[str, Any] | None = None,
        active_count: int = 0,
    ) -> RunOperatorFrame:
        started_at = _utc_now()
        row = self._conn.fetchrow(
            """
            INSERT INTO run_operator_frames (
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            ) VALUES (
                $1, $2, $3, $4, 'created', $5, $6, $7::jsonb, '{}'::jsonb, $8, NULL, $9, NULL
            )
            RETURNING
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            """,
            operator_frame_id,
            run_id,
            node_id,
            operator_kind,
            item_index,
            iteration_index,
            _encode_jsonb(dict(source_snapshot or {}), field_name="source_snapshot"),
            max(0, int(active_count)),
            started_at,
        )
        if row is None:
            raise RuntimeError(
                f"failed to create run_operator_frames row for {operator_frame_id!r}"
            )
        return _frame_from_row(row)

    def mark_running(
        self,
        *,
        operator_frame_id: str,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        row = self._conn.fetchrow(
            """
            UPDATE run_operator_frames
            SET frame_state = 'running',
                active_count = COALESCE($2, active_count),
                finished_at = NULL
            WHERE operator_frame_id = $1
            RETURNING
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            """,
            operator_frame_id,
            max(0, int(active_count)) if active_count is not None else None,
        )
        if row is None:
            raise KeyError(f"unknown operator_frame_id={operator_frame_id!r}")
        return _frame_from_row(row)

    def mark_succeeded(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        row = self._conn.fetchrow(
            """
            UPDATE run_operator_frames
            SET frame_state = 'succeeded',
                aggregate_outputs = $2::jsonb,
                stop_reason = $3,
                active_count = COALESCE($4, active_count),
                finished_at = $5
            WHERE operator_frame_id = $1
            RETURNING
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            """,
            operator_frame_id,
            _encode_jsonb(dict(aggregate_outputs or {}), field_name="aggregate_outputs"),
            stop_reason,
            max(0, int(active_count)) if active_count is not None else None,
            _utc_now(),
        )
        if row is None:
            raise KeyError(f"unknown operator_frame_id={operator_frame_id!r}")
        return _frame_from_row(row)

    def mark_failed(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        row = self._conn.fetchrow(
            """
            UPDATE run_operator_frames
            SET frame_state = 'failed',
                aggregate_outputs = $2::jsonb,
                stop_reason = $3,
                active_count = COALESCE($4, active_count),
                finished_at = $5
            WHERE operator_frame_id = $1
            RETURNING
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            """,
            operator_frame_id,
            _encode_jsonb(dict(aggregate_outputs or {}), field_name="aggregate_outputs"),
            stop_reason,
            max(0, int(active_count)) if active_count is not None else None,
            _utc_now(),
        )
        if row is None:
            raise KeyError(f"unknown operator_frame_id={operator_frame_id!r}")
        return _frame_from_row(row)

    def mark_cancelled(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        row = self._conn.fetchrow(
            """
            UPDATE run_operator_frames
            SET frame_state = 'cancelled',
                aggregate_outputs = $2::jsonb,
                stop_reason = $3,
                active_count = COALESCE($4, active_count),
                finished_at = $5
            WHERE operator_frame_id = $1
            RETURNING
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            """,
            operator_frame_id,
            _encode_jsonb(dict(aggregate_outputs or {}), field_name="aggregate_outputs"),
            stop_reason,
            max(0, int(active_count)) if active_count is not None else None,
            _utc_now(),
        )
        if row is None:
            raise KeyError(f"unknown operator_frame_id={operator_frame_id!r}")
        return _frame_from_row(row)

    def list_for_node(self, *, run_id: str, node_id: str) -> tuple[RunOperatorFrame, ...]:
        rows = self._conn.execute(
            """
            SELECT
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            FROM run_operator_frames
            WHERE run_id = $1
              AND node_id = $2
            ORDER BY
                COALESCE(item_index, -1),
                COALESCE(iteration_index, -1),
                operator_frame_id
            """,
            run_id,
            node_id,
        )
        return tuple(_frame_from_row(row) for row in rows)

    def list_for_run(self, *, run_id: str) -> tuple[RunOperatorFrame, ...]:
        rows = self._conn.execute(
            """
            SELECT
                operator_frame_id,
                run_id,
                node_id,
                operator_kind,
                frame_state,
                item_index,
                iteration_index,
                source_snapshot,
                aggregate_outputs,
                active_count,
                stop_reason,
                started_at,
                finished_at
            FROM run_operator_frames
            WHERE run_id = $1
            ORDER BY
                node_id,
                COALESCE(item_index, -1),
                COALESCE(iteration_index, -1),
                operator_frame_id
            """,
            run_id,
        )
        return tuple(_frame_from_row(row) for row in rows)

    def clear_for_run(self, *, run_id: str) -> int:
        rows = self._conn.execute(
            """
            DELETE FROM run_operator_frames
            WHERE run_id = $1
            RETURNING operator_frame_id
            """,
            run_id,
        )
        return len(rows)


__all__ = ["PostgresOperatorFrameRepository"]
