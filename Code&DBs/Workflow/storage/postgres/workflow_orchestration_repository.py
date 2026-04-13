"""Explicit sync Postgres repositories for workflow worker orchestration state."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from .validators import PostgresWriteError, _encode_jsonb, _require_text, _require_utc

_READY_CARD_NODES_QUERY = """
SELECT run_node_id, run_id, node_id, node_type, input_payload
FROM run_nodes
WHERE current_state = 'ready' AND node_type LIKE 'card_%'
ORDER BY started_at ASC NULLS FIRST
"""

_CLAIM_READY_RUN_NODE_QUERY = """
UPDATE run_nodes
SET current_state = 'running',
    started_at = NOW()
WHERE run_node_id = $1
  AND current_state = 'ready'
RETURNING run_node_id
"""

_MARK_TERMINAL_RUN_NODE_QUERY = """
UPDATE run_nodes
SET current_state = $2,
    finished_at = NOW(),
    output_payload = $3::jsonb,
    failure_code = $4
WHERE run_node_id = $1
RETURNING run_node_id
"""

_MARK_FAILED_RUN_NODE_QUERY = """
UPDATE run_nodes
SET current_state = 'failed',
    finished_at = NOW(),
    failure_code = $2
WHERE run_node_id = $1
RETURNING run_node_id
"""

_INSERT_WORKFLOW_NOTIFICATION_QUERY = """
INSERT INTO workflow_notifications
    (run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

_INSERT_WORKFLOW_NOTIFICATION_WITH_TIMESTAMP_QUERY = """
INSERT INTO workflow_notifications
    (run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
"""


def _normalize_duration_seconds(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "duration_seconds must be a non-negative number",
            details={"field": "duration_seconds"},
        )
    duration = float(value)
    if duration < 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "duration_seconds must be a non-negative number",
            details={"field": "duration_seconds"},
        )
    return duration


def _normalize_failure_code(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text[:200]


def _require_terminal_state(value: object) -> str:
    state = _require_text(value, field_name="state")
    if state not in {"succeeded", "failed"}:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "state must be either 'succeeded' or 'failed'",
            details={"field": "state", "value": state},
        )
    return state


class PostgresRunNodeStateRepository:
    """Owns canonical run-node state transitions for workflow worker flows."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def list_ready_card_nodes(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(_READY_CARD_NODES_QUERY)]

    def claim_ready_run_node(self, *, run_node_id: str) -> bool:
        rows = self._conn.execute(
            _CLAIM_READY_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
        )
        return bool(rows)

    def mark_terminal_state(
        self,
        *,
        run_node_id: str,
        state: str,
        output_payload: Mapping[str, Any] | None = None,
        failure_code: str | None = None,
    ) -> bool:
        rows = self._conn.execute(
            _MARK_TERMINAL_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
            _require_terminal_state(state),
            _encode_jsonb(dict(output_payload or {}), field_name="output_payload"),
            _normalize_failure_code(failure_code),
        )
        return bool(rows)

    def mark_failed(self, *, run_node_id: str, failure_code: str) -> bool:
        rows = self._conn.execute(
            _MARK_FAILED_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
            _require_text(
                _normalize_failure_code(failure_code),
                field_name="failure_code",
            ),
        )
        return bool(rows)


class PostgresWorkflowNotificationRepository:
    """Owns durable workflow notification emission for worker orchestration."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def emit_notification(
        self,
        *,
        run_id: str,
        job_label: str,
        spec_name: str,
        agent_slug: str,
        status: str,
        failure_code: str | None = None,
        duration_seconds: float = 0.0,
        created_at: datetime | None = None,
    ) -> None:
        args = (
            _require_text(run_id, field_name="run_id"),
            _require_text(job_label, field_name="job_label"),
            str(spec_name or ""),
            _require_text(agent_slug, field_name="agent_slug"),
            _require_text(status, field_name="status"),
            _normalize_failure_code(failure_code),
            _normalize_duration_seconds(duration_seconds),
        )
        if created_at is None:
            self._conn.execute(_INSERT_WORKFLOW_NOTIFICATION_QUERY, *args)
            return

        self._conn.execute(
            _INSERT_WORKFLOW_NOTIFICATION_WITH_TIMESTAMP_QUERY,
            *args,
            _require_utc(created_at, field_name="created_at"),
        )
