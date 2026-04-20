"""Explicit sync Postgres repositories for workflow worker orchestration state."""

from __future__ import annotations

from typing import Any, Mapping

from .validators import PostgresWriteError, _encode_jsonb, _require_text

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
    failure_code = $4,
    receipt_id = COALESCE($5, receipt_id)
WHERE run_node_id = $1
RETURNING run_node_id
"""

_MARK_AWAITING_HUMAN_RUN_NODE_QUERY = """
UPDATE run_nodes
SET current_state = 'awaiting_human',
    output_payload = $2::jsonb,
    receipt_id = COALESCE($3, receipt_id)
WHERE run_node_id = $1
  AND current_state = 'running'
RETURNING run_node_id
"""

_MARK_FAILED_RUN_NODE_QUERY = """
UPDATE run_nodes
SET current_state = 'failed',
    finished_at = NOW(),
    failure_code = $2,
    receipt_id = COALESCE($3, receipt_id)
WHERE run_node_id = $1
RETURNING run_node_id
"""

def _normalize_failure_code(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


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
        receipt_id: str | None = None,
    ) -> bool:
        rows = self._conn.execute(
            _MARK_TERMINAL_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
            _require_terminal_state(state),
            _encode_jsonb(dict(output_payload or {}), field_name="output_payload"),
            _normalize_failure_code(failure_code),
            _require_text(receipt_id, field_name="receipt_id") if receipt_id else None,
        )
        return bool(rows)

    def mark_awaiting_human(
        self,
        *,
        run_node_id: str,
        output_payload: Mapping[str, Any] | None = None,
        receipt_id: str | None = None,
    ) -> bool:
        rows = self._conn.execute(
            _MARK_AWAITING_HUMAN_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
            _encode_jsonb(dict(output_payload or {}), field_name="output_payload"),
            _require_text(receipt_id, field_name="receipt_id") if receipt_id else None,
        )
        return bool(rows)

    def mark_failed(
        self,
        *,
        run_node_id: str,
        failure_code: str,
        receipt_id: str | None = None,
    ) -> bool:
        rows = self._conn.execute(
            _MARK_FAILED_RUN_NODE_QUERY,
            _require_text(run_node_id, field_name="run_node_id"),
            _require_text(
                _normalize_failure_code(failure_code),
                field_name="failure_code",
            ),
            _require_text(receipt_id, field_name="receipt_id") if receipt_id else None,
        )
        return bool(rows)
