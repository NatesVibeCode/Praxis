"""Canonical receipt writing for operating-model run_nodes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from runtime.workflow.evidence_sequence_allocator import (
    insert_receipt_if_absent_with_deterministic_seq,
)
from storage.postgres.receipt_repository import PostgresReceiptRepository


def _require_text(value: object | None, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{field_name} must be a non-empty string")
    return text


def _fetchrow_compat(conn, query: str, *args: Any) -> Any:
    if hasattr(conn, "fetchrow"):
        return conn.fetchrow(query, *args)
    rows = conn.execute(query, *args)
    if not rows:
        return None
    return rows[0]


def _load_run_node_receipt_context(
    conn,
    *,
    run_node_id: str | None = None,
    run_id: str | None = None,
    node_id: str | None = None,
) -> dict[str, Any]:
    if run_node_id:
        row = _fetchrow_compat(
            conn,
            """
            SELECT rn.run_node_id,
                   rn.run_id,
                   rn.node_id,
                   rn.node_type,
                   rn.attempt_number,
                   rn.current_state,
                   rn.started_at,
                   rn.finished_at,
                   rn.receipt_id,
                   wr.workflow_id,
                   wr.request_id
            FROM run_nodes AS rn
            JOIN workflow_runs AS wr
              ON wr.run_id = rn.run_id
            WHERE rn.run_node_id = $1
            LIMIT 1
            """,
            _require_text(run_node_id, field_name="run_node_id"),
        )
    else:
        row = _fetchrow_compat(
            conn,
            """
            SELECT rn.run_node_id,
                   rn.run_id,
                   rn.node_id,
                   rn.node_type,
                   rn.attempt_number,
                   rn.current_state,
                   rn.started_at,
                   rn.finished_at,
                   rn.receipt_id,
                   wr.workflow_id,
                   wr.request_id
            FROM run_nodes AS rn
            JOIN workflow_runs AS wr
              ON wr.run_id = rn.run_id
            WHERE rn.run_id = $1
              AND rn.node_id = $2
            ORDER BY rn.attempt_number DESC
            LIMIT 1
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(node_id, field_name="node_id"),
        )
    if row is None:
        details = {"run_node_id": run_node_id, "run_id": run_id, "node_id": node_id}
        raise RuntimeError(f"run_node receipt context not found: {details}")
    return dict(row)


def _run_node_receipt_id(
    *,
    run_id: str,
    node_id: str,
    attempt_no: int,
    phase: str,
) -> str:
    return f"receipt:{run_id}:{node_id}:{attempt_no}:{phase}"


def write_run_node_receipt(
    conn,
    *,
    run_node_id: str | None = None,
    run_id: str | None = None,
    node_id: str | None = None,
    phase: str,
    receipt_type: str,
    status: str,
    outputs: Mapping[str, Any] | None = None,
    failure_code: str | None = None,
    agent_slug: str = "",
    executor_type: str = "runtime.workflow.worker",
    inputs: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
    decision_refs: Sequence[Mapping[str, Any]] | None = None,
    notify: bool = True,
) -> str:
    """Persist one canonical receipt for a run_node transition."""
    context = _load_run_node_receipt_context(
        conn,
        run_node_id=run_node_id,
        run_id=run_id,
        node_id=node_id,
    )
    normalized_run_id = _require_text(context.get("run_id"), field_name="run_id")
    normalized_node_id = _require_text(context.get("node_id"), field_name="node_id")
    attempt_no = max(1, int(context.get("attempt_number") or 1))
    now = datetime.now(timezone.utc)
    started_at = context.get("started_at") or now
    finished_at = context.get("finished_at") or now
    receipt_id = _run_node_receipt_id(
        run_id=normalized_run_id,
        node_id=normalized_node_id,
        attempt_no=attempt_no,
        phase=_require_text(phase, field_name="phase"),
    )

    receipt_inputs = {
        "run_node_id": str(context.get("run_node_id") or ""),
        "node_type": str(context.get("node_type") or ""),
        "attempt": attempt_no,
        "agent_slug": str(agent_slug or "").strip(),
        "phase": phase,
    }
    if inputs:
        receipt_inputs.update(dict(inputs))

    receipt_outputs = {
        "status": str(status or "").strip(),
    }
    if outputs:
        receipt_outputs.update(dict(outputs))

    insert_receipt_if_absent_with_deterministic_seq(
        conn,
        receipt_id=receipt_id,
        receipt_type=_require_text(receipt_type, field_name="receipt_type"),
        workflow_id=_require_text(context.get("workflow_id"), field_name="workflow_id"),
        run_id=normalized_run_id,
        request_id=_require_text(context.get("request_id"), field_name="request_id"),
        node_id=normalized_node_id,
        attempt_no=attempt_no,
        started_at=started_at,
        finished_at=finished_at,
        supersedes_receipt_id=str(context.get("receipt_id") or "").strip() or None,
        status=_require_text(status, field_name="status"),
        inputs=receipt_inputs,
        outputs=receipt_outputs,
        artifacts=dict(artifacts or {}),
        failure_code=str(failure_code or "").strip() or None,
        executor_type=_require_text(executor_type, field_name="executor_type"),
        decision_refs=list(decision_refs or []),
    )
    if notify:
        PostgresReceiptRepository(conn).notify_job_completed(run_id=normalized_run_id)
    return receipt_id


__all__ = ["write_run_node_receipt"]
