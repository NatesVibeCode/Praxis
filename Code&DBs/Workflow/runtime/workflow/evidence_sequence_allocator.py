"""Atomic evidence-sequence allocation and idempotent evidence writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import hashlib
import json

from storage.postgres.receipt_repository import PostgresReceiptRepository


def _require_text(value: object | None, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{field_name} must be a non-empty string")
    return text


def _run_lock_key(run_id: str) -> int:
    normalized_run_id = _require_text(run_id, field_name="run_id")
    return int.from_bytes(
        hashlib.blake2b(normalized_run_id.encode("utf-8"), digest_size=8).digest(),
        "big",
        signed=True,
    )


def _fetchrow_compat(conn, query: str, *args: Any) -> Any:
    if hasattr(conn, "fetchrow"):
        return conn.fetchrow(query, *args)
    rows = conn.execute(query, *args)
    if not rows:
        return None
    return rows[0]


def _extract_evidence_seq(row: Any) -> int:
    if row is None:
        raise RuntimeError("evidence sequence was not allocated")
    if isinstance(row, Mapping):
        return int(row.get("evidence_seq"))
    if isinstance(row, (list, tuple)) and row:
        return int(row[0])
    if isinstance(row, dict):
        return int(row.get("evidence_seq"))
    try:
        return int(row["evidence_seq"])
    except TypeError as exc:
        raise RuntimeError("invalid evidence sequence row shape") from exc


def insert_workflow_event_if_absent_with_deterministic_seq(
    conn,
    *,
    event_id: str,
    event_type: str,
    workflow_id: str,
    run_id: str,
    request_id: str,
    node_id: str,
    occurred_at: Any,
    actor_type: str,
    reason_code: str,
    payload: Mapping[str, Any],
    schema_version: int = 1,
) -> int:
    normalized_run_id = _require_text(run_id, field_name="run_id")
    normalized_event_id = _require_text(event_id, field_name="event_id")
    _require_text(workflow_id, field_name="workflow_id")
    _require_text(request_id, field_name="request_id")
    _require_text(node_id, field_name="node_id")
    row = _fetchrow_compat(
        conn,
        """
        WITH lock_token AS (
            SELECT pg_advisory_xact_lock($2::bigint)
        ),
        existing AS (
            SELECT evidence_seq
            FROM workflow_events
            WHERE event_id = $1
        ),
        next_seq AS (
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM existing) THEN
                    (SELECT evidence_seq FROM existing LIMIT 1)
                ELSE 1 + GREATEST(
                    COALESCE((SELECT MAX(evidence_seq) FROM workflow_events WHERE run_id = $3), 0),
                    COALESCE((SELECT MAX(evidence_seq) FROM receipts WHERE run_id = $3), 0)
                )
            END AS evidence_seq
            FROM lock_token
        ),
        inserted AS (
            INSERT INTO workflow_events (
                event_id,
                event_type,
                schema_version,
                workflow_id,
                run_id,
                request_id,
                causation_id,
                node_id,
                occurred_at,
                evidence_seq,
                actor_type,
                reason_code,
                payload
            )
            SELECT
                $1,
                $4,
                $5,
                $6,
                $3,
                $7,
                NULL,
                $8,
                $9,
                next_seq.evidence_seq,
                $10,
                $11,
                $12::jsonb
            FROM next_seq
            WHERE NOT EXISTS (SELECT 1 FROM existing)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING evidence_seq
        )
        SELECT evidence_seq FROM inserted
        UNION ALL
        SELECT evidence_seq FROM existing
        LIMIT 1;
        """,
        normalized_event_id,
        _run_lock_key(normalized_run_id),
        normalized_run_id,
        event_type,
        schema_version,
        workflow_id,
        request_id,
        node_id,
        occurred_at,
        actor_type,
        reason_code,
        json.dumps(dict(payload), sort_keys=True, default=str),
    )
    return _extract_evidence_seq(row)


def insert_receipt_if_absent_with_deterministic_seq(
    conn,
    *,
    receipt_id: str,
    receipt_type: str = "workflow_job",
    schema_version: int = 1,
    workflow_id: str,
    run_id: str,
    request_id: str,
    node_id: str,
    attempt_no: int,
    started_at: Any,
    finished_at: Any,
    causation_id: str | None = None,
    supersedes_receipt_id: str | None = None,
    status: str,
    inputs: Mapping[str, Any],
    outputs: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    failure_code: str | None,
    executor_type: str = "workflow_unified",
    decision_refs: Sequence[Mapping[str, Any]] | None = None,
) -> int:
    repository = PostgresReceiptRepository(conn)
    return repository.insert_receipt_if_absent_with_deterministic_seq(
        receipt_id=_require_text(receipt_id, field_name="receipt_id"),
        receipt_type=_require_text(receipt_type, field_name="receipt_type"),
        schema_version=schema_version,
        workflow_id=_require_text(workflow_id, field_name="workflow_id"),
        run_id=_require_text(run_id, field_name="run_id"),
        request_id=_require_text(request_id, field_name="request_id"),
        causation_id=None if causation_id is None else _require_text(
            causation_id,
            field_name="causation_id",
        ),
        node_id=_require_text(node_id, field_name="node_id"),
        attempt_no=attempt_no,
        started_at=started_at,
        finished_at=finished_at,
        supersedes_receipt_id=None
        if supersedes_receipt_id is None
        else _require_text(
            supersedes_receipt_id,
            field_name="supersedes_receipt_id",
        ),
        status=_require_text(status, field_name="status"),
        inputs=dict(inputs),
        outputs=dict(outputs),
        artifacts=dict(artifacts),
        failure_code=None if failure_code is None else str(failure_code),
        executor_type=_require_text(executor_type, field_name="executor_type"),
        decision_refs=list(decision_refs or []),
    )
