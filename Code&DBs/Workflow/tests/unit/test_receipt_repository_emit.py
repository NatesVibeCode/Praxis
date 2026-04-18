"""Unit tests for ``PostgresReceiptRepository.insert_receipt_if_absent_with_deterministic_seq``.

Covers:
  - On a fresh insert the repository emits a ``receipt_recorded`` event
    on ``CHANNEL_RECEIPT``.
  - On a duplicate (existing receipt) insert no event is emitted, so
    idempotent re-runs do not flood the log.
  - Event payload carries the fields cursor-based subscribers need to
    filter (run_id, status, failure_code, evidence_seq).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone


class _ReceiptConn:
    def __init__(self, *, was_inserted: bool, evidence_seq: int = 7) -> None:
        self._was_inserted = was_inserted
        self._evidence_seq = evidence_seq
        self.event_rows: list[dict[str, object]] = []
        self.notifications: list[tuple[str, str]] = []
        self._next_event_id = 1

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("WITH lock_token"):
            return {"evidence_seq": self._evidence_seq, "was_inserted": self._was_inserted}
        if normalized.startswith("INSERT INTO event_log"):
            row = {"id": self._next_event_id}
            self.event_rows.append(
                {
                    "id": self._next_event_id,
                    "channel": args[0],
                    "event_type": args[1],
                    "entity_id": args[2],
                    "entity_kind": args[3],
                    "payload": json.loads(args[4]),
                    "emitted_by": args[5],
                }
            )
            self._next_event_id += 1
            return row
        raise AssertionError(f"unexpected fetchrow: {normalized}")

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("WITH lock_token"):
            return [{"evidence_seq": self._evidence_seq, "was_inserted": self._was_inserted}]
        if normalized.startswith("INSERT INTO event_log"):
            row = {"id": self._next_event_id}
            self.event_rows.append(
                {
                    "id": self._next_event_id,
                    "channel": args[0],
                    "event_type": args[1],
                    "entity_id": args[2],
                    "entity_kind": args[3],
                    "payload": json.loads(args[4]),
                    "emitted_by": args[5],
                }
            )
            self._next_event_id += 1
            return [row]
        if normalized.startswith("SELECT pg_notify"):
            self.notifications.append((args[0], args[1]))
            return []
        raise AssertionError(f"unexpected execute: {normalized}")


def _insert_args() -> dict[str, object]:
    now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
    return {
        "receipt_id": "receipt-42",
        "workflow_id": "workflow-a",
        "run_id": "run-a",
        "request_id": "request-a",
        "node_id": "node-a",
        "attempt_no": 1,
        "started_at": now,
        "finished_at": now,
        "status": "succeeded",
        "inputs": {"prompt": "p"},
        "outputs": {"result": "r"},
        "artifacts": {},
        "failure_code": None,
    }


def test_first_insert_emits_receipt_recorded_on_receipt_channel() -> None:
    from storage.postgres.receipt_repository import (
        EVENT_RECEIPT_RECORDED,
        PostgresReceiptRepository,
    )

    conn = _ReceiptConn(was_inserted=True, evidence_seq=11)
    repo = PostgresReceiptRepository(conn)

    evidence_seq = repo.insert_receipt_if_absent_with_deterministic_seq(**_insert_args())

    assert evidence_seq == 11
    assert len(conn.event_rows) == 1
    event = conn.event_rows[0]
    assert event["channel"] == "receipt"
    assert event["event_type"] == EVENT_RECEIPT_RECORDED
    assert event["entity_id"] == "receipt-42"
    assert event["entity_kind"] == "receipt"
    assert event["emitted_by"] == (
        "receipt_repository.insert_receipt_if_absent_with_deterministic_seq"
    )
    payload = event["payload"]
    assert payload["receipt_id"] == "receipt-42"
    assert payload["run_id"] == "run-a"
    assert payload["workflow_id"] == "workflow-a"
    assert payload["status"] == "succeeded"
    assert payload["failure_code"] is None
    assert payload["evidence_seq"] == 11
    assert conn.notifications == [("receipt", json.dumps({"id": 1, "type": EVENT_RECEIPT_RECORDED, "entity": "receipt-42"}))]


def test_duplicate_insert_does_not_emit_again() -> None:
    from storage.postgres.receipt_repository import PostgresReceiptRepository

    conn = _ReceiptConn(was_inserted=False, evidence_seq=5)
    repo = PostgresReceiptRepository(conn)

    evidence_seq = repo.insert_receipt_if_absent_with_deterministic_seq(**_insert_args())

    assert evidence_seq == 5
    assert conn.event_rows == []
    assert conn.notifications == []


def test_first_insert_with_failure_code_propagates_to_payload() -> None:
    from storage.postgres.receipt_repository import PostgresReceiptRepository

    conn = _ReceiptConn(was_inserted=True, evidence_seq=3)
    repo = PostgresReceiptRepository(conn)

    args = _insert_args()
    args["status"] = "failed"
    args["failure_code"] = "adapter.timeout"

    repo.insert_receipt_if_absent_with_deterministic_seq(**args)

    assert len(conn.event_rows) == 1
    payload = conn.event_rows[0]["payload"]
    assert payload["status"] == "failed"
    assert payload["failure_code"] == "adapter.timeout"
