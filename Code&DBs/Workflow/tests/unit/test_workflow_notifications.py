from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import runtime.workflow_notifications as workflow_notifications
from runtime.workflow_notifications import WorkflowNotificationConsumer


def _notification_row(
    notification_id: int,
    *,
    run_id: str = "run-1",
    status: str = "succeeded",
    agent_slug: str = "openai/gpt-5.4-mini",
    created_at: datetime,
) -> dict[str, object]:
    return {
        "id": notification_id,
        "run_id": run_id,
        "job_label": f"job-{notification_id}",
        "spec_name": "model_run",
        "agent_slug": agent_slug,
        "status": status,
        "failure_code": "",
        "duration_seconds": 1.5,
        "cpu_percent": None,
        "mem_bytes": None,
        "created_at": created_at,
    }


class _FakeReceiptRepository:
    def __init__(self, _conn, *, rows: list[dict[str, object]], run_query_batches: list[list[int]] | None = None) -> None:
        self.rows = [dict(row) for row in rows]
        self.run_query_batches = [list(batch) for batch in (run_query_batches or [])]
        self.run_query_calls = 0

    def list_workflow_notification_projection(
        self,
        *,
        since_evidence_seq: int = 0,
        limit: int | None = None,
        run_id: str | None = None,
        descending: bool = False,
    ) -> list[dict[str, object]]:
        rows = [
            row
            for row in self.rows
            if int(row["id"]) > since_evidence_seq
            and (run_id is None or str(row["run_id"]) == run_id)
        ]
        rows.sort(key=lambda row: int(row["id"]), reverse=descending)
        if run_id is not None and self.run_query_calls < len(self.run_query_batches):
            visible_ids = set(self.run_query_batches[self.run_query_calls])
            self.run_query_calls += 1
            rows = [row for row in rows if int(row["id"]) in visible_ids]
        if limit is not None:
            rows = rows[:limit]
        return [dict(row) for row in rows]

    def count_workflow_notification_projection(
        self,
        *,
        since_evidence_seq: int = 0,
        run_id: str | None = None,
    ) -> int:
        return len(
            self.list_workflow_notification_projection(
                since_evidence_seq=since_evidence_seq,
                run_id=run_id,
            )
        )


class _FakeConn:
    def execute(self, _query: str, *_args: Any) -> list[dict[str, Any]]:
        return []


def test_fake_connection_implements_execute_contract_notifications() -> None:
    conn = _FakeConn()
    # Verify it matches the expected contract: returns a list
    results = conn.execute("SELECT 1")
    assert isinstance(results, list)


def test_poll_tracks_canonical_receipts_once(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    repository = _FakeReceiptRepository(
        _FakeConn(),
        rows=[
            _notification_row(11, created_at=now),
            _notification_row(12, created_at=now + timedelta(seconds=1), status="awaiting_human", agent_slug="human"),
        ],
    )
    monkeypatch.setattr(
        workflow_notifications,
        "PostgresReceiptRepository",
        lambda conn: repository,
    )
    consumer = WorkflowNotificationConsumer(_FakeConn())

    peeked = consumer.peek(limit=10)
    first_batch = consumer.poll(limit=10)
    second_batch = consumer.poll(limit=10)

    assert [notification.id for notification in peeked] == [11, 12]
    assert [notification.id for notification in first_batch] == [11, 12]
    assert second_batch == []
    assert consumer.pending_count() == 0


def test_iter_run_streams_canonical_projection_batches(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    repository = _FakeReceiptRepository(
        _FakeConn(),
        rows=[
            _notification_row(21, created_at=now),
            _notification_row(22, created_at=now + timedelta(seconds=1), status="awaiting_human", agent_slug="human"),
            _notification_row(23, created_at=now + timedelta(seconds=2)),
        ],
        run_query_batches=[[21], [21, 22], [21, 22, 23]],
    )
    monkeypatch.setattr(
        workflow_notifications,
        "PostgresReceiptRepository",
        lambda conn: repository,
    )
    consumer = WorkflowNotificationConsumer(_FakeConn())
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    yielded = list(
        consumer.iter_run(
            "run-1",
            total_jobs=3,
            timeout_seconds=None,
            poll_interval=0,
        )
    )

    assert [notification.id for notification in yielded] == [21, 22, 23]
    assert consumer.pending_count() == 0


def test_recent_returns_latest_receipts_in_created_order(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    repository = _FakeReceiptRepository(
        _FakeConn(),
        rows=[
            _notification_row(31, created_at=now + timedelta(seconds=2)),
            _notification_row(29, created_at=now),
            _notification_row(30, created_at=now + timedelta(seconds=1), status="failed"),
        ],
    )
    monkeypatch.setattr(
        workflow_notifications,
        "PostgresReceiptRepository",
        lambda conn: repository,
    )

    consumer = WorkflowNotificationConsumer(_FakeConn())
    recent = consumer.recent(limit=2)

    assert [notification.id for notification in recent] == [30, 31]
    assert [notification.status for notification in recent] == ["failed", "succeeded"]
