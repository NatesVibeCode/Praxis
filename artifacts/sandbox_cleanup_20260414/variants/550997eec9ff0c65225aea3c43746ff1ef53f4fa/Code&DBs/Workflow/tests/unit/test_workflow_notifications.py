from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime.workflow_notifications import WorkflowNotificationConsumer


class _FakeConn:
    def __init__(
        self,
        rows: list[dict[str, object]],
        *,
        run_query_batches: list[list[int]] | None = None,
    ) -> None:
        self.rows = [dict(row) for row in rows]
        self.run_query_batches = [list(batch) for batch in (run_query_batches or [])]
        self.run_query_calls = 0

    @property
    def delivered_ids(self) -> set[int]:
        return {int(row["id"]) for row in self.rows if row["delivered"]}

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith(
            "UPDATE workflow_notifications SET delivered = true WHERE id IN ("
        ):
            limit = int(args[0])
            claimed = self._undelivered_rows()[:limit]
            for row in claimed:
                row["delivered"] = True
            return [dict(row) for row in claimed]

        if normalized.startswith(
            "SELECT id, run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds, created_at FROM workflow_notifications WHERE delivered = false"
        ):
            limit = int(args[0])
            return [dict(row) for row in self._undelivered_rows()[:limit]]

        if normalized.startswith(
            "SELECT count(*) AS c FROM workflow_notifications WHERE delivered = false"
        ):
            return [{"c": len(self._undelivered_rows())}]

        if normalized.startswith(
            "SELECT id, run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds, created_at FROM workflow_notifications WHERE run_id = $1 AND id NOT IN ( SELECT unnest($2::int[]) ) ORDER BY created_at ASC"
        ):
            run_id = str(args[0])
            excluded_ids = {int(value) for value in args[1]}
            if self.run_query_calls < len(self.run_query_batches):
                visible_ids = set(self.run_query_batches[self.run_query_calls])
            else:
                visible_ids = {
                    int(row["id"]) for row in self.rows if str(row["run_id"]) == run_id
                }
            self.run_query_calls += 1
            return [
                dict(row)
                for row in self._rows_for_run(run_id)
                if int(row["id"]) in visible_ids and int(row["id"]) not in excluded_ids
            ]

        if normalized.startswith(
            "UPDATE workflow_notifications SET delivered = true WHERE id = ANY($1::int[])"
        ):
            delivered_ids = {int(value) for value in args[0]}
            for row in self.rows:
                if int(row["id"]) in delivered_ids:
                    row["delivered"] = True
            return []

        raise AssertionError(f"Unexpected SQL: {normalized}")

    def _rows_for_run(self, run_id: str) -> list[dict[str, object]]:
        return [
            row
            for row in sorted(self.rows, key=lambda candidate: candidate["created_at"])
            if str(row["run_id"]) == run_id
        ]

    def _undelivered_rows(self) -> list[dict[str, object]]:
        return [
            row
            for row in sorted(self.rows, key=lambda candidate: candidate["created_at"])
            if not row["delivered"]
        ]


def _notification_row(
    notification_id: int,
    *,
    run_id: str = "run-1",
    delivered: bool = False,
    created_at: datetime,
) -> dict[str, object]:
    return {
        "id": notification_id,
        "run_id": run_id,
        "job_label": f"job-{notification_id}",
        "spec_name": "wave-1-proof",
        "agent_slug": "openai/gpt-5.4-mini",
        "status": "succeeded",
        "failure_code": "",
        "duration_seconds": 1.5,
        "created_at": created_at,
        "delivered": delivered,
    }


def test_poll_marks_notifications_delivered_and_does_not_redeliver_old_rows() -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    conn = _FakeConn(
        [
            _notification_row(1, created_at=now),
            _notification_row(2, created_at=now + timedelta(seconds=1)),
        ]
    )
    consumer = WorkflowNotificationConsumer(conn)

    peeked = consumer.peek(limit=10)
    first_batch = consumer.poll(limit=10)
    second_batch = consumer.poll(limit=10)

    assert [notification.id for notification in peeked] == [1, 2]
    assert [notification.id for notification in first_batch] == [1, 2]
    assert second_batch == []
    assert consumer.pending_count() == 0
    assert conn.delivered_ids == {1, 2}


def test_iter_run_deduplicates_replayed_rows_and_marks_them_delivered(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    conn = _FakeConn(
        [
            _notification_row(1, created_at=now),
            _notification_row(2, created_at=now + timedelta(seconds=1)),
        ],
        run_query_batches=[[1], [1, 2]],
    )
    consumer = WorkflowNotificationConsumer(conn)
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    yielded = list(
        consumer.iter_run(
            "run-1",
            total_jobs=2,
            timeout_seconds=None,
            poll_interval=0,
        )
    )

    assert [notification.id for notification in yielded] == [1, 2]
    assert conn.delivered_ids == {1, 2}
    assert consumer.pending_count() == 0
