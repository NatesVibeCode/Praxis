from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime.workflow_notifications import WorkflowNotificationConsumer


class _FakeConn:
    def __init__(
        self,
        canonical_rows: list[dict[str, object]],
        *,
        legacy_rows: list[dict[str, object]] | None = None,
        run_query_batches: list[list[int]] | None = None,
    ) -> None:
        self.canonical_rows = [dict(row) for row in canonical_rows]
        self.legacy_rows = [dict(row) for row in (legacy_rows or [])]
        self.run_query_batches = [list(batch) for batch in (run_query_batches or [])]
        self.run_query_calls = 0

    @property
    def delivered_legacy_ids(self) -> set[int]:
        return {int(row["id"]) for row in self.legacy_rows if row["delivered"]}

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())

        if normalized.startswith("SELECT r.evidence_seq AS id,"):
            since_evidence_seq = int(args[0])
            run_id = args[1] if len(args) >= 2 and isinstance(args[1], str) else None
            limit = next(
                (int(value) for value in reversed(args) if isinstance(value, int) and value != since_evidence_seq),
                None,
            )
            descending = "ORDER BY r.evidence_seq DESC" in normalized
            rows = self._canonical_projection_rows(
                since_evidence_seq=since_evidence_seq,
                run_id=run_id,
                descending=descending,
            )
            if run_id is not None and self.run_query_calls < len(self.run_query_batches):
                visible_ids = set(self.run_query_batches[self.run_query_calls])
                self.run_query_calls += 1
                rows = [row for row in rows if int(row["id"]) in visible_ids]
            if limit is not None:
                rows = rows[:limit]
            return [dict(row) for row in rows]

        if normalized.startswith("SELECT COUNT(*) AS c FROM receipts AS r JOIN workflow_jobs AS j"):
            since_evidence_seq = int(args[0])
            run_id = args[1] if len(args) > 1 else None
            count = len(
                self._canonical_projection_rows(
                    since_evidence_seq=since_evidence_seq,
                    run_id=run_id if isinstance(run_id, str) else None,
                )
            )
            return [{"c": count}]

        if normalized.startswith(
            "UPDATE workflow_notifications SET delivered = true WHERE id IN ("
        ):
            limit = int(args[0])
            claimed = self._undelivered_legacy_rows()[:limit]
            for row in claimed:
                row["delivered"] = True
            return [dict(row) for row in claimed]

        if normalized.startswith(
            "SELECT id, run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds, cpu_percent, mem_bytes, created_at FROM workflow_notifications WHERE delivered = false"
        ):
            limit = int(args[0]) if args else None
            rows = self._undelivered_legacy_rows()
            if limit is not None:
                rows = rows[:limit]
            return [dict(row) for row in rows]

        if normalized.startswith(
            "SELECT count(*) AS c FROM workflow_notifications WHERE delivered = false"
        ):
            return [{"c": len(self._undelivered_legacy_rows())}]

        if normalized.startswith(
            "SELECT id, run_id, job_label, spec_name, agent_slug, status, failure_code, duration_seconds, cpu_percent, mem_bytes, created_at FROM workflow_notifications WHERE run_id = $1 AND id NOT IN ( SELECT unnest($2::int[]) ) ORDER BY created_at ASC"
        ):
            run_id = str(args[0])
            excluded_ids = {int(value) for value in args[1]}
            return [
                dict(row)
                for row in self._legacy_rows_for_run(run_id)
                if int(row["id"]) not in excluded_ids
            ]

        if normalized.startswith(
            "UPDATE workflow_notifications SET delivered = true WHERE id = ANY($1::int[])"
        ):
            delivered_ids = {int(value) for value in args[0]}
            for row in self.legacy_rows:
                if int(row["id"]) in delivered_ids:
                    row["delivered"] = True
            return []

        raise AssertionError(f"Unexpected SQL: {normalized}")

    def _canonical_projection_rows(
        self,
        *,
        since_evidence_seq: int,
        run_id: str | None = None,
        descending: bool = False,
    ) -> list[dict[str, object]]:
        rows = [
            row
            for row in self.canonical_rows
            if int(row["id"]) > since_evidence_seq
            and (run_id is None or str(row["run_id"]) == run_id)
        ]
        rows = sorted(
            rows,
            key=lambda candidate: int(candidate["id"]),
            reverse=descending,
        )
        return rows

    def _legacy_rows_for_run(self, run_id: str) -> list[dict[str, object]]:
        return [
            row
            for row in sorted(self.legacy_rows, key=lambda candidate: candidate["created_at"])
            if str(row["run_id"]) == run_id
        ]

    def _undelivered_legacy_rows(self) -> list[dict[str, object]]:
        return [
            row
            for row in sorted(self.legacy_rows, key=lambda candidate: candidate["created_at"])
            if not row["delivered"]
        ]


def _canonical_notification_row(
    notification_id: int,
    *,
    run_id: str = "run-1",
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
        "cpu_percent": None,
        "mem_bytes": None,
        "created_at": created_at,
    }


def _legacy_notification_row(
    notification_id: int,
    *,
    run_id: str = "run-1",
    delivered: bool = False,
    created_at: datetime,
) -> dict[str, object]:
    return {
        "id": notification_id,
        "run_id": run_id,
        "job_label": f"legacy-{notification_id}",
        "spec_name": "model_run",
        "agent_slug": "human",
        "status": "awaiting_human",
        "failure_code": "",
        "duration_seconds": 0.0,
        "cpu_percent": None,
        "mem_bytes": None,
        "created_at": created_at,
        "delivered": delivered,
    }


def test_poll_tracks_canonical_receipts_and_drains_legacy_rows_once() -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    conn = _FakeConn(
        [
            _canonical_notification_row(11, created_at=now),
            _canonical_notification_row(12, created_at=now + timedelta(seconds=1)),
        ],
        legacy_rows=[
            _legacy_notification_row(7, created_at=now + timedelta(seconds=2)),
        ],
    )
    consumer = WorkflowNotificationConsumer(conn)

    peeked = consumer.peek(limit=10)
    first_batch = consumer.poll(limit=10)
    second_batch = consumer.poll(limit=10)

    assert [notification.id for notification in peeked] == [11, 12, -7]
    assert [notification.id for notification in first_batch] == [11, 12, -7]
    assert second_batch == []
    assert consumer.pending_count() == 0
    assert conn.delivered_legacy_ids == {7}


def test_iter_run_uses_canonical_projection_batches_and_marks_legacy_rows_delivered(monkeypatch) -> None:
    now = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    conn = _FakeConn(
        [
            _canonical_notification_row(21, created_at=now),
            _canonical_notification_row(22, created_at=now + timedelta(seconds=1)),
        ],
        legacy_rows=[
            _legacy_notification_row(5, created_at=now + timedelta(seconds=2)),
        ],
        run_query_batches=[[21], [21, 22]],
    )
    consumer = WorkflowNotificationConsumer(conn)
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    yielded = list(
        consumer.iter_run(
            "run-1",
            total_jobs=3,
            timeout_seconds=None,
            poll_interval=0,
        )
    )

    assert [notification.id for notification in yielded] == [21, -5, 22]
    assert consumer.pending_count() == 0
    assert conn.delivered_legacy_ids == {5}
