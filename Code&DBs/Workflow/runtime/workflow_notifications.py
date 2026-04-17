"""Workflow notification projection over canonical receipts."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from storage.postgres.receipt_repository import PostgresReceiptRepository

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkflowNotification:
    """A single workflow job completion notification."""
    id: int
    run_id: str
    job_label: str
    spec_name: str
    agent_slug: str
    status: str
    failure_code: str
    duration_seconds: float
    created_at: datetime
    cpu_percent: float | None = None
    mem_bytes: int | None = None

    def to_dict(self) -> dict:
        return {
            "job_label": self.job_label,
            "spec_name": self.spec_name,
            "agent_slug": self.agent_slug,
            "status": self.status,
            "failure_code": self.failure_code,
            "duration_seconds": round(self.duration_seconds, 1),
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "cpu_percent": self.cpu_percent,
            "mem_bytes": self.mem_bytes,
        }

    def summary(self) -> str:
        icon = "+" if self.status == "succeeded" else "x"
        msg = f"[{icon}] {self.job_label} ({self.agent_slug}) — {self.status}"
        if self.failure_code:
            msg += f" [{self.failure_code}]"
        if self.duration_seconds > 0:
            msg += f" ({self.duration_seconds:.0f}s)"
        return msg


class WorkflowNotificationConsumer:
    """Reads canonical workflow notifications from the receipt projection only."""

    def __init__(self, conn: SyncPostgresConnection) -> None:
        self._receipt_repository = PostgresReceiptRepository(conn)
        self._last_seen_evidence_seq = 0

    def _canonical_notifications(
        self,
        *,
        limit: int | None,
        run_id: str | None = None,
        descending: bool = False,
    ) -> list[WorkflowNotification]:
        rows = self._receipt_repository.list_workflow_notification_projection(
            since_evidence_seq=self._last_seen_evidence_seq,
            limit=limit,
            run_id=run_id,
            descending=descending,
        )
        notifications = [
            WorkflowNotification(
                id=int(row["id"]),
                run_id=str(row["run_id"] or ""),
                job_label=str(row["job_label"] or ""),
                spec_name=str(row["spec_name"] or ""),
                agent_slug=str(row["agent_slug"] or ""),
                status=str(row["status"] or ""),
                failure_code=str(row["failure_code"] or ""),
                duration_seconds=float(row["duration_seconds"] or 0),
                created_at=row["created_at"],
                cpu_percent=row["cpu_percent"],
                mem_bytes=row["mem_bytes"],
            )
            for row in rows
        ]
        return notifications

    def _advance_canonical_cursor(self, notifications: list[WorkflowNotification]) -> None:
        if notifications:
            self._last_seen_evidence_seq = max(
                self._last_seen_evidence_seq,
                max(notification.id for notification in notifications if notification.id > 0),
            )

    def poll(self, limit: int = 50) -> list[WorkflowNotification]:
        """Read new canonical notifications."""
        canonical = self._canonical_notifications(limit=limit)
        self._advance_canonical_cursor(canonical)
        return canonical

    def peek(self, limit: int = 50) -> list[WorkflowNotification]:
        """Read notifications without mutating the local cursor."""
        return self._canonical_notifications(limit=limit)

    def recent(self, limit: int | None = 50) -> list[WorkflowNotification]:
        """Return the most recent notifications for operator inspection."""
        canonical = self._receipt_repository.list_workflow_notification_projection(
            since_evidence_seq=0,
            limit=limit,
            descending=True,
        )
        notifications = [
            WorkflowNotification(
                id=int(row["id"]),
                run_id=str(row["run_id"] or ""),
                job_label=str(row["job_label"] or ""),
                spec_name=str(row["spec_name"] or ""),
                agent_slug=str(row["agent_slug"] or ""),
                status=str(row["status"] or ""),
                failure_code=str(row["failure_code"] or ""),
                duration_seconds=float(row["duration_seconds"] or 0),
                created_at=row["created_at"],
                cpu_percent=row["cpu_percent"],
                mem_bytes=row["mem_bytes"],
            )
            for row in canonical
        ]
        notifications.sort(key=lambda item: (item.created_at, item.id), reverse=True)
        if limit is not None:
            notifications = notifications[:limit]
        notifications.sort(key=lambda item: (item.created_at, item.id))
        return notifications

    def pending_count(self) -> int:
        """Count of unread canonical notifications."""
        return self._receipt_repository.count_workflow_notification_projection(
            since_evidence_seq=self._last_seen_evidence_seq,
        )

    def wait_for_run(
        self,
        run_id: str,
        total_jobs: int,
        timeout_seconds: float | None = 600,
        poll_interval: float = 3.0,
    ) -> list[WorkflowNotification]:
        """Block until all jobs for a run_id are observed by polling.

        If timeout_seconds is None, block indefinitely.

        Reads canonical receipt-backed notifications for the given run_id.
        Returns all notifications once total_jobs are collected, or whatever
        we have at timeout.
        """
        use_timeout = timeout_seconds is not None
        deadline = time.monotonic() + timeout_seconds if use_timeout else None
        collected: list[WorkflowNotification] = []
        seen_canonical_ids: set[int] = set()
        run_cursor = self._last_seen_evidence_seq

        while not use_timeout or time.monotonic() < (deadline or 0):
            rows = self._receipt_repository.list_workflow_notification_projection(
                since_evidence_seq=run_cursor,
                run_id=run_id,
            )
            for row in rows:
                nid = int(row["id"])
                if nid in seen_canonical_ids:
                    continue
                seen_canonical_ids.add(nid)
                run_cursor = max(run_cursor, nid)
                collected.append(
                    WorkflowNotification(
                        id=nid,
                        run_id=str(row["run_id"] or ""),
                        job_label=str(row["job_label"] or ""),
                        spec_name=str(row["spec_name"] or ""),
                        agent_slug=str(row["agent_slug"] or ""),
                        status=str(row["status"] or ""),
                        failure_code=str(row["failure_code"] or ""),
                        duration_seconds=float(row["duration_seconds"] or 0),
                        created_at=row["created_at"],
                        cpu_percent=row["cpu_percent"],
                        mem_bytes=row["mem_bytes"],
                    )
                )

            if len(collected) >= total_jobs:
                break

            time.sleep(poll_interval)

        self._last_seen_evidence_seq = max(self._last_seen_evidence_seq, run_cursor)
        return collected

    def iter_run(
        self,
        run_id: str,
        total_jobs: int,
        timeout_seconds: float | None = 600,
        poll_interval: float = 2.0,
        wakeup_event: "threading.Event | None" = None,
    ):
        """Yield notifications for a run_id as they are found.

        Like wait_for_run but yields each notification incrementally,
        enabling callers to emit progress in real time.
        If timeout_seconds is None, stream until all notifications are seen.

        wakeup_event: optional threading.Event that is set by a pg_notify
        LISTEN thread to wake this loop early instead of sleeping the full
        poll_interval.
        """
        use_timeout = timeout_seconds is not None
        deadline = time.monotonic() + timeout_seconds if use_timeout else None
        seen_canonical_ids: set[int] = set()
        run_cursor = self._last_seen_evidence_seq
        count = 0

        while count < total_jobs and (not use_timeout or time.monotonic() < (deadline or 0)):
            rows = self._receipt_repository.list_workflow_notification_projection(
                since_evidence_seq=run_cursor,
                run_id=run_id,
            )
            for row in rows:
                nid = int(row["id"])
                if nid in seen_canonical_ids:
                    continue
                seen_canonical_ids.add(nid)
                run_cursor = max(run_cursor, nid)
                count += 1
                yield WorkflowNotification(
                    id=nid,
                    run_id=str(row["run_id"] or ""),
                    job_label=str(row["job_label"] or ""),
                    spec_name=str(row["spec_name"] or ""),
                    agent_slug=str(row["agent_slug"] or ""),
                    status=str(row["status"] or ""),
                    failure_code=str(row["failure_code"] or ""),
                    duration_seconds=float(row["duration_seconds"] or 0),
                    created_at=row["created_at"],
                    cpu_percent=row["cpu_percent"],
                    mem_bytes=row["mem_bytes"],
                )

            if count < total_jobs:
                if wakeup_event is not None:
                    wakeup_event.wait(timeout=poll_interval)
                    wakeup_event.clear()
                else:
                    time.sleep(poll_interval)

        self._last_seen_evidence_seq = max(self._last_seen_evidence_seq, run_cursor)

    def format_batch(self, notifications: list[WorkflowNotification]) -> str:
        """Format a batch of notifications as a concise status block."""
        if not notifications:
            return ""

        # Group by spec
        by_spec: dict[str, list[WorkflowNotification]] = {}
        for n in notifications:
            by_spec.setdefault(n.spec_name, []).append(n)

        lines = []
        for spec, notifs in by_spec.items():
            passed = sum(1 for n in notifs if n.status == "succeeded")
            failed = sum(1 for n in notifs if n.status in ("failed", "error"))
            total = len(notifs)
            header = f"[workflow] {spec}: {passed}/{total} passed"
            if failed:
                header += f", {failed} failed"
            lines.append(header)
            for n in notifs:
                lines.append(f"  {n.summary()}")

        return "\n".join(lines)


class WorkflowRunWakeupListener:
    """Background LISTEN/NOTIFY consumer that wakes run observers quickly."""

    def __init__(
        self,
        *,
        database_url: str,
        run_id: str,
        wakeup_event: threading.Event,
        channels: tuple[str, ...] = ("job_completed", "run_complete"),
        reconnect_delay: float = 2.0,
    ) -> None:
        self._database_url = database_url
        self._run_id = run_id
        self._channels = channels
        self._wakeup_event = wakeup_event
        self._reconnect_delay = reconnect_delay
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="workflow-run-wakeup-listener",
        )

    def start(self) -> None:
        import asyncpg  # noqa: F401

        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        self._thread.join(timeout=5)

    def _on_notify(self, _connection, _pid, channel: str, payload: str) -> None:
        if payload and payload != self._run_id:
            return
        logger.debug("Workflow run notification received on %s: %s", channel, payload)
        self._wakeup_event.set()

    def _run(self) -> None:
        import asyncio
        import asyncpg

        async def _listen() -> None:
            while not self._stop_event.is_set():
                conn = None
                try:
                    conn = await asyncpg.connect(self._database_url, timeout=5.0)
                    for channel in self._channels:
                        await conn.add_listener(channel, self._on_notify)
                    while not self._stop_event.is_set():
                        await asyncio.sleep(1.0)
                except Exception as exc:
                    if not self._stop_event.is_set():
                        logger.debug("Workflow LISTEN loop error, reconnecting: %s", exc)
                        await asyncio.sleep(self._reconnect_delay)
                finally:
                    if conn is not None:
                        await conn.close()

        asyncio.run(_listen())


def start_run_wakeup_listener(
    *,
    run_id: str,
    wakeup_event: threading.Event,
    database_url: str | None = None,
    channels: tuple[str, ...] = ("job_completed", "run_complete"),
) -> WorkflowRunWakeupListener | None:
    """Start a best-effort LISTEN helper for one run."""
    from storage.postgres.connection import (
        PostgresConfigurationError,
        resolve_workflow_database_url,
    )

    try:
        resolved_database_url = str(database_url or resolve_workflow_database_url()).strip()
    except (PostgresConfigurationError, RuntimeError):
        return None
    if not resolved_database_url:
        return None
    try:
        listener = WorkflowRunWakeupListener(
            database_url=resolved_database_url,
            run_id=run_id,
            wakeup_event=wakeup_event,
            channels=channels,
        )
        listener.start()
        return listener
    except Exception as exc:
        logger.debug("Workflow run wakeup unavailable for %s: %s", run_id, exc)
        return None
