"""Single authority for workflow queue admission and queue-depth status."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

DEFAULT_QUEUE_WARNING_THRESHOLD = 500
DEFAULT_QUEUE_CRITICAL_THRESHOLD = 1000

QUEUE_DEPTH_BREAKDOWN_QUERY = """
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'ready') AS ready,
    COUNT(*) FILTER (WHERE status = 'claimed') AS claimed,
    COUNT(*) FILTER (WHERE status = 'running') AS running
FROM workflow_jobs
WHERE status IN ('pending', 'ready', 'claimed', 'running')
"""

QUEUE_DEPTH_TOTAL_QUERY = "SELECT COUNT(*) FROM workflow_jobs WHERE status IN ('pending', 'ready')"


def _resolve_database_url(database_url: str | None) -> str | None:
    if database_url:
        try:
            return resolve_workflow_database_url(env={"WORKFLOW_DATABASE_URL": database_url})
        except PostgresConfigurationError:
            return None
    try:
        return resolve_workflow_database_url()
    except PostgresConfigurationError:
        return None


def _psycopg2_module():
    import psycopg2

    return psycopg2


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _mapping_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    try:
        keys = row.keys()
    except Exception:
        return None
    try:
        if key in keys:
            return row[key]
    except Exception:
        return None
    return None


def queue_utilization_pct(total_queued: int, critical_threshold: int) -> float:
    if critical_threshold <= 0:
        return 999.9 if total_queued > 0 else 0.0
    return min(round(total_queued / critical_threshold * 100, 1), 999.9)


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    admitted: bool
    reason: str
    queue_depth: int
    utilization_pct: float


@dataclass(frozen=True, slots=True)
class QueueDepthSnapshot:
    pending: int
    ready: int
    claimed: int
    running: int
    warning_threshold: int = DEFAULT_QUEUE_WARNING_THRESHOLD
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD

    @property
    def total_queued(self) -> int:
        return self.pending + self.ready

    @property
    def utilization_pct(self) -> float:
        return queue_utilization_pct(self.total_queued, self.critical_threshold)

    @property
    def status(self) -> str:
        if self.total_queued >= self.critical_threshold:
            return "critical"
        if self.total_queued >= self.warning_threshold:
            return "warning"
        return "ok"

    @property
    def passed(self) -> bool:
        return self.status != "critical"

    def details(self) -> dict[str, int | float]:
        return {
            "pending": self.pending,
            "ready": self.ready,
            "claimed": self.claimed,
            "running": self.running,
            "total_queued": self.total_queued,
            "warning_threshold": self.warning_threshold,
            "critical_threshold": self.critical_threshold,
            "utilization_pct": self.utilization_pct,
        }


def queue_depth_snapshot_from_row(
    row: Any,
    *,
    warning_threshold: int = DEFAULT_QUEUE_WARNING_THRESHOLD,
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
) -> QueueDepthSnapshot:
    pending = _int_or_zero(_mapping_value(row, "pending"))
    ready = _int_or_zero(_mapping_value(row, "ready"))
    claimed = _int_or_zero(_mapping_value(row, "claimed"))
    running = _int_or_zero(_mapping_value(row, "running"))
    return QueueDepthSnapshot(
        pending=pending,
        ready=ready,
        claimed=claimed,
        running=running,
        warning_threshold=warning_threshold,
        critical_threshold=critical_threshold,
    )


def query_queue_depth_snapshot(
    conn: SyncPostgresConnection,
    *,
    warning_threshold: int = DEFAULT_QUEUE_WARNING_THRESHOLD,
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
) -> QueueDepthSnapshot:
    rows = conn.execute(QUEUE_DEPTH_BREAKDOWN_QUERY)
    row = rows[0] if rows else {}
    return queue_depth_snapshot_from_row(
        row,
        warning_threshold=warning_threshold,
        critical_threshold=critical_threshold,
    )


def query_queue_depth(conn: SyncPostgresConnection) -> int:
    rows = conn.execute(QUEUE_DEPTH_TOTAL_QUERY)
    if not rows:
        return 0
    row = rows[0]
    count = _mapping_value(row, "count")
    if count is None:
        count = _mapping_value(row, "?column?")
    if count is not None:
        return _int_or_zero(count)
    if isinstance(row, (tuple, list)):
        return _int_or_zero(row[0] if row else 0)
    return _int_or_zero(row)


def evaluate_queue_admission(
    queue_depth: int,
    *,
    job_count: int = 1,
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
) -> AdmissionDecision:
    queue_depth = max(0, _int_or_zero(queue_depth))
    admitted_jobs = max(0, _int_or_zero(job_count))
    projected_depth = queue_depth + admitted_jobs
    utilization_pct = queue_utilization_pct(queue_depth, critical_threshold)
    if queue_depth >= critical_threshold:
        return AdmissionDecision(
            admitted=False,
            reason=(
                f"queue depth {queue_depth} is at or above critical threshold "
                f"{critical_threshold}"
            ),
            queue_depth=queue_depth,
            utilization_pct=utilization_pct,
        )
    if projected_depth > critical_threshold:
        return AdmissionDecision(
            admitted=False,
            reason=(
                f"queue depth {queue_depth} + {admitted_jobs} new jobs "
                f"would exceed critical threshold {critical_threshold}"
            ),
            queue_depth=queue_depth,
            utilization_pct=utilization_pct,
        )
    return AdmissionDecision(
        admitted=True,
        reason=f"queue depth {queue_depth} within critical threshold {critical_threshold}",
        queue_depth=queue_depth,
        utilization_pct=utilization_pct,
    )


class QueueAdmissionGate:
    """Shared queue gate used by runtime submission and compatibility callers."""

    def __init__(
        self,
        database_url: str | None = None,
        critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._database_url = database_url
        self._critical_threshold = critical_threshold
        self._timeout_seconds = timeout_seconds

    def check_connection(
        self,
        conn: SyncPostgresConnection,
        *,
        job_count: int = 1,
    ) -> AdmissionDecision:
        return evaluate_queue_admission(
            query_queue_depth(conn),
            job_count=job_count,
            critical_threshold=self._critical_threshold,
        )

    def check(self, job_count: int = 1) -> AdmissionDecision:
        database_url = str(_resolve_database_url(self._database_url) or "").strip()
        if not database_url:
            raise RuntimeError("QueueAdmissionGate requires explicit WORKFLOW_DATABASE_URL")

        psycopg2 = _psycopg2_module()
        connect_timeout = max(1, math.ceil(self._timeout_seconds))
        statement_timeout_ms = max(1, int(self._timeout_seconds * 1000))
        try:
            with psycopg2.connect(
                database_url,
                connect_timeout=connect_timeout,
                options=f"-c statement_timeout={statement_timeout_ms}",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(QUEUE_DEPTH_TOTAL_QUERY)
                    row = cur.fetchone()
        except Exception as exc:
            return AdmissionDecision(
                admitted=False,
                reason=f"queue admission check failed: {exc}",
                queue_depth=-1,
                utilization_pct=0.0,
            )
        queue_depth = _int_or_zero(row[0] if isinstance(row, (tuple, list)) and row else row)
        return evaluate_queue_admission(
            queue_depth,
            job_count=job_count,
            critical_threshold=self._critical_threshold,
        )


def queue_admission_check(
    job_count: int = 1,
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
) -> AdmissionDecision:
    gate = QueueAdmissionGate(critical_threshold=critical_threshold)
    return gate.check(job_count)


__all__ = [
    "AdmissionDecision",
    "DEFAULT_QUEUE_CRITICAL_THRESHOLD",
    "DEFAULT_QUEUE_WARNING_THRESHOLD",
    "QUEUE_DEPTH_BREAKDOWN_QUERY",
    "QUEUE_DEPTH_TOTAL_QUERY",
    "QueueAdmissionGate",
    "QueueDepthSnapshot",
    "evaluate_queue_admission",
    "query_queue_depth",
    "query_queue_depth_snapshot",
    "queue_admission_check",
    "queue_depth_snapshot_from_row",
    "queue_utilization_pct",
]
