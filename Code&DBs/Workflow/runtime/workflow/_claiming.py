"""Job claiming, completion, and stale claim recovery."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from registry.runtime_profile_admission import RuntimeProfileAdmissionError

from ._shared import (
    STALE_REAPER_QUERY,
    _TERMINAL_JOB_STATUSES,
)
from ._routing import (
    _job_has_touch_conflict,
    _record_task_route_outcome,
    _select_claim_route,
)
from ._workflow_state import (
    _block_descendants,
    _recompute_workflow_run_state,
    _release_ready_children,
)
from ._retry_manager import record_provider_outcome, resolve_failed_job
from runtime.workflow.submission_capture import (
    list_latest_submission_summaries_for_run as _submission_list_latest_submission_summaries_for_run,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

_TERMINAL_STATUS_SQL = ", ".join(f"'{status}'" for status in sorted(_TERMINAL_JOB_STATUSES))

__all__ = [
    "claim_one",
    "mark_running",
    "complete_job",
    "reap_stale_claims",
    "reap_stale_runs",
    "_submission_state_by_job_label",
]


def _fail_unclaimable_ready_job(
    conn: SyncPostgresConnection,
    job: dict,
    *,
    error_code: str,
    stdout_preview: str,
    failure_category: str = "authority_missing",
    failure_zone: str = "routing",
) -> None:
    rows = conn.execute(
        """UPDATE workflow_jobs
           SET status = 'failed',
               finished_at = now(),
               last_error_code = $2,
               stdout_preview = $3,
               failure_category = $4,
               failure_zone = $5,
               is_transient = false
           WHERE id = $1
             AND status = 'ready'
           RETURNING id, run_id, route_task_type, COALESCE(resolved_agent, agent_slug) AS effective_agent""",
        int(job["id"]),
        error_code,
        stdout_preview[:2000],
        failure_category,
        failure_zone,
    )
    if not rows:
        return
    row = rows[0]
    _record_task_route_outcome(
        conn,
        task_type=str(row.get("route_task_type") or "").strip(),
        effective_agent=str(row.get("effective_agent") or "").strip(),
        succeeded=False,
        failure_code=error_code,
        failure_category=failure_category,
        failure_zone=failure_zone,
    )
    _block_descendants(conn, int(row["id"]), error_code)
    _recompute_workflow_run_state(conn, str(row["run_id"]))


def _update_cancelled_job(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
    exit_code: int | None,
    output_path: str,
    receipt_id: str,
    stdout_preview: str,
    token_input: int,
    token_output: int,
    cost_usd: float,
    duration_ms: int,
    error_code: str,
) -> bool:
    updated = conn.execute(
        """UPDATE workflow_jobs
           SET status = 'cancelled', finished_at = now(), exit_code = $2, output_path = $3,
               receipt_id = $4, stdout_preview = $5, token_input = $6, token_output = $7,
               cost_usd = $8, duration_ms = $9, last_error_code = $10,
               failure_category = '', failure_zone = '', is_transient = false
           WHERE id = $1
             AND status NOT IN ("""
        + _TERMINAL_STATUS_SQL
        + """)
           RETURNING id""",
        job_id,
        exit_code,
        output_path,
        receipt_id,
        stdout_preview[:2000],
        token_input,
        token_output,
        cost_usd,
        duration_ms,
        error_code,
    )
    return bool(updated)


def _update_succeeded_job(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
    exit_code: int | None,
    output_path: str,
    receipt_id: str,
    stdout_preview: str,
    token_input: int,
    token_output: int,
    cost_usd: float,
    duration_ms: int,
) -> bool:
    updated = conn.execute(
        """UPDATE workflow_jobs
           SET status = 'succeeded', finished_at = now(), exit_code = $2, output_path = $3,
               receipt_id = $4, stdout_preview = $5, token_input = $6, token_output = $7,
               cost_usd = $8, duration_ms = $9
           WHERE id = $1
             AND status NOT IN ("""
        + _TERMINAL_STATUS_SQL
        + """)
           RETURNING id""",
        job_id,
        exit_code,
        output_path,
        receipt_id,
        stdout_preview[:2000],
        token_input,
        token_output,
        cost_usd,
        duration_ms,
    )
    return bool(updated)


def _update_terminal_job(
    conn: SyncPostgresConnection,
    *,
    job_id: int,
    status: str,
    exit_code: int | None,
    output_path: str,
    receipt_id: str,
    stdout_preview: str,
    token_input: int,
    token_output: int,
    cost_usd: float,
    duration_ms: int,
    error_code: str,
    failure_category: str,
    failure_zone: str,
    is_transient: bool,
) -> bool:
    updated = conn.execute(
        """UPDATE workflow_jobs
           SET status = $2, finished_at = now(), exit_code = $3, output_path = $4,
               receipt_id = $5, stdout_preview = $6, token_input = $7, token_output = $8,
               cost_usd = $9, duration_ms = $10, last_error_code = $11,
               failure_category = $12, failure_zone = $13, is_transient = $14
           WHERE id = $1
             AND status NOT IN ("""
        + _TERMINAL_STATUS_SQL
        + """)
           RETURNING id""",
        job_id,
        status,
        exit_code,
        output_path,
        receipt_id,
        stdout_preview[:2000],
        token_input,
        token_output,
        cost_usd,
        duration_ms,
        error_code,
        failure_category,
        failure_zone,
        is_transient,
    )
    return bool(updated)


_STALE_READY_PROMOTION_INTERVAL_SECONDS = 600


def claim_one(conn: SyncPostgresConnection, worker_id: str) -> dict | None:
    """Claim the next ready job after route and touch-key admission checks.

    Ordering is tiered:
      Tier 0 (promoted): retries (attempt > 0) OR jobs ready longer than
        _STALE_READY_PROMOTION_INTERVAL_SECONDS. Keeps in-flight work
        moving and prevents cross-run starvation.
      Tier 1 (normal): newer runs first, then oldest job within the run.
    """
    candidates = conn.execute(
        """SELECT j.*
           FROM workflow_jobs j
           JOIN workflow_runs r ON r.run_id = j.run_id
           WHERE j.status = 'ready'
             AND (j.next_retry_at IS NULL OR j.next_retry_at <= now())
           ORDER BY
               CASE
                   WHEN j.attempt > 0 THEN 0
                   WHEN COALESCE(j.ready_at, j.created_at) < now() - ($1 || ' seconds')::interval THEN 0
                   ELSE 1
               END ASC,
               r.requested_at DESC,
               j.created_at ASC
           LIMIT 50""",
        str(_STALE_READY_PROMOTION_INTERVAL_SECONDS),
    )
    for candidate in candidates or []:
        job = dict(candidate)
        if _job_has_touch_conflict(conn, job):
            continue
        try:
            resolved_agent = _select_claim_route(conn, job)
        except RuntimeProfileAdmissionError as exc:
            logger.error(
                "Claim rejected for job %s (%s): %s",
                job.get("id"),
                job.get("label"),
                exc,
            )
            _fail_unclaimable_ready_job(
                conn,
                job,
                error_code=exc.reason_code,
                stdout_preview=str(exc),
            )
            continue
        rows = conn.execute(
            """UPDATE workflow_jobs
               SET status = 'claimed',
                   claimed_at = now(),
                   heartbeat_at = now(),
                   claimed_by = $2,
                   resolved_agent = $3,
                   attempt = attempt + 1
               WHERE id = $1 AND status = 'ready'
               RETURNING *""",
            job["id"],
            worker_id,
            resolved_agent,
        )
        if rows:
            _recompute_workflow_run_state(conn, str(rows[0]["run_id"]))
            return dict(rows[0])
    return None


def mark_running(conn: SyncPostgresConnection, job_id: int) -> None:
    conn.execute(
        "UPDATE workflow_jobs SET status = 'running', started_at = now() WHERE id = $1",
        job_id,
    )
    run_row = conn.execute("SELECT run_id FROM workflow_jobs WHERE id = $1", job_id)
    if run_row:
        _recompute_workflow_run_state(conn, str(run_row[0]["run_id"]))


def complete_job(
    conn: SyncPostgresConnection,
    job_id: int,
    *,
    status: str = "succeeded",
    exit_code: int | None = None,
    output_path: str = "",
    receipt_id: str = "",
    stdout_preview: str = "",
    token_input: int = 0,
    token_output: int = 0,
    cost_usd: float = 0.0,
    duration_ms: int = 0,
    error_code: str = "",
) -> None:
    """Mark a job terminal, release or block descendants, then recompute workflow state."""
    current = conn.execute(
        """SELECT status, run_id, route_task_type,
                  COALESCE(resolved_agent, agent_slug) AS effective_agent
           FROM workflow_jobs
           WHERE id = $1""",
        job_id,
    )
    if not current:
        return
    current_status = current[0].get("status")
    run_id = str(current[0].get("run_id"))
    route_task_type = str(current[0].get("route_task_type") or "").strip()
    effective_agent = str(current[0].get("effective_agent") or "").strip()
    if current_status in _TERMINAL_JOB_STATUSES:
        return

    final_status = status
    failure_category = ""
    failure_zone = ""
    is_transient = False

    record_provider_outcome(conn, job_id=job_id, succeeded=(status != "failed"), error_code=error_code)

    if status == "failed":
        outcome = resolve_failed_job(
            conn, job_id=job_id, error_code=error_code,
            stdout_preview=stdout_preview, exit_code=exit_code,
        )
        failure_category = outcome.failure_category
        failure_zone = outcome.failure_zone
        is_transient = outcome.is_transient

        if outcome.requeue:
            updated = conn.execute(
                """UPDATE workflow_jobs
                   SET status = 'ready', last_error_code = $2,
                       failure_category = $3, failure_zone = $4, is_transient = $5,
                       resolved_agent = COALESCE($6, resolved_agent),
                       next_retry_at = now() + ($7 || ' seconds')::interval,
                       finished_at = NULL, claimed_by = NULL, claimed_at = NULL,
                       heartbeat_at = NULL
                   WHERE id = $1
                     AND status NOT IN ("""
                + _TERMINAL_STATUS_SQL
                + """)
                   RETURNING id""",
                job_id,
                error_code,
                failure_category,
                failure_zone,
                is_transient,
                outcome.next_agent,
                str(outcome.backoff_seconds),
            )
            if updated:
                conn.execute("SELECT pg_notify('job_ready', $1)", str(job_id))
                logger.info("Job %d requeued: %s", job_id, outcome.reason)
            else:
                logger.info("Job %d requeue skipped (already terminal)", job_id)
            return

        final_status = outcome.final_status
        logger.info("Job %d terminal: %s", job_id, outcome.reason)

    if status == "cancelled":
        if not _update_cancelled_job(
            conn,
            job_id=job_id,
            exit_code=exit_code,
            output_path=output_path,
            receipt_id=receipt_id,
            stdout_preview=stdout_preview,
            token_input=token_input,
            token_output=token_output,
            cost_usd=cost_usd,
            duration_ms=duration_ms,
            error_code=error_code,
        ):
            return
        _block_descendants(conn, job_id, error_code or "workflow_cancelled")
        _recompute_workflow_run_state(conn, run_id)
        return

    if status == "succeeded":
        if not _update_succeeded_job(
            conn,
            job_id=job_id,
            exit_code=exit_code,
            output_path=output_path,
            receipt_id=receipt_id,
            stdout_preview=stdout_preview,
            token_input=token_input,
            token_output=token_output,
            cost_usd=cost_usd,
            duration_ms=duration_ms,
        ):
            return
        _record_task_route_outcome(
            conn,
            task_type=route_task_type,
            effective_agent=effective_agent,
            succeeded=True,
        )
        _release_ready_children(conn, job_id)
        _recompute_workflow_run_state(conn, run_id)
        return

    if not _update_terminal_job(
        conn,
        job_id=job_id,
        status=final_status,
        exit_code=exit_code,
        output_path=output_path,
        receipt_id=receipt_id,
        stdout_preview=stdout_preview,
        token_input=token_input,
        token_output=token_output,
        cost_usd=cost_usd,
        duration_ms=duration_ms,
        error_code=error_code,
        failure_category=failure_category,
        failure_zone=failure_zone,
        is_transient=is_transient,
    ):
        return
    _record_task_route_outcome(
        conn,
        task_type=route_task_type,
        effective_agent=effective_agent,
        succeeded=False,
        failure_code=error_code or None,
        failure_category=failure_category,
        failure_zone=failure_zone,
    )
    _block_descendants(conn, job_id, error_code or final_status)
    _recompute_workflow_run_state(conn, run_id)


def reap_stale_claims(conn: SyncPostgresConnection) -> int:
    """Reset jobs that were claimed but never completed (worker crash recovery)."""
    rows = conn.execute(STALE_REAPER_QUERY)
    if rows:
        touched_run_ids: set[str] = set()
        for r in rows:
            logger.warning("Reaped stale job %d (%s) in run %s", r["id"], r["label"], r["run_id"])
            run_id = str(r.get("run_id") or "").strip()
            if run_id:
                touched_run_ids.add(run_id)
        for run_id in sorted(touched_run_ids):
            _recompute_workflow_run_state(conn, run_id)
    return len(rows) if rows else 0


# Runs idle for longer than this with no active jobs are cancelled automatically.
_STALE_RUN_IDLE_HOURS = 2


def reap_stale_runs(conn: SyncPostgresConnection) -> int:
    """Cancel workflow runs that have been queued or running with no job activity
    for more than _STALE_RUN_IDLE_HOURS hours.

    Targets two cases:
    - Runs stuck in 'queued' with no ready/pending jobs (e.g. after a partial cancel)
    - Runs stuck in 'running' where all jobs finished but run state never updated
    """
    stale = conn.execute(
        """SELECT r.run_id
           FROM workflow_runs r
           WHERE r.current_state IN ('queued', 'running')
             AND r.requested_at < now() - ($1 || ' hours')::interval
             AND NOT EXISTS (
               SELECT 1 FROM workflow_jobs j
               WHERE j.run_id = r.run_id
                 AND j.status IN ('claimed', 'running')
             )
             AND NOT EXISTS (
               SELECT 1 FROM workflow_jobs j
               WHERE j.run_id = r.run_id
                 AND COALESCE(j.heartbeat_at, j.claimed_at) > now() - interval '10 minutes'
             )""",
        str(_STALE_RUN_IDLE_HOURS),
    )
    if not stale:
        return 0

    count = 0
    for row in stale:
        run_id = str(row["run_id"])
        # Cancel any non-terminal jobs
        conn.execute(
            """UPDATE workflow_jobs
               SET status = 'cancelled',
                   finished_at = COALESCE(finished_at, now()),
                   claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL,
                   failure_category = COALESCE(NULLIF(failure_category, ''), 'stale_run_reaped'),
                   failure_zone = COALESCE(NULLIF(failure_zone, ''), 'internal')
               WHERE run_id = $1
                 AND status NOT IN ('succeeded', 'failed', 'dead_letter', 'cancelled')""",
            run_id,
        )
        conn.execute(
            """UPDATE workflow_runs
               SET current_state = CASE
                       WHEN current_state NOT IN ('succeeded', 'failed', 'dead_letter', 'cancelled')
                       THEN 'cancelled'
                       ELSE current_state
                   END,
                   terminal_reason_code = COALESCE(terminal_reason_code, 'stale_run_reaped'),
                   started_at = COALESCE(started_at, admitted_at, requested_at),
                   finished_at = GREATEST(COALESCE(started_at, admitted_at, requested_at), now())
               WHERE run_id = $1
                 AND current_state NOT IN ('succeeded', 'failed', 'dead_letter', 'cancelled')""",
            run_id,
        )
        logger.warning("Reaped stale run %s (idle >%dh)", run_id, _STALE_RUN_IDLE_HOURS)
        count += 1

    return count


def _submission_state_by_job_label(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
) -> dict[str, dict[str, object]]:
    try:
        return _submission_list_latest_submission_summaries_for_run(conn, run_id=run_id)
    except Exception:
        return {}
