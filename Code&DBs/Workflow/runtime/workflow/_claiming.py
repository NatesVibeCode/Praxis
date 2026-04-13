"""Job claiming, completion, and stale claim recovery."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ._shared import (
    STALE_REAPER_QUERY,
    _TERMINAL_JOB_STATUSES,
    _circuit_breakers,
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
from ._context_building import _terminal_failure_classification
from ._routing import _failure_zone_lookup
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


def claim_one(conn: SyncPostgresConnection, worker_id: str) -> dict | None:
    """Claim the next ready job after route and touch-key admission checks.

    Ordering: newer runs first (by run requested_at DESC), then oldest job
    within each run (by job created_at ASC).  This prevents ancient stale
    runs from starving newer work.
    """
    candidates = conn.execute(
        """SELECT j.*
           FROM workflow_jobs j
           JOIN workflow_runs r ON r.run_id = j.run_id
           WHERE j.status = 'ready'
             AND (j.next_retry_at IS NULL OR j.next_retry_at <= now())
           ORDER BY r.requested_at DESC, j.created_at ASC
           LIMIT 50""",
    )
    for candidate in candidates or []:
        job = dict(candidate)
        if _job_has_touch_conflict(conn, job):
            continue
        resolved_agent = _select_claim_route(conn, job)
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

    failure_zone = ""
    is_transient = False
    failure_category = ""
    is_retryable: bool | None = None
    pre_classified_failure = None
    final_status = status

    if status == "failed":
        _classification = _terminal_failure_classification(
            error_code=error_code,
            stderr=stdout_preview,
            exit_code=exit_code,
        )
        if _classification is not None:
            pre_classified_failure = _classification
            failure_category = _classification.category.value
            is_retryable = _classification.is_retryable
            is_transient = _classification.is_transient
        else:
            failure_category = "unknown"
        failure_zone = _failure_zone_lookup(conn).get(failure_category, "internal")

    # Feed circuit breaker using canonical failure codes. The breaker itself
    # decides whether a failure is retryable enough to count.
    circuit_breakers = _circuit_breakers()
    if circuit_breakers:
        agent_row = conn.execute(
            "SELECT resolved_agent, agent_slug FROM workflow_jobs WHERE id = $1", job_id,
        )
        if agent_row:
            _agent = agent_row[0].get("resolved_agent") or agent_row[0].get("agent_slug") or ""
            _provider = _agent.split("/")[0] if "/" in _agent else _agent
            if _provider:
                circuit_breakers.record_outcome(
                    _provider,
                    succeeded=(status != "failed"),
                    failure_code=error_code if status == "failed" else None,
                )

        # Use retry orchestrator for intelligent retry/failover decisions
        job = conn.execute(
            "SELECT attempt, max_attempts, failover_chain, resolved_agent FROM workflow_jobs WHERE id = $1",
            job_id,
        )
        if job:
            row = job[0]
            decision = None
            if is_retryable is False:
                # is_retryable is checked here: False = immediate fail, True/None = delegate to decide() (BUG-C487AEB4 verified)
                final_status = "failed"
            else:
                from runtime.retry_orchestrator import decide

                decision = decide(
                    error_code=error_code,
                    stderr=stdout_preview,  # stdout_preview often contains stderr for failed jobs
                    attempt=row["attempt"],
                    max_attempts=row["max_attempts"],
                    failover_chain=row["failover_chain"],
                    resolved_agent=row["resolved_agent"],
                    pre_classified=pre_classified_failure,
                )

            if decision and decision.should_requeue:
                updated = conn.execute(
                    """UPDATE workflow_jobs
                       SET status = 'ready', last_error_code = $2,
                           failure_category = $3,
                           failure_zone = $4,
                           is_transient = $5,
                           resolved_agent = COALESCE($6, resolved_agent),
                           next_retry_at = now() + ($7 || ' seconds')::interval,
                           finished_at = NULL, claimed_by = NULL, claimed_at = NULL,
                           heartbeat_at = NULL
                       WHERE id = $1
                         AND status NOT IN ("""
                    + _TERMINAL_STATUS_SQL
                    + """)
                       RETURNING id""",
                    job_id, error_code, failure_category, failure_zone, is_transient,
                    decision.next_agent, str(decision.backoff_seconds),
                )
                if not updated:
                    logger.info("Job %d requeue skipped because it was already terminal", job_id)
                    return
                logger.info("Job %d %s: %s",
                            job_id, decision.action, decision.reason)
                return

            # Terminal failure — use orchestrator's decision for status
            if decision:
                final_status = "dead_letter" if decision.action == "dead_letter" else "failed"
                logger.info("Job %d terminal: %s", job_id, decision.reason)
        else:
            final_status = "failed"
    elif status == "failed":
        final_status = "failed"

    if status == "cancelled":
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
            job_id, exit_code, output_path, receipt_id,
            stdout_preview[:2000], token_input, token_output, cost_usd, duration_ms, error_code,
        )
        if not updated:
            return
        _block_descendants(conn, job_id, error_code or "workflow_cancelled")
        _recompute_workflow_run_state(conn, run_id)
        return

    if status == "succeeded":
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
            job_id, exit_code, output_path, receipt_id,
            stdout_preview[:2000], token_input, token_output, cost_usd, duration_ms,
        )
        if not updated:
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
        job_id, final_status, exit_code, output_path, receipt_id,
        stdout_preview[:2000], token_input, token_output, cost_usd, duration_ms, error_code,
        failure_category, failure_zone, is_transient,
    )
    if not updated:
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
               SET current_state = 'cancelled',
                   terminal_reason_code = 'stale_run_reaped',
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
