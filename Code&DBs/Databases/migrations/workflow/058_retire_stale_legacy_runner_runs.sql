BEGIN;

WITH legacy_candidates AS (
    SELECT
        r.run_id,
        r.workflow_id,
        COALESCE(job_counts.total_jobs, 0) AS total_jobs,
        COALESCE(job_counts.nonterminal_jobs, 0) AS nonterminal_jobs,
        COALESCE(job_counts.failed_jobs, 0) AS failed_jobs,
        COALESCE(job_counts.cancelled_jobs, 0) AS cancelled_jobs
    FROM workflow_runs r
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) AS total_jobs,
            COUNT(*) FILTER (
                WHERE status IN ('pending', 'ready', 'claimed', 'running')
            ) AS nonterminal_jobs,
            COUNT(*) FILTER (
                WHERE status IN ('failed', 'dead_letter', 'blocked')
            ) AS failed_jobs,
            COUNT(*) FILTER (
                WHERE status = 'cancelled'
            ) AS cancelled_jobs
        FROM workflow_jobs j
        WHERE j.run_id = r.run_id
    ) job_counts ON TRUE
    WHERE r.current_state IN ('queued', 'running')
      AND (
          r.run_id LIKE 'dispatch_%'
          OR r.run_id LIKE 'run:workflow.dispatch.%'
          OR r.workflow_id LIKE 'dispatch.%'
          OR r.workflow_id LIKE 'workflow.dispatch.%'
          OR r.workflow_id LIKE 'workflow.legacy.%'
      )
      AND COALESCE(job_counts.nonterminal_jobs, 0) = 0
),
retired AS (
    UPDATE workflow_runs r
    SET current_state = CASE
            WHEN c.failed_jobs > 0 THEN 'failed'
            WHEN c.total_jobs = 0 THEN 'failed'
            WHEN c.cancelled_jobs > 0 THEN 'cancelled'
            ELSE 'succeeded'
        END,
        terminal_reason_code = 'legacy_runner_retired',
        started_at = COALESCE(r.started_at, r.admitted_at, r.requested_at, now()),
        finished_at = COALESCE(r.finished_at, now())
    FROM legacy_candidates c
    WHERE r.run_id = c.run_id
    RETURNING r.run_id
)
SELECT COUNT(*) AS retired_legacy_runs FROM retired;

COMMIT;
