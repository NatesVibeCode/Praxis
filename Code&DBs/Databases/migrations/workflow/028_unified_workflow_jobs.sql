-- Migration 028: Unified workflow_jobs table
--
-- Consolidates three separate queue systems into one:
--   1. job_queue (queue_worker path) — separate connections, separate claim semantics
--   2. workflow_runs + workflow_events (dispatch_worker path) — run-level only, no per-job state
--   3. run_nodes + run_edges (operating model cards) — card-level, different status model
--
-- After this migration, all dispatchable work is a row in workflow_jobs.
-- One worker loop, one claim query, one status model.

BEGIN;

-- ============================================================
-- 1. Dispatch runs (lightweight grouping for a spec submission)
-- ============================================================
CREATE TABLE IF NOT EXISTS dispatch_runs (
    run_id          TEXT PRIMARY KEY,
    spec_name       TEXT NOT NULL,
    spec_snapshot   JSONB,                          -- full spec at dispatch time (audit)
    phase           TEXT NOT NULL DEFAULT 'build',
    total_jobs      INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending, running, succeeded, failed, cancelled
    outcome_goal    TEXT DEFAULT '',
    output_dir      TEXT DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    terminal_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_dispatch_runs_status ON dispatch_runs (status) WHERE status IN ('pending', 'running');

-- ============================================================
-- 2. Workflow jobs (the unified per-job table)
-- ============================================================
CREATE TABLE IF NOT EXISTS workflow_jobs (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES dispatch_runs(run_id) ON DELETE CASCADE,

    -- Identity
    label           TEXT NOT NULL,
    job_type        TEXT NOT NULL DEFAULT 'dispatch',  -- dispatch, card, scheduled
    phase           TEXT NOT NULL DEFAULT 'build',

    -- Execution target
    agent_slug      TEXT NOT NULL,                     -- "auto/debate", "anthropic/claude-opus-4-6"
    resolved_agent  TEXT,                              -- filled on claim after routing
    prompt          TEXT,
    prompt_hash     TEXT,                              -- SHA256 for dedup/idempotency

    -- Lifecycle
    status          TEXT NOT NULL DEFAULT 'pending',
                    -- pending → ready → claimed → running → succeeded / failed / dead_letter
                    -- retrying (back to ready after backoff)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ready_at        TIMESTAMPTZ,
    claimed_at      TIMESTAMPTZ,
    claimed_by      TEXT,                              -- worker_id (hostname:pid)
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,

    -- Retry / failover
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    last_error_code TEXT,
    failover_chain  TEXT[],                            -- ["openai/gpt-5.4", "anthropic/claude-opus-4-6", ...]
    next_retry_at   TIMESTAMPTZ,

    -- Idempotency (prevents duplicate side effects on retry)
    idempotency_key TEXT,                              -- spec_name:label:prompt_hash

    -- Results
    exit_code       INT,
    output_path     TEXT,
    receipt_id      TEXT,
    stdout_preview  TEXT,                              -- first 2000 chars for quick inspection
    token_input     INT DEFAULT 0,
    token_output    INT DEFAULT 0,
    cost_usd        NUMERIC(10,6) DEFAULT 0,
    duration_ms     INT DEFAULT 0,

    -- Constraints
    CONSTRAINT uq_workflow_jobs_run_label UNIQUE (run_id, label)
);

-- Claimable jobs index: the single query the worker runs
CREATE INDEX idx_wj_claimable
    ON workflow_jobs (status, next_retry_at NULLS FIRST, created_at)
    WHERE status IN ('ready', 'pending');

-- Per-run status rollup
CREATE INDEX idx_wj_run_status ON workflow_jobs (run_id, status);

-- Idempotency lookup
CREATE INDEX idx_wj_idempotency ON workflow_jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Stale claim detection (jobs claimed but not completed within timeout)
CREATE INDEX idx_wj_stale_claims
    ON workflow_jobs (claimed_at)
    WHERE status = 'claimed' OR status = 'running';

-- ============================================================
-- 3. Job dependency edges
-- ============================================================
CREATE TABLE IF NOT EXISTS workflow_job_edges (
    parent_id   BIGINT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE,
    child_id    BIGINT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (parent_id, child_id)
);

CREATE INDEX idx_wje_child ON workflow_job_edges (child_id);

-- ============================================================
-- 4. NOTIFY trigger for instant worker wakeup
-- ============================================================
CREATE OR REPLACE FUNCTION notify_job_ready() RETURNS trigger AS $$
BEGIN
    IF NEW.status IN ('ready', 'pending') THEN
        PERFORM pg_notify('job_ready', NEW.run_id || ':' || NEW.label);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_job_ready ON workflow_jobs;
CREATE TRIGGER trg_notify_job_ready
    AFTER INSERT OR UPDATE OF status ON workflow_jobs
    FOR EACH ROW
    EXECUTE FUNCTION notify_job_ready();

-- ============================================================
-- 5. Auto-release downstream jobs when a parent succeeds
-- ============================================================
CREATE OR REPLACE FUNCTION release_downstream_jobs() RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'succeeded' AND (OLD.status IS NULL OR OLD.status != 'succeeded') THEN
        -- Mark children as 'ready' if ALL their parents have succeeded
        UPDATE workflow_jobs child
        SET    status = 'ready', ready_at = now()
        WHERE  child.id IN (
            SELECT e.child_id
            FROM   workflow_job_edges e
            WHERE  e.parent_id = NEW.id
        )
        AND    child.status = 'pending'
        AND    NOT EXISTS (
            SELECT 1
            FROM   workflow_job_edges e2
            JOIN   workflow_jobs parent ON parent.id = e2.parent_id
            WHERE  e2.child_id = child.id
            AND    parent.status != 'succeeded'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_release_downstream ON workflow_jobs;
CREATE TRIGGER trg_release_downstream
    AFTER UPDATE OF status ON workflow_jobs
    FOR EACH ROW
    EXECUTE FUNCTION release_downstream_jobs();

-- ============================================================
-- 6. Auto-complete dispatch_runs when all jobs terminal
-- ============================================================
CREATE OR REPLACE FUNCTION finalize_dispatch_run_if_terminal() RETURNS trigger AS $$
DECLARE
    total INT;
    terminal INT;
    succeeded INT;
BEGIN
    IF NEW.status IN ('succeeded', 'failed', 'dead_letter') AND
       (OLD.status IS NULL OR OLD.status NOT IN ('succeeded', 'failed', 'dead_letter')) THEN

        SELECT count(*),
               count(*) FILTER (WHERE status IN ('succeeded', 'failed', 'dead_letter')),
               count(*) FILTER (WHERE status = 'succeeded')
        INTO total, terminal, succeeded
        FROM workflow_jobs WHERE run_id = NEW.run_id;

        IF terminal = total THEN
            UPDATE dispatch_runs
            SET    status = CASE WHEN succeeded = total THEN 'succeeded' ELSE 'failed' END,
                   finished_at = now(),
                   terminal_reason = CASE
                       WHEN succeeded = total THEN 'all_succeeded'
                       ELSE succeeded || '/' || total || ' succeeded'
                   END
            WHERE  run_id = NEW.run_id
            AND    status = 'running';

            -- Notify completion
            PERFORM pg_notify('run_complete', NEW.run_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_finalize_dispatch_run_if_terminal ON workflow_jobs;
CREATE TRIGGER trg_finalize_dispatch_run_if_terminal
    AFTER UPDATE OF status ON workflow_jobs
    FOR EACH ROW
    EXECUTE FUNCTION finalize_dispatch_run_if_terminal();

-- ============================================================
-- 7. Stale claim reaper (run periodically by worker)
-- ============================================================
-- Jobs claimed but not completed after 15 minutes → reset to ready
-- (Worker calls this as part of its poll loop, not a trigger)
-- SELECT count(*) FROM workflow_jobs
-- WHERE status IN ('claimed', 'running')
-- AND claimed_at < now() - interval '15 minutes'
-- → UPDATE SET status = 'ready', claimed_by = NULL, claimed_at = NULL

COMMIT;
