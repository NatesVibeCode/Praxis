-- Migration 029: Consolidation fixes (heartbeat, idempotency, completion counters)

BEGIN;

ALTER TABLE workflow_jobs
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_wj_heartbeat
    ON workflow_jobs (heartbeat_at)
    WHERE status IN ('claimed', 'running');

DROP INDEX IF EXISTS idx_wj_idempotency;

CREATE UNIQUE INDEX idx_wj_idempotency
    ON workflow_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE dispatch_runs
    ADD COLUMN IF NOT EXISTS completed_count INT NOT NULL DEFAULT 0;

ALTER TABLE dispatch_runs
    ADD COLUMN IF NOT EXISTS failed_count INT NOT NULL DEFAULT 0;

UPDATE dispatch_runs AS dr
SET completed_count = counts.completed_count,
    failed_count = counts.failed_count
FROM (
    SELECT run_id,
           count(*) FILTER (WHERE status = 'succeeded')::INT AS completed_count,
           count(*) FILTER (WHERE status IN ('failed', 'dead_letter'))::INT AS failed_count
    FROM workflow_jobs
    GROUP BY run_id
) AS counts
WHERE dr.run_id = counts.run_id
  AND (dr.completed_count, dr.failed_count) IS DISTINCT FROM (counts.completed_count, counts.failed_count);

CREATE OR REPLACE FUNCTION finalize_dispatch_run_if_terminal() RETURNS trigger AS $$
DECLARE
    total INT;
    completed INT;
    failed INT;
BEGIN
    IF NEW.status IN ('succeeded', 'failed', 'dead_letter')
       AND (OLD.status IS NULL OR OLD.status NOT IN ('succeeded', 'failed', 'dead_letter')) THEN

        IF NEW.status = 'succeeded' THEN
            UPDATE dispatch_runs
            SET completed_count = completed_count + 1
            WHERE run_id = NEW.run_id
            RETURNING total_jobs, completed_count, failed_count
            INTO total, completed, failed;
        ELSIF NEW.status IN ('failed', 'dead_letter') THEN
            UPDATE dispatch_runs
            SET failed_count = failed_count + 1
            WHERE run_id = NEW.run_id
            RETURNING total_jobs, completed_count, failed_count
            INTO total, completed, failed;
        END IF;

        IF total IS NOT NULL AND (completed + failed) >= total THEN
            UPDATE dispatch_runs
            SET status = CASE WHEN completed = total THEN 'succeeded' ELSE 'failed' END,
                finished_at = now(),
                terminal_reason = completed || '/' || total || ' succeeded'
            WHERE run_id = NEW.run_id;

            PERFORM pg_notify('run_complete', NEW.run_id);
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
