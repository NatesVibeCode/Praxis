-- Migration 106: index for acceptance_status queries
--
-- Partial index covering only rows with actual acceptance evaluations.
-- Fresh-cluster bootstrap runs inside one transaction, so use the regular
-- index build here instead of CONCURRENTLY.

CREATE INDEX IF NOT EXISTS idx_wjs_acceptance_status
    ON workflow_job_submissions(acceptance_status)
    WHERE acceptance_status != 'not_requested';
