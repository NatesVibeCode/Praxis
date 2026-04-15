-- Migration 106: index for acceptance_status queries
--
-- Partial index covering only rows with actual acceptance evaluations.
-- CONCURRENTLY requires no transaction wrapper.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wjs_acceptance_status
    ON workflow_job_submissions(acceptance_status)
    WHERE acceptance_status != 'not_requested';
