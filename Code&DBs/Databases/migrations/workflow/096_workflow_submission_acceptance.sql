-- Migration 096: workflow submission acceptance authority
--
-- Separates submission integrity (`comparison_*`) from acceptance evaluation.

BEGIN;

ALTER TABLE workflow_job_submissions
    ADD COLUMN IF NOT EXISTS acceptance_status text NOT NULL DEFAULT 'not_requested',
    ADD COLUMN IF NOT EXISTS acceptance_report jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN workflow_job_submissions.comparison_status IS 'Submission integrity status comparing declared and measured sandbox operations. This is not the acceptance decision.';
COMMENT ON COLUMN workflow_job_submissions.comparison_report IS 'Structured integrity report between declared and measured submission state.';
COMMENT ON COLUMN workflow_job_submissions.acceptance_status IS 'Canonical acceptance state for the sealed submission, evaluated against the acceptance contract.';
COMMENT ON COLUMN workflow_job_submissions.acceptance_report IS 'Structured acceptance evaluation report for the sealed submission.';

COMMIT;
