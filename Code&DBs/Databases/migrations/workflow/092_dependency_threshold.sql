-- Migration 092: Add dependency_threshold to workflow_jobs
--
-- Enables threshold-based dependency release for fan-out patterns.
-- When set, a child job releases after N parents succeed instead of
-- requiring all parents to succeed. NULL preserves current all-or-nothing behavior.

BEGIN;

ALTER TABLE workflow_jobs
  ADD COLUMN IF NOT EXISTS dependency_threshold INT;

COMMENT ON COLUMN workflow_jobs.dependency_threshold IS
  'Minimum number of succeeded parents required to release this job. NULL means all parents must succeed.';

COMMIT;
