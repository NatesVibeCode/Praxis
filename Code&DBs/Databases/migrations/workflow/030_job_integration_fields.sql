-- Add integration execution fields to workflow_jobs
-- Allows jobs to be routed to integration tools instead of LLM CLI

ALTER TABLE workflow_jobs
  ADD COLUMN IF NOT EXISTS integration_id text,
  ADD COLUMN IF NOT EXISTS integration_action text,
  ADD COLUMN IF NOT EXISTS integration_args jsonb DEFAULT '{}';

-- Index for quick lookup of integration jobs
CREATE INDEX IF NOT EXISTS idx_workflow_jobs_integration
  ON workflow_jobs (integration_id)
  WHERE integration_id IS NOT NULL;
