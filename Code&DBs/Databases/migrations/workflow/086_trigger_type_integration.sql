-- Extend workflow_triggers to support integration actions as trigger targets.

-- Relax NOT NULL on workflow_id so integration triggers can omit it.
ALTER TABLE workflow_triggers ALTER COLUMN workflow_id DROP NOT NULL;

ALTER TABLE workflow_triggers
  ADD COLUMN IF NOT EXISTS trigger_type text NOT NULL DEFAULT 'workflow',
  ADD COLUMN IF NOT EXISTS integration_id text,
  ADD COLUMN IF NOT EXISTS integration_action text,
  ADD COLUMN IF NOT EXISTS integration_args jsonb NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_workflow_triggers_integration
    ON workflow_triggers (integration_id)
    WHERE trigger_type = 'integration' AND enabled = TRUE;
