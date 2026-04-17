-- Track compiled workflow trigger ownership separately from manual triggers.

ALTER TABLE workflow_triggers
    ADD COLUMN IF NOT EXISTS source_trigger_id text;

CREATE INDEX IF NOT EXISTS idx_workflow_triggers_source_trigger_id
    ON workflow_triggers (workflow_id, source_trigger_id)
    WHERE source_trigger_id IS NOT NULL;
