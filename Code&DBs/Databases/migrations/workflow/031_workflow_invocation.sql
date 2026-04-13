-- Workflow invocation support: compiled specs, run ancestry, loop protection

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS compiled_spec JSONB;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS invocation_count INT DEFAULT 0;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS last_invoked_at TIMESTAMPTZ;

ALTER TABLE dispatch_runs ADD COLUMN IF NOT EXISTS parent_run_id TEXT;
ALTER TABLE dispatch_runs ADD COLUMN IF NOT EXISTS trigger_depth INT DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_dispatch_runs_parent ON dispatch_runs (parent_run_id) WHERE parent_run_id IS NOT NULL;
