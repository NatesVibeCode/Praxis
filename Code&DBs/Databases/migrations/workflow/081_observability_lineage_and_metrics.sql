-- 081: observability lineage and richer workflow metrics
--
-- Extend the event log and workflow metrics surfaces so run lineage,
-- failure categorization, and retry/tooling metadata are queryable in SQL.

BEGIN;

ALTER TABLE IF EXISTS platform_events
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT,
    ADD COLUMN IF NOT EXISTS workflow_label TEXT,
    ADD COLUMN IF NOT EXISTS task_type TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS failure_code TEXT,
    ADD COLUMN IF NOT EXISTS failure_category TEXT,
    ADD COLUMN IF NOT EXISTS adapter_type TEXT,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS latency_ms INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS platform_events_parent_run_idx
    ON platform_events (parent_run_id);
CREATE INDEX IF NOT EXISTS platform_events_failure_category_idx
    ON platform_events (failure_category, created_at DESC);

ALTER TABLE IF EXISTS workflow_metrics
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT,
    ADD COLUMN IF NOT EXISTS reviews_workflow_id TEXT,
    ADD COLUMN IF NOT EXISTS review_target_modules TEXT,
    ADD COLUMN IF NOT EXISTS failure_category TEXT,
    ADD COLUMN IF NOT EXISTS failure_zone TEXT,
    ADD COLUMN IF NOT EXISTS is_retryable BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_transient BOOLEAN,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tool_use_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duration_api_ms INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS workflow_label TEXT;

CREATE INDEX IF NOT EXISTS workflow_metrics_parent_idx
    ON workflow_metrics (parent_run_id);
CREATE INDEX IF NOT EXISTS workflow_metrics_failure_category_idx
    ON workflow_metrics (failure_category, created_at DESC);

COMMIT;
