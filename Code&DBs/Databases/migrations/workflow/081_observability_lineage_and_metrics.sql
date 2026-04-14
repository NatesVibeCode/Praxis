-- 081: observability lineage and richer workflow metrics
--
-- Extend the event log and workflow metrics surfaces so run lineage,
-- failure categorization, and retry/tooling metadata are queryable in SQL.

BEGIN;

ALTER TABLE IF EXISTS system_events
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT,
    ADD COLUMN IF NOT EXISTS workflow_label TEXT,
    ADD COLUMN IF NOT EXISTS task_type TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS failure_code TEXT,
    ADD COLUMN IF NOT EXISTS failure_category TEXT,
    ADD COLUMN IF NOT EXISTS adapter_type TEXT,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS latency_ms INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'system_events'
    ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS system_events_parent_run_idx ON system_events (parent_run_id)';
        EXECUTE 'CREATE INDEX IF NOT EXISTS system_events_failure_category_idx ON system_events (failure_category, created_at DESC)';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS workflow_metrics (
    run_id TEXT PRIMARY KEY,
    parent_run_id TEXT,
    reviews_workflow_id TEXT,
    review_target_modules TEXT,
    author_model TEXT,
    provider_slug TEXT NOT NULL,
    model_slug TEXT,
    status TEXT NOT NULL,
    failure_code TEXT,
    failure_category TEXT,
    failure_zone TEXT,
    is_retryable BOOLEAN,
    is_transient BOOLEAN,
    latency_ms INTEGER,
    cost_usd REAL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    attempts INTEGER NOT NULL DEFAULT 1,
    retry_count INTEGER NOT NULL DEFAULT 0,
    tool_use_count INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    duration_api_ms INTEGER NOT NULL DEFAULT 0,
    task_type TEXT,
    workflow_label TEXT,
    capabilities TEXT,
    label TEXT,
    adapter_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE workflow_metrics
    ADD COLUMN IF NOT EXISTS parent_run_id TEXT,
    ADD COLUMN IF NOT EXISTS reviews_workflow_id TEXT,
    ADD COLUMN IF NOT EXISTS review_target_modules TEXT,
    ADD COLUMN IF NOT EXISTS author_model TEXT,
    ADD COLUMN IF NOT EXISTS provider_slug TEXT,
    ADD COLUMN IF NOT EXISTS model_slug TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS failure_code TEXT,
    ADD COLUMN IF NOT EXISTS failure_category TEXT,
    ADD COLUMN IF NOT EXISTS failure_zone TEXT,
    ADD COLUMN IF NOT EXISTS is_retryable BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_transient BOOLEAN,
    ADD COLUMN IF NOT EXISTS latency_ms INTEGER,
    ADD COLUMN IF NOT EXISTS cost_usd REAL,
    ADD COLUMN IF NOT EXISTS input_tokens INTEGER,
    ADD COLUMN IF NOT EXISTS output_tokens INTEGER,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tool_use_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duration_api_ms INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS task_type TEXT,
    ADD COLUMN IF NOT EXISTS workflow_label TEXT,
    ADD COLUMN IF NOT EXISTS capabilities TEXT,
    ADD COLUMN IF NOT EXISTS label TEXT,
    ADD COLUMN IF NOT EXISTS adapter_type TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS workflow_metrics_author_idx
    ON workflow_metrics (author_model);

CREATE INDEX IF NOT EXISTS workflow_metrics_status_idx
    ON workflow_metrics (status, created_at DESC);

CREATE INDEX IF NOT EXISTS workflow_metrics_provider_idx
    ON workflow_metrics (provider_slug, model_slug);

CREATE INDEX IF NOT EXISTS workflow_metrics_parent_idx
    ON workflow_metrics (parent_run_id);

CREATE INDEX IF NOT EXISTS workflow_metrics_failure_category_idx
    ON workflow_metrics (failure_category, created_at DESC);

COMMIT;
