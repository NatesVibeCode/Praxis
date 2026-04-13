BEGIN;

CREATE TABLE IF NOT EXISTS workflow_job_runtime_context (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    job_label TEXT NOT NULL,
    workflow_id TEXT,
    execution_context_shard JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_bundle JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, job_label)
);

CREATE INDEX IF NOT EXISTS idx_workflow_job_runtime_context_workflow
    ON workflow_job_runtime_context (workflow_id)
    WHERE workflow_id IS NOT NULL;

COMMIT;
