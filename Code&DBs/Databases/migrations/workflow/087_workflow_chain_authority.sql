BEGIN;

ALTER TABLE control_commands
    DROP CONSTRAINT IF EXISTS control_commands_command_type_check;

ALTER TABLE control_commands
    ADD CONSTRAINT control_commands_command_type_check
    CHECK (
        command_type IN (
            'workflow.submit',
            'workflow.chain.submit',
            'workflow.retry',
            'workflow.cancel',
            'sync.repair'
        )
    );

CREATE TABLE IF NOT EXISTS workflow_chains (
    chain_id TEXT PRIMARY KEY,
    command_id TEXT REFERENCES control_commands (command_id) ON DELETE SET NULL,
    coordination_path TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    program TEXT NOT NULL,
    mode TEXT,
    why TEXT,
    definition JSONB NOT NULL DEFAULT '{}'::jsonb,
    adopt_active BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'queued',
    current_wave_id TEXT,
    requested_by_kind TEXT NOT NULL DEFAULT 'system',
    requested_by_ref TEXT NOT NULL DEFAULT 'workflow.chain',
    last_error_code TEXT,
    last_error_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT workflow_chains_status_check
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS workflow_chain_waves (
    chain_id TEXT NOT NULL REFERENCES workflow_chains (chain_id) ON DELETE CASCADE,
    wave_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    depends_on_wave_id TEXT,
    blocked_by_wave_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (chain_id, wave_id),
    CONSTRAINT workflow_chain_waves_status_check
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'blocked'))
);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_chain_waves_chain_ordinal_idx
    ON workflow_chain_waves (chain_id, ordinal);

CREATE INDEX IF NOT EXISTS workflow_chain_waves_status_idx
    ON workflow_chain_waves (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS workflow_chain_wave_runs (
    chain_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    spec_path TEXT NOT NULL,
    spec_name TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    spec_workflow_id TEXT NOT NULL,
    queue_id TEXT,
    command_id TEXT,
    run_id TEXT,
    submission_status TEXT NOT NULL DEFAULT 'pending',
    run_status TEXT NOT NULL DEFAULT 'pending',
    completed_jobs INTEGER NOT NULL DEFAULT 0,
    total_jobs INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (chain_id, wave_id, spec_path),
    CONSTRAINT workflow_chain_wave_runs_wave_fkey
        FOREIGN KEY (chain_id, wave_id)
        REFERENCES workflow_chain_waves (chain_id, wave_id)
        ON DELETE CASCADE,
    CONSTRAINT workflow_chain_wave_runs_submission_status_check
        CHECK (
            submission_status IN (
                'pending',
                'dispatching',
                'queued',
                'adopted_active',
                'running',
                'succeeded',
                'failed',
                'dead_letter',
                'cancelled',
                'missing'
            )
        ),
    CONSTRAINT workflow_chain_wave_runs_run_status_check
        CHECK (
            run_status IN (
                'pending',
                'queued',
                'running',
                'succeeded',
                'failed',
                'dead_letter',
                'cancelled',
                'missing'
            )
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_chain_wave_runs_chain_wave_ordinal_idx
    ON workflow_chain_wave_runs (chain_id, wave_id, ordinal);

CREATE INDEX IF NOT EXISTS workflow_chain_wave_runs_run_idx
    ON workflow_chain_wave_runs (run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS workflow_chain_wave_runs_status_idx
    ON workflow_chain_wave_runs (submission_status, run_status, updated_at DESC);

COMMIT;
