-- Migration 161: workflow_spec_ready staging table.
--
-- Holds spec files that are ready to run but not yet fired. Lets an operator
-- pre-build a batch of workflow specs, walk away, and kick them off (manually
-- or on a schedule) later. The table is deliberately thin: file path is the
-- source of truth for spec content, this row only tracks lifecycle and timing.
--
--   status: 'staged' | 'firing' | 'fired' | 'failed' | 'cancelled'
--   scheduled_at: optional — NULL means "fire whenever the launcher runs"
--
-- Fire lifecycle:
--   1. Stage: insert row with spec_path, status='staged'
--   2. Launcher selects staged rows whose scheduled_at is NULL or <= now()
--   3. For each, mark 'firing', call `praxis workflow run <spec_path>`
--   4. On success set status='fired', run_id=..., fired_at=now()
--   5. On failure set status='failed', last_error=...

CREATE TABLE IF NOT EXISTS workflow_spec_ready (
    spec_id       TEXT PRIMARY KEY,
    spec_path     TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'staged'
        CHECK (status IN ('staged', 'firing', 'fired', 'failed', 'cancelled')),
    scheduled_at  TIMESTAMPTZ,
    run_id        TEXT,
    note          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    fired_at      TIMESTAMPTZ,
    last_error    TEXT
);

CREATE INDEX IF NOT EXISTS workflow_spec_ready_status_idx
    ON workflow_spec_ready (status, scheduled_at NULLS FIRST, created_at);

COMMENT ON TABLE workflow_spec_ready IS
    'Thin staging queue: spec files waiting to be fired, optionally scheduled.';
