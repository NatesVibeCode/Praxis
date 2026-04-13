BEGIN;

CREATE TABLE IF NOT EXISTS run_operator_frames (
    operator_frame_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES workflow_runs (run_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    operator_kind TEXT NOT NULL,
    frame_state TEXT NOT NULL,
    item_index INTEGER,
    iteration_index INTEGER,
    source_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    aggregate_outputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    active_count INTEGER NOT NULL DEFAULT 0,
    stop_reason TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT run_operator_frames_state_check
        CHECK (frame_state IN ('created', 'running', 'succeeded', 'failed')),
    CONSTRAINT run_operator_frames_active_count_check
        CHECK (active_count >= 0),
    CONSTRAINT run_operator_frames_started_before_finished_check
        CHECK (finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX IF NOT EXISTS run_operator_frames_run_node_idx
    ON run_operator_frames (run_id, node_id);

CREATE INDEX IF NOT EXISTS run_operator_frames_run_state_idx
    ON run_operator_frames (run_id, frame_state);

CREATE INDEX IF NOT EXISTS run_operator_frames_run_item_idx
    ON run_operator_frames (run_id, node_id, item_index);

CREATE INDEX IF NOT EXISTS run_operator_frames_run_iteration_idx
    ON run_operator_frames (run_id, node_id, iteration_index);

COMMENT ON TABLE run_operator_frames IS 'Canonical item / iteration frame truth for dynamic control operators. Owned by runtime/.';
COMMENT ON COLUMN run_operator_frames.run_id IS 'Binds the operator frame to exactly one admitted run.';
COMMENT ON COLUMN run_operator_frames.node_id IS 'Logical operator node that owns the frame within the run.';
COMMENT ON COLUMN run_operator_frames.source_snapshot IS 'Frozen source payload snapshot used to expand this frame.';
COMMENT ON COLUMN run_operator_frames.aggregate_outputs IS 'Frame-owned outputs persisted for replay, retry, and inspection.';
COMMENT ON COLUMN run_operator_frames.stop_reason IS 'Explicit terminal reason for why the frame stopped advancing.';

COMMIT;
