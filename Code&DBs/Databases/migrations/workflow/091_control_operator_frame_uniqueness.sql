BEGIN;

WITH ranked_item_frames AS (
    SELECT
        operator_frame_id,
        row_number() OVER (
            PARTITION BY run_id, node_id, item_index
            ORDER BY started_at DESC, operator_frame_id DESC
        ) AS rn
    FROM run_operator_frames
    WHERE item_index IS NOT NULL
),
ranked_iteration_frames AS (
    SELECT
        operator_frame_id,
        row_number() OVER (
            PARTITION BY run_id, node_id, iteration_index
            ORDER BY started_at DESC, operator_frame_id DESC
        ) AS rn
    FROM run_operator_frames
    WHERE iteration_index IS NOT NULL
),
dedupe_targets AS (
    SELECT operator_frame_id
    FROM ranked_item_frames
    WHERE rn > 1
    UNION
    SELECT operator_frame_id
    FROM ranked_iteration_frames
    WHERE rn > 1
)
DELETE FROM run_operator_frames
WHERE operator_frame_id IN (SELECT operator_frame_id FROM dedupe_targets);

ALTER TABLE run_operator_frames
    DROP CONSTRAINT IF EXISTS run_operator_frames_state_check;

ALTER TABLE run_operator_frames
    ADD CONSTRAINT run_operator_frames_state_check
    CHECK (frame_state IN ('created', 'running', 'succeeded', 'failed', 'cancelled'));

ALTER TABLE run_operator_frames
    ADD CONSTRAINT run_operator_frames_single_position_check
    CHECK (item_index IS NULL OR iteration_index IS NULL);

CREATE UNIQUE INDEX IF NOT EXISTS run_operator_frames_run_node_item_unique_idx
    ON run_operator_frames (run_id, node_id, item_index)
    WHERE item_index IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS run_operator_frames_run_node_iteration_unique_idx
    ON run_operator_frames (run_id, node_id, iteration_index)
    WHERE iteration_index IS NOT NULL;

CREATE INDEX IF NOT EXISTS run_operator_frames_run_open_started_idx
    ON run_operator_frames (run_id, frame_state, started_at)
    WHERE frame_state IN ('created', 'running');

COMMENT ON CONSTRAINT run_operator_frames_single_position_check ON run_operator_frames IS
    'Ensures a control-operator frame is keyed by at most one loop axis in the canonical runtime table.';

COMMIT;
