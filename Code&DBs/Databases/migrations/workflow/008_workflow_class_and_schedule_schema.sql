-- Canonical native-workflow breadth and recurring-scheduler tables.

CREATE TABLE workflow_classes (
    workflow_class_id text PRIMARY KEY,
    class_name text NOT NULL,
    class_kind text NOT NULL,
    workflow_lane_id text NOT NULL,
    status text NOT NULL,
    queue_shape jsonb NOT NULL,
    throttle_policy jsonb NOT NULL,
    review_required boolean NOT NULL DEFAULT false,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT workflow_classes_lane_fkey
        FOREIGN KEY (workflow_lane_id)
        REFERENCES workflow_lanes (workflow_lane_id)
        ON DELETE RESTRICT
);

CREATE INDEX workflow_classes_name_status_idx
    ON workflow_classes (class_name, status);

CREATE INDEX workflow_classes_kind_lane_idx
    ON workflow_classes (class_kind, workflow_lane_id, effective_from DESC);

COMMENT ON TABLE workflow_classes IS 'Canonical native workflow classes such as review, repair, smoke, hourly, and fanout. Owned by policy/.';
COMMENT ON COLUMN workflow_classes.queue_shape IS 'Declarative class parameters for queue/build shape. Do not replace this with prompt lore or wrapper defaults.';

CREATE TABLE schedule_definitions (
    schedule_definition_id text PRIMARY KEY,
    workflow_class_id text NOT NULL,
    schedule_name text NOT NULL,
    schedule_kind text NOT NULL,
    status text NOT NULL,
    cadence_policy jsonb NOT NULL,
    throttle_policy jsonb NOT NULL,
    target_ref text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT schedule_definitions_workflow_class_fkey
        FOREIGN KEY (workflow_class_id)
        REFERENCES workflow_classes (workflow_class_id)
        ON DELETE CASCADE
);

CREATE INDEX schedule_definitions_workflow_class_status_idx
    ON schedule_definitions (workflow_class_id, status, effective_from DESC);

CREATE INDEX schedule_definitions_target_kind_idx
    ON schedule_definitions (target_ref, schedule_kind, effective_from DESC);

COMMENT ON TABLE schedule_definitions IS 'Canonical recurring schedule definitions over native workflow classes. Owned by runtime/.';
COMMENT ON COLUMN schedule_definitions.cadence_policy IS 'Stored recurring cadence, never inferred only from shell automation or queue wrappers.';

CREATE TABLE recurring_run_windows (
    recurring_run_window_id text PRIMARY KEY,
    schedule_definition_id text NOT NULL,
    window_started_at timestamptz NOT NULL,
    window_ended_at timestamptz NOT NULL,
    window_status text NOT NULL,
    capacity_limit integer,
    capacity_used integer NOT NULL CHECK (capacity_used >= 0),
    last_workflow_at timestamptz,
    created_at timestamptz NOT NULL,
    CONSTRAINT recurring_run_windows_schedule_fkey
        FOREIGN KEY (schedule_definition_id)
        REFERENCES schedule_definitions (schedule_definition_id)
        ON DELETE CASCADE,
    CONSTRAINT recurring_run_windows_window_range
        CHECK (window_ended_at >= window_started_at),
    CONSTRAINT recurring_run_windows_unique_window
        UNIQUE (schedule_definition_id, window_started_at)
);

CREATE INDEX recurring_run_windows_schedule_status_idx
    ON recurring_run_windows (schedule_definition_id, window_status, window_started_at DESC);

CREATE INDEX recurring_run_windows_window_status_idx
    ON recurring_run_windows (window_status, window_started_at DESC);

COMMENT ON TABLE recurring_run_windows IS 'Canonical recurring run windows and throttles for native scheduler depth. Owned by runtime/.';
COMMENT ON COLUMN recurring_run_windows.capacity_limit IS 'Optional hard cap for workflows in one recurring window. Null means no explicit cap.';
