-- Canonical operator-control authority tables.

CREATE TABLE operator_decisions (
    operator_decision_id text PRIMARY KEY,
    decision_key text NOT NULL,
    decision_kind text NOT NULL,
    decision_status text NOT NULL,
    title text NOT NULL,
    rationale text NOT NULL,
    decided_by text NOT NULL,
    decision_source text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT operator_decisions_decision_key_key UNIQUE (decision_key),
    CONSTRAINT operator_decisions_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX operator_decisions_kind_status_decided_idx
    ON operator_decisions (decision_kind, decision_status, decided_at DESC);

CREATE INDEX operator_decisions_source_effective_idx
    ON operator_decisions (decision_source, effective_from DESC);

COMMENT ON TABLE operator_decisions IS 'Canonical operator decision nodes over cutover, planning, and runtime-control actions. Owned by surfaces/.';
COMMENT ON COLUMN operator_decisions.rationale IS 'Durable reason for one operator decision. Do not hide control intent only in markdown or shell history.';

CREATE TABLE cutover_gates (
    cutover_gate_id text PRIMARY KEY,
    gate_key text NOT NULL,
    gate_name text NOT NULL,
    gate_kind text NOT NULL,
    gate_status text NOT NULL,
    roadmap_item_id text,
    workflow_class_id text,
    schedule_definition_id text,
    gate_policy jsonb NOT NULL,
    required_evidence jsonb NOT NULL,
    opened_by_decision_id text NOT NULL,
    closed_by_decision_id text,
    opened_at timestamptz NOT NULL,
    closed_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT cutover_gates_gate_key_key UNIQUE (gate_key),
    CONSTRAINT cutover_gates_roadmap_item_fkey
        FOREIGN KEY (roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE SET NULL,
    CONSTRAINT cutover_gates_workflow_class_fkey
        FOREIGN KEY (workflow_class_id)
        REFERENCES workflow_classes (workflow_class_id)
        ON DELETE SET NULL,
    CONSTRAINT cutover_gates_schedule_definition_fkey
        FOREIGN KEY (schedule_definition_id)
        REFERENCES schedule_definitions (schedule_definition_id)
        ON DELETE SET NULL,
    CONSTRAINT cutover_gates_opened_by_decision_fkey
        FOREIGN KEY (opened_by_decision_id)
        REFERENCES operator_decisions (operator_decision_id)
        ON DELETE RESTRICT,
    CONSTRAINT cutover_gates_closed_by_decision_fkey
        FOREIGN KEY (closed_by_decision_id)
        REFERENCES operator_decisions (operator_decision_id)
        ON DELETE SET NULL,
    CONSTRAINT cutover_gates_target_exactly_one
        CHECK (
            ((roadmap_item_id IS NOT NULL)::integer +
             (workflow_class_id IS NOT NULL)::integer +
             (schedule_definition_id IS NOT NULL)::integer) = 1
        ),
    CONSTRAINT cutover_gates_closed_window
        CHECK (closed_at IS NULL OR closed_at >= opened_at)
);

CREATE INDEX cutover_gates_status_kind_opened_idx
    ON cutover_gates (gate_status, gate_kind, opened_at DESC);

CREATE INDEX cutover_gates_roadmap_idx
    ON cutover_gates (roadmap_item_id);

CREATE INDEX cutover_gates_workflow_class_idx
    ON cutover_gates (workflow_class_id);

CREATE INDEX cutover_gates_schedule_definition_idx
    ON cutover_gates (schedule_definition_id);

COMMENT ON TABLE cutover_gates IS 'Canonical cutover and rollout gate nodes over roadmap items, workflow classes, or schedules. Owned by surfaces/.';
COMMENT ON COLUMN cutover_gates.required_evidence IS 'Stored evidence contract for one cutover gate. Do not infer gate requirements only from docs.';

CREATE TABLE work_item_workflow_bindings (
    work_item_workflow_binding_id text PRIMARY KEY,
    binding_kind text NOT NULL,
    binding_status text NOT NULL,
    roadmap_item_id text,
    bug_id text,
    cutover_gate_id text,
    workflow_class_id text,
    schedule_definition_id text,
    workflow_run_id text,
    bound_by_decision_id text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT work_item_workflow_bindings_roadmap_fkey
        FOREIGN KEY (roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE CASCADE,
    CONSTRAINT work_item_workflow_bindings_bug_fkey
        FOREIGN KEY (bug_id)
        REFERENCES bugs (bug_id)
        ON DELETE CASCADE,
    CONSTRAINT work_item_workflow_bindings_cutover_gate_fkey
        FOREIGN KEY (cutover_gate_id)
        REFERENCES cutover_gates (cutover_gate_id)
        ON DELETE CASCADE,
    CONSTRAINT work_item_workflow_bindings_workflow_class_fkey
        FOREIGN KEY (workflow_class_id)
        REFERENCES workflow_classes (workflow_class_id)
        ON DELETE SET NULL,
    CONSTRAINT work_item_workflow_bindings_schedule_definition_fkey
        FOREIGN KEY (schedule_definition_id)
        REFERENCES schedule_definitions (schedule_definition_id)
        ON DELETE SET NULL,
    CONSTRAINT work_item_workflow_bindings_workflow_run_fkey
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE SET NULL,
    CONSTRAINT work_item_workflow_bindings_bound_by_decision_fkey
        FOREIGN KEY (bound_by_decision_id)
        REFERENCES operator_decisions (operator_decision_id)
        ON DELETE SET NULL,
    CONSTRAINT work_item_workflow_bindings_source_exactly_one
        CHECK (
            ((roadmap_item_id IS NOT NULL)::integer +
             (bug_id IS NOT NULL)::integer +
             (cutover_gate_id IS NOT NULL)::integer) = 1
        ),
    CONSTRAINT work_item_workflow_bindings_target_at_least_one
        CHECK (
            ((workflow_class_id IS NOT NULL)::integer +
             (schedule_definition_id IS NOT NULL)::integer +
             (workflow_run_id IS NOT NULL)::integer) >= 1
        ),
    CONSTRAINT work_item_workflow_bindings_unique_edge
        UNIQUE (
            binding_kind,
            roadmap_item_id,
            bug_id,
            cutover_gate_id,
            workflow_class_id,
            schedule_definition_id,
            workflow_run_id
        )
);

CREATE INDEX work_item_workflow_bindings_status_kind_idx
    ON work_item_workflow_bindings (binding_status, binding_kind, created_at DESC);

CREATE INDEX work_item_workflow_bindings_roadmap_idx
    ON work_item_workflow_bindings (roadmap_item_id, created_at DESC);

CREATE INDEX work_item_workflow_bindings_bug_idx
    ON work_item_workflow_bindings (bug_id, created_at DESC);

CREATE INDEX work_item_workflow_bindings_cutover_gate_idx
    ON work_item_workflow_bindings (cutover_gate_id, created_at DESC);

CREATE INDEX work_item_workflow_bindings_workflow_class_idx
    ON work_item_workflow_bindings (workflow_class_id, created_at DESC);

CREATE INDEX work_item_workflow_bindings_workflow_run_idx
    ON work_item_workflow_bindings (workflow_run_id, created_at DESC);

COMMENT ON TABLE work_item_workflow_bindings IS 'Canonical graph edges from bugs, roadmap items, or cutover gates onto native workflow classes, schedules, or runs. Owned by surfaces/.';
COMMENT ON COLUMN work_item_workflow_bindings.binding_kind IS 'Examples: governed_by, queued_as, verified_by. Keep work-to-workflow linkage explicit instead of inferring it from queue names or wrapper history.';
