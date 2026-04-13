-- Rename remaining dispatch-shaped authority tables/columns to workflow names.

BEGIN;

ALTER TABLE IF EXISTS dispatch_lanes
    RENAME TO workflow_lanes;

ALTER TABLE IF EXISTS dispatch_lane_policies
    RENAME TO workflow_lane_policies;

ALTER TABLE IF EXISTS dispatch_classes
    RENAME TO workflow_classes;

ALTER TABLE IF EXISTS work_item_dispatch_bindings
    RENAME TO work_item_workflow_bindings;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workflow_lane_policies'
          AND column_name = 'dispatch_lane_policy_id'
    ) THEN
        EXECUTE 'ALTER TABLE workflow_lane_policies RENAME COLUMN dispatch_lane_policy_id TO workflow_lane_policy_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workflow_lane_policies'
          AND column_name = 'dispatch_lane_id'
    ) THEN
        EXECUTE 'ALTER TABLE workflow_lane_policies RENAME COLUMN dispatch_lane_id TO workflow_lane_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workflow_classes'
          AND column_name = 'dispatch_class_id'
    ) THEN
        EXECUTE 'ALTER TABLE workflow_classes RENAME COLUMN dispatch_class_id TO workflow_class_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workflow_classes'
          AND column_name = 'dispatch_lane_id'
    ) THEN
        EXECUTE 'ALTER TABLE workflow_classes RENAME COLUMN dispatch_lane_id TO workflow_lane_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'schedule_definitions'
          AND column_name = 'dispatch_class_id'
    ) THEN
        EXECUTE 'ALTER TABLE schedule_definitions RENAME COLUMN dispatch_class_id TO workflow_class_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'recurring_run_windows'
          AND column_name = 'last_dispatch_at'
    ) THEN
        EXECUTE 'ALTER TABLE recurring_run_windows RENAME COLUMN last_dispatch_at TO last_workflow_at';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'cutover_gates'
          AND column_name = 'dispatch_class_id'
    ) THEN
        EXECUTE 'ALTER TABLE cutover_gates RENAME COLUMN dispatch_class_id TO workflow_class_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'work_item_workflow_bindings'
          AND column_name = 'work_item_dispatch_binding_id'
    ) THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME COLUMN work_item_dispatch_binding_id TO work_item_workflow_binding_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'work_item_workflow_bindings'
          AND column_name = 'dispatch_class_id'
    ) THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME COLUMN dispatch_class_id TO workflow_class_id';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_lanes_pkey') THEN
        EXECUTE 'ALTER TABLE workflow_lanes RENAME CONSTRAINT dispatch_lanes_pkey TO workflow_lanes_pkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_lane_policies_pkey') THEN
        EXECUTE 'ALTER TABLE workflow_lane_policies RENAME CONSTRAINT dispatch_lane_policies_pkey TO workflow_lane_policies_pkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_lane_policies_lane_fkey') THEN
        EXECUTE 'ALTER TABLE workflow_lane_policies RENAME CONSTRAINT dispatch_lane_policies_lane_fkey TO workflow_lane_policies_lane_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_lane_policies_unique_window') THEN
        EXECUTE 'ALTER TABLE workflow_lane_policies RENAME CONSTRAINT dispatch_lane_policies_unique_window TO workflow_lane_policies_unique_window';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_classes_pkey') THEN
        EXECUTE 'ALTER TABLE workflow_classes RENAME CONSTRAINT dispatch_classes_pkey TO workflow_classes_pkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dispatch_classes_lane_fkey') THEN
        EXECUTE 'ALTER TABLE workflow_classes RENAME CONSTRAINT dispatch_classes_lane_fkey TO workflow_classes_lane_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'schedule_definitions_dispatch_class_fkey') THEN
        EXECUTE 'ALTER TABLE schedule_definitions RENAME CONSTRAINT schedule_definitions_dispatch_class_fkey TO schedule_definitions_workflow_class_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'cutover_gates_dispatch_class_fkey') THEN
        EXECUTE 'ALTER TABLE cutover_gates RENAME CONSTRAINT cutover_gates_dispatch_class_fkey TO cutover_gates_workflow_class_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_pkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_pkey TO work_item_workflow_bindings_pkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_roadmap_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_roadmap_fkey TO work_item_workflow_bindings_roadmap_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_bug_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_bug_fkey TO work_item_workflow_bindings_bug_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_cutover_gate_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_cutover_gate_fkey TO work_item_workflow_bindings_cutover_gate_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_dispatch_class_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_dispatch_class_fkey TO work_item_workflow_bindings_workflow_class_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_schedule_definition_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_schedule_definition_fkey TO work_item_workflow_bindings_schedule_definition_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_workflow_run_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_workflow_run_fkey TO work_item_workflow_bindings_workflow_run_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_bound_by_decision_fkey') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_bound_by_decision_fkey TO work_item_workflow_bindings_bound_by_decision_fkey';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_source_exactly_one') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_source_exactly_one TO work_item_workflow_bindings_source_exactly_one';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_target_at_least_one') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_target_at_least_one TO work_item_workflow_bindings_target_at_least_one';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'work_item_dispatch_bindings_unique_edge') THEN
        EXECUTE 'ALTER TABLE work_item_workflow_bindings RENAME CONSTRAINT work_item_dispatch_bindings_unique_edge TO work_item_workflow_bindings_unique_edge';
    END IF;
END $$;

ALTER INDEX IF EXISTS dispatch_lanes_name_status_idx
    RENAME TO workflow_lanes_name_status_idx;
ALTER INDEX IF EXISTS dispatch_lanes_kind_effective_idx
    RENAME TO workflow_lanes_kind_effective_idx;
ALTER INDEX IF EXISTS dispatch_lane_policies_lane_idx
    RENAME TO workflow_lane_policies_lane_idx;
ALTER INDEX IF EXISTS dispatch_lane_policies_scope_kind_idx
    RENAME TO workflow_lane_policies_scope_kind_idx;
ALTER INDEX IF EXISTS dispatch_lane_policies_decision_ref_idx
    RENAME TO workflow_lane_policies_decision_ref_idx;
ALTER INDEX IF EXISTS dispatch_classes_name_status_idx
    RENAME TO workflow_classes_name_status_idx;
ALTER INDEX IF EXISTS dispatch_classes_kind_lane_idx
    RENAME TO workflow_classes_kind_lane_idx;
ALTER INDEX IF EXISTS schedule_definitions_class_status_idx
    RENAME TO schedule_definitions_workflow_class_status_idx;
ALTER INDEX IF EXISTS cutover_gates_dispatch_class_idx
    RENAME TO cutover_gates_workflow_class_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_status_kind_idx
    RENAME TO work_item_workflow_bindings_status_kind_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_roadmap_idx
    RENAME TO work_item_workflow_bindings_roadmap_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_bug_idx
    RENAME TO work_item_workflow_bindings_bug_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_cutover_gate_idx
    RENAME TO work_item_workflow_bindings_cutover_gate_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_dispatch_class_idx
    RENAME TO work_item_workflow_bindings_workflow_class_idx;
ALTER INDEX IF EXISTS work_item_dispatch_bindings_workflow_run_idx
    RENAME TO work_item_workflow_bindings_workflow_run_idx;

COMMENT ON TABLE workflow_lanes IS 'Canonical native workflow lane catalog. Owned by policy/.';
COMMENT ON TABLE workflow_lane_policies IS 'Canonical policy bindings that map work classes onto native workflow lanes. Owned by policy/.';
COMMENT ON TABLE workflow_classes IS 'Canonical native workflow classes such as review, repair, smoke, hourly, and fanout. Owned by policy/.';
COMMENT ON TABLE cutover_gates IS 'Canonical cutover and rollout gate nodes over roadmap items, workflow classes, or schedules. Owned by surfaces/.';
COMMENT ON TABLE work_item_workflow_bindings IS 'Canonical graph edges from bugs, roadmap items, or cutover gates onto native workflow classes, schedules, or runs. Owned by surfaces/.';
COMMENT ON COLUMN work_item_workflow_bindings.binding_kind IS 'Examples: governed_by, queued_as, verified_by. Keep work-to-workflow linkage explicit instead of inferring it from queue names or wrapper history.';
COMMENT ON COLUMN recurring_run_windows.capacity_limit IS 'Optional hard cap for workflows in one recurring window. Null means no explicit cap.';

COMMIT;
