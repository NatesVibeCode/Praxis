DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_constraints'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_constraints'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_constraints RENAME TO workflow_constraints';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_metrics'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_metrics'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_metrics RENAME TO workflow_metrics';
    END IF;
END
$$;

ALTER INDEX IF EXISTS dispatch_constraints_pkey
    RENAME TO workflow_constraints_pkey;
ALTER INDEX IF EXISTS dispatch_constraints_hnsw_idx
    RENAME TO workflow_constraints_hnsw_idx;
ALTER INDEX IF EXISTS idx_dispatch_constraints_scope
    RENAME TO idx_workflow_constraints_scope;
ALTER INDEX IF EXISTS dispatch_metrics_pkey
    RENAME TO workflow_metrics_pkey;
ALTER INDEX IF EXISTS dispatch_metrics_author_idx
    RENAME TO workflow_metrics_author_idx;
ALTER INDEX IF EXISTS dispatch_metrics_status_idx
    RENAME TO workflow_metrics_status_idx;
ALTER INDEX IF EXISTS dispatch_metrics_provider_idx
    RENAME TO workflow_metrics_provider_idx;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_runs_legacy'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_runs_legacy'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_runs_legacy RENAME TO workflow_runs_legacy';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_results'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_results'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_results RENAME TO workflow_results';
    END IF;
END
$$;

ALTER INDEX IF EXISTS dispatch_runs_pkey
    RENAME TO workflow_runs_legacy_pkey;
ALTER INDEX IF EXISTS idx_dispatch_runs_parent
    RENAME TO idx_workflow_runs_legacy_parent;
ALTER INDEX IF EXISTS idx_dispatch_runs_status
    RENAME TO idx_workflow_runs_legacy_status;
ALTER INDEX IF EXISTS dispatch_results_pkey
    RENAME TO workflow_results_pkey;
ALTER INDEX IF EXISTS dispatch_results_provider_idx
    RENAME TO workflow_results_provider_idx;
ALTER INDEX IF EXISTS dispatch_results_status_idx
    RENAME TO workflow_results_status_idx;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'workflow_lanes'
          AND column_name = 'dispatch_lane_id'
    ) THEN
        EXECUTE 'ALTER TABLE workflow_lanes RENAME COLUMN dispatch_lane_id TO workflow_lane_id';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'review_records'
          AND column_name = 'reviewed_dispatch_id'
    ) THEN
        EXECUTE 'ALTER TABLE review_records RENAME COLUMN reviewed_dispatch_id TO reviewed_workflow_id';
    END IF;
END
$$;

ALTER INDEX IF EXISTS dispatch_notifications_pkey
    RENAME TO workflow_notifications_pkey;
ALTER INDEX IF EXISTS dispatch_notifications_undelivered_idx
    RENAME TO workflow_notifications_undelivered_idx;
ALTER INDEX IF EXISTS dispatch_run_sync_status_pkey
    RENAME TO workflow_run_sync_status_pkey;

UPDATE workflow_lane_policies
SET policy_scope = regexp_replace(policy_scope, '^dispatch\\.', 'workflow.')
WHERE policy_scope LIKE 'dispatch.%';

UPDATE workflow_classes
SET decision_ref = replace(decision_ref, 'dispatch-class', 'workflow-class')
WHERE decision_ref LIKE '%dispatch-class%';
