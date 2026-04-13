-- Rename lingering dispatch-era workflow authority tables to workflow-native names.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_run_sync_status'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_run_sync_status'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_run_sync_status RENAME TO workflow_run_sync_status';
    END IF;
END $$;

ALTER INDEX IF EXISTS dispatch_run_sync_status_updated_at_idx
    RENAME TO workflow_run_sync_status_updated_at_idx;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dispatch_notifications'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'workflow_notifications'
    ) THEN
        EXECUTE 'ALTER TABLE dispatch_notifications RENAME TO workflow_notifications';
    END IF;
END $$;
