BEGIN;

-- Attach the shared DB-change trigger function to remaining workflow/receipt surfaces.
-- Keep operation coverage aligned with mutation patterns:
-- - workflow tables: insert + update semantics are meaningful
-- - receipt tables: append-only in normal runtime flow, so insert-only avoids noisy storms.
DROP TRIGGER IF EXISTS trg_workflow_jobs_change ON workflow_jobs;
CREATE TRIGGER trg_workflow_jobs_change
    AFTER INSERT OR UPDATE ON workflow_jobs
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

DROP TRIGGER IF EXISTS trg_workflow_runs_change ON workflow_runs;
CREATE TRIGGER trg_workflow_runs_change
    AFTER INSERT OR UPDATE ON workflow_runs
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

DROP TRIGGER IF EXISTS trg_receipts_change ON receipts;
CREATE TRIGGER trg_receipts_change
    AFTER INSERT ON receipts
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

DROP TRIGGER IF EXISTS trg_receipt_search_change ON receipt_search;
CREATE TRIGGER trg_receipt_search_change
    AFTER INSERT ON receipt_search
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

COMMIT;
