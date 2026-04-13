BEGIN;

-- Repair the DB-change event emitter so it can identify run-scoped rows too.
CREATE OR REPLACE FUNCTION emit_db_change_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    new_jsonb jsonb;
    source_id_val text;
BEGIN
    new_jsonb := to_jsonb(NEW);
    source_id_val := COALESCE(
        new_jsonb->>'id',
        new_jsonb->>'run_id',
        new_jsonb->>'job_id',
        new_jsonb->>'receipt_id',
        new_jsonb->>'bug_id',
        ''
    );

    INSERT INTO system_events (event_type, source_id, source_type, payload)
    VALUES (
        'db.' || TG_TABLE_NAME || '.' || lower(TG_OP),
        source_id_val,
        TG_TABLE_NAME,
        new_jsonb
    );

    RETURN NEW;
END;
$$;

-- Wire the still-missing authority surfaces into the shared DB-change stream.
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

DROP TRIGGER IF EXISTS trg_receipt_search_change ON receipt_search;
CREATE TRIGGER trg_receipt_search_change
    AFTER INSERT OR UPDATE ON receipt_search
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

COMMIT;
