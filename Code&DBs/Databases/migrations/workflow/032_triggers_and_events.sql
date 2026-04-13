-- System events and workflow triggers

CREATE TABLE IF NOT EXISTS system_events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    source_id TEXT,
    source_type TEXT,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_system_events_unprocessed
    ON system_events (event_type, created_at) WHERE processed = FALSE;

CREATE TABLE IF NOT EXISTS workflow_triggers (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    filter JSONB DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    cron_expression TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_fired_at TIMESTAMPTZ,
    fire_count INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_workflow_triggers_event
    ON workflow_triggers (event_type) WHERE enabled = TRUE;

-- Emit system events when dispatch runs complete
-- Replace the dispatch run finalization function to also emit events
CREATE OR REPLACE FUNCTION finalize_dispatch_run_if_terminal_with_events() RETURNS trigger AS $$
DECLARE
    total INT;
    completed INT;
    succeeded INT;
    failed INT;
    run_spec_name TEXT;
    run_parent TEXT;
    run_depth INT;
BEGIN
    IF NEW.status IN ('succeeded', 'failed', 'dead_letter') AND
       (OLD.status IS NULL OR OLD.status NOT IN ('succeeded', 'failed', 'dead_letter')) THEN

        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE status IN ('succeeded', 'failed', 'dead_letter')),
               COUNT(*) FILTER (WHERE status = 'succeeded'),
               COUNT(*) FILTER (WHERE status IN ('failed', 'dead_letter'))
        INTO total, completed, succeeded, failed
        FROM workflow_jobs WHERE run_id = NEW.run_id;

        IF completed >= total AND total > 0 THEN
            -- Update run status
            IF failed > 0 THEN
                UPDATE dispatch_runs SET status = 'failed', finished_at = now() WHERE run_id = NEW.run_id AND status = 'running';
            ELSE
                UPDATE dispatch_runs SET status = 'succeeded', finished_at = now() WHERE run_id = NEW.run_id AND status = 'running';
            END IF;

            -- Get run metadata for event payload
            SELECT spec_name, parent_run_id, trigger_depth
            INTO run_spec_name, run_parent, run_depth
            FROM dispatch_runs WHERE run_id = NEW.run_id;

            -- Emit system event
            INSERT INTO system_events (event_type, source_id, source_type, payload)
            VALUES (
                CASE WHEN failed = 0 THEN 'run.succeeded' ELSE 'run.failed' END,
                NEW.run_id,
                'dispatch_run',
                jsonb_build_object(
                    'run_id', NEW.run_id,
                    'spec_name', COALESCE(run_spec_name, ''),
                    'total_jobs', total,
                    'succeeded', succeeded,
                    'failed', failed,
                    'parent_run_id', run_parent,
                    'trigger_depth', COALESCE(run_depth, 0)
                )
            );

            -- Existing pg_notify for worker polling
            PERFORM pg_notify('run_complete', NEW.run_id);
        END IF;

        -- Release downstream jobs (existing logic)
        UPDATE workflow_jobs child
        SET status = 'ready', ready_at = now()
        FROM workflow_job_edges edge
        WHERE edge.child_id = child.id
          AND edge.parent_id = NEW.id
          AND child.status = 'pending'
          AND NEW.status = 'succeeded'
          AND NOT EXISTS (
              SELECT 1 FROM workflow_job_edges other_edge
              JOIN workflow_jobs other_parent ON other_parent.id = other_edge.parent_id
              WHERE other_edge.child_id = child.id
                AND other_parent.status NOT IN ('succeeded')
          );

        -- Cancel children if parent failed
        IF NEW.status IN ('failed', 'dead_letter') THEN
            UPDATE workflow_jobs child
            SET status = 'cancelled', finished_at = now(), last_error_code = 'parent_failed'
            FROM workflow_job_edges edge
            WHERE edge.child_id = child.id
              AND edge.parent_id = NEW.id
              AND child.status IN ('pending', 'ready');
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Replace the existing trigger with the new function
DROP TRIGGER IF EXISTS trg_finalize_dispatch_run_if_terminal ON workflow_jobs;
CREATE TRIGGER trg_finalize_dispatch_run_if_terminal
    AFTER UPDATE OF status ON workflow_jobs
    FOR EACH ROW
    EXECUTE FUNCTION finalize_dispatch_run_if_terminal_with_events();

-- DB-change event emitter (generic, attach to any table)
CREATE OR REPLACE FUNCTION emit_db_change_event() RETURNS trigger AS $$
BEGIN
    INSERT INTO system_events (event_type, source_id, source_type, payload)
    VALUES (
        'db.' || TG_TABLE_NAME || '.' || lower(TG_OP),
        COALESCE(NEW.id::text, ''),
        TG_TABLE_NAME,
        to_jsonb(NEW)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach DB-change triggers to key tables
DROP TRIGGER IF EXISTS trg_bugs_change ON bugs;
CREATE TRIGGER trg_bugs_change
    AFTER INSERT OR UPDATE ON bugs
    FOR EACH ROW EXECUTE FUNCTION emit_db_change_event();
