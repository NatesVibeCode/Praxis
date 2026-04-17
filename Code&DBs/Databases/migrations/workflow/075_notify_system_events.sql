-- Wake trigger consumers from system_events inserts.
-- The consumer still resumes from subscription checkpoints; this just removes
-- the last timer-based fallback and lets LISTEN/NOTIFY drive evaluation.

CREATE OR REPLACE FUNCTION notify_system_event_ready() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'system_event',
        json_build_object(
            'id', NEW.id,
            'event_type', NEW.event_type,
            'source_id', COALESCE(NEW.source_id, ''),
            'source_type', COALESCE(NEW.source_type, '')
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF to_regclass('public.system_events') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE 'DROP TRIGGER IF EXISTS trg_notify_system_event_ready ON system_events';
    EXECUTE $trigger$
        CREATE TRIGGER trg_notify_system_event_ready
            AFTER INSERT ON system_events
            FOR EACH ROW
            EXECUTE FUNCTION notify_system_event_ready()
    $trigger$;
END;
$$;
