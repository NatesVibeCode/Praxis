-- 036: Fix emit_db_change_event trigger for tables without 'id' column.
--
-- The bugs table uses 'bug_id' as its PK, not 'id'. The trigger was referencing
-- NEW.id directly which fails on INSERT. Use JSONB extraction to find the
-- most appropriate ID column dynamically.

BEGIN;

CREATE OR REPLACE FUNCTION emit_db_change_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    source_id_val text;
    new_jsonb jsonb;
BEGIN
    new_jsonb := to_jsonb(NEW);
    source_id_val := COALESCE(
        new_jsonb->>'id',
        new_jsonb->>'bug_id',
        new_jsonb->>'run_id',
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

COMMIT;
