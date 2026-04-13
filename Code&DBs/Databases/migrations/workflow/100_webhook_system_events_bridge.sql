-- Migration 100: Bridge webhook_events into system_events and maintenance_intents
--
-- 1. emit_db_change_event trigger on webhook_events → system_events (durable event stream)
-- 2. Maintenance intent trigger on webhook_events → maintenance_intents (processing queue)
-- 3. Maintenance policy for webhook processing
-- 4. Drop the superseded pg_notify-only trigger from migration 099

-- ============================================================
-- 1. Bridge webhook_events → system_events
--    Uses the existing emit_db_change_event() function (migration 059).
--    Event type: 'db.webhook_events.insert', payload: full row as JSONB.
--    The existing trg_notify_system_event_ready then fires pg_notify('system_event').
-- ============================================================

-- Extend emit_db_change_event to extract event_id (for webhook_events table)
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
        new_jsonb->>'event_id',
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

DROP TRIGGER IF EXISTS trg_webhook_events_change ON webhook_events;
CREATE TRIGGER trg_webhook_events_change
    AFTER INSERT ON webhook_events
    FOR EACH ROW
    EXECUTE FUNCTION emit_db_change_event();

-- ============================================================
-- 2. Enqueue webhook processing as a maintenance intent
--    Uses the existing enqueue_maintenance_intent() function (migration 064).
--    Fingerprint deduplication prevents double-processing.
-- ============================================================

CREATE OR REPLACE FUNCTION queue_webhook_processing_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM enqueue_maintenance_intent(
        'process_webhook',
        'webhook_event',
        NEW.event_id,
        'process_webhook:' || NEW.event_id,
        110,
        jsonb_build_object(
            'endpoint_id', NEW.endpoint_id,
            'event_id', NEW.event_id
        ),
        now(),
        5,
        'webhook_event.process'
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_queue_webhook_processing ON webhook_events;
CREATE TRIGGER trg_queue_webhook_processing
    AFTER INSERT ON webhook_events
    FOR EACH ROW
    EXECUTE FUNCTION queue_webhook_processing_intent();

-- ============================================================
-- 3. Maintenance policy for webhook processing
-- ============================================================

INSERT INTO maintenance_policies (policy_key, subject_kind, intent_kind, enabled, priority, max_attempts)
VALUES ('webhook_event.process', 'webhook_event', 'process_webhook', true, 110, 5)
ON CONFLICT (policy_key) DO NOTHING;

-- ============================================================
-- 4. Drop the superseded pg_notify-only trigger (migration 099)
--    Now handled by: emit_db_change_event → system_events → trg_notify_system_event_ready
-- ============================================================

DROP TRIGGER IF EXISTS trg_webhook_events_notify ON webhook_events;
DROP FUNCTION IF EXISTS notify_webhook_event();
