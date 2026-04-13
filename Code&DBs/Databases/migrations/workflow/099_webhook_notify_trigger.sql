-- Migration 099: Postgres trigger for instant webhook event notifications
-- Fires pg_notify('webhook') on INSERT to webhook_events so the worker
-- wakes immediately instead of polling.

CREATE OR REPLACE FUNCTION notify_webhook_event()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('webhook', json_build_object(
        'event_id', NEW.event_id,
        'endpoint_id', NEW.endpoint_id
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_webhook_events_notify ON webhook_events;
CREATE TRIGGER trg_webhook_events_notify
    AFTER INSERT ON webhook_events
    FOR EACH ROW
    EXECUTE FUNCTION notify_webhook_event();
