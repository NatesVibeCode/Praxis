BEGIN;

DROP INDEX IF EXISTS idx_system_events_unprocessed;

ALTER TABLE system_events
    DROP COLUMN IF EXISTS processed;

COMMIT;
