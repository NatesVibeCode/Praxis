BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'maintenance_policies'
          AND column_name = 'last_dispatch_fingerprint'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'maintenance_policies'
          AND column_name = 'last_start_fingerprint'
    ) THEN
        EXECUTE 'ALTER TABLE maintenance_policies RENAME COLUMN last_dispatch_fingerprint TO last_start_fingerprint';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'maintenance_policies'
          AND column_name = 'last_dispatched_at'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'maintenance_policies'
          AND column_name = 'last_started_at'
    ) THEN
        EXECUTE 'ALTER TABLE maintenance_policies RENAME COLUMN last_dispatched_at TO last_started_at';
    END IF;
END
$$;

UPDATE maintenance_policies
SET intent_kind = CASE intent_kind
    WHEN 'dispatch_maintenance_review' THEN 'start_maintenance_review'
    WHEN 'dispatch_maintenance_repair' THEN 'start_maintenance_repair'
    ELSE intent_kind
END,
    updated_at = now()
WHERE intent_kind IN ('dispatch_maintenance_review', 'dispatch_maintenance_repair');

UPDATE maintenance_intents
SET intent_kind = CASE intent_kind
    WHEN 'dispatch_maintenance_review' THEN 'start_maintenance_review'
    WHEN 'dispatch_maintenance_repair' THEN 'start_maintenance_repair'
    ELSE intent_kind
END,
    updated_at = now()
WHERE intent_kind IN ('dispatch_maintenance_review', 'dispatch_maintenance_repair');

UPDATE system_events
SET event_type = CASE event_type
    WHEN 'maintenance.review.dispatched' THEN 'maintenance.review.started'
    WHEN 'maintenance.repair.dispatched' THEN 'maintenance.repair.started'
    ELSE event_type
END
WHERE event_type IN ('maintenance.review.dispatched', 'maintenance.repair.dispatched');

COMMIT;
