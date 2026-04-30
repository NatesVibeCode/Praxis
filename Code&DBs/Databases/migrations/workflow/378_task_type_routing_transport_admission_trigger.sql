-- Migration 378: Enforce task_type_routing transports against provider_transport_admissions.
--
-- Prerequisite / pairing:
--   Migration 375_cleanup_invalid_task_type_routing_transports.sql removed historical rows
--   whose (provider_slug, transport_type) had no active admission. This migration prevents
--   those invalid routes from being re-inserted by migrations or runtime derivation.
--
-- Why a trigger (not CHECK / FK):
--   Postgres CHECK cannot reference other tables. A foreign key would need a normalized key
--   (e.g. generated column) because task_type_routing.transport_type uses API/CLI while
--   provider_transport_admissions.transport_kind uses http/cli.
--
-- Rule:
--   API -> http, CLI -> cli. A row is allowed only if provider_transport_admissions contains
--   an active row for the same provider_slug and mapped transport_kind.

BEGIN;

CREATE OR REPLACE FUNCTION validate_task_type_routing_transport_admission()
RETURNS trigger AS $$
DECLARE
    expected_kind TEXT;
BEGIN
    expected_kind := CASE NEW.transport_type
        WHEN 'API' THEN 'http'
        WHEN 'CLI' THEN 'cli'
        ELSE NULL
    END;

    IF expected_kind IS NULL THEN
        RAISE EXCEPTION 'task_type_routing.transport_type must be API or CLI, got %',
            NEW.transport_type
            USING ERRCODE = '23514';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM provider_transport_admissions
        WHERE provider_slug = NEW.provider_slug
          AND transport_kind = expected_kind
          AND status = 'active'
    ) THEN
        RAISE EXCEPTION
            'task_type_routing rejects provider=% transport_type=% (transport_kind=%): no active row in provider_transport_admissions',
            NEW.provider_slug,
            NEW.transport_type,
            expected_kind
            USING ERRCODE = '23514',
                  HINT = 'Register the transport in provider_transport_admissions before adding routes that use it.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_type_routing_transport_admission_check ON task_type_routing;

CREATE TRIGGER task_type_routing_transport_admission_check
    BEFORE INSERT OR UPDATE ON task_type_routing
    FOR EACH ROW
    EXECUTE FUNCTION validate_task_type_routing_transport_admission();

COMMIT;
