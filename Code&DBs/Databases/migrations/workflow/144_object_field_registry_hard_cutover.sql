-- Migration 144: Object Field Registry Hard Cutover
-- Remove the legacy property_definitions mirror now that field rows are authoritative.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'object_field_registry_field_name_nonblank'
    ) THEN
        ALTER TABLE object_field_registry
            ADD CONSTRAINT object_field_registry_field_name_nonblank
            CHECK (btrim(field_name) <> '');
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_object_field_registry_sync_property_definitions
    ON object_field_registry;

DROP FUNCTION IF EXISTS sync_object_type_property_definitions();

ALTER TABLE object_types
    DROP COLUMN IF EXISTS property_definitions;
