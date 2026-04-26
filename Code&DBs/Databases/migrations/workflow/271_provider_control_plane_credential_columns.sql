-- Migration 271: Provider control-plane credential columns.
--
-- Some live instances applied the provider control-plane snapshot before the
-- credential-observation columns were added to the CREATE TABLE body. The
-- CQRS query expects those columns, so this migration makes the table shape
-- converge without requiring a rebuild.

BEGIN;

ALTER TABLE private_provider_control_plane_snapshot
    ADD COLUMN IF NOT EXISTS credential_availability_state TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS credential_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS credential_observations JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE private_provider_control_plane_snapshot
    DROP CONSTRAINT IF EXISTS private_provider_control_plane_snapshot_credential_state_check,
    DROP CONSTRAINT IF EXISTS private_provider_control_plane_snapshot_credential_sources_chec,
    DROP CONSTRAINT IF EXISTS private_provider_control_plane_snapshot_credential_observations;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ppcps_credential_state_check'
           AND conrelid = 'private_provider_control_plane_snapshot'::regclass
    ) THEN
        ALTER TABLE private_provider_control_plane_snapshot
            ADD CONSTRAINT ppcps_credential_state_check
            CHECK (
                credential_availability_state IN (
                    'available',
                    'missing',
                    'not_required',
                    'unknown'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ppcps_credential_sources_check'
           AND conrelid = 'private_provider_control_plane_snapshot'::regclass
    ) THEN
        ALTER TABLE private_provider_control_plane_snapshot
            ADD CONSTRAINT ppcps_credential_sources_check
            CHECK (jsonb_typeof(credential_sources) = 'array');
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ppcps_credential_observations_check'
           AND conrelid = 'private_provider_control_plane_snapshot'::regclass
    ) THEN
        ALTER TABLE private_provider_control_plane_snapshot
            ADD CONSTRAINT ppcps_credential_observations_check
            CHECK (jsonb_typeof(credential_observations) = 'array');
    END IF;
END;
$$;

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    default_value,
    valid_values,
    examples,
    deprecation_notes,
    display_order,
    origin_ref,
    metadata
) VALUES
('table:private_provider_control_plane_snapshot', 'credential_availability_state', 'operator', 'enum', 'Credential availability state', 'Credential/readiness state projected for this provider access method.', true, '"unknown"'::jsonb, '["available","missing","not_required","unknown"]'::jsonb, '[]'::jsonb, '', 85, '{"source":"migration.271_provider_control_plane_credential_columns"}'::jsonb, '{}'::jsonb),
('table:private_provider_control_plane_snapshot', 'credential_sources', 'operator', 'array', 'Credential sources', 'Credential refs or ambient auth sources observed for this access method.', true, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '', 86, '{"source":"migration.271_provider_control_plane_credential_columns"}'::jsonb, '{}'::jsonb),
('table:private_provider_control_plane_snapshot', 'credential_observations', 'operator', 'array', 'Credential observations', 'Structured probe observations supporting the credential availability state.', true, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '', 87, '{"source":"migration.271_provider_control_plane_credential_columns"}'::jsonb, '{}'::jsonb)
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    default_value = EXCLUDED.default_value,
    valid_values = EXCLUDED.valid_values,
    examples = EXCLUDED.examples,
    deprecation_notes = EXCLUDED.deprecation_notes,
    display_order = EXCLUDED.display_order,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
