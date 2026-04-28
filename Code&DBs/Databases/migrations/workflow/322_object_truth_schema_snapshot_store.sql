-- Migration 322: Durable object-truth schema snapshot store.

BEGIN;

CREATE TABLE IF NOT EXISTS object_truth_schema_snapshots (
    schema_snapshot_digest text PRIMARY KEY CHECK (btrim(schema_snapshot_digest) <> ''),
    schema_snapshot_ref text NOT NULL UNIQUE CHECK (btrim(schema_snapshot_ref) <> ''),
    system_ref text NOT NULL CHECK (btrim(system_ref) <> ''),
    object_ref text NOT NULL CHECK (btrim(object_ref) <> ''),
    field_count integer NOT NULL CHECK (field_count >= 0),
    schema_snapshot_json jsonb NOT NULL,
    observed_by_ref text NULL CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text NULL CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_schema_snapshots_object
    ON object_truth_schema_snapshots (system_ref, object_ref);

CREATE INDEX IF NOT EXISTS idx_object_truth_schema_snapshots_payload_gin
    ON object_truth_schema_snapshots USING gin (schema_snapshot_json);

CREATE OR REPLACE FUNCTION touch_object_truth_schema_snapshots_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_schema_snapshots_touch ON object_truth_schema_snapshots;
CREATE TRIGGER trg_object_truth_schema_snapshots_touch
    BEFORE UPDATE ON object_truth_schema_snapshots
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_schema_snapshots_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'object_truth_schema_snapshots',
    'Object truth schema snapshots',
    'table',
    'Durable normalized schema snapshots from external systems for object-truth comparison.',
    '{"migration":"322_object_truth_schema_snapshot_store.sql"}'::jsonb,
    '{"authority_domain_ref":"authority.object_truth"}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES (
    'table.public.object_truth_schema_snapshots',
    'table',
    'object_truth_schema_snapshots',
    'public',
    'authority.object_truth',
    'object_truth_schema_snapshots',
    'active',
    'registry',
    'praxis.engine',
    'operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate',
    '{"purpose":"schema snapshot evidence authority"}'::jsonb
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
