-- Migration 320: Durable object-truth evidence store.
--
-- Object truth needs evidence first and interpretation second. These tables
-- store deterministic object-version packets and row-per-field observations so
-- future comparison, process mining, and contract generation can query facts
-- instead of re-parsing payloads or asking an LLM to remember.

BEGIN;

CREATE TABLE IF NOT EXISTS object_truth_object_versions (
    object_version_digest text PRIMARY KEY CHECK (btrim(object_version_digest) <> ''),
    object_version_ref text NOT NULL UNIQUE CHECK (btrim(object_version_ref) <> ''),
    system_ref text NOT NULL CHECK (btrim(system_ref) <> ''),
    object_ref text NOT NULL CHECK (btrim(object_ref) <> ''),
    identity_digest text NOT NULL CHECK (btrim(identity_digest) <> ''),
    identity_values_json jsonb NOT NULL,
    payload_digest text NOT NULL CHECK (btrim(payload_digest) <> ''),
    schema_snapshot_digest text NULL CHECK (schema_snapshot_digest IS NULL OR btrim(schema_snapshot_digest) <> ''),
    source_metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    hierarchy_signals_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    object_version_json jsonb NOT NULL,
    observed_by_ref text NULL CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text NULL CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_object_versions_identity
    ON object_truth_object_versions (system_ref, object_ref, identity_digest);

CREATE INDEX IF NOT EXISTS idx_object_truth_object_versions_payload
    ON object_truth_object_versions (payload_digest);

CREATE INDEX IF NOT EXISTS idx_object_truth_object_versions_source_metadata_gin
    ON object_truth_object_versions USING gin (source_metadata_json);

CREATE TABLE IF NOT EXISTS object_truth_field_observations (
    object_version_digest text NOT NULL REFERENCES object_truth_object_versions (object_version_digest) ON DELETE CASCADE,
    field_path text NOT NULL CHECK (btrim(field_path) <> ''),
    field_kind text NOT NULL CHECK (btrim(field_kind) <> ''),
    presence text NOT NULL CHECK (presence IN ('present', 'empty')),
    cardinality_kind text NOT NULL CHECK (btrim(cardinality_kind) <> ''),
    cardinality_count integer NULL CHECK (cardinality_count IS NULL OR cardinality_count >= 0),
    sensitive boolean NOT NULL DEFAULT false,
    normalized_value_digest text NOT NULL CHECK (btrim(normalized_value_digest) <> ''),
    redacted_value_preview_json jsonb,
    observation_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_version_digest, field_path)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_field_observations_path
    ON object_truth_field_observations (field_path);

CREATE INDEX IF NOT EXISTS idx_object_truth_field_observations_value_digest
    ON object_truth_field_observations (normalized_value_digest);

CREATE OR REPLACE FUNCTION touch_object_truth_object_versions_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_object_versions_touch ON object_truth_object_versions;
CREATE TRIGGER trg_object_truth_object_versions_touch
    BEFORE UPDATE ON object_truth_object_versions
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_object_versions_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'object_truth_object_versions',
        'Object truth object versions',
        'table',
        'Durable deterministic object-version evidence sampled from external systems.',
        '{"migration":"320_object_truth_evidence_store.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_field_observations',
        'Object truth field observations',
        'table',
        'Field-level presence, type, cardinality, digest, redaction, and hierarchy evidence for object truth.',
        '{"migration":"320_object_truth_evidence_store.sql"}'::jsonb,
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
) VALUES
    (
        'table.public.object_truth_object_versions',
        'table',
        'object_truth_object_versions',
        'public',
        'authority.object_truth',
        'object_truth_object_versions',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.product_architecture.object_truth_inference_from_cross_system_samples',
        '{"purpose":"object version evidence authority"}'::jsonb
    ),
    (
        'table.public.object_truth_field_observations',
        'table',
        'object_truth_field_observations',
        'public',
        'authority.object_truth',
        'object_truth_field_observations',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.product_architecture.object_truth_inference_from_cross_system_samples',
        '{"purpose":"field observation evidence authority"}'::jsonb
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
