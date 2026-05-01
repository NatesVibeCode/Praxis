-- Migration 387: Synthetic Data authority.
--
-- CQRS Forge build report:
-- - synthetic_data_generate: receipt 8efe7870-70fa-46c6-a4f6-f1eba273376b,
--   operation_ref synthetic-data-generate, command, event synthetic_data.generated.
-- - synthetic_data_read: receipt c68ec71d-4644-4a42-8373-7e10033ae9d1,
--   operation_ref synthetic-data-read, query, read_only.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.synthetic_data',
    'praxis.engine',
    'stream.authority.synthetic_data',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::synthetic-data::synthetic-data-first-class-authority'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS synthetic_data_sets (
    dataset_ref text PRIMARY KEY CHECK (btrim(dataset_ref) <> ''),
    namespace text NOT NULL CHECK (btrim(namespace) <> ''),
    workflow_ref text CHECK (workflow_ref IS NULL OR btrim(workflow_ref) <> ''),
    source_context_ref text CHECK (source_context_ref IS NULL OR btrim(source_context_ref) <> ''),
    source_object_truth_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    generator_ref text NOT NULL CHECK (btrim(generator_ref) <> ''),
    generator_version text NOT NULL CHECK (btrim(generator_version) <> ''),
    seed text NOT NULL CHECK (btrim(seed) <> ''),
    domain_pack text NOT NULL CHECK (btrim(domain_pack) <> ''),
    locale_ref text NOT NULL CHECK (btrim(locale_ref) <> ''),
    privacy_mode text NOT NULL CHECK (privacy_mode IN ('synthetic_only', 'schema_only', 'anonymized_operational_seeded')),
    evidence_tier text NOT NULL CHECK (evidence_tier = 'synthetic'),
    scenario_pack_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    object_counts_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    record_count integer NOT NULL CHECK (record_count > 0),
    quality_state text NOT NULL CHECK (quality_state IN ('accepted', 'rejected')),
    quality_score numeric(8, 4) NOT NULL CHECK (quality_score >= 0 AND quality_score <= 1),
    name_plan_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    generation_spec_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    schema_contract_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    quality_report_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    permissions_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_by_ref text CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_sets_namespace
    ON synthetic_data_sets (namespace, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_sets_source_context
    ON synthetic_data_sets (source_context_ref, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_sets_quality
    ON synthetic_data_sets (quality_state, quality_score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_sets_metadata_gin
    ON synthetic_data_sets USING gin (metadata_json);

CREATE TABLE IF NOT EXISTS synthetic_data_records (
    record_ref text PRIMARY KEY CHECK (btrim(record_ref) <> ''),
    dataset_ref text NOT NULL REFERENCES synthetic_data_sets (dataset_ref) ON DELETE CASCADE,
    object_kind text NOT NULL CHECK (btrim(object_kind) <> ''),
    object_slug text NOT NULL CHECK (btrim(object_slug) <> ''),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    display_name text NOT NULL CHECK (btrim(display_name) <> ''),
    name_ref text NOT NULL CHECK (btrim(name_ref) <> ''),
    fields_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    name_components_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    lineage_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    quality_flags_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset_ref, object_kind, ordinal),
    UNIQUE (dataset_ref, name_ref)
);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_records_dataset_object
    ON synthetic_data_records (dataset_ref, object_kind, ordinal);

CREATE INDEX IF NOT EXISTS idx_synthetic_data_records_display_name
    ON synthetic_data_records (dataset_ref, lower(display_name));

CREATE INDEX IF NOT EXISTS idx_synthetic_data_records_fields_gin
    ON synthetic_data_records USING gin (fields_json);

CREATE OR REPLACE FUNCTION touch_synthetic_data_sets_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_synthetic_data_sets_touch ON synthetic_data_sets;
CREATE TRIGGER trg_synthetic_data_sets_touch
    BEFORE UPDATE ON synthetic_data_sets
    FOR EACH ROW EXECUTE FUNCTION touch_synthetic_data_sets_updated_at();

CREATE OR REPLACE FUNCTION touch_synthetic_data_records_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_synthetic_data_records_touch ON synthetic_data_records;
CREATE TRIGGER trg_synthetic_data_records_touch
    BEFORE UPDATE ON synthetic_data_records
    FOR EACH ROW EXECUTE FUNCTION touch_synthetic_data_records_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('synthetic_data_sets', 'Synthetic Data sets', 'table', 'Durable generated dataset revisions with naming plans, schema contracts, quality reports, privacy posture, and authority lineage.', '{"migration":"387_synthetic_data_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.synthetic_data"}'::jsonb),
    ('synthetic_data_records', 'Synthetic Data records', 'table', 'Generated records with stable refs, stable name refs, display names, fields, name components, lineage, and quality flags.', '{"migration":"387_synthetic_data_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.synthetic_data"}'::jsonb)
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
    ('table.public.synthetic_data_sets', 'table', 'synthetic_data_sets', 'public', 'authority.synthetic_data', 'synthetic_data_sets', 'active', 'registry', 'praxis.engine', 'architecture-policy::synthetic-data::synthetic-data-first-class-authority', '{"purpose":"synthetic dataset authority"}'::jsonb),
    ('table.public.synthetic_data_records', 'table', 'synthetic_data_records', 'public', 'authority.synthetic_data', 'synthetic_data_records', 'active', 'registry', 'praxis.engine', 'architecture-policy::synthetic-data::synthetic-data-first-class-authority', '{"purpose":"synthetic record projection"}'::jsonb)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'event_contract.synthetic_data.generated',
    'synthetic_data.generated',
    'authority.synthetic_data',
    'data_dictionary.object.synthetic_data_generated_event',
    'custom',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::synthetic-data::synthetic-data-first-class-authority',
    '{"aggregate_ref_field":"dataset_ref","expected_payload_fields":["dataset_ref","namespace","record_count","quality_state","quality_score","name_plan_ref","scenario_pack_refs","privacy_mode"]}'::jsonb
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-data-generate',
    p_operation_name        := 'synthetic_data_generate',
    p_handler_ref           := 'runtime.operations.commands.synthetic_data.handle_synthetic_data_generate',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_data.GenerateSyntheticDataCommand',
    p_authority_domain_ref  := 'authority.synthetic_data',
    p_authority_ref         := 'authority.synthetic_data',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-data/generate',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_data.generated',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-data::synthetic-data-first-class-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_data_generate.20260501',
    p_label                 := 'Synthetic Data Generate',
    p_summary               := 'Generate quality-gated synthetic datasets with deterministic naming plans and stable record refs.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-data-read',
    p_operation_name        := 'synthetic_data_read',
    p_handler_ref           := 'runtime.operations.queries.synthetic_data.handle_synthetic_data_read',
    p_input_model_ref       := 'runtime.operations.queries.synthetic_data.QuerySyntheticDataRead',
    p_authority_domain_ref  := 'authority.synthetic_data',
    p_authority_ref         := 'authority.synthetic_data',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/synthetic-data',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-data::synthetic-data-first-class-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_data_read.20260501',
    p_label                 := 'Synthetic Data Read',
    p_summary               := 'Read synthetic datasets, records, naming plans, schema contracts, and quality reports.'
);

COMMIT;
