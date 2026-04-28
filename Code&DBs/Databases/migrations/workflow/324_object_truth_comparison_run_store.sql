-- Migration 324: Durable object-truth comparison run store.

BEGIN;

CREATE TABLE IF NOT EXISTS object_truth_comparison_runs (
    comparison_run_digest text PRIMARY KEY CHECK (btrim(comparison_run_digest) <> ''),
    comparison_run_ref text NOT NULL UNIQUE CHECK (btrim(comparison_run_ref) <> ''),
    comparison_digest text NOT NULL CHECK (btrim(comparison_digest) <> ''),
    left_object_version_digest text NOT NULL REFERENCES object_truth_object_versions (object_version_digest) ON DELETE RESTRICT,
    right_object_version_digest text NOT NULL REFERENCES object_truth_object_versions (object_version_digest) ON DELETE RESTRICT,
    left_identity_digest text NOT NULL CHECK (btrim(left_identity_digest) <> ''),
    right_identity_digest text NOT NULL CHECK (btrim(right_identity_digest) <> ''),
    summary_json jsonb NOT NULL,
    freshness_json jsonb NOT NULL,
    comparison_json jsonb NOT NULL,
    observed_by_ref text NULL CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text NULL CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_comparison_runs_versions
    ON object_truth_comparison_runs (left_object_version_digest, right_object_version_digest);

CREATE INDEX IF NOT EXISTS idx_object_truth_comparison_runs_identity
    ON object_truth_comparison_runs (left_identity_digest, right_identity_digest);

CREATE INDEX IF NOT EXISTS idx_object_truth_comparison_runs_summary_gin
    ON object_truth_comparison_runs USING gin (summary_json);

CREATE OR REPLACE FUNCTION touch_object_truth_comparison_runs_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_comparison_runs_touch ON object_truth_comparison_runs;
CREATE TRIGGER trg_object_truth_comparison_runs_touch
    BEFORE UPDATE ON object_truth_comparison_runs
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_comparison_runs_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'object_truth_comparison_runs',
    'Object truth comparison runs',
    'table',
    'Durable deterministic comparison outputs between two stored object-truth object versions.',
    '{"migration":"324_object_truth_comparison_run_store.sql"}'::jsonb,
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
    'table.public.object_truth_comparison_runs',
    'table',
    'object_truth_comparison_runs',
    'public',
    'authority.object_truth',
    'object_truth_comparison_runs',
    'active',
    'registry',
    'praxis.engine',
    'operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate',
    '{"purpose":"comparison run evidence authority"}'::jsonb
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
