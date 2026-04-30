-- Migration 376: Portable cartridge deployment contract authority.
--
-- Phase 9 already owns the pure manifest validator under runtime.cartridge.
-- This migration adds the durable CQRS shell: persisted cartridge contract
-- records, queryable dependency/binding/verifier/drift facets, and gateway
-- operations for recording and reading the authority.

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
    'authority.portable_cartridges',
    'praxis.engine',
    'stream.authority.portable_cartridges',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS portable_cartridge_records (
    cartridge_record_id text PRIMARY KEY,
    cartridge_id text NOT NULL,
    cartridge_version text NOT NULL,
    build_id text NOT NULL,
    manifest_version text NOT NULL,
    manifest_digest text NOT NULL,
    deployment_mode text NOT NULL CHECK (deployment_mode IN ('local_verification', 'staged_deployment', 'production_deployment', 'offline_air_gapped')),
    readiness_status text NOT NULL CHECK (readiness_status IN ('ready', 'blocked')),
    error_count integer NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    warning_count integer NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
    object_truth_dependency_count integer NOT NULL DEFAULT 0 CHECK (object_truth_dependency_count >= 0),
    asset_count integer NOT NULL DEFAULT 0 CHECK (asset_count >= 0),
    binding_count integer NOT NULL DEFAULT 0 CHECK (binding_count >= 0),
    required_binding_count integer NOT NULL DEFAULT 0 CHECK (required_binding_count >= 0),
    verifier_check_count integer NOT NULL DEFAULT 0 CHECK (verifier_check_count >= 0),
    drift_hook_count integer NOT NULL DEFAULT 0 CHECK (drift_hook_count >= 0),
    runtime_sizing_class text NOT NULL,
    manifest_json jsonb NOT NULL,
    validation_report_json jsonb NOT NULL,
    deployment_contract_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_records_identity
    ON portable_cartridge_records (cartridge_id, cartridge_version, build_id);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_records_readiness
    ON portable_cartridge_records (readiness_status, deployment_mode, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_records_digest
    ON portable_cartridge_records (manifest_digest);

CREATE TABLE IF NOT EXISTS portable_cartridge_object_truth_dependencies (
    cartridge_record_id text NOT NULL,
    dependency_id text NOT NULL,
    dependency_class text NOT NULL CHECK (dependency_class IN ('primary', 'optional', 'derived')),
    object_ref text,
    authority_source text NOT NULL,
    version text,
    digest text,
    failure_policy text NOT NULL,
    required boolean NOT NULL,
    dependency_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cartridge_record_id, dependency_id),
    FOREIGN KEY (cartridge_record_id)
        REFERENCES portable_cartridge_records(cartridge_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_deps_class
    ON portable_cartridge_object_truth_dependencies (dependency_class, required, authority_source);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_deps_object
    ON portable_cartridge_object_truth_dependencies (object_ref, version);

CREATE TABLE IF NOT EXISTS portable_cartridge_assets (
    cartridge_record_id text NOT NULL,
    asset_path text NOT NULL,
    role text NOT NULL,
    media_type text NOT NULL,
    size_bytes integer NOT NULL CHECK (size_bytes >= 0),
    digest text NOT NULL,
    executable boolean NOT NULL,
    required boolean NOT NULL,
    asset_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cartridge_record_id, asset_path),
    FOREIGN KEY (cartridge_record_id)
        REFERENCES portable_cartridge_records(cartridge_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_assets_role
    ON portable_cartridge_assets (role, required);

CREATE TABLE IF NOT EXISTS portable_cartridge_binding_contracts (
    cartridge_record_id text NOT NULL,
    binding_id text NOT NULL,
    kind text NOT NULL,
    required boolean NOT NULL,
    resolution_phase text NOT NULL,
    source text NOT NULL,
    target text NOT NULL,
    contract_ref text NOT NULL,
    binding_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cartridge_record_id, binding_id),
    FOREIGN KEY (cartridge_record_id)
        REFERENCES portable_cartridge_records(cartridge_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_bindings_kind
    ON portable_cartridge_binding_contracts (kind, required, resolution_phase);

CREATE TABLE IF NOT EXISTS portable_cartridge_verifier_checks (
    cartridge_record_id text NOT NULL,
    check_id text NOT NULL,
    category text NOT NULL,
    required boolean NOT NULL,
    contract_ref text,
    entrypoint text,
    reason_code_family text NOT NULL,
    verifier_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cartridge_record_id, check_id),
    FOREIGN KEY (cartridge_record_id)
        REFERENCES portable_cartridge_records(cartridge_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_verifiers_category
    ON portable_cartridge_verifier_checks (category, required);

CREATE TABLE IF NOT EXISTS portable_cartridge_drift_hooks (
    cartridge_record_id text NOT NULL,
    hook_id text NOT NULL,
    hook_point text NOT NULL,
    required boolean NOT NULL,
    drift_dimensions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    evidence_contract_ref text NOT NULL,
    hook_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cartridge_record_id, hook_id),
    FOREIGN KEY (cartridge_record_id)
        REFERENCES portable_cartridge_records(cartridge_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portable_cartridge_drift_hooks_point
    ON portable_cartridge_drift_hooks (hook_point, required);

CREATE OR REPLACE FUNCTION touch_portable_cartridge_records_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_portable_cartridge_records_touch ON portable_cartridge_records;
CREATE TRIGGER trg_portable_cartridge_records_touch
    BEFORE UPDATE ON portable_cartridge_records
    FOR EACH ROW EXECUTE FUNCTION touch_portable_cartridge_records_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('portable_cartridge_records', 'Portable cartridge records', 'table', 'Manifest-level portable cartridge deployment contract records with readiness, validation, manifest digest, and deployment contract JSON.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb),
    ('portable_cartridge_object_truth_dependencies', 'Portable cartridge Object Truth dependencies', 'table', 'Queryable Object Truth dependencies referenced by portable cartridge contracts.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb),
    ('portable_cartridge_assets', 'Portable cartridge assets', 'table', 'Packaged asset records declared by portable cartridge manifests, including role, digest, media type, and executable flag.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb),
    ('portable_cartridge_binding_contracts', 'Portable cartridge binding contracts', 'table', 'Environment-specific binding contracts required to mount portable cartridges without embedding secrets or environment-specific values.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb),
    ('portable_cartridge_verifier_checks', 'Portable cartridge verifier checks', 'table', 'Required verifier suite checks for portable cartridge deployment contracts.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb),
    ('portable_cartridge_drift_hooks', 'Portable cartridge drift hooks', 'table', 'Drift audit hook points and evidence contracts required by portable cartridge deployment contracts.', '{"migration":"376_portable_cartridge_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.portable_cartridges"}'::jsonb)
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
    ('table.public.portable_cartridge_records', 'table', 'portable_cartridge_records', 'public', 'authority.portable_cartridges', 'portable_cartridge_records', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.portable_cartridge_object_truth_dependencies', 'table', 'portable_cartridge_object_truth_dependencies', 'public', 'authority.portable_cartridges', 'portable_cartridge_object_truth_dependencies', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.portable_cartridge_assets', 'table', 'portable_cartridge_assets', 'public', 'authority.portable_cartridges', 'portable_cartridge_assets', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.portable_cartridge_binding_contracts', 'table', 'portable_cartridge_binding_contracts', 'public', 'authority.portable_cartridges', 'portable_cartridge_binding_contracts', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.portable_cartridge_verifier_checks', 'table', 'portable_cartridge_verifier_checks', 'public', 'authority.portable_cartridges', 'portable_cartridge_verifier_checks', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.portable_cartridge_drift_hooks', 'table', 'portable_cartridge_drift_hooks', 'public', 'authority.portable_cartridges', 'portable_cartridge_drift_hooks', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.portable_cartridge.recorded',
    'portable_cartridge.recorded',
    'authority.portable_cartridges',
    'data_dictionary.object.portable_cartridge_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["cartridge_record_id","cartridge_id","cartridge_version","build_id","manifest_digest","deployment_mode","readiness_status","error_count","warning_count","object_truth_dependency_count","binding_count","verifier_check_count","drift_hook_count","runtime_sizing_class"]}'::jsonb
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
    p_operation_ref         := 'authority-portable-cartridge-record',
    p_operation_name        := 'authority.portable_cartridge.record',
    p_handler_ref           := 'runtime.operations.commands.portable_cartridge.handle_record_portable_cartridge',
    p_input_model_ref       := 'runtime.operations.commands.portable_cartridge.RecordPortableCartridgeCommand',
    p_authority_domain_ref  := 'authority.portable_cartridges',
    p_authority_ref         := 'authority.portable_cartridges',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority/portable-cartridges',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'portable_cartridge.recorded',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.portable_cartridge_record.20260430',
    p_label                 := 'Portable Cartridge Record',
    p_summary               := 'Validate and persist portable cartridge manifests, Object Truth dependencies, binding contracts, verifier suite, drift hooks, runtime assumptions, and deployment readiness through CQRS.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'authority-portable-cartridge-read',
    p_operation_name        := 'authority.portable_cartridge.read',
    p_handler_ref           := 'runtime.operations.queries.portable_cartridge.handle_read_portable_cartridge',
    p_input_model_ref       := 'runtime.operations.queries.portable_cartridge.ReadPortableCartridgeQuery',
    p_authority_domain_ref  := 'authority.portable_cartridges',
    p_authority_ref         := 'authority.portable_cartridges',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/authority/portable-cartridges',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.portable_cartridge_read.20260430',
    p_label                 := 'Portable Cartridge Read',
    p_summary               := 'Read persisted portable cartridge deployment contract records, dependencies, assets, bindings, verifier checks, drift hooks, and readiness state through CQRS.'
);

COMMIT;
