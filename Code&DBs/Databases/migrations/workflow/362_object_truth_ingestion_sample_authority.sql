-- Migration 362: Object Truth ingestion sample authority.
--
-- Phase 2's pure ingestion primitives become durable authority here:
-- system snapshots, sample captures, redacted payload references, replay
-- fixtures, and gateway-dispatched record/read operations.

BEGIN;

CREATE TABLE IF NOT EXISTS object_truth_system_snapshots (
    system_snapshot_id text PRIMARY KEY,
    system_snapshot_digest text NOT NULL,
    client_ref text NOT NULL,
    system_ref text NOT NULL,
    integration_id text NOT NULL,
    connector_ref text NOT NULL,
    environment_ref text NOT NULL,
    auth_context_hash text NOT NULL,
    captured_at timestamptz NOT NULL,
    capture_receipt_id text NOT NULL,
    schema_snapshot_count integer NOT NULL DEFAULT 0,
    sample_count integer NOT NULL DEFAULT 0,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    system_snapshot_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_system_snapshots_client_system
    ON object_truth_system_snapshots (client_ref, system_ref, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_object_truth_system_snapshots_connector
    ON object_truth_system_snapshots (connector_ref, environment_ref);

CREATE TABLE IF NOT EXISTS object_truth_sample_captures (
    sample_id text PRIMARY KEY,
    system_snapshot_id text NOT NULL REFERENCES object_truth_system_snapshots(system_snapshot_id) ON DELETE CASCADE,
    sample_capture_digest text NOT NULL,
    schema_snapshot_digest text NOT NULL,
    system_ref text NOT NULL,
    object_ref text NOT NULL,
    sample_strategy text NOT NULL,
    source_query_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    cursor_ref text,
    sample_size_requested integer NOT NULL DEFAULT 0,
    sample_size_returned integer NOT NULL DEFAULT 0,
    sample_hash text NOT NULL,
    status text NOT NULL DEFAULT 'captured',
    receipt_id text,
    source_window_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_evidence_digest text,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    sample_capture_json jsonb NOT NULL,
    replay_fixture_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    object_version_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_sample_captures_snapshot
    ON object_truth_sample_captures (system_snapshot_id, object_ref);

CREATE INDEX IF NOT EXISTS idx_object_truth_sample_captures_object
    ON object_truth_sample_captures (system_ref, object_ref, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_object_truth_sample_captures_digest
    ON object_truth_sample_captures (sample_capture_digest);

CREATE TABLE IF NOT EXISTS object_truth_raw_payload_references (
    sample_id text NOT NULL REFERENCES object_truth_sample_captures(sample_id) ON DELETE CASCADE,
    payload_index integer NOT NULL,
    external_record_id text,
    source_metadata_digest text NOT NULL,
    raw_payload_ref text,
    raw_payload_hash text,
    normalized_payload_hash text,
    privacy_classification text NOT NULL DEFAULT 'internal',
    retention_policy_ref text,
    privacy_policy_ref text,
    inline_payload_stored boolean NOT NULL DEFAULT false,
    reference_digest text NOT NULL,
    redacted_preview_digest text,
    source_metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    redacted_preview_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    raw_payload_reference_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sample_id, payload_index)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_raw_payload_refs_record
    ON object_truth_raw_payload_references (external_record_id);

CREATE INDEX IF NOT EXISTS idx_object_truth_raw_payload_refs_hash
    ON object_truth_raw_payload_references (normalized_payload_hash);

CREATE INDEX IF NOT EXISTS idx_object_truth_raw_payload_refs_privacy
    ON object_truth_raw_payload_references (privacy_classification);

CREATE OR REPLACE FUNCTION touch_object_truth_system_snapshots_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_system_snapshots_touch ON object_truth_system_snapshots;
CREATE TRIGGER trg_object_truth_system_snapshots_touch
    BEFORE UPDATE ON object_truth_system_snapshots
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_system_snapshots_updated_at();

CREATE OR REPLACE FUNCTION touch_object_truth_sample_captures_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_sample_captures_touch ON object_truth_sample_captures;
CREATE TRIGGER trg_object_truth_sample_captures_touch
    BEFORE UPDATE ON object_truth_sample_captures
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_sample_captures_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'object_truth_system_snapshots',
        'Object Truth system snapshots',
        'table',
        'Observed client-system state snapshots captured before Object Truth ingestion samples are interpreted.',
        '{"migration":"362_object_truth_ingestion_sample_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_sample_captures',
        'Object Truth sample captures',
        'table',
        'Hash-addressed sample capture evidence with source-query, window, status, replay fixture, and object-version refs.',
        '{"migration":"362_object_truth_ingestion_sample_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_raw_payload_references',
        'Object Truth raw payload references',
        'table',
        'Redaction-safe raw payload refs, hashes, source metadata, and structure-preserving previews for ingestion samples.',
        '{"migration":"362_object_truth_ingestion_sample_authority.sql"}'::jsonb,
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
        'table.public.object_truth_system_snapshots',
        'table',
        'object_truth_system_snapshots',
        'public',
        'authority.object_truth',
        'object_truth_system_snapshots',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.object_truth_sample_captures',
        'table',
        'object_truth_sample_captures',
        'public',
        'authority.object_truth',
        'object_truth_sample_captures',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.object_truth_raw_payload_references',
        'table',
        'object_truth_raw_payload_references',
        'public',
        'authority.object_truth',
        'object_truth_raw_payload_references',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
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
    'event_contract.object_truth.ingestion_sample_recorded',
    'object_truth.ingestion_sample_recorded',
    'authority.object_truth',
    'data_dictionary.object.object_truth_ingestion_sample_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["system_snapshot_id","sample_id","client_ref","system_ref","object_ref","sample_size_returned","payload_reference_count","object_version_count","fixture_digest"]}'::jsonb
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
    p_operation_ref         := 'object_truth.command.ingestion_sample_record',
    p_operation_name        := 'object_truth_ingestion_sample_record',
    p_handler_ref           := 'runtime.operations.commands.object_truth_ingestion.handle_object_truth_ingestion_sample_record',
    p_input_model_ref       := 'runtime.operations.commands.object_truth_ingestion.RecordObjectTruthIngestionSampleCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/ingestion/samples',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'object_truth.ingestion_sample_recorded',
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_ingestion_sample_record.20260430',
    p_label                 := 'Object Truth Record Ingestion Sample',
    p_summary               := 'Record receipt-backed Object Truth ingestion sample evidence, including system snapshot, source-query evidence, sample digest, redacted previews, raw payload references, object versions, and replay fixture evidence.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'object_truth.query.ingestion_sample_read',
    p_operation_name        := 'object_truth_ingestion_sample_read',
    p_handler_ref           := 'runtime.operations.queries.object_truth_ingestion.handle_object_truth_ingestion_sample_read',
    p_input_model_ref       := 'runtime.operations.queries.object_truth_ingestion.QueryObjectTruthIngestionSampleRead',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/object-truth/ingestion/samples',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_ingestion_sample_read.20260430',
    p_label                 := 'Object Truth Read Ingestion Samples',
    p_summary               := 'Read queryable Object Truth ingestion sample evidence and reconstruct replay fixture packets from stored snapshots and sample captures.'
);

COMMIT;
