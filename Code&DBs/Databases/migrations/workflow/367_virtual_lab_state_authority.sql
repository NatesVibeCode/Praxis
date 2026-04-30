-- Migration 367: Virtual Lab state authority.

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
    'authority.virtual_lab_state',
    'praxis.engine',
    'stream.authority.virtual_lab_state',
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

CREATE TABLE IF NOT EXISTS virtual_lab_environment_heads (
    environment_id text PRIMARY KEY,
    current_revision_id text NOT NULL,
    current_revision_digest text NOT NULL,
    status text NOT NULL,
    seed_digest text NOT NULL,
    object_state_count integer NOT NULL DEFAULT 0,
    event_count integer NOT NULL DEFAULT 0,
    receipt_count integer NOT NULL DEFAULT 0,
    typed_gap_count integer NOT NULL DEFAULT 0,
    event_chain_digest text,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_environment_heads_status
    ON virtual_lab_environment_heads (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_environment_revisions (
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    parent_revision_id text,
    revision_reason text NOT NULL,
    status text NOT NULL,
    seed_digest text NOT NULL,
    config_digest text NOT NULL,
    policy_digest text NOT NULL,
    revision_digest text NOT NULL,
    created_at_source timestamptz NOT NULL,
    created_by text NOT NULL,
    seed_manifest_json jsonb NOT NULL,
    revision_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, revision_id)
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_environment_revisions_created
    ON virtual_lab_environment_revisions (environment_id, created_at_source DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_environment_revisions_digest
    ON virtual_lab_environment_revisions (revision_digest);

CREATE TABLE IF NOT EXISTS virtual_lab_seed_entries (
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    object_id text NOT NULL,
    instance_id text NOT NULL,
    object_truth_ref text NOT NULL,
    object_truth_version text NOT NULL,
    projection_version text NOT NULL,
    base_state_digest text NOT NULL,
    seed_digest text NOT NULL,
    seed_entry_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, revision_id, object_id, instance_id),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_seed_entries_object_truth
    ON virtual_lab_seed_entries (object_truth_ref, object_truth_version);

CREATE TABLE IF NOT EXISTS virtual_lab_object_states (
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    object_id text NOT NULL,
    instance_id text NOT NULL,
    stream_id text NOT NULL,
    source_ref_json jsonb NOT NULL,
    base_state_digest text NOT NULL,
    overlay_state_digest text NOT NULL,
    effective_state_digest text NOT NULL,
    state_digest text NOT NULL,
    last_event_id text,
    tombstone boolean NOT NULL DEFAULT false,
    state_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, revision_id, object_id, instance_id),
    UNIQUE (stream_id),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_object_states_stream
    ON virtual_lab_object_states (stream_id);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_object_states_digest
    ON virtual_lab_object_states (state_digest, tombstone);

CREATE TABLE IF NOT EXISTS virtual_lab_events (
    event_id text PRIMARY KEY,
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    stream_id text NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    occurred_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    actor_id text NOT NULL,
    actor_type text NOT NULL,
    command_id text NOT NULL,
    causation_id text,
    correlation_id text,
    parent_event_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    sequence_number integer NOT NULL CHECK (sequence_number > 0),
    pre_state_digest text NOT NULL,
    post_state_digest text NOT NULL,
    payload_digest text NOT NULL,
    schema_digest text NOT NULL,
    event_json jsonb NOT NULL,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream_id, sequence_number),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_events_stream
    ON virtual_lab_events (environment_id, revision_id, stream_id, sequence_number);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_events_command
    ON virtual_lab_events (environment_id, revision_id, command_id);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_events_type
    ON virtual_lab_events (event_type, recorded_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_command_receipts (
    receipt_id text PRIMARY KEY,
    command_id text NOT NULL,
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    status text NOT NULL,
    resulting_event_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    precondition_digest text,
    result_digest text,
    errors_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    warnings_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    issued_at timestamptz NOT NULL,
    issued_by text NOT NULL,
    receipt_json jsonb NOT NULL,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_command_receipts_command
    ON virtual_lab_command_receipts (environment_id, revision_id, command_id);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_command_receipts_status
    ON virtual_lab_command_receipts (status, issued_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_typed_gaps (
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    gap_id text NOT NULL,
    gap_kind text NOT NULL,
    severity text NOT NULL,
    related_ref text NOT NULL,
    disposition text NOT NULL DEFAULT 'open',
    gap_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (environment_id, revision_id, gap_id),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_typed_gaps_kind
    ON virtual_lab_typed_gaps (gap_kind, severity, disposition);

CREATE OR REPLACE FUNCTION touch_virtual_lab_environment_heads_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_virtual_lab_environment_heads_touch ON virtual_lab_environment_heads;
CREATE TRIGGER trg_virtual_lab_environment_heads_touch
    BEFORE UPDATE ON virtual_lab_environment_heads
    FOR EACH ROW EXECUTE FUNCTION touch_virtual_lab_environment_heads_updated_at();

CREATE OR REPLACE FUNCTION touch_virtual_lab_object_states_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_virtual_lab_object_states_touch ON virtual_lab_object_states;
CREATE TRIGGER trg_virtual_lab_object_states_touch
    BEFORE UPDATE ON virtual_lab_object_states
    FOR EACH ROW EXECUTE FUNCTION touch_virtual_lab_object_states_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('virtual_lab_environment_heads', 'Virtual Lab environment heads', 'table', 'Current Virtual Lab environment heads with active revision, digest counts, event-chain digest, and receipt-backed source refs.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_environment_revisions', 'Virtual Lab environment revisions', 'table', 'Immutable Virtual Lab revision packets seeded from Object Truth and configuration/policy digests.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_seed_entries', 'Virtual Lab seed entries', 'table', 'Per-object seed entries that bind Virtual Lab object instances to Object Truth refs, versions, projection versions, and seed digests.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_object_states', 'Virtual Lab object state projections', 'table', 'Copy-on-write object state projections with base, overlay, effective, and state digests for a revision.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_events', 'Virtual Lab event store', 'table', 'Event envelopes for predicted Virtual Lab state transitions, ordered per stream with pre/post state digests.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_command_receipts', 'Virtual Lab command receipts', 'table', 'Terminal receipts for Virtual Lab state commands with result digests, errors, warnings, and event linkage.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb),
    ('virtual_lab_typed_gaps', 'Virtual Lab typed gaps', 'table', 'Typed validation gaps for Virtual Lab replay, seed, state, event, and promotion-readiness issues.', '{"migration":"367_virtual_lab_state_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_state"}'::jsonb)
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
    ('table.public.virtual_lab_environment_heads', 'table', 'virtual_lab_environment_heads', 'public', 'authority.virtual_lab_state', 'virtual_lab_environment_heads', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_environment_revisions', 'table', 'virtual_lab_environment_revisions', 'public', 'authority.virtual_lab_state', 'virtual_lab_environment_revisions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_seed_entries', 'table', 'virtual_lab_seed_entries', 'public', 'authority.virtual_lab_state', 'virtual_lab_seed_entries', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_object_states', 'table', 'virtual_lab_object_states', 'public', 'authority.virtual_lab_state', 'virtual_lab_object_states', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_events', 'table', 'virtual_lab_events', 'public', 'authority.virtual_lab_state', 'virtual_lab_events', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_command_receipts', 'table', 'virtual_lab_command_receipts', 'public', 'authority.virtual_lab_state', 'virtual_lab_command_receipts', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_typed_gaps', 'table', 'virtual_lab_typed_gaps', 'public', 'authority.virtual_lab_state', 'virtual_lab_typed_gaps', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.virtual_lab_state.recorded',
    'virtual_lab_state.recorded',
    'authority.virtual_lab_state',
    'data_dictionary.object.virtual_lab_state_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["environment_id","revision_id","revision_digest","seed_digest","object_state_count","event_count","receipt_count","typed_gap_count","event_chain_digest"]}'::jsonb
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
    p_operation_ref         := 'virtual_lab.command.state_record',
    p_operation_name        := 'virtual_lab_state_record',
    p_handler_ref           := 'runtime.operations.commands.virtual_lab_state.handle_virtual_lab_state_record',
    p_input_model_ref       := 'runtime.operations.commands.virtual_lab_state.RecordVirtualLabStateCommand',
    p_authority_domain_ref  := 'authority.virtual_lab_state',
    p_authority_ref         := 'authority.virtual_lab_state',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/virtual-lab/state',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'virtual_lab_state.recorded',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_state_record.20260430',
    p_label                 := 'Virtual Lab State Record',
    p_summary               := 'Record receipt-backed Virtual Lab environment revisions, object states, events, receipts, and replay validation.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'virtual_lab.query.state_read',
    p_operation_name        := 'virtual_lab_state_read',
    p_handler_ref           := 'runtime.operations.queries.virtual_lab_state.handle_virtual_lab_state_read',
    p_input_model_ref       := 'runtime.operations.queries.virtual_lab_state.QueryVirtualLabStateRead',
    p_authority_domain_ref  := 'authority.virtual_lab_state',
    p_authority_ref         := 'authority.virtual_lab_state',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/virtual-lab/state',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_state_read.20260430',
    p_label                 := 'Virtual Lab State Read',
    p_summary               := 'Read queryable Virtual Lab environment revisions, object states, event streams, command receipts, and typed gaps.'
);

COMMIT;
