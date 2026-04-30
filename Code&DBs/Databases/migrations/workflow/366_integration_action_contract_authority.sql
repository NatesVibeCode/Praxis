-- Migration 366: Integration action and automation contract authority.

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
    'authority.integration_action_contracts',
    'praxis.engine',
    'stream.authority.integration_action_contracts',
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

CREATE TABLE IF NOT EXISTS integration_action_contract_heads (
    action_contract_id text PRIMARY KEY,
    action_id text NOT NULL,
    name text NOT NULL,
    owner_ref text,
    status text NOT NULL,
    source_system_ref text NOT NULL,
    target_system_ref text NOT NULL,
    target_provider text,
    execution_mode text,
    idempotency_state text,
    rollback_class text,
    mutating boolean NOT NULL DEFAULT false,
    current_revision_id text NOT NULL,
    current_contract_hash text NOT NULL,
    typed_gap_count integer NOT NULL DEFAULT 0,
    automation_rule_count integer NOT NULL DEFAULT 0,
    contract_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_integration_action_contract_heads_target
    ON integration_action_contract_heads (target_system_ref, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_action_contract_heads_hash
    ON integration_action_contract_heads (current_contract_hash);

CREATE INDEX IF NOT EXISTS idx_integration_action_contract_heads_gaps
    ON integration_action_contract_heads (typed_gap_count, mutating);

CREATE TABLE IF NOT EXISTS integration_action_contract_revisions (
    action_contract_id text NOT NULL REFERENCES integration_action_contract_heads(action_contract_id) ON DELETE CASCADE,
    revision_id text NOT NULL,
    revision_no integer NOT NULL,
    parent_revision_id text,
    action_id text NOT NULL,
    contract_hash text NOT NULL,
    status text NOT NULL,
    captured_at timestamptz,
    contract_json jsonb NOT NULL,
    validation_gaps_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (action_contract_id, revision_id)
);

CREATE INDEX IF NOT EXISTS idx_integration_action_contract_revisions_order
    ON integration_action_contract_revisions (action_contract_id, revision_no);

CREATE TABLE IF NOT EXISTS integration_action_contract_typed_gaps (
    action_contract_id text NOT NULL REFERENCES integration_action_contract_heads(action_contract_id) ON DELETE CASCADE,
    revision_id text NOT NULL,
    gap_id text NOT NULL,
    gap_kind text NOT NULL,
    severity text NOT NULL,
    related_ref text NOT NULL,
    disposition text NOT NULL DEFAULT 'open',
    gap_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (action_contract_id, revision_id, gap_id),
    FOREIGN KEY (action_contract_id, revision_id)
        REFERENCES integration_action_contract_revisions(action_contract_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_integration_action_contract_typed_gaps_kind
    ON integration_action_contract_typed_gaps (gap_kind, severity, disposition);

CREATE TABLE IF NOT EXISTS integration_automation_rule_snapshot_heads (
    automation_rule_id text PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL,
    owner_ref text,
    source_of_truth_ref text,
    current_snapshot_id text NOT NULL,
    current_snapshot_hash text NOT NULL,
    capture_method text,
    linked_action_count integer NOT NULL DEFAULT 0,
    typed_gap_count integer NOT NULL DEFAULT 0,
    snapshot_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_integration_automation_rule_snapshot_heads_status
    ON integration_automation_rule_snapshot_heads (status, owner_ref, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_automation_rule_snapshot_heads_hash
    ON integration_automation_rule_snapshot_heads (current_snapshot_hash);

CREATE TABLE IF NOT EXISTS integration_automation_rule_snapshot_revisions (
    automation_rule_id text NOT NULL REFERENCES integration_automation_rule_snapshot_heads(automation_rule_id) ON DELETE CASCADE,
    snapshot_id text NOT NULL,
    snapshot_hash text NOT NULL,
    status text NOT NULL,
    snapshot_timestamp timestamptz,
    snapshot_json jsonb NOT NULL,
    validation_gaps_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (automation_rule_id, snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_integration_automation_rule_snapshot_revisions_time
    ON integration_automation_rule_snapshot_revisions (automation_rule_id, snapshot_timestamp DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS integration_automation_rule_snapshot_gaps (
    automation_rule_id text NOT NULL REFERENCES integration_automation_rule_snapshot_heads(automation_rule_id) ON DELETE CASCADE,
    snapshot_id text NOT NULL,
    gap_id text NOT NULL,
    gap_kind text NOT NULL,
    severity text NOT NULL,
    related_ref text NOT NULL,
    disposition text NOT NULL DEFAULT 'open',
    gap_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (automation_rule_id, snapshot_id, gap_id),
    FOREIGN KEY (automation_rule_id, snapshot_id)
        REFERENCES integration_automation_rule_snapshot_revisions(automation_rule_id, snapshot_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_integration_automation_rule_snapshot_gaps_kind
    ON integration_automation_rule_snapshot_gaps (gap_kind, severity, disposition);

CREATE TABLE IF NOT EXISTS integration_automation_action_links (
    automation_rule_id text NOT NULL REFERENCES integration_automation_rule_snapshot_heads(automation_rule_id) ON DELETE CASCADE,
    snapshot_id text NOT NULL,
    action_contract_id text NOT NULL REFERENCES integration_action_contract_heads(action_contract_id) ON DELETE CASCADE,
    link_source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (automation_rule_id, snapshot_id, action_contract_id),
    FOREIGN KEY (automation_rule_id, snapshot_id)
        REFERENCES integration_automation_rule_snapshot_revisions(automation_rule_id, snapshot_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_integration_automation_action_links_action
    ON integration_automation_action_links (action_contract_id, automation_rule_id);

CREATE OR REPLACE FUNCTION touch_integration_action_contract_heads_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_integration_action_contract_heads_touch ON integration_action_contract_heads;
CREATE TRIGGER trg_integration_action_contract_heads_touch
    BEFORE UPDATE ON integration_action_contract_heads
    FOR EACH ROW EXECUTE FUNCTION touch_integration_action_contract_heads_updated_at();

CREATE OR REPLACE FUNCTION touch_integration_automation_rule_snapshot_heads_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_integration_automation_rule_snapshot_heads_touch ON integration_automation_rule_snapshot_heads;
CREATE TRIGGER trg_integration_automation_rule_snapshot_heads_touch
    BEFORE UPDATE ON integration_automation_rule_snapshot_heads
    FOR EACH ROW EXECUTE FUNCTION touch_integration_automation_rule_snapshot_heads_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('integration_action_contract_heads', 'Integration action contract heads', 'table', 'Current integration action contract heads with behavior hashes, target systems, idempotency state, rollback class, and gap counts.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_action_contract_revisions', 'Integration action contract revisions', 'table', 'Append-only integration action contract revision rows with deterministic contract hashes and validation gap payloads.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_action_contract_typed_gaps', 'Integration action contract typed gaps', 'table', 'Typed contract gaps that block blind automation trust for integration actions.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_automation_rule_snapshot_heads', 'Integration automation rule snapshot heads', 'table', 'Current automation rule snapshot heads with source-of-truth refs, capture methods, linked actions, and gap counts.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_automation_rule_snapshot_revisions', 'Integration automation rule snapshot revisions', 'table', 'Append-only automation rule snapshot revisions captured from source systems or owner-reviewed exports.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_automation_rule_snapshot_gaps', 'Integration automation rule snapshot gaps', 'table', 'Typed automation snapshot gaps for missing source evidence, pause method, live status, or linked action contracts.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb),
    ('integration_automation_action_links', 'Integration automation action links', 'table', 'Links automation rule snapshots to versioned integration action contract heads.', '{"migration":"366_integration_action_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.integration_action_contracts"}'::jsonb)
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
    ('table.public.integration_action_contract_heads', 'table', 'integration_action_contract_heads', 'public', 'authority.integration_action_contracts', 'integration_action_contract_heads', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_action_contract_revisions', 'table', 'integration_action_contract_revisions', 'public', 'authority.integration_action_contracts', 'integration_action_contract_revisions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_action_contract_typed_gaps', 'table', 'integration_action_contract_typed_gaps', 'public', 'authority.integration_action_contracts', 'integration_action_contract_typed_gaps', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_automation_rule_snapshot_heads', 'table', 'integration_automation_rule_snapshot_heads', 'public', 'authority.integration_action_contracts', 'integration_automation_rule_snapshot_heads', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_automation_rule_snapshot_revisions', 'table', 'integration_automation_rule_snapshot_revisions', 'public', 'authority.integration_action_contracts', 'integration_automation_rule_snapshot_revisions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_automation_rule_snapshot_gaps', 'table', 'integration_automation_rule_snapshot_gaps', 'public', 'authority.integration_action_contracts', 'integration_automation_rule_snapshot_gaps', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.integration_automation_action_links', 'table', 'integration_automation_action_links', 'public', 'authority.integration_action_contracts', 'integration_automation_action_links', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.integration_action_contract.recorded',
    'integration_action_contract.recorded',
    'authority.integration_action_contracts',
    'data_dictionary.object.integration_action_contract_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["contract_count","automation_snapshot_count","contract_typed_gap_count","automation_snapshot_gap_count","automation_action_link_count","action_contract_ids","automation_rule_ids"]}'::jsonb
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
    p_operation_ref         := 'integration_action.command.contract_record',
    p_operation_name        := 'integration_action_contract_record',
    p_handler_ref           := 'runtime.operations.commands.integration_action_contracts.handle_integration_action_contract_record',
    p_input_model_ref       := 'runtime.operations.commands.integration_action_contracts.RecordIntegrationActionContractCommand',
    p_authority_domain_ref  := 'authority.integration_action_contracts',
    p_authority_ref         := 'authority.integration_action_contracts',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/integration-action/contracts',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'integration_action_contract.recorded',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.integration_action_contract_record.20260430',
    p_label                 := 'Integration Action Contract Record',
    p_summary               := 'Record receipt-backed integration action contracts, automation snapshots, typed gaps, and action links.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'integration_action.query.contract_read',
    p_operation_name        := 'integration_action_contract_read',
    p_handler_ref           := 'runtime.operations.queries.integration_action_contracts.handle_integration_action_contract_read',
    p_input_model_ref       := 'runtime.operations.queries.integration_action_contracts.QueryIntegrationActionContractRead',
    p_authority_domain_ref  := 'authority.integration_action_contracts',
    p_authority_ref         := 'authority.integration_action_contracts',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/integration-action/contracts',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.integration_action_contract_read.20260430',
    p_label                 := 'Integration Action Contract Read',
    p_summary               := 'Read queryable integration action contracts, automation snapshots, typed gaps, and action links.'
);

COMMIT;
