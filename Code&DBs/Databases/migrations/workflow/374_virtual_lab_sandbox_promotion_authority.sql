-- Migration 374: Virtual Lab sandbox promotion and drift feedback authority.

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
    'authority.virtual_lab_sandbox_promotion',
    'praxis.engine',
    'stream.authority.virtual_lab_sandbox_promotion',
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

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_promotion_records (
    promotion_record_id text PRIMARY KEY,
    manifest_id text NOT NULL,
    manifest_digest text NOT NULL,
    summary_id text NOT NULL,
    summary_digest text NOT NULL,
    recommendation text NOT NULL CHECK (recommendation IN ('continue', 'continue_with_constraints', 'rerun_phase', 'stop')),
    candidate_count integer NOT NULL DEFAULT 0 CHECK (candidate_count > 0),
    report_count integer NOT NULL DEFAULT 0 CHECK (report_count > 0),
    drift_classification_count integer NOT NULL DEFAULT 0 CHECK (drift_classification_count >= 0),
    handoff_count integer NOT NULL DEFAULT 0 CHECK (handoff_count >= 0),
    simulation_run_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    status_counts_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_json jsonb NOT NULL,
    summary_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_promotion_records_manifest
    ON virtual_lab_sandbox_promotion_records (manifest_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_promotion_records_recommendation
    ON virtual_lab_sandbox_promotion_records (recommendation, updated_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_promotion_candidates (
    promotion_record_id text NOT NULL,
    candidate_id text NOT NULL,
    simulation_run_id text NOT NULL,
    owner text NOT NULL,
    build_ref text NOT NULL,
    sandbox_target text NOT NULL,
    scope_ref text NOT NULL,
    candidate_json jsonb NOT NULL,
    simulation_proof_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, candidate_id),
    FOREIGN KEY (promotion_record_id)
        REFERENCES virtual_lab_sandbox_promotion_records(promotion_record_id)
        ON DELETE CASCADE,
    FOREIGN KEY (simulation_run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_promotion_candidates_simulation
    ON virtual_lab_sandbox_promotion_candidates (simulation_run_id, candidate_id);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_promotion_candidates_target
    ON virtual_lab_sandbox_promotion_candidates (sandbox_target, build_ref);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_executions (
    promotion_record_id text NOT NULL,
    execution_id text NOT NULL,
    candidate_id text NOT NULL,
    scenario_ref text NOT NULL,
    sandbox_target text NOT NULL,
    environment_ref text NOT NULL,
    config_ref text NOT NULL,
    seed_data_ref text NOT NULL,
    status text NOT NULL CHECK (status IN ('completed', 'failed', 'blocked', 'aborted')),
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    execution_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, execution_id),
    FOREIGN KEY (promotion_record_id, candidate_id)
        REFERENCES virtual_lab_sandbox_promotion_candidates(promotion_record_id, candidate_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_executions_status
    ON virtual_lab_sandbox_executions (status, started_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_readback_evidence (
    promotion_record_id text NOT NULL,
    evidence_package_id text NOT NULL,
    execution_id text NOT NULL,
    evidence_id text NOT NULL,
    candidate_id text NOT NULL,
    scenario_ref text NOT NULL,
    observable_ref text NOT NULL,
    evidence_kind text NOT NULL,
    captured_at timestamptz NOT NULL,
    available boolean NOT NULL,
    trusted boolean NOT NULL,
    immutable_ref text,
    evidence_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, evidence_id),
    FOREIGN KEY (promotion_record_id, execution_id)
        REFERENCES virtual_lab_sandbox_executions(promotion_record_id, execution_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_readback_evidence_candidate
    ON virtual_lab_sandbox_readback_evidence (candidate_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_readback_evidence_trust
    ON virtual_lab_sandbox_readback_evidence (available, trusted, captured_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_comparison_reports (
    promotion_record_id text NOT NULL,
    report_id text NOT NULL,
    candidate_id text NOT NULL,
    scenario_ref text NOT NULL,
    execution_id text NOT NULL,
    evidence_package_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('match', 'partial_match', 'drift', 'blocked')),
    report_digest text NOT NULL,
    report_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, report_id),
    FOREIGN KEY (promotion_record_id, execution_id)
        REFERENCES virtual_lab_sandbox_executions(promotion_record_id, execution_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_comparison_reports_status
    ON virtual_lab_sandbox_comparison_reports (status, candidate_id);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_comparison_rows (
    promotion_record_id text NOT NULL,
    report_id text NOT NULL,
    row_id text NOT NULL,
    check_id text NOT NULL,
    dimension text NOT NULL CHECK (dimension IN ('contract', 'output', 'state_transition', 'error_path', 'sequencing', 'data_shape', 'operational')),
    status text NOT NULL CHECK (status IN ('match', 'partial_match', 'drift', 'blocked')),
    disposition text CHECK (disposition IN ('fix_now', 'document', 'defer', 'rerun_required', 'stop_phase')),
    blocker_reason text,
    row_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, report_id, row_id),
    FOREIGN KEY (promotion_record_id, report_id)
        REFERENCES virtual_lab_sandbox_comparison_reports(promotion_record_id, report_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_comparison_rows_status
    ON virtual_lab_sandbox_comparison_rows (status, dimension);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_drift_ledgers (
    promotion_record_id text NOT NULL,
    ledger_id text NOT NULL,
    comparison_report_id text NOT NULL,
    ledger_digest text NOT NULL,
    classification_count integer NOT NULL DEFAULT 0 CHECK (classification_count >= 0),
    ledger_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, ledger_id),
    FOREIGN KEY (promotion_record_id, comparison_report_id)
        REFERENCES virtual_lab_sandbox_comparison_reports(promotion_record_id, report_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_drift_classifications (
    promotion_record_id text NOT NULL,
    ledger_id text NOT NULL,
    classification_id text NOT NULL,
    comparison_report_id text NOT NULL,
    candidate_id text NOT NULL,
    row_id text NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    severity text NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    layer text NOT NULL CHECK (layer IN ('contract', 'workflow', 'integration', 'data', 'environment', 'observability')),
    disposition text NOT NULL CHECK (disposition IN ('fix_now', 'document', 'defer', 'rerun_required', 'stop_phase')),
    owner text NOT NULL,
    classification_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, classification_id),
    FOREIGN KEY (promotion_record_id, ledger_id)
        REFERENCES virtual_lab_sandbox_drift_ledgers(promotion_record_id, ledger_id)
        ON DELETE CASCADE,
    FOREIGN KEY (promotion_record_id, comparison_report_id, row_id)
        REFERENCES virtual_lab_sandbox_comparison_rows(promotion_record_id, report_id, row_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_drift_classifications_reason
    ON virtual_lab_sandbox_drift_classifications USING gin (reason_codes_json);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_drift_classifications_owner
    ON virtual_lab_sandbox_drift_classifications (candidate_id, severity, disposition);

CREATE TABLE IF NOT EXISTS virtual_lab_sandbox_handoffs (
    promotion_record_id text NOT NULL,
    classification_id text NOT NULL,
    candidate_id text NOT NULL,
    handoff_kind text NOT NULL CHECK (handoff_kind IN ('bug', 'gap', 'contract_note', 'evidence', 'receipt')),
    target_ref text NOT NULL,
    status text NOT NULL CHECK (status IN ('proposed', 'open', 'linked', 'closed')),
    handoff_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promotion_record_id, classification_id, handoff_kind, target_ref),
    FOREIGN KEY (promotion_record_id, classification_id)
        REFERENCES virtual_lab_sandbox_drift_classifications(promotion_record_id, classification_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_sandbox_handoffs_kind
    ON virtual_lab_sandbox_handoffs (handoff_kind, status, target_ref);

CREATE OR REPLACE FUNCTION touch_virtual_lab_sandbox_promotion_records_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_virtual_lab_sandbox_promotion_records_touch ON virtual_lab_sandbox_promotion_records;
CREATE TRIGGER trg_virtual_lab_sandbox_promotion_records_touch
    BEFORE UPDATE ON virtual_lab_sandbox_promotion_records
    FOR EACH ROW EXECUTE FUNCTION touch_virtual_lab_sandbox_promotion_records_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('virtual_lab_sandbox_promotion_records', 'Virtual Lab sandbox promotion records', 'table', 'Manifest-level sandbox promotion records with simulation proof refs, status counts, stop/continue summary, and source refs.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_promotion_candidates', 'Virtual Lab sandbox promotion candidates', 'table', 'Per-candidate promotion entries bound to verified simulation runs and candidate metadata.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_executions', 'Virtual Lab sandbox executions', 'table', 'Controlled live sandbox execution records with environment/config/seed refs and terminal execution status.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_readback_evidence', 'Virtual Lab sandbox readback evidence', 'table', 'Actual sandbox observations with availability, trust, immutable refs, and raw evidence JSON.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_comparison_reports', 'Virtual Lab sandbox comparison reports', 'table', 'Candidate-level predicted-vs-actual report rollups for sandbox readback.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_comparison_rows', 'Virtual Lab sandbox comparison rows', 'table', 'Per-check predicted-vs-actual comparison rows with dimensions, statuses, deltas, impacts, and blockers.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_drift_ledgers', 'Virtual Lab sandbox drift ledgers', 'table', 'Per-report drift ledgers requiring classification coverage for non-match comparison rows.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_drift_classifications', 'Virtual Lab sandbox drift classifications', 'table', 'Queryable drift reason codes, severity, layer, disposition, owner, and cause assessment JSON.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb),
    ('virtual_lab_sandbox_handoffs', 'Virtual Lab sandbox handoffs', 'table', 'Downstream bug, gap, contract note, evidence, and receipt handoff references produced by drift classifications.', '{"migration":"374_virtual_lab_sandbox_promotion_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_sandbox_promotion"}'::jsonb)
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
    ('table.public.virtual_lab_sandbox_promotion_records', 'table', 'virtual_lab_sandbox_promotion_records', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_promotion_records', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_promotion_candidates', 'table', 'virtual_lab_sandbox_promotion_candidates', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_promotion_candidates', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_executions', 'table', 'virtual_lab_sandbox_executions', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_executions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_readback_evidence', 'table', 'virtual_lab_sandbox_readback_evidence', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_readback_evidence', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_comparison_reports', 'table', 'virtual_lab_sandbox_comparison_reports', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_comparison_reports', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_comparison_rows', 'table', 'virtual_lab_sandbox_comparison_rows', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_comparison_rows', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_drift_ledgers', 'table', 'virtual_lab_sandbox_drift_ledgers', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_drift_ledgers', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_drift_classifications', 'table', 'virtual_lab_sandbox_drift_classifications', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_drift_classifications', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_sandbox_handoffs', 'table', 'virtual_lab_sandbox_handoffs', 'public', 'authority.virtual_lab_sandbox_promotion', 'virtual_lab_sandbox_handoffs', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.virtual_lab_sandbox_promotion.recorded',
    'virtual_lab_sandbox_promotion.recorded',
    'authority.virtual_lab_sandbox_promotion',
    'data_dictionary.object.virtual_lab_sandbox_promotion_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["promotion_record_id","manifest_id","manifest_digest","recommendation","candidate_ids","simulation_run_ids","report_ids","report_statuses","drift_classification_count","handoff_count","summary_digest"]}'::jsonb
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
    p_operation_ref         := 'virtual_lab.command.sandbox_promotion_record',
    p_operation_name        := 'virtual_lab_sandbox_promotion_record',
    p_handler_ref           := 'runtime.operations.commands.virtual_lab_sandbox_promotion.handle_virtual_lab_sandbox_promotion_record',
    p_input_model_ref       := 'runtime.operations.commands.virtual_lab_sandbox_promotion.RecordVirtualLabSandboxPromotionCommand',
    p_authority_domain_ref  := 'authority.virtual_lab_sandbox_promotion',
    p_authority_ref         := 'authority.virtual_lab_sandbox_promotion',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/virtual-lab/sandbox-promotions',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'virtual_lab_sandbox_promotion.recorded',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_sandbox_promotion_record.20260430',
    p_label                 := 'Virtual Lab Sandbox Promotion Record',
    p_summary               := 'Persist live sandbox promotion manifests, verified simulation proof refs, execution/readback evidence, predicted-vs-actual comparisons, drift ledgers, handoffs, and stop/continue summaries.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'virtual_lab.query.sandbox_promotion_read',
    p_operation_name        := 'virtual_lab_sandbox_promotion_read',
    p_handler_ref           := 'runtime.operations.queries.virtual_lab_sandbox_promotion.handle_virtual_lab_sandbox_promotion_read',
    p_input_model_ref       := 'runtime.operations.queries.virtual_lab_sandbox_promotion.QueryVirtualLabSandboxPromotionRead',
    p_authority_domain_ref  := 'authority.virtual_lab_sandbox_promotion',
    p_authority_ref         := 'authority.virtual_lab_sandbox_promotion',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/virtual-lab/sandbox-promotions',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_sandbox_promotion_read.20260430',
    p_label                 := 'Virtual Lab Sandbox Promotion Read',
    p_summary               := 'Read persisted live sandbox promotion records, readback evidence, drift classifications, handoffs, and stop/continue recommendations.'
);

COMMIT;
