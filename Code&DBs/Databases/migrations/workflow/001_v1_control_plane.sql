-- Workflow v1 control-plane spine.
-- Authority tables only. Derived support tables and projections are intentionally excluded.
-- The schema follows the control-plane packet and lifecycle invariants.

CREATE TABLE workflow_definitions (
    workflow_definition_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    definition_version integer NOT NULL CHECK (definition_version > 0),
    definition_hash text NOT NULL,
    status text NOT NULL,
    request_envelope jsonb NOT NULL,
    normalized_definition jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    supersedes_workflow_definition_id text,
    CONSTRAINT workflow_definitions_workflow_id_definition_version_key UNIQUE (workflow_id, definition_version),
    CONSTRAINT workflow_definitions_definition_hash_key UNIQUE (definition_hash),
    CONSTRAINT workflow_definitions_supersedes_workflow_definition_id_fkey
        FOREIGN KEY (supersedes_workflow_definition_id)
        REFERENCES workflow_definitions (workflow_definition_id)
        ON DELETE RESTRICT
);

CREATE INDEX workflow_definitions_status_created_at_idx
    ON workflow_definitions (status, created_at DESC);

COMMENT ON TABLE workflow_definitions IS 'Canonical admitted workflow definition header and version chain. Owned by contracts/.';
COMMENT ON COLUMN workflow_definitions.supersedes_workflow_definition_id IS 'Version chain pointer only. Historical rows stay intact for replay.';
COMMENT ON COLUMN workflow_definitions.definition_hash IS 'Immutable admitted definition hash. Runs must bind to this exact content.';

CREATE TABLE workflow_definition_nodes (
    workflow_definition_node_id text PRIMARY KEY,
    workflow_definition_id text NOT NULL,
    node_id text NOT NULL,
    node_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    adapter_type text NOT NULL,
    display_name text NOT NULL,
    inputs jsonb NOT NULL,
    expected_outputs jsonb NOT NULL,
    success_condition jsonb NOT NULL,
    failure_behavior jsonb NOT NULL,
    authority_requirements jsonb NOT NULL,
    execution_boundary jsonb NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    CONSTRAINT workflow_definition_nodes_workflow_definition_id_fkey
        FOREIGN KEY (workflow_definition_id)
        REFERENCES workflow_definitions (workflow_definition_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_definition_nodes_workflow_definition_id_node_id_key UNIQUE (workflow_definition_id, node_id)
);

CREATE INDEX workflow_definition_nodes_workflow_definition_id_node_type_idx
    ON workflow_definition_nodes (workflow_definition_id, node_type);

CREATE INDEX workflow_definition_nodes_workflow_definition_id_position_index_idx
    ON workflow_definition_nodes (workflow_definition_id, position_index);

COMMENT ON TABLE workflow_definition_nodes IS 'Canonical node contract rows for one admitted workflow definition. Owned by contracts/.';
COMMENT ON COLUMN workflow_definition_nodes.workflow_definition_id IS 'Parent admitted definition. Node rows are inserted with the definition, not later as a second truth source.';
COMMENT ON COLUMN workflow_definition_nodes.position_index IS 'Deterministic definition order. Do not infer order from timestamps.';

CREATE TABLE workflow_definition_edges (
    workflow_definition_edge_id text PRIMARY KEY,
    workflow_definition_id text NOT NULL,
    edge_id text NOT NULL,
    edge_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    from_node_id text NOT NULL,
    to_node_id text NOT NULL,
    release_condition jsonb NOT NULL,
    payload_mapping jsonb NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    CONSTRAINT workflow_definition_edges_workflow_definition_id_fkey
        FOREIGN KEY (workflow_definition_id)
        REFERENCES workflow_definitions (workflow_definition_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_definition_edges_workflow_definition_id_edge_id_key UNIQUE (workflow_definition_id, edge_id),
    CONSTRAINT workflow_definition_edges_from_node_fkey
        FOREIGN KEY (workflow_definition_id, from_node_id)
        REFERENCES workflow_definition_nodes (workflow_definition_id, node_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_definition_edges_to_node_fkey
        FOREIGN KEY (workflow_definition_id, to_node_id)
        REFERENCES workflow_definition_nodes (workflow_definition_id, node_id)
        ON DELETE RESTRICT
);

CREATE INDEX workflow_definition_edges_workflow_definition_id_from_node_id_idx
    ON workflow_definition_edges (workflow_definition_id, from_node_id);

CREATE INDEX workflow_definition_edges_workflow_definition_id_to_node_id_idx
    ON workflow_definition_edges (workflow_definition_id, to_node_id);

CREATE INDEX workflow_definition_edges_workflow_definition_id_position_index_idx
    ON workflow_definition_edges (workflow_definition_id, position_index);

COMMENT ON TABLE workflow_definition_edges IS 'Canonical edge contract rows for one admitted workflow definition. Owned by contracts/.';
COMMENT ON COLUMN workflow_definition_edges.workflow_definition_id IS 'Parent admitted definition. Dependency meaning lives in the relational rows, not in a graph projection.';
COMMENT ON COLUMN workflow_definition_edges.from_node_id IS 'Source node within the admitted definition.';
COMMENT ON COLUMN workflow_definition_edges.to_node_id IS 'Target node within the admitted definition.';

CREATE TABLE admission_decisions (
    admission_decision_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    request_id text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('admit', 'reject')),
    reason_code text NOT NULL,
    decided_at timestamptz NOT NULL,
    decided_by text NOT NULL,
    policy_snapshot_ref text NOT NULL,
    validation_result_ref text NOT NULL,
    authority_context_ref text NOT NULL
);

CREATE INDEX admission_decisions_workflow_id_decided_at_idx
    ON admission_decisions (workflow_id, decided_at DESC);

CREATE INDEX admission_decisions_request_id_idx
    ON admission_decisions (request_id);

CREATE INDEX admission_decisions_decision_decided_at_idx
    ON admission_decisions (decision, decided_at DESC);

COMMENT ON TABLE admission_decisions IS 'Canonical admit or reject decision rows for submitted runs. Owned by policy/.';
COMMENT ON COLUMN admission_decisions.decision IS 'The authoritative admission outcome. No implied decision rows.';
COMMENT ON COLUMN admission_decisions.reason_code IS 'Machine-readable admission reason. Prose is not enough.';

CREATE TABLE workflow_runs (
    run_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    request_id text NOT NULL,
    request_digest text NOT NULL,
    authority_context_digest text NOT NULL,
    workflow_definition_id text NOT NULL,
    admitted_definition_hash text NOT NULL,
    run_idempotency_key text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    request_envelope jsonb NOT NULL,
    context_bundle_id text NOT NULL,
    admission_decision_id text NOT NULL,
    current_state text NOT NULL CHECK (
        current_state IN (
            'claim_received',
            'claim_validating',
            'claim_blocked',
            'claim_rejected',
            'claim_accepted',
            'lease_requested',
            'lease_blocked',
            'lease_active',
            'lease_expired',
            'proposal_submitted',
            'proposal_invalid',
            'gate_evaluating',
            'gate_blocked',
            'promotion_decision_recorded',
            'promoted',
            'promotion_rejected',
            'promotion_failed',
            'cancelled'
        )
    ),
    terminal_reason_code text,
    requested_at timestamptz NOT NULL,
    admitted_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    last_event_id text,
    CONSTRAINT workflow_runs_workflow_definition_id_fkey
        FOREIGN KEY (workflow_definition_id)
        REFERENCES workflow_definitions (workflow_definition_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_runs_admission_decision_id_fkey
        FOREIGN KEY (admission_decision_id)
        REFERENCES admission_decisions (admission_decision_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_runs_workflow_id_run_idempotency_key_key UNIQUE (workflow_id, run_idempotency_key),
    CONSTRAINT workflow_runs_requested_before_admitted_check CHECK (requested_at <= admitted_at),
    CONSTRAINT workflow_runs_started_after_admitted_check CHECK (started_at IS NULL OR started_at >= admitted_at),
    CONSTRAINT workflow_runs_finished_after_started_check CHECK (finished_at IS NULL OR started_at IS NOT NULL),
    CONSTRAINT workflow_runs_started_before_finished_check CHECK (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX workflow_runs_workflow_id_requested_at_idx
    ON workflow_runs (workflow_id, requested_at DESC);

CREATE INDEX workflow_runs_current_state_requested_at_idx
    ON workflow_runs (current_state, requested_at DESC);

CREATE INDEX workflow_runs_context_bundle_id_idx
    ON workflow_runs (context_bundle_id);

COMMENT ON TABLE workflow_runs IS 'Canonical run identity and lifecycle truth. Owned by runtime/.';
COMMENT ON COLUMN workflow_runs.workflow_definition_id IS 'Immutable admitted definition binding. Do not rewrite this after admission.';
COMMENT ON COLUMN workflow_runs.admitted_definition_hash IS 'Content hash of the exact admitted definition snapshot used by replay.';
COMMENT ON COLUMN workflow_runs.admission_decision_id IS 'One authoritative admission decision path for this run.';
COMMENT ON COLUMN workflow_runs.current_state IS 'Runtime-owned lifecycle state. Do not advance it outside the canonical state machine.';
COMMENT ON COLUMN workflow_runs.last_event_id IS 'Latest proven event id. This is a pointer, not a second truth source.';

CREATE TABLE run_nodes (
    run_node_id text PRIMARY KEY,
    run_id text NOT NULL,
    workflow_definition_node_id text NOT NULL,
    node_id text NOT NULL,
    node_type text NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number >= 1),
    current_state text NOT NULL,
    adapter_type text NOT NULL,
    context_bundle_id text NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    receipt_id text,
    start_event_id text,
    terminal_event_id text,
    failure_code text,
    input_payload jsonb NOT NULL,
    output_payload jsonb NOT NULL,
    CONSTRAINT run_nodes_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_nodes_workflow_definition_node_id_fkey
        FOREIGN KEY (workflow_definition_node_id)
        REFERENCES workflow_definition_nodes (workflow_definition_node_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_nodes_run_id_node_id_attempt_number_key UNIQUE (run_id, node_id, attempt_number),
    CONSTRAINT run_nodes_started_before_finished_check CHECK (finished_at IS NULL OR started_at IS NOT NULL),
    CONSTRAINT run_nodes_started_before_finished_time_check CHECK (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX run_nodes_workflow_definition_node_id_idx
    ON run_nodes (workflow_definition_node_id);

CREATE INDEX run_nodes_run_id_current_state_idx
    ON run_nodes (run_id, current_state);

CREATE INDEX run_nodes_run_id_node_type_idx
    ON run_nodes (run_id, node_type);

CREATE INDEX run_nodes_receipt_id_idx
    ON run_nodes (receipt_id);

COMMENT ON TABLE run_nodes IS 'Canonical per-node execution state for a run. Owned by runtime/.';
COMMENT ON COLUMN run_nodes.workflow_definition_node_id IS 'Binds the node attempt back to the admitted definition node row.';
COMMENT ON COLUMN run_nodes.receipt_id IS 'Durable action receipt for the node attempt. This is evidence, not authority.';
COMMENT ON COLUMN run_nodes.start_event_id IS 'Start event that proves the node entered execution.';
COMMENT ON COLUMN run_nodes.terminal_event_id IS 'Terminal event that proves the node finished.';

CREATE TABLE run_edges (
    run_edge_id text PRIMARY KEY,
    run_id text NOT NULL,
    workflow_definition_edge_id text NOT NULL,
    edge_id text NOT NULL,
    from_node_id text NOT NULL,
    to_node_id text NOT NULL,
    edge_type text NOT NULL,
    release_state text NOT NULL,
    release_reason_code text,
    released_at timestamptz,
    upstream_run_node_id text,
    downstream_run_node_id text,
    payload_mapping_resolved jsonb NOT NULL,
    CONSTRAINT run_edges_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_edges_workflow_definition_edge_id_fkey
        FOREIGN KEY (workflow_definition_edge_id)
        REFERENCES workflow_definition_edges (workflow_definition_edge_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_edges_run_id_edge_id_key UNIQUE (run_id, edge_id),
    CONSTRAINT run_edges_upstream_run_node_id_fkey
        FOREIGN KEY (upstream_run_node_id)
        REFERENCES run_nodes (run_node_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_edges_downstream_run_node_id_fkey
        FOREIGN KEY (downstream_run_node_id)
        REFERENCES run_nodes (run_node_id)
        ON DELETE RESTRICT
);

CREATE INDEX run_edges_workflow_definition_edge_id_idx
    ON run_edges (workflow_definition_edge_id);

CREATE INDEX run_edges_run_id_from_node_id_idx
    ON run_edges (run_id, from_node_id);

CREATE INDEX run_edges_run_id_to_node_id_idx
    ON run_edges (run_id, to_node_id);

CREATE INDEX run_edges_run_id_release_state_idx
    ON run_edges (run_id, release_state);

COMMENT ON TABLE run_edges IS 'Canonical dependency release state for each admitted run. Owned by runtime/.';
COMMENT ON COLUMN run_edges.workflow_definition_edge_id IS 'Binds the edge row back to the admitted definition edge row.';
COMMENT ON COLUMN run_edges.release_state IS 'Runtime-owned dependency release truth. Do not infer this from a graph view.';
COMMENT ON COLUMN run_edges.upstream_run_node_id IS 'Upstream node evidence that explains why release is true.';
COMMENT ON COLUMN run_edges.downstream_run_node_id IS 'Downstream node that became eligible because of the upstream evidence.';

CREATE TABLE capability_grants (
    capability_grant_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    capability_name text NOT NULL,
    grant_state text NOT NULL,
    reason_code text NOT NULL,
    decision_ref text NOT NULL,
    scope_json jsonb NOT NULL,
    granted_at timestamptz NOT NULL,
    expires_at timestamptz,
    CONSTRAINT capability_grants_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE INDEX capability_grants_subject_type_subject_id_capability_name_grant_state_idx
    ON capability_grants (subject_type, subject_id, capability_name, grant_state);

CREATE INDEX capability_grants_run_id_capability_name_idx
    ON capability_grants (run_id, capability_name);

CREATE INDEX capability_grants_expires_at_idx
    ON capability_grants (expires_at);

COMMENT ON TABLE capability_grants IS 'Canonical authorization decisions that allow later work to proceed. Owned by policy/.';
COMMENT ON COLUMN capability_grants.decision_ref IS 'Decision reference only. The policy row is the authority, not this grant record.';

CREATE TABLE workflow_events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    request_id text NOT NULL,
    causation_id text,
    node_id text,
    occurred_at timestamptz NOT NULL,
    evidence_seq bigint NOT NULL CHECK (evidence_seq > 0),
    actor_type text NOT NULL,
    reason_code text NOT NULL,
    payload jsonb NOT NULL,
    CONSTRAINT workflow_events_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE INDEX workflow_events_run_id_evidence_seq_idx
    ON workflow_events (run_id, evidence_seq);

CREATE INDEX workflow_events_workflow_id_occurred_at_evidence_seq_idx
    ON workflow_events (workflow_id, occurred_at DESC, evidence_seq DESC);

CREATE INDEX workflow_events_node_id_evidence_seq_idx
    ON workflow_events (node_id, evidence_seq);

COMMENT ON TABLE workflow_events IS 'Append-only event stream for lifecycle and decision evidence. Owned by receipts/.';
COMMENT ON COLUMN workflow_events.evidence_seq IS 'Shared run-scoped ordering across workflow_events and receipts.';
COMMENT ON COLUMN workflow_events.causation_id IS 'Explicit prior row or decision that caused this event.';
COMMENT ON COLUMN workflow_events.payload IS 'Structured event detail. Do not collapse this into prose.';

CREATE TABLE receipts (
    receipt_id text PRIMARY KEY,
    receipt_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    request_id text NOT NULL,
    causation_id text,
    node_id text,
    attempt_no integer,
    supersedes_receipt_id text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    evidence_seq bigint NOT NULL CHECK (evidence_seq > 0),
    executor_type text NOT NULL,
    status text NOT NULL,
    inputs jsonb NOT NULL,
    outputs jsonb NOT NULL,
    artifacts jsonb NOT NULL,
    failure_code text,
    decision_refs jsonb NOT NULL,
    CONSTRAINT receipts_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT receipts_started_before_finished_check CHECK (started_at <= finished_at),
    CONSTRAINT receipts_attempt_no_check CHECK (attempt_no IS NULL OR attempt_no >= 1)
);

CREATE INDEX receipts_run_id_evidence_seq_idx
    ON receipts (run_id, evidence_seq);

CREATE INDEX receipts_workflow_id_started_at_idx
    ON receipts (workflow_id, started_at DESC);

CREATE INDEX receipts_receipt_type_started_at_idx
    ON receipts (receipt_type, started_at DESC);

COMMENT ON TABLE receipts IS 'Append-only durable action records with inputs, outputs, status, and evidence refs. Owned by receipts/.';
COMMENT ON COLUMN receipts.evidence_seq IS 'Shared run-scoped ordering across workflow_events and receipts.';
COMMENT ON COLUMN receipts.decision_refs IS 'Typed decision references only. Do not store free-form cause text here.';

CREATE TABLE promotion_decisions (
    promotion_decision_id text PRIMARY KEY,
    proposal_id text NOT NULL,
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('accept', 'reject', 'block')),
    reason_code text NOT NULL,
    decided_at timestamptz NOT NULL,
    decided_by text NOT NULL,
    policy_snapshot_ref text NOT NULL,
    validation_receipt_ref text NOT NULL,
    proposal_manifest_hash text NOT NULL,
    validated_head_ref text,
    promotion_intent_at timestamptz,
    finalized_at timestamptz,
    canonical_commit_ref text,
    target_kind text,
    target_ref text,
    CONSTRAINT promotion_decisions_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT promotion_decisions_proposal_id_key UNIQUE (proposal_id)
);

CREATE INDEX promotion_decisions_decision_decided_at_idx
    ON promotion_decisions (decision, decided_at DESC);

CREATE INDEX promotion_decisions_workflow_id_decided_at_idx
    ON promotion_decisions (workflow_id, decided_at DESC);

COMMENT ON TABLE promotion_decisions IS 'Canonical gate decisions for promotion into the canonical repo. Owned by policy/.';
COMMENT ON COLUMN promotion_decisions.proposal_id IS 'One authoritative decision path per proposal id. Do not create a second row.';
COMMENT ON COLUMN promotion_decisions.proposal_manifest_hash IS 'Proposal manifest hash that must match the validated evidence set.';
COMMENT ON COLUMN promotion_decisions.decision IS 'Gate outcome. Accept, reject, or block are the only allowed canonical outcomes.';

CREATE TABLE model_profiles (
    model_profile_id text PRIMARY KEY,
    profile_name text NOT NULL,
    provider_name text NOT NULL,
    model_name text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    status text NOT NULL,
    budget_policy jsonb NOT NULL,
    routing_policy jsonb NOT NULL,
    default_parameters jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    supersedes_model_profile_id text,
    created_at timestamptz NOT NULL,
    CONSTRAINT model_profiles_supersedes_model_profile_id_fkey
        FOREIGN KEY (supersedes_model_profile_id)
        REFERENCES model_profiles (model_profile_id)
        ON DELETE RESTRICT
);

CREATE INDEX model_profiles_profile_name_status_idx
    ON model_profiles (profile_name, status);

CREATE INDEX model_profiles_provider_name_model_name_idx
    ON model_profiles (provider_name, model_name);

CREATE INDEX model_profiles_effective_from_idx
    ON model_profiles (effective_from DESC);

COMMENT ON TABLE model_profiles IS 'Canonical model and provider resolution records. Owned by registry/.';
COMMENT ON COLUMN model_profiles.supersedes_model_profile_id IS 'Version chain pointer only. Do not silently edit an active profile in place.';

CREATE TABLE provider_policies (
    provider_policy_id text PRIMARY KEY,
    policy_name text NOT NULL,
    provider_name text NOT NULL,
    scope text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    status text NOT NULL,
    allowed_models jsonb NOT NULL,
    retry_policy jsonb NOT NULL,
    budget_policy jsonb NOT NULL,
    routing_rules jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL
);

CREATE INDEX provider_policies_scope_provider_name_status_idx
    ON provider_policies (scope, provider_name, status);

CREATE INDEX provider_policies_provider_name_effective_from_idx
    ON provider_policies (provider_name, effective_from DESC);

CREATE INDEX provider_policies_decision_ref_idx
    ON provider_policies (decision_ref);

COMMENT ON TABLE provider_policies IS 'Canonical provider selection and usage rules. Owned by policy/.';
COMMENT ON COLUMN provider_policies.decision_ref IS 'Policy decision reference only. Fail closed if no policy covers the requested path.';
