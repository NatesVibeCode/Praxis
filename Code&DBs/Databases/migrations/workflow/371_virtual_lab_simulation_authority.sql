-- Migration 371: Virtual Lab simulation authority.

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
    'authority.virtual_lab_simulation',
    'praxis.engine',
    'stream.authority.virtual_lab_simulation',
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

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_runs (
    run_id text PRIMARY KEY,
    scenario_id text NOT NULL,
    scenario_digest text NOT NULL,
    config_digest text NOT NULL,
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    revision_digest text NOT NULL,
    status text NOT NULL CHECK (status IN ('passed', 'failed', 'blocked')),
    stop_reason text NOT NULL,
    trace_digest text NOT NULL,
    result_digest text NOT NULL,
    runtime_version text NOT NULL,
    action_count integer NOT NULL DEFAULT 0,
    runtime_event_count integer NOT NULL DEFAULT 0,
    state_event_count integer NOT NULL DEFAULT 0,
    transition_count integer NOT NULL DEFAULT 0,
    automation_evaluation_count integer NOT NULL DEFAULT 0,
    automation_firing_count integer NOT NULL DEFAULT 0,
    assertion_count integer NOT NULL DEFAULT 0,
    verifier_count integer NOT NULL DEFAULT 0,
    typed_gap_count integer NOT NULL DEFAULT 0,
    blocker_count integer NOT NULL DEFAULT 0,
    task_contract_ref text,
    integration_action_contract_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    automation_snapshot_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    scenario_json jsonb NOT NULL,
    result_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (environment_id, revision_id)
        REFERENCES virtual_lab_environment_revisions(environment_id, revision_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_runs_environment
    ON virtual_lab_simulation_runs (environment_id, revision_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_runs_status
    ON virtual_lab_simulation_runs (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_runs_scenario
    ON virtual_lab_simulation_runs (scenario_id, scenario_digest);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_runtime_events (
    run_id text NOT NULL,
    event_id text NOT NULL,
    sequence_number integer NOT NULL CHECK (sequence_number > 0),
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    source_area text NOT NULL,
    causation_id text,
    correlation_id text NOT NULL,
    event_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, event_id),
    UNIQUE (run_id, sequence_number),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_runtime_events_type
    ON virtual_lab_simulation_runtime_events (event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_runtime_events_source
    ON virtual_lab_simulation_runtime_events (source_area, occurred_at DESC);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_state_events (
    run_id text NOT NULL,
    event_id text NOT NULL,
    environment_id text NOT NULL,
    revision_id text NOT NULL,
    stream_id text NOT NULL,
    event_type text NOT NULL,
    sequence_number integer NOT NULL CHECK (sequence_number > 0),
    command_id text NOT NULL,
    pre_state_digest text NOT NULL,
    post_state_digest text NOT NULL,
    event_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, event_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_state_events_stream
    ON virtual_lab_simulation_state_events (run_id, stream_id, sequence_number);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_transitions (
    run_id text NOT NULL,
    transition_id text NOT NULL,
    object_id text NOT NULL,
    instance_id text NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    sequence_number integer NOT NULL CHECK (sequence_number > 0),
    pre_state_digest text NOT NULL,
    post_state_digest text NOT NULL,
    causation_id text,
    action_id text NOT NULL,
    transition_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, transition_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_transitions_object
    ON virtual_lab_simulation_transitions (run_id, object_id, instance_id, sequence_number);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_action_results (
    run_id text NOT NULL,
    action_id text NOT NULL,
    action_kind text NOT NULL,
    status text NOT NULL,
    command_id text NOT NULL,
    receipt_status text,
    result_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, action_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_action_results_status
    ON virtual_lab_simulation_action_results (status, action_kind);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_automation_evaluations (
    evaluation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text NOT NULL,
    rule_id text NOT NULL,
    triggering_event_id text NOT NULL,
    eligible boolean NOT NULL,
    reason_code text NOT NULL,
    evaluation_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_automation_evaluations_rule
    ON virtual_lab_simulation_automation_evaluations (run_id, rule_id, eligible);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_automation_firings (
    run_id text NOT NULL,
    firing_id text NOT NULL,
    rule_id text NOT NULL,
    triggering_event_id text NOT NULL,
    recursion_depth integer NOT NULL,
    firing_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, firing_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_automation_firings_rule
    ON virtual_lab_simulation_automation_firings (run_id, rule_id, recursion_depth);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_assertion_results (
    run_id text NOT NULL,
    assertion_id text NOT NULL,
    assertion_kind text NOT NULL,
    passed boolean NOT NULL,
    severity text NOT NULL,
    result_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, assertion_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_assertion_results_passed
    ON virtual_lab_simulation_assertion_results (passed, severity);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_verifier_results (
    run_id text NOT NULL,
    verifier_id text NOT NULL,
    verifier_kind text NOT NULL,
    status text NOT NULL,
    severity text NOT NULL,
    result_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, verifier_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_verifier_results_status
    ON virtual_lab_simulation_verifier_results (status, severity);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_typed_gaps (
    run_id text NOT NULL,
    gap_id text NOT NULL,
    code text NOT NULL,
    severity text NOT NULL,
    source_area text NOT NULL,
    trace_event_id text,
    gap_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, gap_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_typed_gaps_code
    ON virtual_lab_simulation_typed_gaps (code, severity, source_area);

CREATE TABLE IF NOT EXISTS virtual_lab_simulation_promotion_blockers (
    run_id text NOT NULL,
    blocker_id text NOT NULL,
    code text NOT NULL,
    source_area text NOT NULL,
    gap_id text,
    trace_event_id text,
    blocker_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, blocker_id),
    FOREIGN KEY (run_id)
        REFERENCES virtual_lab_simulation_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_virtual_lab_simulation_promotion_blockers_code
    ON virtual_lab_simulation_promotion_blockers (code, source_area);

CREATE OR REPLACE FUNCTION touch_virtual_lab_simulation_runs_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_virtual_lab_simulation_runs_touch ON virtual_lab_simulation_runs;
CREATE TRIGGER trg_virtual_lab_simulation_runs_touch
    BEFORE UPDATE ON virtual_lab_simulation_runs
    FOR EACH ROW EXECUTE FUNCTION touch_virtual_lab_simulation_runs_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('virtual_lab_simulation_runs', 'Virtual Lab simulation runs', 'table', 'Receipt-backed simulation runs with scenario/result digests, environment revision refs, final verdict counts, contract refs, and full scenario/result JSON.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_runtime_events', 'Virtual Lab simulation runtime events', 'table', 'Ordered runtime trace events emitted during simulated actions, automations, assertions, verifiers, transitions, and guardrails.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_state_events', 'Virtual Lab simulation state events', 'table', 'Predicted Virtual Lab state event envelopes produced by a simulation run.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_transitions', 'Virtual Lab simulation transitions', 'table', 'Per-object state transition records with pre/post state digests and causating action refs.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_action_results', 'Virtual Lab simulation action results', 'table', 'Typed simulated action outcomes with command ids, receipt statuses, warnings, gaps, and blockers.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_automation_evaluations', 'Virtual Lab simulation automation evaluations', 'table', 'Per-rule automation eligibility decisions for each triggering event in a simulation trace.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_automation_firings', 'Virtual Lab simulation automation firings', 'table', 'Automation rule firings with triggering event refs, effect action refs, and recursion depth.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_assertion_results', 'Virtual Lab simulation assertion results', 'table', 'Final-state and trace assertion results that explain pass/fail outcomes.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_verifier_results', 'Virtual Lab simulation verifier results', 'table', 'Independent verifier results that block green promotion when missing or failed.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_typed_gaps', 'Virtual Lab simulation typed gaps', 'table', 'Typed unresolved simulation behavior, unsupported capabilities, guardrail failures, and proof gaps.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb),
    ('virtual_lab_simulation_promotion_blockers', 'Virtual Lab simulation promotion blockers', 'table', 'Promotion-blocking simulation findings that prevent live sandbox rollout until resolved.', '{"migration":"371_virtual_lab_simulation_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.virtual_lab_simulation"}'::jsonb)
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
    ('table.public.virtual_lab_simulation_runs', 'table', 'virtual_lab_simulation_runs', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_runs', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_runtime_events', 'table', 'virtual_lab_simulation_runtime_events', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_runtime_events', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_state_events', 'table', 'virtual_lab_simulation_state_events', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_state_events', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_transitions', 'table', 'virtual_lab_simulation_transitions', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_transitions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_action_results', 'table', 'virtual_lab_simulation_action_results', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_action_results', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_automation_evaluations', 'table', 'virtual_lab_simulation_automation_evaluations', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_automation_evaluations', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_automation_firings', 'table', 'virtual_lab_simulation_automation_firings', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_automation_firings', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_assertion_results', 'table', 'virtual_lab_simulation_assertion_results', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_assertion_results', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_verifier_results', 'table', 'virtual_lab_simulation_verifier_results', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_verifier_results', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_typed_gaps', 'table', 'virtual_lab_simulation_typed_gaps', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_typed_gaps', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.virtual_lab_simulation_promotion_blockers', 'table', 'virtual_lab_simulation_promotion_blockers', 'public', 'authority.virtual_lab_simulation', 'virtual_lab_simulation_promotion_blockers', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.virtual_lab_simulation.completed',
    'virtual_lab_simulation.completed',
    'authority.virtual_lab_simulation',
    'data_dictionary.object.virtual_lab_simulation_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["run_id","scenario_id","status","stop_reason","scenario_digest","trace_digest","result_digest","action_count","verifier_count","blocker_count","environment_id","revision_id"]}'::jsonb
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
    p_operation_ref         := 'virtual_lab.command.simulation_run',
    p_operation_name        := 'virtual_lab_simulation_run',
    p_handler_ref           := 'runtime.operations.commands.virtual_lab_simulation.handle_virtual_lab_simulation_run',
    p_input_model_ref       := 'runtime.operations.commands.virtual_lab_simulation.RunVirtualLabSimulationCommand',
    p_authority_domain_ref  := 'authority.virtual_lab_simulation',
    p_authority_ref         := 'authority.virtual_lab_simulation',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/virtual-lab/simulations',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'virtual_lab_simulation.completed',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_simulation_run.20260430',
    p_label                 := 'Virtual Lab Simulation Run',
    p_summary               := 'Run and persist deterministic Virtual Lab simulations with traces, transitions, automation firings, assertions, verifier results, gaps, and promotion blockers.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'virtual_lab.query.simulation_read',
    p_operation_name        := 'virtual_lab_simulation_read',
    p_handler_ref           := 'runtime.operations.queries.virtual_lab_simulation.handle_virtual_lab_simulation_read',
    p_input_model_ref       := 'runtime.operations.queries.virtual_lab_simulation.QueryVirtualLabSimulationRead',
    p_authority_domain_ref  := 'authority.virtual_lab_simulation',
    p_authority_ref         := 'authority.virtual_lab_simulation',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/virtual-lab/simulations',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.virtual_lab_simulation_read.20260430',
    p_label                 := 'Virtual Lab Simulation Read',
    p_summary               := 'Read persisted Virtual Lab simulation runs, events, verifier results, typed gaps, and promotion blockers.'
);

COMMIT;
