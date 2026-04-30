-- Migration 365: Task environment contract authority.

BEGIN;

CREATE TABLE IF NOT EXISTS task_environment_contract_heads (
    contract_id text PRIMARY KEY,
    task_ref text NOT NULL,
    hierarchy_node_id text NOT NULL,
    status text NOT NULL,
    current_revision_id text NOT NULL,
    current_contract_hash text NOT NULL,
    dependency_hash text,
    owner_ref text,
    steward_ref text,
    evaluation_status text NOT NULL,
    invalid_state_count integer NOT NULL DEFAULT 0,
    warning_count integer NOT NULL DEFAULT 0,
    contract_json jsonb NOT NULL,
    evaluation_result_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_task_environment_contract_heads_task
    ON task_environment_contract_heads (task_ref, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_environment_contract_heads_hash
    ON task_environment_contract_heads (current_contract_hash);

CREATE TABLE IF NOT EXISTS task_environment_contract_revisions (
    contract_id text NOT NULL REFERENCES task_environment_contract_heads(contract_id) ON DELETE CASCADE,
    revision_id text NOT NULL,
    revision_no integer NOT NULL,
    parent_revision_id text,
    contract_hash text NOT NULL,
    dependency_hash text,
    status text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    contract_json jsonb NOT NULL,
    evaluation_result_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, revision_id)
);

CREATE INDEX IF NOT EXISTS idx_task_environment_contract_revisions_order
    ON task_environment_contract_revisions (contract_id, revision_no);

CREATE TABLE IF NOT EXISTS task_environment_hierarchy_nodes (
    contract_id text NOT NULL REFERENCES task_environment_contract_heads(contract_id) ON DELETE CASCADE,
    node_id text NOT NULL,
    revision_id text NOT NULL,
    parent_node_id text,
    node_type text NOT NULL,
    node_name text NOT NULL,
    status text NOT NULL,
    owner_ref text,
    steward_ref text,
    node_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, node_id, revision_id)
);

CREATE INDEX IF NOT EXISTS idx_task_environment_hierarchy_nodes_parent
    ON task_environment_hierarchy_nodes (contract_id, parent_node_id);

CREATE TABLE IF NOT EXISTS task_environment_contract_invalid_states (
    contract_id text NOT NULL REFERENCES task_environment_contract_heads(contract_id) ON DELETE CASCADE,
    revision_id text NOT NULL,
    state_index integer NOT NULL,
    reason_code text NOT NULL,
    severity text NOT NULL,
    field_ref text,
    state_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, revision_id, state_index),
    FOREIGN KEY (contract_id, revision_id)
        REFERENCES task_environment_contract_revisions(contract_id, revision_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_environment_contract_invalid_states_reason
    ON task_environment_contract_invalid_states (reason_code, severity);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'task_environment_contract_invalid_states_revision_fkey'
    ) THEN
        ALTER TABLE task_environment_contract_invalid_states
            ADD CONSTRAINT task_environment_contract_invalid_states_revision_fkey
            FOREIGN KEY (contract_id, revision_id)
            REFERENCES task_environment_contract_revisions(contract_id, revision_id)
            ON DELETE CASCADE;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION touch_task_environment_contract_heads_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_task_environment_contract_heads_touch ON task_environment_contract_heads;
CREATE TRIGGER trg_task_environment_contract_heads_touch
    BEFORE UPDATE ON task_environment_contract_heads
    FOR EACH ROW EXECUTE FUNCTION touch_task_environment_contract_heads_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('task_environment_contract_heads', 'Task environment contract heads', 'table', 'Current task-environment contract heads with active revision, hashes, evaluation status, and ownership.', '{"migration":"365_task_environment_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.object_truth"}'::jsonb),
    ('task_environment_contract_revisions', 'Task environment contract revisions', 'table', 'Append-only task-environment contract revision rows with contract/dependency hashes and evaluation payloads.', '{"migration":"365_task_environment_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.object_truth"}'::jsonb),
    ('task_environment_hierarchy_nodes', 'Task environment hierarchy nodes', 'table', 'Hierarchy nodes captured with task-environment contract revisions for path/accountability inspection.', '{"migration":"365_task_environment_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.object_truth"}'::jsonb),
    ('task_environment_contract_invalid_states', 'Task environment contract invalid states', 'table', 'Typed invalid states and warnings emitted by task-environment contract evaluation.', '{"migration":"365_task_environment_contract_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.object_truth"}'::jsonb)
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
    ('table.public.task_environment_contract_heads', 'table', 'task_environment_contract_heads', 'public', 'authority.object_truth', 'task_environment_contract_heads', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.task_environment_contract_revisions', 'table', 'task_environment_contract_revisions', 'public', 'authority.object_truth', 'task_environment_contract_revisions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.task_environment_hierarchy_nodes', 'table', 'task_environment_hierarchy_nodes', 'public', 'authority.object_truth', 'task_environment_hierarchy_nodes', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.task_environment_contract_invalid_states', 'table', 'task_environment_contract_invalid_states', 'public', 'authority.object_truth', 'task_environment_contract_invalid_states', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.task_environment_contract.recorded',
    'task_environment_contract.recorded',
    'authority.object_truth',
    'data_dictionary.object.task_environment_contract_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["contract_id","task_ref","revision_id","contract_hash","dependency_hash","evaluation_status","invalid_state_count","warning_count","hierarchy_node_count"]}'::jsonb
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
    p_operation_ref         := 'task_environment.command.contract_record',
    p_operation_name        := 'task_environment_contract_record',
    p_handler_ref           := 'runtime.operations.commands.task_environment_contracts.handle_task_environment_contract_record',
    p_input_model_ref       := 'runtime.operations.commands.task_environment_contracts.RecordTaskEnvironmentContractCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/task-environment/contracts',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'task_environment_contract.recorded',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.task_environment_contract_record.20260430',
    p_label                 := 'Task Environment Contract Record',
    p_summary               := 'Record receipt-backed task-environment contract heads, revisions, hierarchy nodes, and evaluation invalid states.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'task_environment.query.contract_read',
    p_operation_name        := 'task_environment_contract_read',
    p_handler_ref           := 'runtime.operations.queries.task_environment_contracts.handle_task_environment_contract_read',
    p_input_model_ref       := 'runtime.operations.queries.task_environment_contracts.QueryTaskEnvironmentContractRead',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/task-environment/contracts',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.task_environment_contract_read.20260430',
    p_label                 := 'Task Environment Contract Read',
    p_summary               := 'Read queryable task-environment contract heads, revisions, hierarchy nodes, and typed invalid states.'
);

COMMIT;
