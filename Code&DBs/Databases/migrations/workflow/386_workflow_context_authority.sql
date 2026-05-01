-- Migration 386: Workflow Context authority and Object Truth latest lookup.
--
-- CQRS Forge build report:
-- - workflow_context_compile: receipt 34f5f1a5-c01a-48fc-be3e-05b567b7ac56,
--   operation_ref workflow-context-compile, command, event workflow_context.compiled.
-- - workflow_context_read: receipt 901bc8ca-4432-49cf-9112-65e469475978,
--   operation_ref workflow-context-read, query, read_only.
-- - workflow_context_transition: receipt 85c45a4b-6e15-4d55-a4d0-d35550bd4973,
--   operation_ref workflow-context-transition, command, event workflow_context.transitioned.
-- - workflow_context_bind: receipt 9b4188b4-cad2-4c74-8b96-0a26e89eef4d,
--   operation_ref workflow-context-bind, command, event workflow_context.bound.
-- - workflow_context_guardrail_check: receipt feed9a4c-a0b3-4912-afc6-2a244db9fbb5,
--   operation_ref workflow-context-guardrail-check, query, read_only.
-- - object_truth_latest_version_read: receipt 4fc66691-6269-4825-a6c6-fd8a02e61e2d,
--   operation_ref object-truth-latest-version-read, query, read_only.

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
    'authority.workflow_context',
    'praxis.engine',
    'stream.authority.workflow_context',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS workflow_context_packs (
    context_ref text PRIMARY KEY CHECK (btrim(context_ref) <> ''),
    workflow_ref text CHECK (workflow_ref IS NULL OR btrim(workflow_ref) <> ''),
    context_mode text NOT NULL CHECK (context_mode IN ('standalone', 'inferred', 'synthetic', 'bound', 'hybrid')),
    truth_state text NOT NULL CHECK (truth_state IN ('none', 'inferred', 'synthetic', 'documented', 'anonymized_operational', 'schema_bound', 'observed', 'verified', 'promoted', 'stale', 'contradicted', 'blocked')),
    seed text NOT NULL CHECK (btrim(seed) <> ''),
    intent text NOT NULL CHECK (btrim(intent) <> ''),
    graph_ref text CHECK (graph_ref IS NULL OR btrim(graph_ref) <> ''),
    source_prompt_ref text CHECK (source_prompt_ref IS NULL OR btrim(source_prompt_ref) <> ''),
    confidence_score numeric(8, 4) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    confidence_state text NOT NULL CHECK (btrim(confidence_state) <> ''),
    unknown_mutator_risk boolean NOT NULL DEFAULT FALSE,
    scenario_pack_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    compiled_from_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    blockers_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    verifier_expectations_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    confidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    guardrail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    review_packet_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    synthetic_world_json jsonb,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_by_ref text CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_context_packs_workflow
    ON workflow_context_packs (workflow_ref, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_context_packs_truth_state
    ON workflow_context_packs (truth_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_context_packs_metadata_gin
    ON workflow_context_packs USING gin (metadata_json);

CREATE TABLE IF NOT EXISTS workflow_context_entities (
    entity_ref text PRIMARY KEY CHECK (btrim(entity_ref) <> ''),
    context_ref text NOT NULL REFERENCES workflow_context_packs (context_ref) ON DELETE CASCADE,
    entity_kind text NOT NULL CHECK (btrim(entity_kind) <> ''),
    label text NOT NULL CHECK (btrim(label) <> ''),
    truth_state text NOT NULL CHECK (truth_state IN ('none', 'inferred', 'synthetic', 'documented', 'anonymized_operational', 'schema_bound', 'observed', 'verified', 'promoted', 'stale', 'contradicted', 'blocked')),
    io_mode text NOT NULL CHECK (io_mode IN ('none', 'inferred', 'synthetic', 'bound', 'runtime_generated', 'hybrid')),
    payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    confidence_score numeric(8, 4) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_context_entities_context_kind
    ON workflow_context_entities (context_ref, entity_kind, label);

CREATE INDEX IF NOT EXISTS idx_workflow_context_entities_payload_gin
    ON workflow_context_entities USING gin (payload_json);

CREATE TABLE IF NOT EXISTS workflow_context_bindings (
    binding_ref text PRIMARY KEY CHECK (btrim(binding_ref) <> ''),
    context_ref text NOT NULL REFERENCES workflow_context_packs (context_ref) ON DELETE CASCADE,
    entity_ref text NOT NULL REFERENCES workflow_context_entities (entity_ref) ON DELETE CASCADE,
    target_authority_domain text NOT NULL CHECK (btrim(target_authority_domain) <> ''),
    target_ref text NOT NULL CHECK (btrim(target_ref) <> ''),
    binding_state text NOT NULL CHECK (binding_state IN ('proposed', 'accepted', 'rejected', 'revoked')),
    risk_level text NOT NULL CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    requires_review boolean NOT NULL DEFAULT FALSE,
    reversible boolean NOT NULL DEFAULT TRUE,
    reviewed_by_ref text CHECK (reviewed_by_ref IS NULL OR btrim(reviewed_by_ref) <> ''),
    confidence_score numeric(8, 4) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    evidence_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    confidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    guardrail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_by_ref text CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_context_bindings_context
    ON workflow_context_bindings (context_ref, binding_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_context_bindings_target
    ON workflow_context_bindings (target_authority_domain, target_ref);

CREATE TABLE IF NOT EXISTS workflow_context_transitions (
    transition_ref text PRIMARY KEY CHECK (btrim(transition_ref) <> ''),
    context_ref text NOT NULL REFERENCES workflow_context_packs (context_ref) ON DELETE CASCADE,
    from_truth_state text NOT NULL CHECK (from_truth_state IN ('none', 'inferred', 'synthetic', 'documented', 'anonymized_operational', 'schema_bound', 'observed', 'verified', 'promoted', 'stale', 'contradicted', 'blocked')),
    to_truth_state text NOT NULL CHECK (to_truth_state IN ('none', 'inferred', 'synthetic', 'documented', 'anonymized_operational', 'schema_bound', 'observed', 'verified', 'promoted', 'stale', 'contradicted', 'blocked')),
    transition_reason text NOT NULL CHECK (btrim(transition_reason) <> ''),
    decision_ref text CHECK (decision_ref IS NULL OR btrim(decision_ref) <> ''),
    risk_disposition text CHECK (risk_disposition IS NULL OR btrim(risk_disposition) <> ''),
    evidence_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    guardrail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_by_ref text CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_context_transitions_context
    ON workflow_context_transitions (context_ref, created_at DESC);

CREATE OR REPLACE FUNCTION touch_workflow_context_packs_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_workflow_context_packs_touch ON workflow_context_packs;
CREATE TRIGGER trg_workflow_context_packs_touch
    BEFORE UPDATE ON workflow_context_packs
    FOR EACH ROW EXECUTE FUNCTION touch_workflow_context_packs_updated_at();

CREATE OR REPLACE FUNCTION touch_workflow_context_entities_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_workflow_context_entities_touch ON workflow_context_entities;
CREATE TRIGGER trg_workflow_context_entities_touch
    BEFORE UPDATE ON workflow_context_entities
    FOR EACH ROW EXECUTE FUNCTION touch_workflow_context_entities_updated_at();

CREATE OR REPLACE FUNCTION touch_workflow_context_bindings_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_workflow_context_bindings_touch ON workflow_context_bindings;
CREATE TRIGGER trg_workflow_context_bindings_touch
    BEFORE UPDATE ON workflow_context_bindings
    FOR EACH ROW EXECUTE FUNCTION touch_workflow_context_bindings_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('workflow_context_packs', 'Workflow Context packs', 'table', 'Durable Workflow Context packs with inferred, synthetic, bound, verified, and promoted truth state.', '{"migration":"386_workflow_context_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.workflow_context"}'::jsonb),
    ('workflow_context_entities', 'Workflow Context entities', 'table', 'Systems, objects, fields, events, actions, records, failures, and workflow nodes inferred or bound inside Workflow Context.', '{"migration":"386_workflow_context_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.workflow_context"}'::jsonb),
    ('workflow_context_bindings', 'Workflow Context bindings', 'table', 'Reversible bindings from Workflow Context entities to Object Truth or other authority refs.', '{"migration":"386_workflow_context_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.workflow_context"}'::jsonb),
    ('workflow_context_transitions', 'Workflow Context transitions', 'table', 'Guardrail-evaluated truth-state transition receipts for Workflow Context packs.', '{"migration":"386_workflow_context_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.workflow_context"}'::jsonb)
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
    ('table.public.workflow_context_packs', 'table', 'workflow_context_packs', 'public', 'authority.workflow_context', 'workflow_context_packs', 'active', 'registry', 'praxis.engine', 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"purpose":"workflow context pack authority"}'::jsonb),
    ('table.public.workflow_context_entities', 'table', 'workflow_context_entities', 'public', 'authority.workflow_context', 'workflow_context_entities', 'active', 'registry', 'praxis.engine', 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"purpose":"workflow context entity projection"}'::jsonb),
    ('table.public.workflow_context_bindings', 'table', 'workflow_context_bindings', 'public', 'authority.workflow_context', 'workflow_context_bindings', 'active', 'registry', 'praxis.engine', 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"purpose":"workflow context binding ledger"}'::jsonb),
    ('table.public.workflow_context_transitions', 'table', 'workflow_context_transitions', 'public', 'authority.workflow_context', 'workflow_context_transitions', 'active', 'registry', 'praxis.engine', 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"purpose":"workflow context transition ledger"}'::jsonb)
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
) VALUES
    ('event_contract.workflow_context.compiled', 'workflow_context.compiled', 'authority.workflow_context', 'data_dictionary.object.workflow_context_compiled_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"aggregate_ref_field":"context_ref","expected_payload_fields":["context_ref","workflow_ref","context_mode","truth_state","confidence_score","entity_count","scenario_pack_refs"]}'::jsonb),
    ('event_contract.workflow_context.transitioned', 'workflow_context.transitioned', 'authority.workflow_context', 'data_dictionary.object.workflow_context_transitioned_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"aggregate_ref_field":"context_ref","expected_payload_fields":["context_ref","transition_ref","from_truth_state","to_truth_state","confidence_score"]}'::jsonb),
    ('event_contract.workflow_context.bound', 'workflow_context.bound', 'authority.workflow_context', 'data_dictionary.object.workflow_context_bound_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority', '{"aggregate_ref_field":"context_ref","expected_payload_fields":["context_ref","binding_ref","entity_ref","target_authority_domain","target_ref","binding_state","requires_review"]}'::jsonb)
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
    p_operation_ref         := 'workflow-context-compile',
    p_operation_name        := 'workflow_context_compile',
    p_handler_ref           := 'runtime.operations.commands.workflow_context.handle_workflow_context_compile',
    p_input_model_ref       := 'runtime.operations.commands.workflow_context.CompileWorkflowContextCommand',
    p_authority_domain_ref  := 'authority.workflow_context',
    p_authority_ref         := 'authority.workflow_context',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workflow-context/compile',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workflow_context.compiled',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_context_compile.20260430',
    p_label                 := 'Workflow Context Compile',
    p_summary               := 'Compile inferred and optional synthetic workflow context through Workflow Context Authority.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-context-read',
    p_operation_name        := 'workflow_context_read',
    p_handler_ref           := 'runtime.operations.queries.workflow_context.handle_workflow_context_read',
    p_input_model_ref       := 'runtime.operations.queries.workflow_context.QueryWorkflowContextRead',
    p_authority_domain_ref  := 'authority.workflow_context',
    p_authority_ref         := 'authority.workflow_context',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workflow-context',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_context_read.20260430',
    p_label                 := 'Workflow Context Read',
    p_summary               := 'Read workflow context packs, entities, bindings, guardrails, and review packets.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-context-transition',
    p_operation_name        := 'workflow_context_transition',
    p_handler_ref           := 'runtime.operations.commands.workflow_context.handle_workflow_context_transition',
    p_input_model_ref       := 'runtime.operations.commands.workflow_context.TransitionWorkflowContextCommand',
    p_authority_domain_ref  := 'authority.workflow_context',
    p_authority_ref         := 'authority.workflow_context',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workflow-context/transition',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workflow_context.transitioned',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_context_transition.20260430',
    p_label                 := 'Workflow Context Transition',
    p_summary               := 'Transition Workflow Context truth state through backend guardrail policy.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-context-bind',
    p_operation_name        := 'workflow_context_bind',
    p_handler_ref           := 'runtime.operations.commands.workflow_context.handle_workflow_context_bind',
    p_input_model_ref       := 'runtime.operations.commands.workflow_context.BindWorkflowContextCommand',
    p_authority_domain_ref  := 'authority.workflow_context',
    p_authority_ref         := 'authority.workflow_context',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workflow-context/bind',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workflow_context.bound',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_context_bind.20260430',
    p_label                 := 'Workflow Context Bind',
    p_summary               := 'Bind workflow context entities to Object Truth or external authority refs.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-context-guardrail-check',
    p_operation_name        := 'workflow_context_guardrail_check',
    p_handler_ref           := 'runtime.operations.queries.workflow_context.handle_workflow_context_guardrail_check',
    p_input_model_ref       := 'runtime.operations.queries.workflow_context.QueryWorkflowContextGuardrailCheck',
    p_authority_domain_ref  := 'authority.workflow_context',
    p_authority_ref         := 'authority.workflow_context',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workflow-context/guardrails',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_context_guardrail_check.20260430',
    p_label                 := 'Workflow Context Guardrail Check',
    p_summary               := 'Read allowed next actions and blockers for workflow context truth-state transitions.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'object-truth-latest-version-read',
    p_operation_name        := 'object_truth_latest_version_read',
    p_handler_ref           := 'runtime.operations.queries.object_truth_latest.handle_object_truth_latest_version_read',
    p_input_model_ref       := 'runtime.operations.queries.object_truth_latest.QueryObjectTruthLatestVersionRead',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/object-truth/latest-version',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::authority-workflow-context::workflow-context-standalone-synthetic-binding-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_latest_version_read.20260430',
    p_label                 := 'Object Truth Latest Version Read',
    p_summary               := 'Read latest trusted Object Truth without requiring callers to know exact version digests.'
);

COMMIT;
