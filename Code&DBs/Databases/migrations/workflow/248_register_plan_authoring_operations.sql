-- Migration 248: Register the LLM-authored compile pipeline operations
-- in operation_catalog_registry + authority_object_registry, plus the
-- two new conceptual events the pipeline emits.
--
-- Operator standing order (from this conversation): every workflow item
-- needs a reflected item in the DB CQRS — every field and function.
-- This migration brings the suggester / synthesizer / section-author /
-- validator / compose_via_llm functions into CQRS visibility so they
-- become receipt-backed and replayable like compose_plan and launch_plan
-- already are after migration 234.
--
-- Registers (operation_kind in parentheses):
--   - praxis_suggest_plan_atoms     (query)
--   - praxis_synthesize_skeleton    (query)
--   - praxis_author_plan_section    (command, emits plan_section.authored)
--   - praxis_validate_authored_plan (query)
--   - praxis_compose_plan_via_llm   (command, emits plan.composed)
--
-- Adds event contracts:
--   - plan_section.authored — one row per per-section LLM call

BEGIN;

-- =====================================================================
-- Event contracts
-- =====================================================================
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
    'event_contract.plan_section.authored',
    'plan_section.authored',
    'authority.workflow_runs',
    'data_dictionary.object.plan_section_authored_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Per-section LLM author emission. One event per packet authored by author_plan_section.',
        'expected_payload_fields', jsonb_build_array(
            'packet_label',
            'stage',
            'provider_slug',
            'model_slug',
            'every_required_filled',
            'no_forbidden_placeholders',
            'no_dropped_floors',
            'duration_ms'
        )
    )
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- data_dictionary_objects entries (operations + event payloads)
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('operation.suggest_plan_atoms', 'Operation: suggest_plan_atoms', 'command',
     'Layer 0 (Suggest) of the planning stack. Deterministic — pills + step types + parameters from free prose.',
     '{"source":"migration.248","authority":"runtime.intent_suggestion"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.intent_suggestion.suggest_plan_atoms"}'::jsonb),

    ('operation.synthesize_skeleton', 'Operation: synthesize_skeleton', 'command',
     'Layer 0.5 (Synthesize) of the planning stack. Deterministic — depends_on / floors / gate scaffolds from atoms.',
     '{"source":"migration.248","authority":"runtime.intent_dependency"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.intent_dependency.synthesize_skeleton"}'::jsonb),

    ('operation.author_plan_section', 'Operation: author_plan_section', 'command',
     'Layer 4 (Author) of the planning stack. LLM call — fills one packet section in full menu-level detail.',
     '{"source":"migration.248","authority":"runtime.plan_section_author"}'::jsonb,
     '{"operation_kind":"command","handler_ref":"runtime.plan_section_author.author_plan_section","emits":"plan_section.authored"}'::jsonb),

    ('operation.validate_authored_plan', 'Operation: validate_authored_plan', 'command',
     'Layer 5 (Validate) of the planning stack. Deterministic — confirms every plan_field is filled and no floor is dropped.',
     '{"source":"migration.248","authority":"runtime.plan_section_validator"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.plan_section_validator.validate_authored_plan"}'::jsonb),

    ('operation.compose_plan_via_llm', 'Operation: compose_plan_via_llm', 'command',
     'End-to-end LLM-authored compile. Chains atoms → skeleton → parallel section author → validate.',
     '{"source":"migration.248","authority":"runtime.compose_plan_via_llm"}'::jsonb,
     '{"operation_kind":"command","handler_ref":"runtime.compose_plan_via_llm.compose_plan_via_llm","emits":"plan.composed"}'::jsonb),

    ('plan_section_authored_event', 'plan_section.authored event payload', 'event',
     'Conceptual event emitted by author_plan_section per packet authored by the per-section LLM.',
     '{"source":"migration.248"}'::jsonb,
     '{"event_type":"plan_section.authored","payload_fields":["packet_label","stage","provider_slug","model_slug","every_required_filled","no_forbidden_placeholders","no_dropped_floors","duration_ms"]}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- authority_object_registry entries
-- =====================================================================
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
    ('operation.suggest_plan_atoms', 'command', 'suggest_plan_atoms', NULL,
     'authority.workflow_runs', 'operation.suggest_plan_atoms', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object('handler_ref', 'runtime.intent_suggestion.suggest_plan_atoms', 'source_kind', 'operation_query', 'operation_kind', 'query')),

    ('operation.synthesize_skeleton', 'command', 'synthesize_skeleton', NULL,
     'authority.workflow_runs', 'operation.synthesize_skeleton', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object('handler_ref', 'runtime.intent_dependency.synthesize_skeleton', 'source_kind', 'operation_query', 'operation_kind', 'query')),

    ('operation.author_plan_section', 'command', 'author_plan_section', NULL,
     'authority.workflow_runs', 'operation.author_plan_section', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object('handler_ref', 'runtime.plan_section_author.author_plan_section', 'source_kind', 'operation_command', 'event_type', 'plan_section.authored')),

    ('operation.validate_authored_plan', 'command', 'validate_authored_plan', NULL,
     'authority.workflow_runs', 'operation.validate_authored_plan', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object('handler_ref', 'runtime.plan_section_validator.validate_authored_plan', 'source_kind', 'operation_query', 'operation_kind', 'query')),

    ('operation.compose_plan_via_llm', 'command', 'compose_plan_via_llm', NULL,
     'authority.workflow_runs', 'operation.compose_plan_via_llm', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object('handler_ref', 'runtime.compose_plan_via_llm.compose_plan_via_llm', 'source_kind', 'operation_command', 'event_type', 'plan.composed'))

ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- operation_catalog_registry entries
-- =====================================================================
INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    posture,
    idempotency_policy,
    binding_revision,
    decision_ref,
    authority_domain_ref,
    storage_target_ref,
    input_schema_ref,
    output_schema_ref,
    receipt_required,
    event_required,
    event_type
) VALUES
    ('suggest-plan-atoms', 'suggest_plan_atoms', 'operation_query', 'query',
     'POST', '/api/suggest_plan_atoms',
     'runtime.intent_suggestion.SuggestPlanAtomsQuery',
     'runtime.intent_suggestion.suggest_plan_atoms',
     'authority.workflow_runs', 'observe', 'idempotent',
     'binding.operation_catalog_registry.suggest_plan_atoms.20260424',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.intent_suggestion.SuggestPlanAtomsQuery',
     'runtime.intent_suggestion.SuggestedAtoms',
     TRUE, FALSE, NULL),

    ('synthesize-skeleton', 'synthesize_skeleton', 'operation_query', 'query',
     'POST', '/api/synthesize_skeleton',
     'runtime.intent_dependency.SynthesizeSkeletonQuery',
     'runtime.intent_dependency.synthesize_skeleton',
     'authority.workflow_runs', 'observe', 'idempotent',
     'binding.operation_catalog_registry.synthesize_skeleton.20260424',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.intent_dependency.SynthesizeSkeletonQuery',
     'runtime.intent_dependency.SkeletalPlan',
     TRUE, FALSE, NULL),

    ('author-plan-section', 'author_plan_section', 'operation_command', 'command',
     'POST', '/api/author_plan_section',
     'runtime.plan_section_author.AuthorPlanSectionCommand',
     'runtime.plan_section_author.author_plan_section',
     'authority.workflow_runs', 'operate', 'non_idempotent',
     'binding.operation_catalog_registry.author_plan_section.20260424',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.plan_section_author.AuthorPlanSectionCommand',
     'runtime.plan_section_author.AuthoredPacket',
     TRUE, TRUE, 'plan_section.authored'),

    ('validate-authored-plan', 'validate_authored_plan', 'operation_query', 'query',
     'POST', '/api/validate_authored_plan',
     'runtime.plan_section_validator.ValidateAuthoredPlanQuery',
     'runtime.plan_section_validator.validate_authored_plan',
     'authority.workflow_runs', 'observe', 'idempotent',
     'binding.operation_catalog_registry.validate_authored_plan.20260424',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.plan_section_validator.ValidateAuthoredPlanQuery',
     'runtime.plan_section_validator.ValidationReport',
     TRUE, FALSE, NULL),

    ('compose-plan-via-llm', 'compose_plan_via_llm', 'operation_command', 'command',
     'POST', '/api/compose_plan_via_llm',
     'runtime.compose_plan_via_llm.ComposePlanViaLLMCommand',
     'runtime.compose_plan_via_llm.compose_plan_via_llm',
     'authority.workflow_runs', 'operate', 'non_idempotent',
     'binding.operation_catalog_registry.compose_plan_via_llm.20260424',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.compose_plan_via_llm.ComposePlanViaLLMCommand',
     'runtime.compose_plan_via_llm.ComposeViaLLMResult',
     TRUE, TRUE, 'plan.composed')

ON CONFLICT (operation_ref) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    event_required = EXCLUDED.event_required,
    receipt_required = EXCLUDED.receipt_required,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, event_type FROM operation_catalog_registry
--    WHERE operation_ref LIKE 'suggest%' OR operation_ref LIKE 'synthesize%'
--       OR operation_ref LIKE 'author-plan%' OR operation_ref LIKE 'validate-authored%'
--       OR operation_ref LIKE 'compose-plan-via-llm';
