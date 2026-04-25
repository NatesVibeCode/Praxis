-- Migration 228: Register launch_plan + compose_plan operations in
-- operation_catalog_registry, and add the plan.composed event contract.
--
-- Context: CLAUDE.md Phase 1 follow-up. Migration 224 registered the
-- plan.launched authority event contract; migration 226 registered
-- typed_gap.created. Both events still emit through the
-- emit_system_event observability sidecar (system_events table) instead
-- of the receipt-backed authority_events stream because the operations
-- that produce them (launch_plan, compose_plan_from_intent) were never
-- catalog-registered. Per
-- architecture-policy::platform-architecture::conceptual-events-register-
-- through-operation-catalog-registry (decision.architecture_policy.
-- platform_architecture.conceptual_events_register_through_operation_
-- catalog_registry), conceptual events must be CQRS-visible: registered
-- contract + registered operation + receipt-backed emission.
--
-- This migration adds:
--   1. plan.composed event contract (parallel to plan.launched)
--   2. launch_plan operation row binding event_type='plan.launched'
--   3. compose_plan operation row binding event_type='plan.composed'
--
-- Handler refactor (route launch_plan + compose_plan_from_intent through
-- operation_catalog_gateway so emission flows through
-- operation_receipt.event_ids automatically) is the next step. With the
-- operation rows present, the gateway can auto-generate event_ids for
-- completed receipts (operation_kind='command', event_required=TRUE).

BEGIN;

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
    'event_contract.plan.composed',
    'plan.composed',
    'authority.workflow_runs',
    'data_dictionary.object.plan_composed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Phase 1.2 CQRS registration. Compose-plan emits plan.composed via emit_system_event sidecar today. Operation row landing in this same migration is what unlocks receipt-backed emission once the handler is dispatched through operation_catalog_gateway.',
        'expected_payload_fields', jsonb_build_array(
            'spec_name',
            'total_jobs',
            'detection_mode',
            'step_count',
            'has_unresolved_routes',
            'unbound_pill_count',
            'type_flow_error_count'
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

-- data_dictionary_objects rows are required by the authority_object_registry
-- CHECK (authority object must have a data dictionary binding). Insert
-- these first so the object_registry rows below can reference them.
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
(
    'operation.launch_plan',
    'launch_plan',
    'command',
    'Operation catalog entry owned by authority.workflow_runs. Translates a Plan into a workflow spec, submits via submit_workflow_command, and emits plan.launched as the conceptual envelope-crossing event.',
    jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', 'launch-plan', 'migration', '234_register_plan_operations.sql'),
    jsonb_build_object('operation_kind', 'command', 'authority_domain_ref', 'authority.workflow_runs', 'event_type', 'plan.launched')
),
(
    'operation.compose_plan',
    'compose_plan',
    'command',
    'Operation catalog entry owned by authority.workflow_runs. Decomposes prose intent into packets, calls propose_plan, and emits plan.composed as the conceptual proposal event.',
    jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', 'compose-plan', 'migration', '234_register_plan_operations.sql'),
    jsonb_build_object('operation_kind', 'command', 'authority_domain_ref', 'authority.workflow_runs', 'event_type', 'plan.composed')
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- authority_object_registry rows are required by the
-- operation_catalog_registry CHECK trigger (enabled operation must have
-- a matching authority object). Insert these before the operation rows.
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
    'operation.launch_plan',
    'command',
    'launch_plan',
    NULL,
    'authority.workflow_runs',
    'operation.launch_plan',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.spec_compiler.launch_plan',
        'source_kind', 'operation_command',
        'event_type', 'plan.launched'
    )
),
(
    'operation.compose_plan',
    'command',
    'compose_plan',
    NULL,
    'authority.workflow_runs',
    'operation.compose_plan',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.intent_composition.compose_plan_from_intent',
        'source_kind', 'operation_command',
        'event_type', 'plan.composed'
    )
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
(
    'launch-plan',
    'launch_plan',
    'operation_command',
    'command',
    'POST',
    '/api/launch_plan',
    'runtime.spec_compiler.LaunchPlanCommand',
    'runtime.spec_compiler.launch_plan',
    'authority.workflow_runs',
    'operate',
    'non_idempotent',
    'binding.operation_catalog_registry.launch_plan.20260424',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.workflow_runs',
    'praxis.primary_postgres',
    'runtime.spec_compiler.LaunchPlanCommand',
    'data_dictionary.object.plan_launched_event',
    TRUE,
    TRUE,
    'plan.launched'
),
(
    'compose-plan',
    'compose_plan',
    'operation_command',
    'command',
    'POST',
    '/api/compose_plan',
    'runtime.intent_composition.ComposePlanCommand',
    'runtime.intent_composition.compose_plan_from_intent',
    'authority.workflow_runs',
    'operate',
    'non_idempotent',
    'binding.operation_catalog_registry.compose_plan.20260424',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.workflow_runs',
    'praxis.primary_postgres',
    'runtime.intent_composition.ComposePlanCommand',
    'data_dictionary.object.plan_composed_event',
    TRUE,
    TRUE,
    'plan.composed'
)
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
