-- Migration 236: Register surface.action.performed as an
-- operation_catalog_registry command + event contract.
--
-- Closes the scope_note debt from the action-rail wedge (commit de172040):
-- /api/surface/action previously wrote authority_operation_receipts
-- directly with a self-documenting debt note on every receipt. This
-- migration promotes the action sink to a first-class CQRS command so
-- clicks from the compose surface route through operation_catalog_gateway,
-- produce receipts + events as a side-effect of gateway dispatch, and
-- honor architecture-policy::platform-architecture::conceptual-events-
-- register-through-operation-catalog-registry like every other typed
-- write in Praxis.

BEGIN;

-- 0. authority_object_registry row for the command -----------------------
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
) VALUES (
    'operation.surface.action.performed',
    'command',
    'surface.action.performed',
    NULL,
    'authority.surface_catalog',
    'operation.surface.action.performed',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'migration', '236_operation_catalog_surface_action_performed.sql',
        'handler_ref', 'runtime.operations.commands.surface_actions_command.handle_surface_action_performed'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    schema_name = EXCLUDED.schema_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- 1. Register the command -------------------------------------------------
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
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref,
    authority_domain_ref,
    input_schema_ref,
    output_schema_ref,
    event_required,
    event_type
) VALUES (
    'surface-action-performed',
    'surface.action.performed',
    'operation_command',
    'command',
    'POST',
    '/api/surface/action',
    'runtime.operations.commands.surface_actions_command.SurfaceActionPerformedCommand',
    'runtime.operations.commands.surface_actions_command.handle_surface_action_performed',
    'authority.surface_catalog',
    NULL,
    'operate',
    'non_idempotent',
    TRUE,
    'binding.operation_catalog_registry.surface_action_performed.20260424',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.surface_catalog',
    'runtime.operations.commands.surface_actions_command.SurfaceActionPerformedCommand',
    'operation.output.default',
    TRUE,
    'surface.action.performed'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    event_required = EXCLUDED.event_required,
    event_type = EXCLUDED.event_type,
    updated_at = now();

-- 2. Register the event contract -----------------------------------------
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
    'event_contract.surface.action.performed',
    'surface.action.performed',
    'authority.surface_catalog',
    'runtime.operations.commands.surface_actions_command.SurfaceActionPerformedCommand',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.236_operation_catalog_surface_action_performed',
        'payload_keys', jsonb_build_array(
            'action_ref',
            'intent_ref',
            'template_ref',
            'pill_refs',
            'caller_ref'
        )
    )
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs = EXCLUDED.reducer_refs,
    projection_refs = EXCLUDED.projection_refs,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- 3. Dictionary entries --------------------------------------------------
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
(
    'operation.surface.action.performed',
    'Command: surface.action.performed',
    'command',
    'Typed command fired when a user clicks an action on a composed Surface template. Records typed context (action_ref, intent_ref, template_ref, pill_refs) in authority_operation_receipts and fires surface.action.performed through authority_events.',
    jsonb_build_object(
        'source', 'migration.236_operation_catalog_surface_action_performed',
        'operation_ref', 'surface-action-performed',
        'event_type', 'surface.action.performed'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'handler_ref', 'runtime.operations.commands.surface_actions_command.handle_surface_action_performed'
    )
),
(
    'event.surface.action.performed',
    'Event: surface action performed',
    'event',
    'Fires when a user completes an action on a composed Surface template. Durable record of typed surface interactions; consumers can correlate action_ref with the originating intent + template + pills.',
    jsonb_build_object(
        'source', 'migration.236_operation_catalog_surface_action_performed',
        'event_contract_ref', 'event_contract.surface.action.performed'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'payload_keys', jsonb_build_array(
            'action_ref', 'intent_ref', 'template_ref', 'pill_refs', 'caller_ref'
        )
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
