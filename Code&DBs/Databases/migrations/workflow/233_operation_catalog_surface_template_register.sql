-- Migration 233: Register surface.template.register as an
-- operation_catalog_registry command + event contract.
--
-- Closes the scope_note debt from the action-rail receipt wedge: templates
-- become LLM-composable without a hand-authored migration, by posting to
-- /api/surface/templates. The write-path gate (enforced in the handler)
-- runs the admission checks from
-- architecture-policy::surface-catalog::type-lattice-and-risk-mitigation-
-- is-authority-reuse so parallel plumbing cannot sneak in.
--
-- Every registration emits `template.registered` through authority_events,
-- receipts land in authority_operation_receipts — aligned with
-- architecture-policy::platform-architecture::conceptual-events-register-
-- through-operation-catalog-registry.

BEGIN;

-- 0. authority_object_registry row for the operation ---------------------
-- Required by enforce_operation_catalog_cqrs_contract() trigger.
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
    'operation.surface.template.register',
    'command',
    'surface.template.register',
    NULL,
    'authority.surface_catalog',
    'operation.surface.template.register',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse',
    jsonb_build_object(
        'migration', '233_operation_catalog_surface_template_register.sql',
        'handler_ref', 'runtime.operations.commands.surface_catalog.handle_surface_template_register'
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
    'surface-template-register',
    'surface.template.register',
    'operation_command',
    'command',
    'POST',
    '/api/surface/templates',
    'runtime.operations.commands.surface_catalog.SurfaceTemplateRegisterCommand',
    'runtime.operations.commands.surface_catalog.handle_surface_template_register',
    'authority.surface_catalog',
    NULL,
    'operate',
    'non_idempotent',
    TRUE,
    'binding.operation_catalog_registry.surface_template_register.20260424',
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse',
    'authority.surface_catalog',
    'runtime.operations.commands.surface_catalog.SurfaceTemplateRegisterCommand',
    'operation.output.default',
    TRUE,
    'template.registered'
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
-- Migration 204's auto-seed SELECT has already run on an earlier bootstrap,
-- so we INSERT the contract explicitly for this command.
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
    'event_contract.template.registered',
    'template.registered',
    'authority.surface_catalog',
    'runtime.operations.commands.surface_catalog.SurfaceTemplateRegisterCommand',
    'operation_ref',
    '[]'::jsonb,
    '["projection.surface.legal_templates"]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse',
    jsonb_build_object(
        'source', 'migration.233_operation_catalog_surface_template_register',
        'payload_keys', jsonb_build_array(
            'template_ref',
            'shape_ref',
            'slot_consumes',
            'intent_ref',
            'intent_binding_weight',
            'fallback_template_ref',
            'render_hint',
            'framework_ref'
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

-- 3. Advertise the event object ------------------------------------------
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'operation.surface.template.register',
    'Command: surface.template.register',
    'command',
    'Typed command for registering an experience_template into the outcome graph. Admission gate enforces lattice + fallback + ambiguity checks per architecture-policy::surface-catalog::type-lattice-and-risk-mitigation-is-authority-reuse.',
    jsonb_build_object(
        'source', 'migration.233_operation_catalog_surface_template_register',
        'operation_ref', 'surface-template-register',
        'event_type', 'template.registered'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'handler_ref', 'runtime.operations.commands.surface_catalog.handle_surface_template_register'
    )
),
(
    'event.template.registered',
    'Event: experience template registered',
    'event',
    'Fires when a new experience_template lands through surface.template.register; consumers can invalidate projection.surface.legal_templates on this signal.',
    jsonb_build_object(
        'source', 'migration.233_operation_catalog_surface_template_register',
        'event_contract_ref', 'event_contract.template.registered'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'payload_keys', jsonb_build_array(
            'template_ref', 'shape_ref', 'slot_consumes', 'intent_ref',
            'intent_binding_weight', 'fallback_template_ref', 'render_hint',
            'framework_ref'
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
