-- Migration 414: Register verifier.register as a CQRS command operation,
-- plus the verifier.registered conceptual event contract.
--
-- Companion to migrations 412 (verifier.run) and 413 (healer.run).
-- Closes the forge-path gap noted in the strategic memo
-- (verifier-system-and-gaps.md): adding a new verifier today requires
-- editing verifier_builtins.py + a SQL migration. With this command
-- registered, an operator or agent can call praxis_verifier_register and
-- the runtime upserts the verifier_registry row + any healer bindings
-- through a single receipt-backed gateway dispatch.
--
-- Pattern follows migrations 332 / 400 / 412 / 413.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref, owner_ref, event_stream_ref, current_projection_ref,
    storage_target_ref, enabled, decision_ref
) VALUES (
    'authority.workflow_runs', 'praxis.engine', 'stream.authority.workflow_runs',
    NULL, 'praxis.primary_postgres', TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry'
) ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_event_contracts (
    event_contract_ref, event_type, authority_domain_ref, payload_schema_ref,
    aggregate_ref_policy, reducer_refs, projection_refs, receipt_required,
    replay_policy, enabled, decision_ref, metadata
) VALUES (
    'event_contract.verifier.registered', 'verifier.registered',
    'authority.workflow_runs', 'data_dictionary.object.verifier_registered_event',
    'operation_ref', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.414_register_verifier_register_command_operation',
        'note', 'One event per verifier_registry row registered through the catalog gateway.',
        'expected_payload_fields', jsonb_build_array(
            'verifier_ref', 'verifier_kind', 'enabled', 'bound_healer_refs', 'decision_ref'
        )
    )
) ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    (
        'operation.verifier.register', 'verifier.register', 'command',
        'Operation catalog entry owned by authority.workflow_runs. Upserts a verifier_registry row and any requested verifier_healer_bindings, emits verifier.registered.',
        jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', 'verifier-register', 'migration', '414'),
        jsonb_build_object(
            'operation_kind', 'command',
            'authority_domain_ref', 'authority.workflow_runs',
            'event_type', 'verifier.registered',
            'handler_ref', 'runtime.operations.commands.verifier_register.handle_verifier_register'
        )
    ),
    (
        'verifier_registered_event', 'verifier.registered event payload', 'event',
        'Conceptual event emitted by verifier.register per verifier_registry row written.',
        jsonb_build_object('source', 'migration.414'),
        jsonb_build_object(
            'event_type', 'verifier.registered',
            'payload_fields', jsonb_build_array(
                'verifier_ref', 'verifier_kind', 'enabled', 'bound_healer_refs', 'decision_ref'
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

INSERT INTO authority_object_registry (
    object_ref, object_kind, object_name, schema_name,
    authority_domain_ref, data_dictionary_object_kind, lifecycle_status,
    write_model_kind, owner_ref, source_decision_ref, metadata
) VALUES (
    'operation.verifier.register', 'command', 'verifier_register', NULL,
    'authority.workflow_runs', 'operation.verifier.register', 'active',
    'command_model', 'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.verifier_register.handle_verifier_register',
        'source_kind', 'operation_command',
        'event_type', 'verifier.registered'
    )
) ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO operation_catalog_registry (
    operation_ref, operation_name, source_kind, operation_kind,
    http_method, http_path, input_model_ref, handler_ref, authority_ref,
    posture, idempotency_policy, binding_revision, decision_ref,
    authority_domain_ref, storage_target_ref, input_schema_ref,
    output_schema_ref, receipt_required, event_required, event_type
) VALUES (
    'verifier-register', 'verifier.register', 'operation_command', 'command',
    'POST', '/api/verifier_register',
    'runtime.operations.commands.verifier_register.VerifierRegisterCommand',
    'runtime.operations.commands.verifier_register.handle_verifier_register',
    'authority.workflow_runs', 'operate', 'non_idempotent',
    'binding.operation_catalog_registry.verifier_register.20260501',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.workflow_runs', 'praxis.primary_postgres',
    'runtime.operations.commands.verifier_register.VerifierRegisterCommand',
    'data_dictionary.object.verifier_registered_event',
    TRUE, TRUE, 'verifier.registered'
) ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    handler_ref = EXCLUDED.handler_ref,
    input_model_ref = EXCLUDED.input_model_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    event_type = EXCLUDED.event_type,
    event_required = EXCLUDED.event_required,
    receipt_required = EXCLUDED.receipt_required,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
