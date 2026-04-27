-- Migration 281: Register the access_control mutator in
-- operation_catalog_registry + authority_object_registry, plus the
-- access_control.denial.changed conceptual event it emits.
--
-- This is the missing write surface for the "control panel" the operator
-- has been calling for. Migrations 267 + 269 built the data model
-- (private_provider_transport_control_policy + private_provider_model_access_denials)
-- and the routing-side roll-past for denied rows landed in commit 198ba925.
-- What was never wired: a callable mutator. Operators had no way to flip
-- a denial without writing a one-row migration.
--
-- This migration registers:
--   - operation: access_control (command, emits access_control.denial.changed)
--     handler: runtime.operations.commands.access_control.handle_access_control
--   - event contract: access_control.denial.changed
--   - data_dictionary_objects entries (operation + event payload)
--   - authority_object_registry entry
--
-- Once this lands, MCP tool praxis_access_control + the React checkbox in
-- app/src/control can dispatch through execute_operation_from_subsystems
-- and the gateway records the receipt + emits the event automatically.

BEGIN;

-- =====================================================================
-- Event contract
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
    'event_contract.access_control.denial.changed',
    'access_control.denial.changed',
    'authority.access_control',
    'data_dictionary.object.access_control_denial_changed_event',
    'operation_ref',
    '[]'::jsonb,
    '["projection.private_provider_job_catalog","projection.model_access_control_matrix"]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Emitted whenever the control panel flips a denial row on or off. Payload carries the selector tuple plus the new denied state so subscribers can refresh routing-side caches without re-reading the full matrix.',
        'expected_payload_fields', jsonb_build_array(
            'runtime_profile_ref',
            'selector',
            'denied',
            'decision_ref'
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
-- data_dictionary_objects entries (operation + event payload)
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('operation.access_control', 'Operation: access_control', 'command',
     'Control-panel mutator for the model-access denial table. Actions: list (filter and read denials), disable (upsert a denial row), enable (delete a denial row). Selector is (runtime_profile_ref, job_type, transport_type, adapter_type, provider_slug, model_slug); any field accepts ''*'' as wildcard.',
     '{"source":"migration.280","authority":"runtime.operations.commands.access_control"}'::jsonb,
     '{"operation_kind":"command","handler_ref":"runtime.operations.commands.access_control.handle_access_control","emits":"access_control.denial.changed"}'::jsonb),

    ('access_control_denial_changed_event', 'access_control.denial.changed event payload', 'event',
     'Conceptual event emitted when a control-panel denial row is upserted or deleted. Routing-side caches and Moon control-panel projections subscribe to this event to refresh without re-reading the full matrix.',
     '{"source":"migration.280"}'::jsonb,
     '{"event_type":"access_control.denial.changed","payload_fields":["runtime_profile_ref","selector","denied","decision_ref"]}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- authority_object_registry entry
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
    ('operation.access_control', 'command', 'access_control', NULL,
     'authority.access_control', 'operation.access_control', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.access_control.handle_access_control',
        'source_kind', 'operation_command',
        'event_type', 'access_control.denial.changed'
     ))
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
-- operation_catalog_registry entry
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
    ('access-control', 'access_control', 'operation_command', 'command',
     'POST', '/api/access_control',
     'runtime.operations.commands.access_control.AccessControlCommand',
     'runtime.operations.commands.access_control.handle_access_control',
     'authority.access_control', 'operate', 'non_idempotent',
     'binding.operation_catalog_registry.access_control.20260427',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.access_control', 'praxis.primary_postgres',
     'runtime.operations.commands.access_control.AccessControlCommand',
     'operation.output.default',
     TRUE, TRUE, 'access_control.denial.changed')

ON CONFLICT (operation_ref) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    event_required = EXCLUDED.event_required,
    receipt_required = EXCLUDED.receipt_required,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    handler_ref = EXCLUDED.handler_ref,
    input_model_ref = EXCLUDED.input_model_ref,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, event_type FROM operation_catalog_registry
--    WHERE operation_ref = 'access-control';
--   SELECT event_type, enabled FROM authority_event_contracts
--    WHERE event_type = 'access_control.denial.changed';
