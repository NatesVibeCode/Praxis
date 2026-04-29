-- Migration 332: Register friction.record as a CQRS-visible command
-- operation, plus the friction.recorded conceptual event contract and the
-- authority.friction_events authority domain.
--
-- Context: The JIT trigger-check hook (.claude/hooks/preact_orient_friction.py
-- and the Codex / Gemini siblings) shells into
-- ``bin/praxis-agent praxis_friction --action=record`` for every standing
-- order match. Until now ``tool_praxis_friction`` had no ``record`` branch —
-- the call returned ``{"error": "Unknown friction action: record"}`` with
-- exit_code=0, so the hook treated it as success. Friction emitted from
-- per-harness PreToolUse hooks was lost.
--
-- Per architecture-policy::agent-behavior::cqrs-wizard-before-cqrs-edits and
-- architecture-policy::platform-architecture::conceptual-events-register-
-- through-operation-catalog-registry, the right fix is a properly-registered
-- command operation: receipt-backed, event-emitting, dispatched through the
-- catalog gateway. Migration 234 / 235 did this for launch_plan / compose_plan;
-- this migration follows the same pattern for friction recording.
--
-- Migration 331 (already landed) added the nullable ``task_mode`` column to
-- ``friction_events``; the new operation accepts it on the input model so
-- JIT-fired events can be sliced by mode.

BEGIN;

-- =====================================================================
-- Authority domain — friction events get their own domain so receipts /
-- events can scope to it independently of authority.workflow_runs.
-- =====================================================================
INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.friction_events',
    'praxis.engine',
    'stream.authority.friction_events',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

-- =====================================================================
-- Conceptual event contract — friction.recorded.
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
    'event_contract.friction.recorded',
    'friction.recorded',
    'authority.friction_events',
    'data_dictionary.object.friction_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.332_register_friction_record_operation',
        'note', 'One event per friction_events row recorded through the catalog gateway. Replaces the silent shell-out path the JIT trigger-check hooks used to take.',
        'expected_payload_fields', jsonb_build_array(
            'event_id',
            'friction_type',
            'source',
            'job_label',
            'subject_kind',
            'subject_ref',
            'decision_keys',
            'task_mode',
            'is_test',
            'decision_match_count'
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
-- data_dictionary_objects — the authority_object_registry CHECK requires
-- a data dictionary binding before the operation row can land.
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'operation.friction_record',
        'friction_record',
        'command',
        'Operation catalog entry owned by authority.friction_events. Records one friction event through FrictionLedger.record and emits friction.recorded as the conceptual envelope-crossing event.',
        jsonb_build_object(
            'source', 'operation_catalog_registry',
            'operation_ref', 'friction-record',
            'migration', '332_register_friction_record_operation.sql'
        ),
        jsonb_build_object(
            'operation_kind', 'command',
            'authority_domain_ref', 'authority.friction_events',
            'event_type', 'friction.recorded',
            'handler_ref', 'runtime.operations.commands.friction_record.handle_friction_record'
        )
    ),
    (
        'friction_recorded_event',
        'friction.recorded event payload',
        'event',
        'Conceptual event emitted by friction_record per FrictionEvent persisted in friction_events.',
        jsonb_build_object('source', 'migration.332'),
        jsonb_build_object(
            'event_type', 'friction.recorded',
            'payload_fields', jsonb_build_array(
                'event_id',
                'friction_type',
                'source',
                'job_label',
                'subject_kind',
                'subject_ref',
                'decision_keys',
                'task_mode',
                'is_test',
                'decision_match_count'
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

-- =====================================================================
-- authority_object_registry — pairs the operation_ref with its dictionary
-- entry so the catalog registry CHECK is satisfied.
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
) VALUES (
    'operation.friction_record',
    'command',
    'friction_record',
    NULL,
    'authority.friction_events',
    'operation.friction_record',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.friction_record.handle_friction_record',
        'source_kind', 'operation_command',
        'event_type', 'friction.recorded'
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

-- =====================================================================
-- operation_catalog_registry — the actual binding the gateway resolves.
-- non_idempotent: every fired hook is a distinct event, dedup is wrong.
-- event_required=TRUE + event_type='friction.recorded' so each completed
-- receipt auto-emits one authority_events row (gateway _write_operation_proof).
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
) VALUES (
    'friction-record',
    'friction_record',
    'operation_command',
    'command',
    'POST',
    '/api/friction_record',
    'runtime.operations.commands.friction_record.FrictionRecordInput',
    'runtime.operations.commands.friction_record.handle_friction_record',
    'authority.friction_events',
    'operate',
    'non_idempotent',
    'binding.operation_catalog_registry.friction_record.20260428',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.friction_events',
    'praxis.primary_postgres',
    'runtime.operations.commands.friction_record.FrictionRecordInput',
    'data_dictionary.object.friction_recorded_event',
    TRUE,
    TRUE,
    'friction.recorded'
)
ON CONFLICT (operation_ref) DO UPDATE SET
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

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, event_type, idempotency_policy
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'friction-record';
--
--   SELECT event_contract_ref, event_type, authority_domain_ref
--     FROM authority_event_contracts
--    WHERE event_type = 'friction.recorded';
