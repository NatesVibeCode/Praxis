-- Migration 413: Register healer.run as a CQRS command operation, plus
-- the healer.run.completed conceptual event contract.
--
-- Companion to migration 412 (verifier.run). Healers are the repair side
-- of the verifier subsystem — three built-ins today (schema_bootstrap,
-- receipt_provenance_backfill, proof_backfill), all guarded auto-mode.
-- Until now the only path to a healer was the internal scheduler
-- (run_due_platform_verifications); this operation gives operators and
-- workflow packets a manual receipt-backed trigger.
--
-- Pattern follows migrations 332 / 400 / 412 — authority_domain +
-- event_contract + data_dictionary_objects + authority_object_registry +
-- operation_catalog_registry, all idempotent. Anchored by
-- /Users/nate/.claude/plans/praxis-tool-catalog/verifier-system-and-gaps.md.

BEGIN;

-- =====================================================================
-- Authority domain (idempotent — already exists from earlier verifier
-- ops, but upsert anyway for self-containment).
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
    'authority.workflow_runs',
    'praxis.engine',
    'stream.authority.workflow_runs',
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
-- Conceptual event contract — healer.run.completed.
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
    'event_contract.healer.run.completed',
    'healer.run.completed',
    'authority.workflow_runs',
    'data_dictionary.object.healer_run_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.413_register_healer_run_command_operation',
        'note', 'One event per healing_runs row written through the catalog gateway.',
        'expected_payload_fields', jsonb_build_array(
            'verifier_ref',
            'healer_ref',
            'healing_run_id',
            'status',
            'target_kind',
            'target_ref',
            'duration_ms',
            'bug_id',
            'resolved_bug_id',
            'succeeded'
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
-- data_dictionary_objects — operation row + event payload.
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
        'operation.healer.run',
        'healer.run',
        'command',
        'Operation catalog entry owned by authority.workflow_runs. Runs a registered healer through verifier_authority.run_registered_healer (which auto-reruns the bound verifier as post-verification) and emits healer.run.completed.',
        jsonb_build_object(
            'source', 'operation_catalog_registry',
            'operation_ref', 'healer-run',
            'migration', '413_register_healer_run_command_operation.sql'
        ),
        jsonb_build_object(
            'operation_kind', 'command',
            'authority_domain_ref', 'authority.workflow_runs',
            'event_type', 'healer.run.completed',
            'handler_ref', 'runtime.operations.commands.healer_run.handle_healer_run'
        )
    ),
    (
        'healer_run_completed_event',
        'healer.run.completed event payload',
        'event',
        'Conceptual event emitted by healer.run per healing_runs row written.',
        jsonb_build_object('source', 'migration.413'),
        jsonb_build_object(
            'event_type', 'healer.run.completed',
            'payload_fields', jsonb_build_array(
                'verifier_ref',
                'healer_ref',
                'healing_run_id',
                'status',
                'target_kind',
                'target_ref',
                'duration_ms',
                'bug_id',
                'resolved_bug_id',
                'succeeded'
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
-- authority_object_registry.
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
    'operation.healer.run',
    'command',
    'healer_run',
    NULL,
    'authority.workflow_runs',
    'operation.healer.run',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.healer_run.handle_healer_run',
        'source_kind', 'operation_command',
        'event_type', 'healer.run.completed'
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
-- operation_catalog_registry.
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
    'healer-run',
    'healer.run',
    'operation_command',
    'command',
    'POST',
    '/api/healer_run',
    'runtime.operations.commands.healer_run.HealerRunCommand',
    'runtime.operations.commands.healer_run.handle_healer_run',
    'authority.workflow_runs',
    'operate',
    'non_idempotent',
    'binding.operation_catalog_registry.healer_run.20260501',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.workflow_runs',
    'praxis.primary_postgres',
    'runtime.operations.commands.healer_run.HealerRunCommand',
    'data_dictionary.object.healer_run_completed_event',
    TRUE,
    TRUE,
    'healer.run.completed'
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

-- Verification:
--   SELECT operation_ref, event_type, idempotency_policy
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'healer-run';
