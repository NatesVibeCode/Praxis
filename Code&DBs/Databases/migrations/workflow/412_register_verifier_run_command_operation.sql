-- Migration 412: Register verifier.run as a CQRS command operation, plus
-- the verifier.run.completed conceptual event contract.
--
-- Context: The verifier subsystem (runtime.verifier_authority) has been
-- internally complete for some time — verifier_registry, healer_registry,
-- verification_runs, six built-in verifiers, three healers, full
-- bug-bridge — but the only user-visible path to running a verifier was
-- praxis_bugs action=resolve, which conflates "verify X" with "flip a
-- bug to FIXED". This operation gives workflow packets, operators, and
-- the LLM-first plan composer a first-class verify gate that is
-- receipt-backed, replayable, and emits a conceptual event per run.
--
-- Pattern follows migrations 332 (friction.recorded) and 400
-- (integration.registered) — the canonical CQRS-evolution path filed
-- under architecture-policy::platform-architecture::conceptual-events-
-- register-through-operation-catalog-registry. Anchored by the strategic
-- memo at /Users/nate/.claude/plans/praxis-tool-catalog/verifier-system-and-gaps.md.
--
-- Companion read tools (already landed):
--   praxis_verifier_catalog (verifier.catalog.list — migration 369)
--   praxis_verifier_runs_list (verifier.runs.list — registered via
--                              register_operation_atomic this same session)

BEGIN;

-- =====================================================================
-- Authority domain — verifier runs share authority.workflow_runs with
-- the catalog/runs read ops. Idempotent upsert.
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
-- Conceptual event contract — verifier.run.completed.
-- One event per verification_runs row written through this operation.
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
    'event_contract.verifier.run.completed',
    'verifier.run.completed',
    'authority.workflow_runs',
    'data_dictionary.object.verifier_run_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.412_register_verifier_run_command_operation',
        'note', 'One event per verification_runs row written through the catalog gateway. Replaces the prior path where run_registered_verifier was only reachable from praxis_bugs action=resolve and emitted no conceptual event.',
        'expected_payload_fields', jsonb_build_array(
            'verifier_ref',
            'verification_run_id',
            'status',
            'target_kind',
            'target_ref',
            'duration_ms',
            'suggested_healer_ref',
            'bug_id',
            'passed'
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
-- data_dictionary_objects — the operation row + the event payload entry.
-- The authority_object_registry CHECK requires both before the catalog
-- registry row can land.
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
        'operation.verifier.run',
        'verifier.run',
        'command',
        'Operation catalog entry owned by authority.workflow_runs. Runs a registered verifier against a target through verifier_authority.run_registered_verifier and emits verifier.run.completed as the conceptual envelope-crossing event.',
        jsonb_build_object(
            'source', 'operation_catalog_registry',
            'operation_ref', 'verifier-run',
            'migration', '412_register_verifier_run_command_operation.sql'
        ),
        jsonb_build_object(
            'operation_kind', 'command',
            'authority_domain_ref', 'authority.workflow_runs',
            'event_type', 'verifier.run.completed',
            'handler_ref', 'runtime.operations.commands.verifier_run.handle_verifier_run'
        )
    ),
    (
        'verifier_run_completed_event',
        'verifier.run.completed event payload',
        'event',
        'Conceptual event emitted by verifier.run per verification_runs row written.',
        jsonb_build_object('source', 'migration.412'),
        jsonb_build_object(
            'event_type', 'verifier.run.completed',
            'payload_fields', jsonb_build_array(
                'verifier_ref',
                'verification_run_id',
                'status',
                'target_kind',
                'target_ref',
                'duration_ms',
                'suggested_healer_ref',
                'bug_id',
                'passed'
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
-- authority_object_registry — pairs the operation_ref with its
-- dictionary entry so the catalog registry CHECK is satisfied.
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
    'operation.verifier.run',
    'command',
    'verifier_run',
    NULL,
    'authority.workflow_runs',
    'operation.verifier.run',
    'active',
    'command_model',
    'praxis.engine',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.verifier_run.handle_verifier_run',
        'source_kind', 'operation_command',
        'event_type', 'verifier.run.completed'
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
-- non_idempotent: each verify call is a distinct attempt; dedup would
-- hide flaky verifiers and stale-cache failures.
-- event_required=TRUE + event_type='verifier.run.completed' so each
-- completed receipt auto-emits one authority_events row.
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
    'verifier-run',
    'verifier.run',
    'operation_command',
    'command',
    'POST',
    '/api/verifier_run',
    'runtime.operations.commands.verifier_run.VerifierRunCommand',
    'runtime.operations.commands.verifier_run.handle_verifier_run',
    'authority.workflow_runs',
    'operate',
    'non_idempotent',
    'binding.operation_catalog_registry.verifier_run.20260501',
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    'authority.workflow_runs',
    'praxis.primary_postgres',
    'runtime.operations.commands.verifier_run.VerifierRunCommand',
    'data_dictionary.object.verifier_run_completed_event',
    TRUE,
    TRUE,
    'verifier.run.completed'
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
--    WHERE operation_ref = 'verifier-run';
--
--   SELECT event_contract_ref, event_type, authority_domain_ref
--     FROM authority_event_contracts
--    WHERE event_type = 'verifier.run.completed';
