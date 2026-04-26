-- Migration 274: Register the compose_experiment matrix runner in
-- operation_catalog_registry + authority_object_registry, plus the
-- compose.experiment.completed conceptual event it emits.
--
-- Operator standing order: every workflow item needs a reflected item
-- in the DB CQRS (mirror of migration 248). The compose-experiment
-- runner is the first parallel-experiment primitive on the platform —
-- it fires N compose_plan_via_llm calls side-by-side with knob
-- variation (model_slug / temperature / max_tokens) and rolls them up
-- into a comparison report. Per the LLM-first / trust-compiler
-- standing order, choosing between knob configurations is a
-- moment-of-action the platform must surface natively rather than
-- forcing operators to hand-roll thread pools in scratch scripts.
--
-- Adds (operation_kind in parentheses):
--   - praxis_compose_experiment (command, emits compose.experiment.completed)
--
-- Adds event contract:
--   - compose.experiment.completed — one row per matrix run

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
    'event_contract.compose.experiment.completed',
    'compose.experiment.completed',
    'authority.workflow_runs',
    'data_dictionary.object.compose_experiment_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Matrix-run completion. One event per compose_experiment invocation; payload carries the child receipt ids and the winning config index for forensic replay and ranking analytics.',
        'expected_payload_fields', jsonb_build_array(
            'intent_fingerprint',
            'config_count',
            'success_count',
            'winning_config_index',
            'winning_wall_seconds',
            'total_wall_seconds',
            'matrix_summary'
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
    ('operation.compose_experiment', 'Operation: compose_experiment', 'command',
     'Parallel matrix runner for compose_plan_via_llm. Takes an intent + a list of LLM knob configurations (model_slug / temperature / max_tokens / provider_slug) and fires one compose call per config in parallel, then ranks them by success and wall-time.',
     '{"source":"migration.274","authority":"runtime.compose_experiment"}'::jsonb,
     '{"operation_kind":"command","handler_ref":"runtime.operations.commands.compose_experiment_command.handle_compose_experiment","emits":"compose.experiment.completed"}'::jsonb),

    ('compose_experiment_completed_event', 'compose.experiment.completed event payload', 'event',
     'Conceptual event emitted at the end of a compose_experiment matrix run. Carries the child receipt ids, the winning config index, and a compact summary table for ranking analytics.',
     '{"source":"migration.274"}'::jsonb,
     '{"event_type":"compose.experiment.completed","payload_fields":["intent_fingerprint","config_count","success_count","winning_config_index","winning_wall_seconds","total_wall_seconds","matrix_summary"]}'::jsonb)

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
    ('operation.compose_experiment', 'command', 'compose_experiment', NULL,
     'authority.workflow_runs', 'operation.compose_experiment', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.compose_experiment_command.handle_compose_experiment',
        'source_kind', 'operation_command',
        'event_type', 'compose.experiment.completed'
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
    ('compose-experiment', 'compose_experiment', 'operation_command', 'command',
     'POST', '/api/compose_experiment',
     'runtime.operations.commands.compose_experiment_command.ComposeExperimentCommand',
     'runtime.operations.commands.compose_experiment_command.handle_compose_experiment',
     'authority.workflow_runs', 'operate', 'non_idempotent',
     'binding.operation_catalog_registry.compose_experiment.20260426',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.operations.commands.compose_experiment_command.ComposeExperimentCommand',
     'runtime.compose_experiment.ComposeExperimentReport',
     TRUE, TRUE, 'compose.experiment.completed')

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
--    WHERE operation_ref = 'compose-experiment';
--   SELECT event_type, enabled FROM authority_event_contracts
--    WHERE event_type = 'compose.experiment.completed';
