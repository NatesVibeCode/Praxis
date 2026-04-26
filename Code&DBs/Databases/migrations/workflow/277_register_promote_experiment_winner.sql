-- Migration 277: Register the experiment_promote_winner operation.
--
-- Closes the compose-experiment loop: an operator runs a matrix run via
-- praxis_compose_experiment, picks the winning leg, and calls
-- praxis_promote_experiment_winner with that leg's config_index. The
-- handler reads the parent receipt, pulls the winning leg's
-- resolved_overrides, and UPDATEs the matching task_type_routing row
-- (matched by base_task_type) with the winning temperature + max_tokens.
--
-- Per the operator standing order on operator-authority mutations
-- (architecture-policy::operator-authority::explicit-write-required),
-- this operation IS the explicit write — calling it is the operator's
-- "promote this winner to canonical" instruction. Provider/model
-- overrides surface in the diff but are NOT auto-applied (those are
-- routing identity changes that need a separate, named decision).
--
-- Adds (operation_kind in parentheses):
--   - praxis_experiment_promote_winner (command, emits experiment.winner.promoted)
--
-- Adds event contract:
--   - experiment.winner.promoted — one row per promotion; carries the diff.

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
    'event_contract.experiment.winner.promoted',
    'experiment.winner.promoted',
    'authority.workflow_runs',
    'data_dictionary.object.experiment_winner_promoted_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Records when an operator promotes a compose_experiment winning leg into a task_type_routing row. Carries the experiment receipt id, the winning leg index, the target task_type, and the before/after diff so the promotion is fully auditable and replayable.',
        'expected_payload_fields', jsonb_build_array(
            'source_experiment_receipt_id',
            'source_config_index',
            'target_task_type',
            'target_provider_slug',
            'target_model_slug',
            'before',
            'after',
            'diff_keys',
            'caller_ref'
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
-- data_dictionary_objects entries
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('operation.experiment_promote_winner', 'Operation: experiment_promote_winner', 'command',
     'Promote a winning compose_experiment leg into a task_type_routing row. Reads a matrix-run parent receipt, pulls the chosen leg''s resolved_overrides, and UPDATEs the rank-1 row of the target task_type with the winning temperature + max_tokens. Provider/model overrides surface in the diff but are not auto-applied.',
     '{"source":"migration.277","authority":"runtime.compose_experiment"}'::jsonb,
     '{"operation_kind":"command","handler_ref":"runtime.operations.commands.promote_experiment_winner_command.handle_promote_experiment_winner","emits":"experiment.winner.promoted"}'::jsonb),

    ('experiment_winner_promoted_event', 'experiment.winner.promoted event payload', 'event',
     'Fires when an operator promotes a compose_experiment winning leg into task_type_routing. Carries the source receipt + leg index, the target row, and the before/after column values for replay.',
     '{"source":"migration.277"}'::jsonb,
     '{"event_type":"experiment.winner.promoted","payload_fields":["source_experiment_receipt_id","source_config_index","target_task_type","target_provider_slug","target_model_slug","before","after","diff_keys","caller_ref"]}'::jsonb)

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
    ('operation.experiment_promote_winner', 'command', 'experiment_promote_winner', NULL,
     'authority.workflow_runs', 'operation.experiment_promote_winner', 'active', 'command_model',
     'praxis.engine',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     jsonb_build_object(
        'handler_ref', 'runtime.operations.commands.promote_experiment_winner_command.handle_promote_experiment_winner',
        'source_kind', 'operation_command',
        'event_type', 'experiment.winner.promoted'
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
    ('experiment-promote-winner', 'experiment_promote_winner', 'operation_command', 'command',
     'POST', '/api/experiment_promote_winner',
     'runtime.operations.commands.promote_experiment_winner_command.PromoteExperimentWinnerCommand',
     'runtime.operations.commands.promote_experiment_winner_command.handle_promote_experiment_winner',
     'authority.workflow_runs', 'operate', 'non_idempotent',
     'binding.operation_catalog_registry.experiment_promote_winner.20260426',
     'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
     'authority.workflow_runs', 'praxis.primary_postgres',
     'runtime.operations.commands.promote_experiment_winner_command.PromoteExperimentWinnerCommand',
     'dict',
     TRUE, TRUE, 'experiment.winner.promoted')
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
