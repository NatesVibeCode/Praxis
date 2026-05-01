-- Migration 391: Register Model Eval Authority operations.
--
-- The live operator DB already had these operation rows during the first
-- Model Eval experiments, but the repo had no replayable migration for a
-- fresh DB. That is authority drift: the gateway worked only because local
-- state remembered something the source tree could not recreate.
--
-- This migration makes the V1 praxis_model_eval surface replayable through
-- the CQRS registry triple:
--   operation_catalog_registry
--   authority_object_registry
--   data_dictionary_objects
--
-- It also registers the conceptual events emitted by command operations.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.model_eval',
    'praxis.engine',
    'stream.authority.model_eval',
    'projection.model_eval_runs',
    'praxis.primary_postgres',
    TRUE,
    'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = COALESCE(authority_domains.current_projection_ref, EXCLUDED.current_projection_ref),
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'model_eval_matrix_completed_event',
        'model_eval.matrix.completed event payload',
        'event',
        'Conceptual event emitted after a Model Eval matrix finishes planning or execution. Carries run id, artifact root, cost, counts, and stop reason.',
        '{"migration":"391_register_model_eval_operations.sql"}'::jsonb,
        '{"event_type":"model_eval.matrix.completed","authority_domain_ref":"authority.model_eval"}'::jsonb
    ),
    (
        'model_eval_promotion_proposed_event',
        'model_eval.promotion.proposed event payload',
        'event',
        'Conceptual event emitted when Model Eval creates a routing-promotion proposal. It is proposal-only and does not mutate production routing.',
        '{"migration":"391_register_model_eval_operations.sql"}'::jsonb,
        '{"event_type":"model_eval.promotion.proposed","authority_domain_ref":"authority.model_eval"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

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
) VALUES
    (
        'event_contract.model_eval.matrix.completed',
        'model_eval.matrix.completed',
        'authority.model_eval',
        'data_dictionary.object.model_eval_matrix_completed_event',
        'operation_ref',
        '[]'::jsonb,
        '["projection.model_eval_runs"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array(
                'lab_run_id',
                'artifact_root',
                'dry_run',
                'total_cost_usd',
                'executed_count',
                'passed_count',
                'failed_count',
                'stopped_reason'
            )
        )
    ),
    (
        'event_contract.model_eval.promotion.proposed',
        'model_eval.promotion.proposed',
        'authority.model_eval',
        'data_dictionary.object.model_eval_promotion_proposed_event',
        'operation_ref',
        '[]'::jsonb,
        '["projection.model_eval_runs"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array(
                'proposal_type',
                'lab_run_id',
                'task_type',
                'candidate',
                'promotion_gate'
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

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.query.plan',
    p_operation_name        := 'model_eval_plan',
    p_handler_ref           := 'runtime.operations.queries.model_eval.handle_model_eval_plan',
    p_input_model_ref       := 'runtime.operations.queries.model_eval.ModelEvalPlanQuery',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/model_eval_plan',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_plan.20260501',
    p_label                 := 'Model Eval Plan',
    p_summary               := 'Plan a Model Eval matrix from suites, imported workflow specs, candidates, and prompt variants.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.query.inspect',
    p_operation_name        := 'model_eval_inspect',
    p_handler_ref           := 'runtime.operations.queries.model_eval.handle_model_eval_inspect',
    p_input_model_ref       := 'runtime.operations.queries.model_eval.ModelEvalInspectQuery',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/model_eval_inspect',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_inspect.20260501',
    p_label                 := 'Model Eval Inspect',
    p_summary               := 'Inspect one Model Eval run and its artifacts or persisted scorecards.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.query.compare',
    p_operation_name        := 'model_eval_compare',
    p_handler_ref           := 'runtime.operations.queries.model_eval.handle_model_eval_compare',
    p_input_model_ref       := 'runtime.operations.queries.model_eval.ModelEvalCompareQuery',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/model_eval_compare',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_compare.20260501',
    p_label                 := 'Model Eval Compare',
    p_summary               := 'Rank Model Eval candidates by task family, score, cost, and latency.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.query.export',
    p_operation_name        := 'model_eval_export',
    p_handler_ref           := 'runtime.operations.queries.model_eval.handle_model_eval_export',
    p_input_model_ref       := 'runtime.operations.queries.model_eval.ModelEvalExportQuery',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/model_eval_export',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_export.20260501',
    p_label                 := 'Model Eval Export',
    p_summary               := 'Export Model Eval summaries and scorecards as JSON or Markdown.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.command.run_matrix',
    p_operation_name        := 'model_eval_run_matrix',
    p_handler_ref           := 'runtime.operations.commands.model_eval.handle_model_eval_run_matrix',
    p_input_model_ref       := 'runtime.operations.commands.model_eval.ModelEvalRunMatrixCommand',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/model_eval_run_matrix',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_eval.matrix.completed',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 120000,
    p_execution_lane        := 'background',
    p_kickoff_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_run_matrix.20260501',
    p_label                 := 'Model Eval Run Matrix',
    p_summary               := 'Run a Model Eval matrix under strict privacy gates and emit comparable scorecards.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.command.promote_proposal',
    p_operation_name        := 'model_eval_promote_proposal',
    p_handler_ref           := 'runtime.operations.commands.model_eval.handle_model_eval_promote_proposal',
    p_input_model_ref       := 'runtime.operations.commands.model_eval.ModelEvalPromoteProposalCommand',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/model_eval_promote_proposal',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_eval.promotion.proposed',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_promote_proposal.20260501',
    p_label                 := 'Model Eval Promotion Proposal',
    p_summary               := 'Create a proposal to promote a Model Eval winner without mutating production routing.'
);

COMMIT;
