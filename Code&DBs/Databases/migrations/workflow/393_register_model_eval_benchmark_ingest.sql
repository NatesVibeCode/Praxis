-- Migration 393: Register Model Eval public benchmark prior ingestion.
--
-- Public benchmarks are selection priors only. They enrich
-- provider_model_candidates.benchmark_profile and never participate directly
-- in Model Eval scorecards, compare ranking, or task_type_routing mutation.

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'model_eval_benchmark_ingested_event',
    'model_eval.benchmark.ingested event payload',
    'event',
    'Conceptual event emitted when public benchmark metadata is attached to provider model candidate benchmark_profile as prior-only context.',
    '{"migration":"393_register_model_eval_benchmark_ingest.sql"}'::jsonb,
    '{"event_type":"model_eval.benchmark.ingested","authority_domain_ref":"authority.model_eval"}'::jsonb
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
) VALUES (
    'event_contract.model_eval.benchmark.ingested',
    'model_eval.benchmark.ingested',
    'authority.model_eval',
    'data_dictionary.object.model_eval_benchmark_ingested_event',
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
            'benchmark_slug',
            'source_url',
            'version',
            'updated_count',
            'unmatched_count',
            'routing_effect'
        ),
        'routing_effect',
        'prior_only_not_score_truth'
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
    p_operation_ref         := 'model-eval-benchmark-ingest',
    p_operation_name        := 'model_eval_benchmark_ingest',
    p_handler_ref           := 'runtime.operations.commands.model_eval.handle_model_eval_benchmark_ingest',
    p_input_model_ref       := 'runtime.operations.commands.model_eval.ModelEvalBenchmarkIngestCommand',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/model_eval_benchmark_ingest',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_eval.benchmark.ingested',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 30000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_benchmark_ingest.20260501',
    p_label                 := 'Model Eval Benchmark Ingest',
    p_summary               := 'Ingest public benchmark priors into provider model candidate benchmark profiles without affecting routing scores.'
);

COMMIT;
