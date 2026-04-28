-- Migration 325: Register durable object-truth comparison-run command.

BEGIN;

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
    'event_contract.object_truth.comparison_run_recorded',
    'object_truth.comparison_run_recorded',
    'authority.object_truth',
    'data_dictionary.object.object_truth_comparison_run_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate',
    '{"expected_payload_fields":["comparison_run_digest","comparison_run_ref","comparison_digest","left_object_version_digest","right_object_version_digest","summary","freshness"]}'::jsonb
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'object-truth-record-comparison-run',
    p_operation_name        := 'object_truth_record_comparison_run',
    p_handler_ref           := 'runtime.operations.commands.object_truth.handle_record_comparison_run',
    p_input_model_ref       := 'runtime.operations.commands.object_truth.RecordComparisonRunCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/record-comparison-run',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'object_truth.comparison_run_recorded',
    p_decision_ref          := 'architecture-policy::product-architecture::object-truth-requires-deterministic-parse-compare-substrate',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_record_comparison_run.20260428',
    p_label                 := 'Object Truth Record Comparison Run',
    p_summary               := 'Compare two persisted object-truth object versions and persist the comparison output.'
);

COMMIT;
