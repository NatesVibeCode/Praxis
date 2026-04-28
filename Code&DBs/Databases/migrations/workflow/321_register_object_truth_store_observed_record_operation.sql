-- Migration 321: Register the object-truth persistence command.

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
    'event_contract.object_truth.object_version_stored',
    'object_truth.object_version_stored',
    'authority.object_truth',
    'data_dictionary.object.object_truth_object_version_stored_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'operator_decision.architecture_policy.product_architecture.object_truth_inference_from_cross_system_samples',
    '{"expected_payload_fields":["object_version_digest","object_version_ref","system_ref","object_ref","identity_digest","payload_digest","field_observation_count"]}'::jsonb
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
    p_operation_ref         := 'object_truth.command.store_observed_record',
    p_operation_name        := 'object_truth_store_observed_record',
    p_handler_ref           := 'runtime.operations.commands.object_truth.handle_store_observed_record',
    p_input_model_ref       := 'runtime.operations.commands.object_truth.StoreObservedRecordCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/store-observed-record',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'object_truth.object_version_stored',
    p_decision_ref          := 'architecture-policy::product-architecture::object-truth-inference-from-cross-system-samples',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_store_observed_record.20260428',
    p_label                 := 'Object Truth Store Observed Record',
    p_summary               := 'Build and persist deterministic object-truth evidence from one inline record.'
);

COMMIT;
