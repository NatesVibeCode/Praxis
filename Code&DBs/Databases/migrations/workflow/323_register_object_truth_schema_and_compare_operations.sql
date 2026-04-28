-- Migration 323: Register schema snapshot persistence and persisted comparison.

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
    'event_contract.object_truth.schema_snapshot_stored',
    'object_truth.schema_snapshot_stored',
    'authority.object_truth',
    'data_dictionary.object.object_truth_schema_snapshot_stored_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate',
    '{"expected_payload_fields":["schema_snapshot_digest","schema_snapshot_ref","system_ref","object_ref","field_count"]}'::jsonb
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
    p_operation_ref         := 'object-truth-store-schema-snapshot',
    p_operation_name        := 'object_truth_store_schema_snapshot',
    p_handler_ref           := 'runtime.operations.commands.object_truth.handle_store_schema_snapshot',
    p_input_model_ref       := 'runtime.operations.commands.object_truth.StoreSchemaSnapshotCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/store-schema-snapshot',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'object_truth.schema_snapshot_stored',
    p_decision_ref          := 'architecture-policy::product-architecture::object-truth-requires-deterministic-parse-compare-substrate',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_store_schema_snapshot.20260428',
    p_label                 := 'Object Truth Store Schema Snapshot',
    p_summary               := 'Normalize and persist deterministic schema snapshot evidence for one external object.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'object-truth-compare-versions',
    p_operation_name        := 'object_truth_compare_versions',
    p_handler_ref           := 'runtime.operations.queries.object_truth.handle_compare_versions',
    p_input_model_ref       := 'runtime.operations.queries.object_truth.QueryCompareVersions',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/object-truth/compare-versions',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::product-architecture::object-truth-requires-deterministic-parse-compare-substrate',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_compare_versions.20260428',
    p_label                 := 'Object Truth Compare Versions',
    p_summary               := 'Compare two persisted object-truth object versions without writing durable state.'
);

COMMIT;
