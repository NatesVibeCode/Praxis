-- Migration 318: Register the first object-truth query operation.
--
-- This lands the non-mutating gateway seam for deterministic object evidence:
-- inline record -> schema/version/field observations/identity digest.
--
-- Durable object-truth evidence tables are intentionally deferred. This
-- operation is the safe read-only boundary that future MCP, HTTP, workflow,
-- and persistence slices can call without bypassing operation receipts.

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
    'authority.object_truth',
    'praxis.engine',
    'stream.authority.object_truth',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'object_truth.query.observe_record',
    p_operation_name        := 'object_truth_observe_record',
    p_handler_ref           := 'runtime.operations.queries.object_truth.handle_observe_record',
    p_input_model_ref       := 'runtime.operations.queries.object_truth.QueryObserveRecord',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/observe-record',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::product-architecture::object-truth-requires-deterministic-parse-compare-substrate',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_observe_record.20260428',
    p_label                 := 'Object Truth Observe Record',
    p_summary               := 'Build deterministic object-truth evidence from one inline record without writing durable state.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, operation_kind, authority_domain_ref
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'object_truth.query.observe_record';
