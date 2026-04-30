-- Migration 355: Register the Object Truth readiness query.
--
-- Phase 0 of the client operating model needs one read-only authority gate
-- before downstream discovery, ingestion, and Virtual Lab work fans out.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'object-truth-readiness',
    p_operation_name        := 'object_truth_readiness',
    p_handler_ref           := 'runtime.operations.queries.object_truth.handle_readiness',
    p_input_model_ref       := 'runtime.operations.queries.object_truth.QueryReadiness',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/object-truth/readiness',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_readiness.20260430',
    p_label                 := 'Object Truth Readiness',
    p_summary               := 'Inspect whether Object Truth authority is ready for downstream client-system discovery, ingestion, and Virtual Lab planning.'
);

COMMIT;
