-- Migration 356: Register Client Operating Model operator-view query.
--
-- Phase 12 exposes the Phase 11 read-model substrate through the CQRS
-- gateway. The operation is read-only and derives bounded operator views from
-- provided evidence; durable projection storage remains a later slice.

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
    'authority.client_operating_model',
    'praxis.engine',
    'stream.authority.client_operating_model',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'client-operating-model-operator-view',
    p_operation_name        := 'client_operating_model_operator_view',
    p_handler_ref           := 'runtime.operations.queries.client_operating_model.handle_client_operating_model_view',
    p_input_model_ref       := 'runtime.operations.queries.client_operating_model.QueryClientOperatingModelView',
    p_authority_domain_ref  := 'authority.client_operating_model',
    p_authority_ref         := 'authority.client_operating_model.operator_views',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/client-operating-model/view',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_operating_model_operator_view.20260430',
    p_label                 := 'Client Operating Model Operator View',
    p_summary               := 'Build one read-only Client Operating Model operator view from scoped evidence payloads.'
);

COMMIT;
