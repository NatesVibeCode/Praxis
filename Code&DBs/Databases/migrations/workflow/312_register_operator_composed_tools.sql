-- Migration 312: Register composed operator read tools.
--
-- These operations combine existing gateway-dispatched query operations into
-- higher-signal operator packets. Each child read keeps its own receipt while
-- the composed read receives a parent receipt, preserving traceability.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-execution-truth',
    p_operation_name        := 'operator.execution_truth',
    p_handler_ref           := 'runtime.operations.queries.operator_composed.handle_query_execution_truth',
    p_input_model_ref       := 'runtime.operations.queries.operator_composed.QueryExecutionTruth',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/execution-truth',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.operator_composed.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.operator_composed.20260428',
    p_label                 := 'Operation: operator.execution_truth',
    p_summary               := 'Composed execution-truth packet. Combines status snapshot, optional run views, and optional causal trace through child gateway queries so green-looking workflow state is checked against independent proof.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-next-work',
    p_operation_name        := 'operator.next_work',
    p_handler_ref           := 'runtime.operations.queries.operator_composed.handle_query_next_work',
    p_input_model_ref       := 'runtime.operations.queries.operator_composed.QueryNextWork',
    p_authority_domain_ref  := 'authority.bugs',
    p_authority_ref         := 'authority.bugs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/next-work',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.operator_composed.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.operator_composed.20260428',
    p_label                 := 'Operation: operator.next_work',
    p_summary               := 'Composed next-work packet. Combines refactor heatmap, bug triage, work assignment matrix, and runtime status into one ranked operator read model with proof gates and validation paths.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-provider-route-truth',
    p_operation_name        := 'operator.provider_route_truth',
    p_handler_ref           := 'runtime.operations.queries.operator_composed.handle_query_provider_route_truth',
    p_input_model_ref       := 'runtime.operations.queries.operator_composed.QueryProviderRouteTruth',
    p_authority_domain_ref  := 'authority.provider_onboarding',
    p_authority_ref         := 'authority.provider_onboarding',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/provider-route-truth',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.operator_composed.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.operator_composed.20260428',
    p_label                 := 'Operation: operator.provider_route_truth',
    p_summary               := 'Composed provider-route truth packet. Combines provider control plane and model access control matrix to expose runnable, blocked, mixed, or unknown route state with removal reasons.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-operation-forge',
    p_operation_name        := 'operator.operation_forge',
    p_handler_ref           := 'runtime.operations.queries.operator_composed.handle_query_operation_forge',
    p_input_model_ref       := 'runtime.operations.queries.operator_composed.QueryOperationForge',
    p_authority_domain_ref  := 'authority.cqrs',
    p_authority_ref         := 'authority.cqrs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/operation-forge',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.operator_composed.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.operator_composed.20260428',
    p_label                 := 'Operation: operator.operation_forge',
    p_summary               := 'Plan-only CQRS operation forge. Produces the register_operation payload, MCP wrapper name, row-chain contract, and reject paths before adding a new operation or tool.'
);

COMMIT;
