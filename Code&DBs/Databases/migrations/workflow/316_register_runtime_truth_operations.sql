-- Migration 316: Register runtime truth and remediation read models.
--
-- These operations turn "submitted" versus "actually firing" into a CQRS
-- read surface: runtime evidence, a fail-closed firecheck, and typed
-- remediation plans with required retry deltas.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-runtime-truth-snapshot',
    p_operation_name        := 'operator.runtime_truth_snapshot',
    p_handler_ref           := 'runtime.operations.queries.runtime_truth.handle_query_runtime_truth_snapshot',
    p_input_model_ref       := 'runtime.operations.queries.runtime_truth.QueryRuntimeTruthSnapshot',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/runtime-truth',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.runtime_truth.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.runtime_truth.20260428',
    p_label                 := 'Operation: operator.runtime_truth_snapshot',
    p_summary               := 'Runtime truth snapshot across DB authority, queue state, worker heartbeats, provider slots, host-resource leases, Docker, manifest hydration audit, and recent typed failures.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-firecheck',
    p_operation_name        := 'operator.firecheck',
    p_handler_ref           := 'runtime.operations.queries.runtime_truth.handle_query_firecheck',
    p_input_model_ref       := 'runtime.operations.queries.runtime_truth.QueryFirecheck',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/firecheck',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.runtime_truth.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.runtime_truth.20260428',
    p_label                 := 'Operation: operator.firecheck',
    p_summary               := 'Fail-closed workflow launch preflight that returns can_fire, typed blockers, and remediation plans so submitted state is not mistaken for runtime proof.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-remediation-plan',
    p_operation_name        := 'operator.remediation_plan',
    p_handler_ref           := 'runtime.operations.queries.runtime_truth.handle_query_remediation_plan',
    p_input_model_ref       := 'runtime.operations.queries.runtime_truth.QueryRemediationPlan',
    p_authority_domain_ref  := 'authority.receipts',
    p_authority_ref         := 'authority.receipts',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/remediation-plan',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.operation_catalog_registry.runtime_truth.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.runtime_truth.20260428',
    p_label                 := 'Operation: operator.remediation_plan',
    p_summary               := 'Typed workflow failure remediation planner with safe automation tier, evidence requirements, approval gate, and retry delta.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, http_path
--     FROM operation_catalog_registry
--    WHERE operation_ref IN (
--      'operator-runtime-truth-snapshot',
--      'operator-firecheck',
--      'operator-remediation-plan'
--    );
