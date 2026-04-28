-- Migration 317: Register guarded runtime remediation.
--
-- This command applies only local, typed, low-risk runtime repairs and emits
-- a durable event. It is explicitly not a workflow retry surface.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-remediation-apply',
    p_operation_name        := 'operator.remediation_apply',
    p_handler_ref           := 'runtime.operations.commands.runtime_remediation.handle_runtime_remediation_apply',
    p_input_model_ref       := 'runtime.operations.commands.runtime_remediation.RuntimeRemediationApplyCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator/remediation-apply',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_type            := 'runtime.remediation.applied',
    p_decision_ref          := 'decision.operation_catalog_registry.runtime_remediation.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.runtime_remediation.20260428',
    p_label                 := 'Operation: operator.remediation_apply',
    p_summary               := 'Guarded runtime remediation command. Cleans stale provider slot counters or expired host-resource leases for typed failures, refuses human-gated repairs, and never retries workflow jobs.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, event_type, http_path
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'operator-remediation-apply';
