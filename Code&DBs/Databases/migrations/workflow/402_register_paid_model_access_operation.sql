-- Migration 402: Repair paid-model access CQRS operation registration.
--
-- Migration 399 owns the paid-model lease tables. This migration keeps the
-- operation registration independently replayable for databases that applied
-- the table slice before the gateway operation was added.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-paid-model-access',
    p_operation_name        := 'operator.paid_model_access',
    p_handler_ref           := 'runtime.operations.commands.paid_model_access.handle_paid_model_access',
    p_input_model_ref       := 'runtime.operations.commands.paid_model_access.PaidModelAccessCommand',
    p_authority_domain_ref  := 'authority.access_control',
    p_authority_ref         := 'authority.access_control',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator_paid_model_access',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_access_control.paid_model_lease_changed',
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::model-access-control::paid-model-use-requires-explicit-scoped-approval-and-hard-off',
    p_binding_revision      := 'binding.operation_catalog_registry.paid_model_access.20260501.repair',
    p_label                 := 'Paid Model Access',
    p_summary               := 'Grant, revoke, consume, inspect, and presentation-soft-off exact one-run paid model access leases. Backend hard-off remains private_provider_model_access_denials.'
);

COMMIT;
