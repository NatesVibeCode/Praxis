-- Migration 344: register the authority.compose_binding.resolve query operation.
--
-- This is the compose-time canonical resolution query. Given a set of target
-- authority units, it returns the canonical write scope, the read-only
-- predecessor obligation pack, and explicit blocked-compat units. Plan
-- composition will eventually bind this output into every packet so agents
-- are handed a workspace where duplicate authority is invisible — the active
-- prevention behind the impact contract (decision
-- architecture-policy::platform-architecture::candidate-authority-impact-contract).
--
-- Registration shape produced by praxis_operation_forge wizard.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'authority-compose-binding-resolve',
    p_operation_name        := 'authority.compose_binding.resolve',
    p_handler_ref           := 'runtime.operations.queries.compose_authority_binding.handle_resolve_compose_authority_binding',
    p_input_model_ref       := 'runtime.operations.queries.compose_authority_binding.ResolveComposeAuthorityBinding',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority/compose_binding/resolve',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.compose_time_canonical_resolution',
    p_summary               := 'Compose-time canonical authority resolution: given a set of target authority units, return the canonical write scope, the read-only predecessor obligation pack, and the explicit blocked-compat path list. New packets bind this so agents are handed a workspace where duplicate authority is invisible.'
);

COMMIT;
