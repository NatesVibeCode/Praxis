-- Migration 354: Register `typed_gap.emit` as a gateway-dispatched
--                 operation so its authority_events row carries a receipt_id.
--
-- Anchor decision:
--   architecture-policy::policy-authority::receipts-immutable
--   (operator_decisions row, registered in migration 296)
--   architecture-policy::concurrency::operation-execution-lane-typing
--   (operator_decisions row, registered in migration 348)
--
-- Why this exists
--   Phase D of the public-beta concurrency push: close the event-bypass
--   writers so every authority_events row has a receipt_id linkage.
--
--   Two helpers were emitting authority_events rows without going through
--   the CQRS gateway:
--     1. `runtime.feedback_authority.record_feedback_event` — already had
--        a registered operation `feedback.record`, but the call sites
--        bypassed the gateway. Phase D ships
--        `record_feedback_event_via_gateway` and updates the two callers
--        (`capability_feedback.py`, `task_type_router.py`) to dispatch
--        through it; no schema change needed for that piece.
--     2. `runtime.typed_gap_events.emit_typed_gap` — had NO registered
--        operation. Its INSERT into authority_events declared
--        operation_ref='typed_gap.emit' even though no row in
--        operation_catalog_registry backed that ref. This migration
--        registers the operation so receipts can be attached, and the
--        helper now dispatches through the gateway.
--
-- Lane classification
--   `system` lane (no caller-side timeout enforcement) because typed_gap
--   emission is invoked by runtime internals — compile errors, module
--   indexer findings, intent_composition type-flow errors — and is on the
--   hot path of any operation that produces a structured gap. We do NOT
--   want a 5s/15s gateway deadline to truncate gap emission and lose
--   observability when the host operation is itself slow. The system lane
--   matches the role: runtime/control-plane housekeeping that the
--   gateway should pass through unimpeded.
--
-- Idempotency
--   register_operation_atomic uses ON CONFLICT DO UPDATE so re-applying
--   the migration converges.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'typed-gap-emit',
    p_operation_name        := 'typed_gap.emit',
    p_handler_ref           := 'runtime.typed_gap_events.handle_emit_typed_gap',
    p_input_model_ref       := 'runtime.typed_gap_events.EmitTypedGapCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/typed_gap/emit',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'typed_gap.created',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    p_summary               := 'Emit a typed_gap.created authority event when a gap-producing surface detects unsatisfied preconditions (compile errors, module indexer findings, type-flow errors). Phase D: previously bypassed the gateway; now dispatches through it so the authority_events row carries a receipt_id linkage.',
    p_execution_lane        := 'system',
    p_kickoff_required      := FALSE
);

COMMIT;
