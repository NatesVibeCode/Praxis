-- Migration 291: Register trace.walk as a CQRS query operation.
--
-- Roadmap:
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge
--     .causal.tracing.phase.1.cause.receipt.id.correlation.id
--     .on.receipts.events.plus.praxis.trace.lens
--
-- This is the lens half of Phase 1. Migration 290 added cause_receipt_id +
-- correlation_id columns; the gateway now stamps them on every receipt and
-- event. This migration registers the query op that walks those edges.
--
-- Anchor types: receipt_id, event_id, or correlation_id (exactly one).
-- The handler resolves the anchor to a correlation_id, fetches every
-- receipt + event sharing that correlation, and returns root + nodes +
-- edges + events + orphan_count.
--
-- Idempotency policy is read_only so identical queries can replay from
-- the receipt cache. Posture is observe — strictly read.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'trace-walk',
    p_operation_name        := 'trace.walk',
    p_handler_ref           := 'runtime.operations.queries.trace_walk.handle_query_trace_walk',
    p_input_model_ref       := 'runtime.operations.queries.trace_walk.QueryTraceWalk',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_label                 := 'Operation: trace.walk',
    p_summary               := 'Walk the cause tree for any anchor (receipt_id, event_id, or correlation_id) and return the rooted DAG of receipts plus the events they emitted. Phase 1 of causal tracing — relies on cause_receipt_id + correlation_id columns added by migration 290 and gateway threading of caller_context. Async ContextVar propagation (Phase 2) and cross-process envelope (Phase 3) will close orphan-subtree gaps; this lens already returns orphan_count so callers can see when a trace is incomplete.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, posture, idempotency_policy, handler_ref
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'trace-walk';
--
--   curl -sS -X POST http://localhost:8420/api/operate \
--        -H 'Content-Type: application/json' \
--        -d '{"operation":"trace.walk","input":{"correlation_id":"<uuid>"}}' | jq .result
