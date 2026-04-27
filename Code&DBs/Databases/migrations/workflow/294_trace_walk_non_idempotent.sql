-- Migration 294: Flip trace.walk from read_only to non_idempotent.
--
-- Roadmap:
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge
--     .causal.tracing.phase.1.cause.receipt.id.correlation.id
--     .on.receipts.events.plus.praxis.trace.lens
--
-- Migration 291 registered trace.walk with idempotency_policy='read_only'
-- (queries with the same input_hash replay the cached result). That makes
-- sense for stable-state queries but is wrong for trace.walk specifically:
-- the lens is used to *watch* a flow as receipts land, so a second call
-- with the same correlation_id should refetch fresh data, not replay the
-- empty result the first call captured before the trace finished.
--
-- Detected end-to-end on 2026-04-27 while verifying the spawn_threaded
-- fix on a real compose flow:
--   1. praxis_compose_plan_via_llm kicks off, returns kickoff
--      correlation_id 80bba9b5...
--   2. praxis_trace(correlation_id=80bba9b5...) called immediately —
--      compose still running, returns 0 nodes (correct, but cached)
--   3. ~30s later, compose receipt lands in authority_operation_receipts
--   4. praxis_trace(correlation_id=80bba9b5...) called again — replays
--      the stale empty cache, returns 0 nodes despite the receipt now
--      existing (wrong)
--
-- Read receipts still get written (non_idempotent doesn't disable the
-- ledger) — only the replay-from-cache short-circuit is removed.

BEGIN;

UPDATE operation_catalog_registry
   SET idempotency_policy = 'non_idempotent',
       updated_at = now()
 WHERE operation_ref = 'trace-walk';

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, idempotency_policy
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'trace-walk';
