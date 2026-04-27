-- Migration 292: Drop strict FKs from causal-tracing columns.
--
-- Roadmap:
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge
--     .causal.tracing.phase.1.cause.receipt.id.correlation.id
--     .on.receipts.events.plus.praxis.trace.lens
--
-- Migration 290 added cause_receipt_id (FK → authority_operation_receipts)
-- and causation_event_id (FK → authority_events) with ON DELETE SET NULL.
-- That looked clean on paper but breaks the gateway's natural write order:
--
--   1. Outer gateway call A pre-mints receipt_id_A and sets the
--      ContextVar(cause=A, correlation=X) before calling its handler.
--   2. Handler runs. Inside the handler, a nested gateway call B
--      pre-mints receipt_id_B, reads ContextVar, and tries to commit
--      a row with cause_receipt_id = receipt_id_A.
--   3. But receipt A has not been written yet — A's persist runs AFTER
--      the handler returns. B's transaction commits FIRST, so the FK
--      check fails: ForeignKeyViolationError on
--      authority_operation_receipts_cause_receipt_id_fkey.
--
-- Detected end-to-end on 2026-04-27 while verifying auto-propagation
-- via ContextVar (CURRENT_CALLER_CONTEXT).
--
-- The cause_receipt_id and causation_event_id columns are best-effort
-- causal pointers, not strict references. The trace.walk lens already
-- handles dangling references via orphan_count. Drop the FK constraints
-- so the natural nested write order works; keep the columns NULL-able
-- and indexed.

BEGIN;

ALTER TABLE authority_operation_receipts
    DROP CONSTRAINT IF EXISTS authority_operation_receipts_cause_receipt_id_fkey;

ALTER TABLE authority_events
    DROP CONSTRAINT IF EXISTS authority_events_causation_event_id_fkey;

-- Update the sentinel marker to reflect the FK drop.
UPDATE data_dictionary_objects
   SET summary = (
        'Sentinel row signaling that authority_operation_receipts has '
        || 'cause_receipt_id + correlation_id and authority_events has '
        || 'causation_event_id + correlation_id. Migration 292 dropped '
        || 'the FK constraints because nested gateway calls commit child '
        || 'receipts BEFORE the parent receipt exists; cause pointers are '
        || 'best-effort. Read by the gateway caller_context threading work '
        || 'and the trace.walk query op to confirm schema readiness.'
       ),
       metadata = jsonb_set(
           metadata,
           '{fks_dropped_in}',
           to_jsonb('migration.292'::text)
       ),
       updated_at = now()
 WHERE object_kind = 'definition.authority_operation_receipts.causal_tracing_v290';

COMMIT;

-- Verification (run manually):
--
--   SELECT conname, contype FROM pg_constraint
--    WHERE conrelid = 'authority_operation_receipts'::regclass
--      AND conname LIKE '%cause%';
--   -- Expect: empty result (FK dropped).
--
--   SELECT conname, contype FROM pg_constraint
--    WHERE conrelid = 'authority_events'::regclass
--      AND conname LIKE '%causation%';
--   -- Expect: empty result (FK dropped).
