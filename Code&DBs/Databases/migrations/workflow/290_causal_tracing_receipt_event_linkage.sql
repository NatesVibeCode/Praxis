-- Migration 290: Causal tracing — link receipts and events into a walkable cause tree.
--
-- Roadmap:
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge
--     .causal.tracing.phase.1.cause.receipt.id.correlation.id
--     .on.receipts.events.plus.praxis.trace.lens
--
-- Problem this migration addresses:
--
--   authority_operation_receipts has caller_ref but the gateway hardcodes the
--   string 'authority_gateway' at insert. Every receipt looks like it came
--   from the gateway itself, with no link back to the receipt that caused it.
--   authority_events carries receipt_id (the receipt that emitted the event)
--   but nothing pointing at the event that caused this one.
--
--   Result: praxis_run_lineage walks evidence_timeline for one run_id, but
--   there is no way to walk the causal chain across operations — e.g. from
--   a chat message receipt to the compose receipt it triggered to the
--   verifier receipt that proved the resulting code change.
--
-- Phase 1 of the staged tracing roadmap adds two nullable fields to each
-- side of the proof pair:
--
--   * authority_operation_receipts.cause_receipt_id
--       Self-FK pointing at the receipt that triggered this one. NULL means
--       this is an entry-point receipt (root of a trace tree).
--   * authority_operation_receipts.correlation_id
--       Same UUID for every receipt in the same trace tree. Set at the root,
--       inherited by descendants. Lets a lens fetch a whole trace in one
--       indexed query without walking the tree edge-by-edge.
--   * authority_events.causation_event_id
--       Self-FK to the event that caused this event. Different from
--       receipt_id (which points at the receipt that emitted *this* event).
--   * authority_events.correlation_id
--       Mirror of the receipts column — the same UUID stamps every event
--       in the same trace.
--
-- All four columns are nullable. Existing rows stay valid. The gateway will
-- start populating the new fields when caller_context is threaded through
-- aexecute_operation_binding (separate code change, same roadmap item).
-- The trace lens (trace.walk query op + praxis_trace MCP wrapper) reads
-- these columns and degrades gracefully on orphaned subtrees during
-- incremental adoption.
--
-- Indexes target the two access patterns the lens needs:
--   1. Walk down: "what receipts did this one cause?" → index on cause_receipt_id.
--   2. Fetch trace: "show me everything with this correlation_id" → index on correlation_id.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- (1) authority_operation_receipts: cause_receipt_id + correlation_id
-- ──────────────────────────────────────────────────────────────────────────
ALTER TABLE authority_operation_receipts
    ADD COLUMN IF NOT EXISTS cause_receipt_id UUID
        REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL;

ALTER TABLE authority_operation_receipts
    ADD COLUMN IF NOT EXISTS correlation_id UUID;

CREATE INDEX IF NOT EXISTS authority_operation_receipts_cause_idx
    ON authority_operation_receipts (cause_receipt_id)
    WHERE cause_receipt_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS authority_operation_receipts_correlation_idx
    ON authority_operation_receipts (correlation_id, created_at)
    WHERE correlation_id IS NOT NULL;

-- ──────────────────────────────────────────────────────────────────────────
-- (2) authority_events: causation_event_id + correlation_id
-- ──────────────────────────────────────────────────────────────────────────
ALTER TABLE authority_events
    ADD COLUMN IF NOT EXISTS causation_event_id UUID
        REFERENCES authority_events (event_id) ON DELETE SET NULL;

ALTER TABLE authority_events
    ADD COLUMN IF NOT EXISTS correlation_id UUID;

CREATE INDEX IF NOT EXISTS authority_events_causation_idx
    ON authority_events (causation_event_id)
    WHERE causation_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS authority_events_correlation_idx
    ON authority_events (correlation_id, event_sequence)
    WHERE correlation_id IS NOT NULL;

-- ──────────────────────────────────────────────────────────────────────────
-- (3) Sentinel marker so bootstrap can detect the migration applied
-- ──────────────────────────────────────────────────────────────────────────
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'definition.authority_operation_receipts.causal_tracing_v290',
    'Causal tracing column marker (migration 290)',
    'definition',
    'Sentinel row signaling that authority_operation_receipts has cause_receipt_id + correlation_id and authority_events has causation_event_id + correlation_id. Read by the gateway caller_context threading work and the trace.walk query op to confirm schema readiness before populating or reading the new fields.',
    jsonb_build_object('source', 'migration.290_causal_tracing_receipt_event_linkage'),
    jsonb_build_object(
        'marker_for', 'migration.290',
        'roadmap_item', 'roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge.causal.tracing.phase.1.cause.receipt.id.correlation.id.on.receipts.events.plus.praxis.trace.lens',
        'columns_added', jsonb_build_array(
            'authority_operation_receipts.cause_receipt_id',
            'authority_operation_receipts.correlation_id',
            'authority_events.causation_event_id',
            'authority_events.correlation_id'
        )
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--
--   -- Confirm columns exist:
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name = 'authority_operation_receipts'
--      AND column_name IN ('cause_receipt_id', 'correlation_id');
--
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name = 'authority_events'
--      AND column_name IN ('causation_event_id', 'correlation_id');
--
--   -- Confirm indexes exist:
--   SELECT indexname FROM pg_indexes
--    WHERE tablename IN ('authority_operation_receipts', 'authority_events')
--      AND indexname LIKE '%cause%' OR indexname LIKE '%correlation%';
--
--   -- Confirm sentinel:
--   SELECT object_kind, summary FROM data_dictionary_objects
--    WHERE object_kind = 'definition.authority_operation_receipts.causal_tracing_v290';
