-- Migration 293: Add bugs.correlation_id so trace.walk can resolve bug_id anchors.
--
-- Roadmap:
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge
--     .causal.tracing.phase.1.cause.receipt.id.correlation.id
--     .on.receipts.events.plus.praxis.trace.lens
--
-- After migration 290+292 (causal-tracing columns) and migration 291
-- (trace.walk operation), the bug_id anchor resolution path tried to
-- chain bug.discovered_in_receipt_id → authority_operation_receipts.
-- That failed end-to-end on 2026-04-27 because every existing row in
-- bugs.discovered_in_receipt_id is a legacy receipt-ref string
-- (e.g. ``receipt:workflow_5cdd861469ea:113:1``), not a UUID — those
-- receipts predate authority_operation_receipts entirely.
--
-- The correct linkage is correlation_id, captured at bug-filing time
-- from the CURRENT_CALLER_CONTEXT ContextVar. This migration adds the
-- column nullable so legacy bugs stay valid; bug_tracker.file_bug
-- populates it for new filings in CQRS-routed flows. trace.walk now
-- prefers this column over the legacy receipt-ref chain, so bug_id
-- anchors resolve cleanly for any bug filed inside a gateway call.
--
-- Backfill: skipped on purpose. Legacy bug rows have no correlation_id
-- to recover — the receipts they reference were never threaded through
-- the gateway, so no correlation was ever stamped. The column stays
-- NULL for those rows and trace.walk returns trace.no_correlation
-- (honest behavior).

BEGIN;

ALTER TABLE bugs
    ADD COLUMN IF NOT EXISTS correlation_id UUID;

CREATE INDEX IF NOT EXISTS bugs_correlation_id_idx
    ON bugs (correlation_id)
    WHERE correlation_id IS NOT NULL;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'definition.bugs.correlation_id_v293',
    'Bug correlation_id column marker (migration 293)',
    'definition',
    'Sentinel signaling that bugs.correlation_id was added so trace.walk can resolve bug_id anchors via the same correlation graph as receipt_id and event_id. Populated by bug_tracker.file_bug from current_caller_context() at filing time. Legacy bugs (filed before this migration or outside a CQRS-routed flow) stay NULL and return trace.no_correlation cleanly.',
    jsonb_build_object('source', 'migration.293_bugs_correlation_id_for_tracing'),
    jsonb_build_object(
        'marker_for', 'migration.293',
        'roadmap_item', 'roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge.causal.tracing.phase.1.cause.receipt.id.correlation.id.on.receipts.events.plus.praxis.trace.lens',
        'columns_added', jsonb_build_array('bugs.correlation_id')
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name = 'bugs' AND column_name = 'correlation_id';
--
--   -- After the next CQRS-routed bug filing, a row appears here:
--   SELECT bug_id, correlation_id FROM bugs
--    WHERE correlation_id IS NOT NULL ORDER BY created_at DESC LIMIT 5;
