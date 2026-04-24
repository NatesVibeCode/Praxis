-- Migration 223: add 'retired' to roadmap_items.lifecycle CHECK.
--
-- Context: praxis_operator_write gains update/retire semantics (Phase 1.7 of
-- the public beta ramp). Retiring a roadmap item is an authoring concern,
-- not a proof-backed closeout. 'completed' already exists for work that
-- finished; 'retired' marks rows that were misfiled, superseded, or pulled
-- from scope without proof of delivery. Tracked by BUG-BAC9B36F.

ALTER TABLE roadmap_items
    DROP CONSTRAINT IF EXISTS roadmap_items_lifecycle_check;

ALTER TABLE roadmap_items
    ADD CONSTRAINT roadmap_items_lifecycle_check
    CHECK (lifecycle IN ('idea', 'planned', 'claimed', 'completed', 'retired'));
