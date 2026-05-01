-- Migration 409: Harden agent_wakes dedup by including trigger_event_id
-- in the unique key.
--
-- Phase A trigger convergence — closes a gap that BUG-F28F5090's review
-- pointed at: dedup must use source event identity plus payload hash,
-- not payload hash alone. Two distinct events (different
-- trigger_event_id) with identical payloads are real, separate wakes —
-- not duplicates. The previous unique index on
-- (agent_principal_ref, trigger_kind, payload_hash) collapsed them
-- and silently dropped the second event.
--
-- Replace the index with one that also keys on trigger_event_id.
-- Rows where trigger_event_id is NULL (manual wakes, chat) keep the
-- prior dedup semantics — payload_hash alone — by collapsing NULL to a
-- shared sentinel via COALESCE(trigger_event_id, 0).

BEGIN;

DROP INDEX IF EXISTS idx_agent_wakes_payload_dedup;

CREATE UNIQUE INDEX idx_agent_wakes_payload_event_dedup
    ON agent_wakes (
        agent_principal_ref,
        trigger_kind,
        payload_hash,
        COALESCE(trigger_event_id, 0)
    )
    WHERE payload_hash IS NOT NULL;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'agent_wakes.dedup_key.event_id_inclusive',
    'agent_wakes dedup key includes trigger_event_id',
    'definition',
    'Hardened agent_wakes dedup so two distinct events with identical payloads but different trigger_event_id produce two separate wake rows. The unique key is (agent_principal_ref, trigger_kind, payload_hash, COALESCE(trigger_event_id, 0)).',
    '{"migration":"409_agent_wake_dedup_event_id.sql"}'::jsonb,
    '{"closes_review_finding":"dedup_by_source_event_identity_plus_payload_hash"}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
