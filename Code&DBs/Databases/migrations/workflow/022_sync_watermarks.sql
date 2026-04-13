-- Migration 022: Watermark tracking for incremental knowledge graph sync
--
-- Tracks per-source sync progress so the heartbeat cycle only processes
-- new rows each time. One row per source table.

CREATE TABLE IF NOT EXISTS memory_sync_watermarks (
    source_name   TEXT PRIMARY KEY,
    last_synced_id BIGINT DEFAULT 0,
    last_synced_at TIMESTAMPTZ DEFAULT '1970-01-01',
    rows_synced   BIGINT DEFAULT 0,
    last_cycle_at  TIMESTAMPTZ,
    error_count   INTEGER DEFAULT 0,
    last_error    TEXT DEFAULT ''
);

-- Seed rows for each source
INSERT INTO memory_sync_watermarks (source_name) VALUES
  ('receipt_meta'), ('bugs'), ('dispatch_constraints'),
  ('friction_events'), ('operator_decisions')
ON CONFLICT DO NOTHING;
