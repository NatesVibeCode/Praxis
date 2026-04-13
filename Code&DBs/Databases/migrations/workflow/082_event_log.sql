-- Durable event log: append-only backbone for the Praxis service bus.
-- Replaces the old workflow_notifications "delivered" flag pattern with
-- cursor-based consumption. Events are never deleted by consumers —
-- archival is a separate retention job.

CREATE TABLE IF NOT EXISTS event_log (
    id          BIGSERIAL PRIMARY KEY,
    channel     TEXT        NOT NULL,           -- e.g. 'build_state', 'job_lifecycle', 'system'
    event_type  TEXT        NOT NULL,           -- e.g. 'mutation', 'compilation', 'job_claimed', 'job_completed'
    entity_id   TEXT        NOT NULL DEFAULT '', -- workflow_id, run_id, or other anchor
    entity_kind TEXT        NOT NULL DEFAULT '', -- 'workflow', 'run', 'job', 'session'
    payload     JSONB       NOT NULL DEFAULT '{}',
    emitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    emitted_by  TEXT        NOT NULL DEFAULT ''  -- 'compiler', 'worker', 'api', 'mcp'
);

-- Primary consumption pattern: "give me events on channel X for entity Y since cursor Z"
CREATE INDEX IF NOT EXISTS idx_event_log_channel_entity
    ON event_log (channel, entity_id, id);

-- Secondary: "give me all events since cursor Z" (cross-channel catch-up)
CREATE INDEX IF NOT EXISTS idx_event_log_id ON event_log (id);

-- Retention: older events by time
CREATE INDEX IF NOT EXISTS idx_event_log_emitted_at ON event_log (emitted_at);

-- Consumer cursors: each subscriber tracks their position in the log.
-- Subscribers are identified by (subscriber_id, channel).
CREATE TABLE IF NOT EXISTS event_log_cursors (
    subscriber_id TEXT NOT NULL,
    channel       TEXT NOT NULL,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subscriber_id, channel)
);
