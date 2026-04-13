-- Persistent agent sessions: agents maintain identity, context, and
-- event cursor across tool calls and retries.

CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id  TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    workflow_id TEXT NOT NULL DEFAULT '',
    job_label   TEXT NOT NULL,
    agent_slug  TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active',
    context_json JSONB NOT NULL DEFAULT '{}',
    event_cursor BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_run ON agent_sessions (run_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_active ON agent_sessions (status) WHERE status = 'active';
