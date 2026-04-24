BEGIN;

ALTER TABLE agent_sessions
    ADD COLUMN IF NOT EXISTS session_kind TEXT NOT NULL DEFAULT 'workflow_mcp',
    ADD COLUMN IF NOT EXISTS external_session_id TEXT,
    ADD COLUMN IF NOT EXISTS display_title TEXT,
    ADD COLUMN IF NOT EXISTS principal_ref TEXT,
    ADD COLUMN IF NOT EXISTS workspace_ref TEXT,
    ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS revoked_by TEXT,
    ADD COLUMN IF NOT EXISTS revoke_reason TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'agent_sessions_session_kind_valid_check'
    ) THEN
        ALTER TABLE agent_sessions
            ADD CONSTRAINT agent_sessions_session_kind_valid_check
            CHECK (session_kind IN ('workflow_mcp', 'interactive_cli'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'agent_sessions_revocation_detail_check'
    ) THEN
        ALTER TABLE agent_sessions
            ADD CONSTRAINT agent_sessions_revocation_detail_check
            CHECK (
                revoked_at IS NULL
                OR (
                    revoked_by IS NOT NULL
                    AND btrim(revoked_by) <> ''
                    AND revoke_reason IS NOT NULL
                    AND btrim(revoke_reason) <> ''
                )
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_agent_sessions_kind_activity
    ON agent_sessions (session_kind, last_activity_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_principal_activity
    ON agent_sessions (principal_ref, last_activity_at DESC)
    WHERE principal_ref IS NOT NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS agent_session_events (
    event_id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES agent_sessions (session_id) ON DELETE CASCADE,
    event_kind TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    text_content TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT agent_session_events_kind_nonblank_check
        CHECK (btrim(event_kind) <> '')
);

CREATE INDEX IF NOT EXISTS idx_agent_session_events_session_created
    ON agent_session_events (session_id, created_at, event_id);

COMMENT ON COLUMN agent_sessions.session_kind IS
    'Distinguishes workflow MCP token sessions from interactive CLI conversations.';
COMMENT ON COLUMN agent_sessions.external_session_id IS
    'Provider/CLI resume identifier; for Claude this is the --resume session id.';
COMMENT ON TABLE agent_session_events IS
    'Append-only event ledger for interactive CLI session transcripts and lifecycle receipts.';

COMMIT;
