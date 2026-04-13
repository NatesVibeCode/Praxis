-- Workspace chat conversations
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    title TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conversation_messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role TEXT NOT NULL,                  -- 'user', 'assistant', 'tool_call', 'tool_result'
    content TEXT,                        -- text content
    tool_calls JSONB,                    -- tool calls the assistant wants to make
    tool_results JSONB,                  -- structured results from tool execution
    model_used TEXT,                     -- e.g. 'anthropic/claude-sonnet-4-5'
    latency_ms INTEGER,
    cost_usd NUMERIC(10, 6),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conv_messages_lookup
    ON conversation_messages(conversation_id, created_at);
