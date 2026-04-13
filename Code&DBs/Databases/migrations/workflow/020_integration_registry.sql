-- Migration 020: Integration Registry
-- Tracks available external service integrations and their capabilities

CREATE TABLE IF NOT EXISTS integration_registry (
    id text PRIMARY KEY,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    provider text NOT NULL,
    capabilities jsonb NOT NULL DEFAULT '[]',
    auth_status text NOT NULL DEFAULT 'connected',
    mcp_server_id text,
    icon text,
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_integration_registry_fts
    ON integration_registry USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS idx_integration_registry_provider
    ON integration_registry (provider);
