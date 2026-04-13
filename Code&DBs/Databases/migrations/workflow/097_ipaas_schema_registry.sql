-- Migration 097: Schema registry tables for OpenAPI spec storage

CREATE TABLE IF NOT EXISTS api_schemas (
    schema_id TEXT PRIMARY KEY DEFAULT 'asch_' || substr(md5(random()::text), 1, 12),
    provider_slug TEXT NOT NULL,
    version TEXT NOT NULL,
    title TEXT,
    description TEXT,
    base_url TEXT,
    auth_type TEXT,
    raw_spec JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(provider_slug, version)
);

CREATE TABLE IF NOT EXISTS api_endpoints (
    endpoint_id TEXT PRIMARY KEY DEFAULT 'aend_' || substr(md5(random()::text), 1, 12),
    schema_id TEXT NOT NULL REFERENCES api_schemas(schema_id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    operation_id TEXT,
    summary TEXT,
    request_schema JSONB,
    response_schema JSONB,
    UNIQUE(schema_id, path, method)
);

CREATE TABLE IF NOT EXISTS api_models (
    model_id TEXT PRIMARY KEY DEFAULT 'amod_' || substr(md5(random()::text), 1, 12),
    schema_id TEXT NOT NULL REFERENCES api_schemas(schema_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    fields JSONB NOT NULL,
    UNIQUE(schema_id, name)
);

CREATE INDEX IF NOT EXISTS idx_api_schemas_provider ON api_schemas(provider_slug);
CREATE INDEX IF NOT EXISTS idx_api_endpoints_schema ON api_endpoints(schema_id);
CREATE INDEX IF NOT EXISTS idx_api_models_schema ON api_models(schema_id);
