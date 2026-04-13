-- Migration 098: Connector registry with versioning and health tracking

CREATE TABLE IF NOT EXISTS connector_registry (
    connector_id TEXT PRIMARY KEY DEFAULT 'conn_' || substr(md5(random()::text), 1, 12),
    slug TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '0.1.0',
    status TEXT NOT NULL DEFAULT 'active',
    schema_id TEXT REFERENCES api_schemas(schema_id),
    auth_type TEXT,
    base_url TEXT,
    module_path TEXT,
    health_status TEXT DEFAULT 'unknown',
    last_health_check TIMESTAMPTZ,
    total_calls INT DEFAULT 0,
    total_errors INT DEFAULT 0,
    error_rate REAL DEFAULT 0.0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_connector_registry_status
    ON connector_registry(status) WHERE status = 'active';
