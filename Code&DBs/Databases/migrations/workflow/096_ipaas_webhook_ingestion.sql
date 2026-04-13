-- Migration 096: Webhook ingestion tables

CREATE TABLE IF NOT EXISTS webhook_endpoints (
    endpoint_id TEXT PRIMARY KEY DEFAULT 'whep_' || substr(md5(random()::text), 1, 12),
    slug TEXT UNIQUE NOT NULL,
    provider TEXT NOT NULL,
    secret_env_var TEXT,
    signature_header TEXT,
    signature_algorithm TEXT DEFAULT 'hmac-sha256',
    target_workflow_id TEXT,
    target_trigger_id TEXT,
    filter_expression JSONB,
    transform_spec JSONB,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_events (
    event_id TEXT PRIMARY KEY DEFAULT 'whev_' || substr(md5(random()::text), 1, 12),
    endpoint_id TEXT NOT NULL REFERENCES webhook_endpoints(endpoint_id),
    payload JSONB NOT NULL,
    headers JSONB NOT NULL,
    signature_valid BOOLEAN,
    processing_status TEXT DEFAULT 'received',
    error_message TEXT,
    attempt_count INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    next_retry_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    received_at TIMESTAMPTZ DEFAULT now(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_endpoint
    ON webhook_events(endpoint_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_webhook_events_status
    ON webhook_events(processing_status)
    WHERE processing_status != 'processed';

CREATE INDEX IF NOT EXISTS idx_webhook_events_retry
    ON webhook_events(next_retry_at)
    WHERE processing_status IN ('received', 'failed') AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_webhook_events_dead_letter
    ON webhook_events(dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
