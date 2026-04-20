BEGIN;

-- -----------------------------------------------------------------------------
-- Daily heartbeat: one run ties together many probe snapshots.
-- Scopes: providers (usage/limits), connectors (liveness), credentials
-- (expiry/presence), mcp (server liveness). One table with a discriminator
-- keeps the schema small; per-kind views give each scope its own surface.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS heartbeat_runs (
    heartbeat_run_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ('providers', 'connectors', 'credentials', 'mcp', 'all')),
    triggered_by TEXT NOT NULL CHECK (triggered_by IN ('launchd', 'cli', 'mcp', 'http', 'test')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'succeeded', 'partial', 'failed')),
    probes_total INTEGER NOT NULL DEFAULT 0,
    probes_ok INTEGER NOT NULL DEFAULT 0,
    probes_failed INTEGER NOT NULL DEFAULT 0,
    summary TEXT NOT NULL DEFAULT '',
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT heartbeat_runs_details_object_check
        CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS heartbeat_runs_scope_started_idx
    ON heartbeat_runs (scope, started_at DESC);

CREATE INDEX IF NOT EXISTS heartbeat_runs_triggered_started_idx
    ON heartbeat_runs (triggered_by, started_at DESC);

CREATE TABLE IF NOT EXISTS heartbeat_probe_snapshots (
    heartbeat_probe_snapshot_id TEXT PRIMARY KEY,
    heartbeat_run_id TEXT NOT NULL REFERENCES heartbeat_runs(heartbeat_run_id) ON DELETE CASCADE,
    probe_kind TEXT NOT NULL
        CHECK (probe_kind IN ('provider_usage', 'connector_liveness', 'credential_expiry', 'mcp_liveness')),
    subject_id TEXT NOT NULL,
    subject_sub TEXT,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL
        CHECK (status IN ('ok', 'degraded', 'failed', 'warning', 'skipped')),
    summary TEXT NOT NULL DEFAULT '',
    latency_ms INTEGER,
    input_tokens BIGINT,
    output_tokens BIGINT,
    estimated_cost_usd NUMERIC(12, 6),
    days_until_expiry INTEGER,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT heartbeat_probe_snapshots_details_object_check
        CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS heartbeat_probe_snapshots_kind_subject_idx
    ON heartbeat_probe_snapshots (probe_kind, subject_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS heartbeat_probe_snapshots_run_idx
    ON heartbeat_probe_snapshots (heartbeat_run_id);

CREATE INDEX IF NOT EXISTS heartbeat_probe_snapshots_kind_status_captured_idx
    ON heartbeat_probe_snapshots (probe_kind, status, captured_at DESC);

-- -----------------------------------------------------------------------------
-- Per-kind views — one query surface per scope without separate tables.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_provider_usage_snapshots AS
SELECT
    s.heartbeat_probe_snapshot_id,
    s.heartbeat_run_id,
    s.subject_id AS provider_slug,
    s.subject_sub AS adapter_type,
    s.captured_at,
    s.status,
    s.summary,
    s.latency_ms,
    s.input_tokens,
    s.output_tokens,
    s.estimated_cost_usd,
    s.details ->> 'model_slug' AS model_slug,
    (s.details ->> 'rate_limit_requests_remaining')::BIGINT AS rate_limit_requests_remaining,
    (s.details ->> 'rate_limit_tokens_remaining')::BIGINT AS rate_limit_tokens_remaining,
    s.details ->> 'rate_limit_reset_at' AS rate_limit_reset_at,
    s.details ->> 'billing_mode' AS billing_mode,
    s.details ->> 'budget_bucket' AS budget_bucket,
    s.details AS details
FROM heartbeat_probe_snapshots s
WHERE s.probe_kind = 'provider_usage';

CREATE OR REPLACE VIEW v_connector_liveness_snapshots AS
SELECT
    s.heartbeat_probe_snapshot_id,
    s.heartbeat_run_id,
    s.subject_id AS connector_slug,
    s.captured_at,
    s.status,
    s.summary,
    s.latency_ms,
    s.details ->> 'health_status' AS health_status,
    s.details ->> 'verification_status' AS verification_status,
    (s.details ->> 'error_rate')::NUMERIC AS error_rate,
    (s.details ->> 'total_calls')::BIGINT AS total_calls,
    (s.details ->> 'total_errors')::BIGINT AS total_errors,
    s.details ->> 'last_success_at' AS last_success_at,
    s.details ->> 'last_error_at' AS last_error_at,
    s.details AS details
FROM heartbeat_probe_snapshots s
WHERE s.probe_kind = 'connector_liveness';

CREATE OR REPLACE VIEW v_credential_expiry_snapshots AS
SELECT
    s.heartbeat_probe_snapshot_id,
    s.heartbeat_run_id,
    s.subject_id AS credential_ref,
    s.subject_sub AS credential_kind,
    s.captured_at,
    s.status,
    s.summary,
    s.days_until_expiry,
    s.details ->> 'source_kind' AS source_kind,
    (s.details ->> 'present')::BOOLEAN AS present,
    s.details ->> 'expires_at' AS expires_at,
    s.details ->> 'provider_slug' AS provider_slug,
    s.details ->> 'integration_id' AS integration_id,
    s.details AS details
FROM heartbeat_probe_snapshots s
WHERE s.probe_kind = 'credential_expiry';

CREATE OR REPLACE VIEW v_mcp_liveness_snapshots AS
SELECT
    s.heartbeat_probe_snapshot_id,
    s.heartbeat_run_id,
    s.subject_id AS server_name,
    s.captured_at,
    s.status,
    s.summary,
    s.latency_ms,
    s.details ->> 'transport' AS transport,
    (s.details ->> 'reachable')::BOOLEAN AS reachable,
    (s.details ->> 'handshake_succeeded')::BOOLEAN AS handshake_succeeded,
    (s.details ->> 'tools_count')::INTEGER AS tools_count,
    s.details ->> 'server_version' AS server_version,
    s.details ->> 'error_message' AS error_message,
    s.details AS details
FROM heartbeat_probe_snapshots s
WHERE s.probe_kind = 'mcp_liveness';

COMMIT;
