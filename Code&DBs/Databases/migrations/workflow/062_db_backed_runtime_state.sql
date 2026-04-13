BEGIN;

CREATE TABLE IF NOT EXISTS workflow_result_cache (
    cache_key text PRIMARY KEY,
    cached_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    status text NOT NULL,
    reason_code text NOT NULL,
    completion text,
    outputs jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence_count integer NOT NULL DEFAULT 0,
    latency_ms integer NOT NULL DEFAULT 0,
    provider_slug text NOT NULL,
    model_slug text,
    adapter_type text NOT NULL,
    failure_code text,
    payload_bytes bigint NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_workflow_result_cache_expires_at
    ON workflow_result_cache (expires_at);

CREATE INDEX IF NOT EXISTS idx_workflow_result_cache_cached_at
    ON workflow_result_cache (cached_at DESC);

CREATE TABLE IF NOT EXISTS workflow_cost_ledger (
    run_id text PRIMARY KEY,
    provider_slug text NOT NULL,
    model_slug text,
    cost_usd double precision NOT NULL DEFAULT 0,
    input_tokens integer NOT NULL DEFAULT 0,
    output_tokens integer NOT NULL DEFAULT 0,
    recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_cost_ledger_recorded_at
    ON workflow_cost_ledger (recorded_at DESC);

CREATE TABLE IF NOT EXISTS multimodal_ingest_staging (
    staging_id text PRIMARY KEY,
    source_type text NOT NULL,
    posture text NOT NULL,
    entity_type text NOT NULL,
    entity_data jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_multimodal_ingest_staging_recorded_at
    ON multimodal_ingest_staging (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_multimodal_ingest_staging_source_type
    ON multimodal_ingest_staging (source_type, recorded_at DESC);

COMMIT;
