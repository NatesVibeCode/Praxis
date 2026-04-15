-- Migration 110: durable frontdoor surface-usage telemetry counters and events

CREATE TABLE IF NOT EXISTS workflow_surface_usage_daily (
    usage_date DATE NOT NULL,
    surface_kind TEXT NOT NULL,
    transport_kind TEXT NOT NULL,
    entrypoint_kind TEXT NOT NULL,
    entrypoint_name TEXT NOT NULL,
    caller_kind TEXT NOT NULL DEFAULT 'direct',
    http_method TEXT NOT NULL DEFAULT '',
    invocation_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    client_error_count BIGINT NOT NULL DEFAULT 0,
    server_error_count BIGINT NOT NULL DEFAULT 0,
    first_invoked_at TIMESTAMPTZ NOT NULL,
    last_invoked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        usage_date,
        surface_kind,
        transport_kind,
        entrypoint_kind,
        entrypoint_name,
        caller_kind,
        http_method
    )
);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_daily_entrypoint_idx
    ON workflow_surface_usage_daily (entrypoint_name, usage_date DESC);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_daily_surface_idx
    ON workflow_surface_usage_daily (surface_kind, entrypoint_kind, usage_date DESC);

CREATE TABLE IF NOT EXISTS workflow_surface_usage_events (
    event_id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    surface_kind TEXT NOT NULL,
    transport_kind TEXT NOT NULL,
    entrypoint_kind TEXT NOT NULL,
    entrypoint_name TEXT NOT NULL,
    caller_kind TEXT NOT NULL DEFAULT 'direct',
    http_method TEXT NOT NULL DEFAULT '',
    status_code INTEGER NOT NULL,
    result_state TEXT NOT NULL DEFAULT 'ok',
    reason_code TEXT NOT NULL DEFAULT '',
    routed_to TEXT NOT NULL DEFAULT '',
    workflow_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    job_label TEXT NOT NULL DEFAULT '',
    request_id TEXT NOT NULL DEFAULT '',
    client_version TEXT NOT NULL DEFAULT '',
    payload_size_bytes BIGINT NOT NULL DEFAULT 0,
    response_size_bytes BIGINT NOT NULL DEFAULT 0,
    prose_chars BIGINT NOT NULL DEFAULT 0,
    query_chars BIGINT NOT NULL DEFAULT 0,
    result_count BIGINT NOT NULL DEFAULT 0,
    unresolved_count BIGINT NOT NULL DEFAULT 0,
    capability_count BIGINT NOT NULL DEFAULT 0,
    reference_count BIGINT NOT NULL DEFAULT 0,
    compiled_job_count BIGINT NOT NULL DEFAULT 0,
    trigger_count BIGINT NOT NULL DEFAULT 0,
    definition_hash TEXT NOT NULL DEFAULT '',
    definition_revision TEXT NOT NULL DEFAULT '',
    task_class TEXT NOT NULL DEFAULT '',
    planner_required BOOLEAN NOT NULL DEFAULT FALSE,
    llm_used BOOLEAN NOT NULL DEFAULT FALSE,
    has_current_plan BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_events_entrypoint_idx
    ON workflow_surface_usage_events (entrypoint_name, occurred_at DESC);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_events_definition_idx
    ON workflow_surface_usage_events (definition_hash, occurred_at DESC);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_events_workflow_idx
    ON workflow_surface_usage_events (workflow_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS workflow_surface_usage_events_run_idx
    ON workflow_surface_usage_events (run_id, occurred_at DESC);
