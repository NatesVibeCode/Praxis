-- Migration 013: Dispatch control tables (formerly SQLite)
-- Consolidates constraint_ledger, friction_ledger, sandbox_artifacts,
-- heartbeat_inbox, and quality_views into the workflow Postgres database.

-- Constraint ledger: mined failure constraints
CREATE TABLE IF NOT EXISTS dispatch_constraints (
    constraint_id   text PRIMARY KEY,
    pattern         text NOT NULL,
    constraint_text text NOT NULL,
    confidence      real NOT NULL,
    mined_from_jobs text NOT NULL DEFAULT '',
    scope_prefix    text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dispatch_constraints_scope
    ON dispatch_constraints (scope_prefix);

-- Friction ledger: guardrail bounces, warnings, hard failures
CREATE TABLE IF NOT EXISTS friction_events (
    event_id      text PRIMARY KEY,
    friction_type text NOT NULL,
    source        text NOT NULL,
    job_label     text NOT NULL,
    message       text NOT NULL,
    timestamp     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_friction_events_type
    ON friction_events (friction_type);
CREATE INDEX IF NOT EXISTS idx_friction_events_timestamp
    ON friction_events (timestamp);

-- Sandbox artifacts: captured file contents from sandboxed dispatch runs
CREATE TABLE IF NOT EXISTS sandbox_artifacts (
    artifact_id  text PRIMARY KEY,
    file_path    text NOT NULL,
    sha256       text NOT NULL,
    byte_count   integer NOT NULL,
    line_count   integer NOT NULL,
    captured_at  timestamptz NOT NULL DEFAULT now(),
    sandbox_id   text NOT NULL,
    content      text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sandbox_artifacts_sandbox
    ON sandbox_artifacts (sandbox_id);

-- Heartbeat inbox: items submitted for processing
CREATE TABLE IF NOT EXISTS heartbeat_inbox (
    item_id      text PRIMARY KEY,
    kind         text NOT NULL,
    content      text NOT NULL,
    source       text NOT NULL,
    submitted_at timestamptz NOT NULL DEFAULT now(),
    processed    boolean DEFAULT false,
    resolution   text
);

-- Heartbeat review queue: entities flagged for review
CREATE TABLE IF NOT EXISTS heartbeat_review_queue (
    entry_id    text PRIMARY KEY,
    entity_id   text NOT NULL,
    reason      text NOT NULL,
    priority    real NOT NULL,
    action      text,
    resolved    boolean DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Quality rollups: aggregated quality metrics by time window
CREATE TABLE IF NOT EXISTS quality_rollups (
    "window"       text NOT NULL,
    window_start   text NOT NULL,
    data           jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY ("window", window_start)
);

-- Agent profiles: per-agent quality metrics by time window
CREATE TABLE IF NOT EXISTS agent_profiles (
    agent_slug     text NOT NULL,
    "window"       text NOT NULL,
    window_start   text NOT NULL,
    data           jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (agent_slug, "window", window_start)
);

-- Failure catalog: known failure codes with examples
CREATE TABLE IF NOT EXISTS failure_catalog (
    failure_code text PRIMARY KEY,
    count        integer NOT NULL DEFAULT 0,
    last_seen    timestamptz NOT NULL DEFAULT now(),
    examples     jsonb NOT NULL DEFAULT '[]',
    agents       jsonb NOT NULL DEFAULT '[]'
);

-- Receipt search: FTS over dispatch receipts
CREATE TABLE IF NOT EXISTS receipt_search (
    id            serial PRIMARY KEY,
    label         text NOT NULL DEFAULT '',
    agent         text NOT NULL DEFAULT '',
    status        text NOT NULL DEFAULT '',
    failure_code  text NOT NULL DEFAULT '',
    timestamp     timestamptz,
    raw_json      jsonb NOT NULL DEFAULT '{}',
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english',
            coalesce(label, '') || ' ' ||
            coalesce(agent, '') || ' ' ||
            coalesce(status, '') || ' ' ||
            coalesce(failure_code, '')
        )
    ) STORED
);
CREATE INDEX IF NOT EXISTS idx_receipt_search_fts
    ON receipt_search USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS idx_receipt_search_status
    ON receipt_search (status);
CREATE INDEX IF NOT EXISTS idx_receipt_search_agent
    ON receipt_search (agent);

-- Receipt meta: token/cost analytics
CREATE TABLE IF NOT EXISTS receipt_meta (
    id            serial PRIMARY KEY,
    label         text,
    agent         text,
    status        text,
    input_tokens  integer DEFAULT 0,
    output_tokens integer DEFAULT 0,
    cost          numeric DEFAULT 0,
    timestamp     timestamptz
);
CREATE INDEX IF NOT EXISTS idx_receipt_meta_agent
    ON receipt_meta (agent);
CREATE INDEX IF NOT EXISTS idx_receipt_meta_timestamp
    ON receipt_meta (timestamp);
