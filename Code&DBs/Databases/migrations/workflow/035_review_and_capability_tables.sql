-- 035: Migrate review_records and capability_outcomes from JSON files to Postgres.
--
-- review_tracker.py was persisting to artifacts/review_records.json
-- capability_feedback.py was persisting to artifacts/capability_outcomes.json
-- Both now use Postgres as the single source of truth.

BEGIN;

-- 1. Review records table
CREATE TABLE IF NOT EXISTS review_records (
    id              SERIAL PRIMARY KEY,
    review_run_id   TEXT NOT NULL,
    reviewed_dispatch_id TEXT,
    reviewer_model  TEXT NOT NULL,
    author_model    TEXT NOT NULL,
    task_type       TEXT NOT NULL DEFAULT 'general',
    modules_reviewed TEXT[] NOT NULL DEFAULT '{}',
    findings        JSONB NOT NULL DEFAULT '[]',
    bug_count       INTEGER NOT NULL DEFAULT 0,
    severity_counts JSONB NOT NULL DEFAULT '{}',
    dimension_scores JSONB,
    avg_dimension_scores JSONB,
    reviewed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_review_records_author
    ON review_records (author_model);
CREATE INDEX IF NOT EXISTS idx_review_records_reviewed_at
    ON review_records (reviewed_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_records_review_run
    ON review_records (review_run_id);

-- 2. Capability outcomes table
CREATE TABLE IF NOT EXISTS capability_outcomes (
    id                      SERIAL PRIMARY KEY,
    run_id                  TEXT NOT NULL,
    provider_slug           TEXT NOT NULL,
    model_slug              TEXT NOT NULL,
    inferred_capabilities   TEXT[] NOT NULL DEFAULT '{}',
    succeeded               BOOLEAN NOT NULL DEFAULT false,
    output_quality_signals  JSONB NOT NULL DEFAULT '{}',
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_capability_outcomes_model
    ON capability_outcomes (provider_slug, model_slug);
CREATE INDEX IF NOT EXISTS idx_capability_outcomes_run
    ON capability_outcomes (run_id);
CREATE INDEX IF NOT EXISTS idx_capability_outcomes_recorded
    ON capability_outcomes (recorded_at DESC);

COMMIT;
