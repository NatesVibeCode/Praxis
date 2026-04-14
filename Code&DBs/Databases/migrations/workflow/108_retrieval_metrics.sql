-- Migration 108: retrieval metrics telemetry table
--
-- Moves retrieval_metrics DDL out of memory/retrieval_telemetry.py into the
-- canonical workflow migration tree.

CREATE TABLE IF NOT EXISTS retrieval_metrics (
    id SERIAL PRIMARY KEY,
    query_fingerprint TEXT NOT NULL,
    pattern_name TEXT NOT NULL,
    result_count INTEGER NOT NULL,
    score_min DOUBLE PRECISION NOT NULL,
    score_max DOUBLE PRECISION NOT NULL,
    score_mean DOUBLE PRECISION NOT NULL,
    score_stddev DOUBLE PRECISION NOT NULL,
    tie_break_count INTEGER NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retrieval_metrics_pattern_name
    ON retrieval_metrics (pattern_name);

CREATE INDEX IF NOT EXISTS idx_retrieval_metrics_timestamp
    ON retrieval_metrics (timestamp);
