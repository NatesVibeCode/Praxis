-- 033: Add failure_category columns and zone lookup table for categorized metrics.
--
-- Problem: pass_rate conflates config errors, external provider failures, and
-- real system failures. The failure_classifier.py already categorizes every
-- failure, but there's no queryable column — just raw failure_code strings.
--
-- This migration adds failure_category to receipt/job tables, creates a zone
-- lookup table, and backfills existing rows.

BEGIN;

-- 1. Zone lookup table: maps failure categories to responsibility zones
CREATE TABLE IF NOT EXISTS failure_category_zones (
    category     TEXT PRIMARY KEY,
    zone         TEXT NOT NULL,        -- 'external', 'config', 'internal', 'unknown'
    is_transient BOOLEAN NOT NULL DEFAULT false
);

INSERT INTO failure_category_zones (category, zone, is_transient) VALUES
    ('timeout',              'external', true),
    ('rate_limit',           'external', true),
    ('provider_error',       'external', true),
    ('network_error',        'external', true),
    ('infrastructure',       'external', true),
    ('credential_error',     'config',   false),
    ('model_error',          'config',   false),
    ('input_error',          'config',   false),
    ('context_overflow',     'internal', false),
    ('parse_error',          'internal', true),
    ('sandbox_error',        'internal', false),
    ('scope_violation',      'internal', false),
    ('verification_failed',  'internal', false),
    ('unknown',              'unknown',  false)
ON CONFLICT (category) DO NOTHING;

-- 2. Add failure_category columns
ALTER TABLE receipt_search
    ADD COLUMN IF NOT EXISTS failure_category TEXT NOT NULL DEFAULT '';

ALTER TABLE receipt_meta
    ADD COLUMN IF NOT EXISTS failure_category TEXT NOT NULL DEFAULT '';

ALTER TABLE workflow_jobs
    ADD COLUMN IF NOT EXISTS failure_category TEXT NOT NULL DEFAULT '';

-- 3. Indexes for efficient aggregation queries
CREATE INDEX IF NOT EXISTS idx_receipt_search_failure_category
    ON receipt_search (failure_category) WHERE failure_category != '';

CREATE INDEX IF NOT EXISTS idx_receipt_meta_failure_category
    ON receipt_meta (failure_category) WHERE failure_category != '';

CREATE INDEX IF NOT EXISTS idx_workflow_jobs_failure_category
    ON workflow_jobs (failure_category) WHERE failure_category != '';

-- 4. Backfill receipt_search from raw_json (orchestrator path stores classification)
UPDATE receipt_search
SET failure_category = raw_json->'failure_classification'->>'category'
WHERE failure_code != ''
  AND failure_category = ''
  AND raw_json->'failure_classification'->>'category' IS NOT NULL;

-- 5. Backfill receipt_search where failure_code IS already a category value
--    (unified _execute_cli path stores classification.category.value as error_code)
UPDATE receipt_search
SET failure_category = failure_code
WHERE failure_code != ''
  AND failure_category = ''
  AND failure_code IN (SELECT category FROM failure_category_zones);

-- 6. Backfill workflow_jobs from last_error_code where it matches a known category
UPDATE workflow_jobs
SET failure_category = last_error_code
WHERE status IN ('failed', 'dead_letter')
  AND failure_category = ''
  AND last_error_code IN (SELECT category FROM failure_category_zones);

-- 7. Backfill receipt_meta by joining against receipt_search
UPDATE receipt_meta rm
SET failure_category = rs.failure_category
FROM receipt_search rs
WHERE rm.label = rs.label
  AND rm.agent = rs.agent
  AND rm.status IN ('failed', 'error')
  AND rs.failure_category != ''
  AND rm.failure_category = '';

COMMIT;
