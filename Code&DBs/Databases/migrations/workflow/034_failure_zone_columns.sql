-- 034: Add failure_zone and is_transient columns derived from failure_category.
--
-- These columns let downstream code (routing demotion, circuit breakers)
-- make decisions without re-deriving zone from category every time.

BEGIN;

-- 1. Add columns
ALTER TABLE receipt_search
    ADD COLUMN IF NOT EXISTS failure_zone TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS is_transient BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE receipt_meta
    ADD COLUMN IF NOT EXISTS failure_zone TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS is_transient BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE workflow_jobs
    ADD COLUMN IF NOT EXISTS failure_zone TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS is_transient BOOLEAN NOT NULL DEFAULT false;

-- 2. Backfill from failure_category_zones lookup
UPDATE receipt_search rs
SET failure_zone = fcz.zone,
    is_transient = fcz.is_transient
FROM failure_category_zones fcz
WHERE rs.failure_category = fcz.category
  AND rs.failure_category != ''
  AND rs.failure_zone = '';

UPDATE receipt_meta rm
SET failure_zone = fcz.zone,
    is_transient = fcz.is_transient
FROM failure_category_zones fcz
WHERE rm.failure_category = fcz.category
  AND rm.failure_category != ''
  AND rm.failure_zone = '';

UPDATE workflow_jobs wj
SET failure_zone = fcz.zone,
    is_transient = fcz.is_transient
FROM failure_category_zones fcz
WHERE wj.failure_category = fcz.category
  AND wj.failure_category != ''
  AND wj.failure_zone = '';

-- 3. Index for routing queries that filter by zone
CREATE INDEX IF NOT EXISTS idx_receipt_search_failure_zone
    ON receipt_search (failure_zone) WHERE failure_zone != '';

CREATE INDEX IF NOT EXISTS idx_workflow_jobs_failure_zone
    ON workflow_jobs (failure_zone) WHERE failure_zone != '';

COMMIT;
