-- Migration 048: durable task-type route health and workflow route metadata
--
-- The router needs durable, task-type-specific outcome authority that reflects:
--   * whether a route actually completed jobs
--   * whether failures were internal vs external/config noise
--   * whether downstream review/verification later exposed code quality issues
--
-- This keeps routing grounded in real outcomes rather than over-trusting
-- raw benchmark conventions that may not be directly comparable.

BEGIN;

ALTER TABLE task_type_routing
  ADD COLUMN IF NOT EXISTS route_health_score DOUBLE PRECISION NOT NULL DEFAULT 0.65
    CHECK (route_health_score >= 0.0 AND route_health_score <= 1.0),
  ADD COLUMN IF NOT EXISTS observed_completed_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS observed_execution_failure_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS observed_external_failure_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS observed_config_failure_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS observed_downstream_failure_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS observed_downstream_bug_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS consecutive_internal_failures INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_failure_category TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_failure_zone TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_outcome_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_reviewed_at TIMESTAMPTZ;

UPDATE task_type_routing
SET route_health_score = LEAST(
        1.0,
        GREATEST(
            0.05,
            CASE
                WHEN COALESCE(recent_successes, 0) + COALESCE(recent_failures, 0) = 0 THEN 0.65
                ELSE 0.65
                     + ((COALESCE(recent_successes, 0) - COALESCE(recent_failures, 0))::DOUBLE PRECISION
                        / GREATEST(COALESCE(recent_successes, 0) + COALESCE(recent_failures, 0), 1))
                     * 0.20
            END
        )
    ),
    observed_completed_count = GREATEST(observed_completed_count, COALESCE(recent_successes, 0)),
    observed_execution_failure_count = GREATEST(observed_execution_failure_count, COALESCE(recent_failures, 0)),
    consecutive_internal_failures = GREATEST(consecutive_internal_failures, COALESCE(recent_failures, 0)),
    last_outcome_at = GREATEST(COALESCE(last_success_at, '-infinity'::timestamptz), COALESCE(last_failure_at, '-infinity'::timestamptz));

ALTER TABLE workflow_jobs
  ADD COLUMN IF NOT EXISTS route_task_type TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS route_origin_slug TEXT NOT NULL DEFAULT '';

UPDATE workflow_jobs
SET route_task_type = CASE
        WHEN route_task_type != '' THEN route_task_type
        WHEN agent_slug LIKE 'auto/%' THEN split_part(agent_slug, '/', 2)
        ELSE ''
    END,
    route_origin_slug = CASE
        WHEN route_origin_slug != '' THEN route_origin_slug
        WHEN agent_slug LIKE 'auto/%' THEN agent_slug
        ELSE ''
    END;

CREATE INDEX IF NOT EXISTS task_type_routing_health_idx
    ON task_type_routing (task_type, route_health_score DESC, consecutive_internal_failures ASC)
    WHERE permitted = TRUE;

CREATE INDEX IF NOT EXISTS workflow_jobs_route_task_type_idx
    ON workflow_jobs (route_task_type)
    WHERE route_task_type != '';

COMMIT;
