-- Migration 338: wire the zero-token-silent-failure detector end-to-end.
--
-- Operator direction (2026-04-29, P0 reconciliation): the build_antipattern
-- substrate exists (registry rule, view) but has 2 broken boundaries:
--   (1) view filter requires zero_token_succeeded_count == recent_count, so
--       any mixed streak (zero-token interleaved with failed/blocked/cancelled)
--       is silently dropped — yet that's exactly the operational shape we hit.
--   (2) no projector materializes view rows into build_antipattern_hits, so
--       even if the view fired, the hits table stays empty.
--
-- This migration:
--   * Loosens the view to "majority of agent's recent activity is zero-token-
--     succeeded" — streak_count >= 3 AND streak_count >= 60% of recent_count.
--     Catches the real pathology without requiring a clean unbroken streak.
--   * Adds a partial unique index on (rule_slug, resolved_agent) WHERE
--     cleared_at IS NULL so ON CONFLICT can upsert one open hit per agent.
--   * Adds refresh_build_antipattern_hits() — idempotent UPSERT from view to
--     hits, scoped to the zero_token_silent_failure rule.
--   * Wires an AFTER STATEMENT trigger on workflow_jobs INSERT/UPDATE so hits
--     refresh as jobs land. FOR EACH STATEMENT (not row), so a multi-row UPDATE
--     fires the projector once.
--
-- Rationale: the agent's first attempted run on a freshly-broken provider
-- should self-quarantine via this detector instead of running 442+ jobs
-- before someone notices. P0 reconciliation found 38 zero-token gpt-5.4
-- successes today that the substrate should have flagged on the first wave.

BEGIN;

-- 1. Looser view filter
DROP VIEW IF EXISTS v_zero_token_silent_failures;

CREATE VIEW v_zero_token_silent_failures AS
-- Detect on agent's last 5 SUCCEEDED jobs only — cascade-cancelled / blocked /
-- failed jobs are noise, not signal. The pathology is "this agent's recent
-- SUCCESSES are empty completions," not "the agent's recent activity overall."
-- Including non-succeeded statuses in the window dilutes the detection when
-- a wave hits a gate-failure cascade (which today produces 200+ cancelled
-- jobs that drown the 5-7 zero-token-succeeded ones we actually care about).
WITH succeeded_jobs AS (
    SELECT
        j.id,
        j.run_id,
        j.resolved_agent,
        j.token_input,
        j.token_output,
        j.cost_usd,
        j.finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY j.resolved_agent
            ORDER BY j.finished_at DESC NULLS LAST
        ) AS recency_rank
    FROM workflow_jobs j
    WHERE j.resolved_agent IS NOT NULL
      AND j.resolved_agent <> ''
      AND j.status = 'succeeded'
      AND j.finished_at IS NOT NULL
      AND j.finished_at > NOW() - INTERVAL '6 hours'
),
agent_window AS (
    SELECT
        resolved_agent,
        COUNT(*) FILTER (WHERE recency_rank <= 5) AS recent_count,
        COUNT(*) FILTER (
            WHERE recency_rank <= 5
              AND COALESCE(token_input, 0) = 0
              AND COALESCE(token_output, 0) = 0
              AND COALESCE(cost_usd, 0::numeric) = 0::numeric
        ) AS zero_token_succeeded_count,
        MAX(finished_at) FILTER (WHERE recency_rank <= 5) AS latest_finished_at,
        ARRAY_AGG(run_id) FILTER (WHERE recency_rank <= 5) AS recent_run_ids,
        ARRAY_AGG(id::text) FILTER (WHERE recency_rank <= 5) AS recent_job_ids
    FROM succeeded_jobs
    GROUP BY resolved_agent
)
SELECT
    resolved_agent,
    SPLIT_PART(resolved_agent, '/', 1) AS provider_slug,
    NULLIF(SUBSTRING(resolved_agent FROM (POSITION('/' IN resolved_agent) + 1)), '') AS model_slug,
    recent_count,
    zero_token_succeeded_count AS streak_count,
    latest_finished_at,
    recent_run_ids,
    recent_job_ids
FROM agent_window
-- Loosened (operator decision 2026-04-29):
--   * at least 3 zero-token successes in the recent succeeded window (signal threshold)
--   * AND at least 60% of agent's recent successes are zero-token
--     (separates "this provider is dead" from "agent had one bad run").
WHERE recent_count >= 3
  AND zero_token_succeeded_count >= 3
  AND zero_token_succeeded_count >= (recent_count * 0.6);

COMMENT ON VIEW v_zero_token_silent_failures IS
'Detects agents whose recent activity is dominated by zero-token succeeded jobs — i.e. provider auth/routing is broken but local exit code is 0. Loosened in migration 338 from "all 5 recent must be zero-token-succeeded" to "majority of recent are zero-token-succeeded" so mixed streaks (zero-token interleaved with other failure modes) still trigger.';

-- 2. Partial unique index for upsert target
CREATE UNIQUE INDEX IF NOT EXISTS idx_build_antipattern_hits_open_unique
    ON build_antipattern_hits (rule_slug, resolved_agent)
    WHERE cleared_at IS NULL;

-- 3. Projector function — idempotent UPSERT from view → hits
CREATE OR REPLACE FUNCTION refresh_build_antipattern_hits()
RETURNS void AS $$
BEGIN
    INSERT INTO build_antipattern_hits (
        rule_slug,
        resolved_agent,
        provider_slug,
        model_slug,
        streak_count,
        latest_finished_at,
        sample_run_ids,
        sample_job_ids,
        detected_at
    )
    SELECT
        'zero_token_silent_failure',
        v.resolved_agent,
        v.provider_slug,
        v.model_slug,
        v.streak_count,
        v.latest_finished_at,
        v.recent_run_ids,
        v.recent_job_ids,
        NOW()
    FROM v_zero_token_silent_failures v
    ON CONFLICT (rule_slug, resolved_agent) WHERE cleared_at IS NULL
    DO UPDATE SET
        streak_count = EXCLUDED.streak_count,
        latest_finished_at = EXCLUDED.latest_finished_at,
        sample_run_ids = EXCLUDED.sample_run_ids,
        sample_job_ids = EXCLUDED.sample_job_ids,
        provider_slug = EXCLUDED.provider_slug,
        model_slug = EXCLUDED.model_slug,
        detected_at = EXCLUDED.detected_at;
END;
$$ LANGUAGE plpgsql;

-- 4. Trigger function (wraps the projector for AFTER triggers)
CREATE OR REPLACE FUNCTION refresh_build_antipattern_hits_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM refresh_build_antipattern_hits();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 5. Wire trigger to fire on job state changes. FOR EACH STATEMENT keeps
--    the cost bounded — a single batch UPDATE fires the projector once,
--    not once per row. View evaluation is cheap (6h window, group-by agent).
DROP TRIGGER IF EXISTS trg_refresh_build_antipattern_hits ON workflow_jobs;
CREATE TRIGGER trg_refresh_build_antipattern_hits
    AFTER INSERT OR UPDATE ON workflow_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_build_antipattern_hits_trigger();

-- 6. Backfill: run once now to materialize hits for the existing 24h window.
SELECT refresh_build_antipattern_hits();

COMMIT;
