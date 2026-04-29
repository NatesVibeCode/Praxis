-- Migration 334: zero-token silent-failure detector projection.
--
-- Operator direction (2026-04-29, nate): "I never want this issue to happen
-- again." Today's session ran 442+ jobs that all came back with
-- token_input=0, token_output=0, cost_usd=0, status='succeeded', and zero
-- in-scope file changes. The worker had been recreated without the
-- Keychain-hydrated CLAUDE_CODE_OAUTH_TOKEN, the claude CLI returned
-- 401 "Invalid bearer token" on every call, but the JSON envelope still
-- parsed cleanly so the gate sealed empty no-change completions and the
-- whole batch looked like 442 successful no-op runs.
--
-- This migration installs the structural detector for that anti-pattern:
-- a view that returns (resolved_agent, recent_zero_token_succeeded_count,
-- recent_succeeded_count, latest_finished_at) for any agent whose recent
-- streak of "succeeded" jobs all reported zero tokens. Token=0 with
-- status=succeeded is the precise fingerprint of "LLM call rejected by
-- provider auth, but the local CLI exited cleanly". We can't distinguish
-- it from "LLM legitimately produced zero output" without semantic
-- inspection — but a streak of N is overwhelming evidence: real LLM
-- workloads don't produce 5 consecutive zero-token completions.
--
-- The projection is consulted by:
--   * runtime/observability/zero_token_detector.py — flags the pattern,
--     calls the demotion + bug-file routine.
--   * the heartbeat tick — periodically runs the demotion sweep so the
--     pattern self-quarantines without operator intervention.
--   * praxis_status_snapshot — surfaces a banner when any agent is in
--     the silent-failure cluster.
--
-- Standing-order references:
--   architecture-policy::auth::via-docker-creds-not-shell
--   architecture-policy::deployment::docker-restart-caches-env
--   anti-pattern: build_antipattern.zero_token_silent_failure (registered
--     with seed rules in this same migration; future migrations grow the
--     family — empty_completion_in_build_lane, tracking_doc_only, etc.)

BEGIN;

-- ── Anti-pattern registry ────────────────────────────────────────────────
-- Lightweight registry for the recursive anti-pattern catalog. Future
-- migrations grow it (empty_completion, tracking_doc_only, etc.). For
-- this migration, we seed the one rule that catches today's failure mode.
CREATE TABLE IF NOT EXISTS build_antipattern_registry (
    rule_slug              TEXT PRIMARY KEY,
    title                  TEXT NOT NULL,
    description            TEXT NOT NULL,
    severity               TEXT NOT NULL CHECK (severity IN ('warn', 'fail', 'demote')),
    applies_to_task_types  TEXT[] NOT NULL DEFAULT '{}',
    detector_kind          TEXT NOT NULL CHECK (detector_kind IN (
        'sql_projection',  -- consults a registered DB view
        'inline_check',    -- evaluated inside the gate at seal time
        'post_hoc_metric'  -- aggregated across many jobs
    )),
    detector_ref           TEXT NOT NULL,  -- e.g. view name, function path
    enabled                BOOLEAN NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decision_ref           TEXT,
    rationale              TEXT
);

INSERT INTO build_antipattern_registry (
    rule_slug, title, description, severity, applies_to_task_types,
    detector_kind, detector_ref, decision_ref, rationale
) VALUES (
    'zero_token_silent_failure',
    'Worker is producing zero-token "succeeded" jobs for one provider',
    'A streak of N consecutive succeeded-status jobs from the same resolved_agent with token_input=0, token_output=0, cost_usd=0. Indicates the provider CLI is rejecting auth (or otherwise short-circuiting) but the local exit code is 0 and the JSON envelope parses cleanly. Without intervention, every job in the worker queue continues to seal as a no-change completion.',
    'demote',
    ARRAY['build', 'fix', 'refactor', 'test', 'wiring', 'review', 'analysis', 'research', 'audit', 'debate'],
    'sql_projection',
    'v_zero_token_silent_failures',
    'architecture-policy::auth::via-docker-creds-not-shell',
    'Operator decision 2026-04-29: detect the failure mode at the metric level so it self-quarantines instead of running 442+ jobs through a dead provider before anyone notices.'
)
ON CONFLICT (rule_slug) DO NOTHING;

-- ── Anti-pattern hits log ────────────────────────────────────────────────
-- Every detection writes a row. Auto-bug-file and route-demotion read
-- from here. Aggregations across (rule_slug, resolved_agent) feed the
-- learning layer.
CREATE TABLE IF NOT EXISTS build_antipattern_hits (
    hit_id                BIGSERIAL PRIMARY KEY,
    rule_slug             TEXT NOT NULL REFERENCES build_antipattern_registry(rule_slug),
    resolved_agent        TEXT NOT NULL,
    provider_slug         TEXT,
    model_slug            TEXT,
    streak_count          INTEGER NOT NULL,
    latest_finished_at    TIMESTAMPTZ NOT NULL,
    sample_run_ids        TEXT[] NOT NULL DEFAULT '{}',
    sample_job_ids        TEXT[] NOT NULL DEFAULT '{}',
    detected_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bug_id                TEXT,                      -- filled when auto-file fires
    remediation_action    TEXT,                      -- e.g. 'route_demoted', 'auth_probe_run'
    remediation_at        TIMESTAMPTZ,
    cleared_at            TIMESTAMPTZ,               -- filled when streak ends
    cleared_evidence      TEXT
);

CREATE INDEX IF NOT EXISTS idx_build_antipattern_hits_agent_open
    ON build_antipattern_hits (resolved_agent)
    WHERE cleared_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_build_antipattern_hits_detected
    ON build_antipattern_hits (detected_at DESC);

-- ── The detector view ────────────────────────────────────────────────────
-- Returns one row per resolved_agent currently in a zero-token streak.
-- Streak threshold is 5 (same provider, same most-recent-5-jobs window,
-- all succeeded, all token_in=0 AND token_out=0). 5 is the floor where
-- "real LLM workload looking like zeros" stops being plausible: a
-- legitimate no-op-completion sequence might be 1-2; a coincidental run
-- of 3-4 from a sparse-output prompt template is still possible; 5+ is
-- only auth/transport failure.
CREATE OR REPLACE VIEW v_zero_token_silent_failures AS
WITH recent_jobs AS (
    SELECT
        j.id,
        j.run_id,
        j.resolved_agent,
        j.status,
        j.token_input,
        j.token_output,
        j.cost_usd,
        j.finished_at,
        ROW_NUMBER() OVER (PARTITION BY j.resolved_agent ORDER BY j.finished_at DESC NULLS LAST) AS recency_rank
    FROM workflow_jobs j
    WHERE j.resolved_agent IS NOT NULL
      AND j.resolved_agent <> ''
      AND j.finished_at IS NOT NULL
      AND j.finished_at > NOW() - INTERVAL '6 hours'
),
agent_window AS (
    SELECT
        resolved_agent,
        COUNT(*) FILTER (WHERE recency_rank <= 5) AS recent_count,
        COUNT(*) FILTER (
            WHERE recency_rank <= 5
              AND status = 'succeeded'
              AND COALESCE(token_input, 0) = 0
              AND COALESCE(token_output, 0) = 0
              AND COALESCE(cost_usd, 0) = 0
        ) AS zero_token_succeeded_count,
        MAX(finished_at) FILTER (WHERE recency_rank <= 5) AS latest_finished_at,
        ARRAY_AGG(run_id) FILTER (WHERE recency_rank <= 5) AS recent_run_ids,
        ARRAY_AGG(id::text) FILTER (WHERE recency_rank <= 5) AS recent_job_ids
    FROM recent_jobs
    GROUP BY resolved_agent
)
SELECT
    resolved_agent,
    SPLIT_PART(resolved_agent, '/', 1)              AS provider_slug,
    NULLIF(SUBSTRING(resolved_agent FROM POSITION('/' IN resolved_agent) + 1), '') AS model_slug,
    recent_count,
    zero_token_succeeded_count                      AS streak_count,
    latest_finished_at,
    recent_run_ids,
    recent_job_ids
FROM agent_window
WHERE recent_count >= 5
  AND zero_token_succeeded_count = recent_count;

COMMENT ON VIEW v_zero_token_silent_failures IS
    'Resolved agents whose 5 most-recent (last 6h) finished jobs all came back with status=succeeded but zero tokens and zero cost. This is the silent-failure fingerprint: provider CLI 401s, JSON envelope parses, gate seals empty no-change completion, ledger looks healthy. Detector is consulted by runtime.observability.zero_token_detector to file bugs and demote routes.';

COMMIT;
