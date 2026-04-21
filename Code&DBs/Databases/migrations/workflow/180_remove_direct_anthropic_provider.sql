-- Migration 180: Remove the direct-Anthropic provider surface.
--
-- Nate does not use a direct Anthropic API key. Claude models are reached
-- exclusively through OpenRouter (see migration 175, which promotes
-- `anthropic/claude-sonnet-4.6` via OpenRouter as the primary chat engine).
--
-- The `provider_slug = 'anthropic'` rows in provider_model_candidates were
-- orphan candidate entries: no endpoint_bindings, no credential_tokens, no
-- integration_registry row, no task_type_route_eligibility row points at
-- them, yet the auto/* task-type resolver still picked them because the
-- candidates existed with status='active'. This caused a worker to dispatch
-- to anthropic/claude-opus-4-6 and fail with 401 (see BUG-AB6FF6D1).
--
-- This migration deletes the direct-Anthropic surface so the resolver can
-- only land on providers that have working credentials (openrouter, openai,
-- google, cursor, deepseek).

BEGIN;

CREATE TABLE IF NOT EXISTS public.provider_concurrency (
    provider_slug TEXT PRIMARY KEY,
    max_concurrent INTEGER NOT NULL DEFAULT 4,
    active_slots REAL NOT NULL DEFAULT 0.0,
    cost_weight_default REAL NOT NULL DEFAULT 1.0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 1a. Drop model_profile_candidate_bindings that reference direct-anthropic
--     candidates. Migration 175 routes Claude via OpenRouter; these bindings
--     were the reason auto/* resolved to anthropic/claude-opus-4-6 and hit
--     the 401 (BUG-AB6FF6D1). Profiles praxis.default and
--     scratch_agent.default still have their other (openai/google/cursor)
--     candidates intact.
DELETE FROM public.model_profile_candidate_bindings
WHERE candidate_ref LIKE 'candidate.anthropic.%';

-- 1b. Drop route_eligibility_states rows that reference direct-anthropic
--     candidates. These are route-health snapshots; once the candidate
--     itself is gone, the eligibility state is stale.
DELETE FROM public.route_eligibility_states
WHERE candidate_ref LIKE 'candidate.anthropic.%';

-- 1c. Drop all provider_model_candidates rows for the direct anthropic slug.
--     At this point the four remaining FKs (route_health_windows,
--     endpoint_bindings, failover_bindings, model_market_bindings) all show
--     zero matching rows, so the DELETE will succeed.
DELETE FROM public.provider_model_candidates
WHERE provider_slug = 'anthropic';

-- 2. Drop the anthropic CLI profile. The `claude` CLI binary is not wired
--    into the autonomous dispatch path; interactive `claude` sessions are
--    Nate's local tool, not a worker provider.
DELETE FROM public.provider_cli_profiles
WHERE provider_slug = 'anthropic';

-- 3. Drop the anthropic lane policy (migration 159 locked anthropic to
--    cli_llm). With no candidates and no CLI profile, the lane policy has
--    nothing to govern.
DELETE FROM public.provider_lane_policy
WHERE provider_slug = 'anthropic';

-- 4. Drop the anthropic concurrency slot record.
DELETE FROM public.provider_concurrency
WHERE provider_slug = 'anthropic';

-- 5. Drop anthropic cost rates (these were for billing the direct API; we
--    bill through OpenRouter now, which has its own cost rows).
DO $$
BEGIN
    IF to_regclass('public.provider_model_cost_rates') IS NOT NULL THEN
        DELETE FROM public.provider_model_cost_rates
        WHERE provider_slug = 'anthropic';
    END IF;
END $$;

-- 6. Sanity: if any task_type_route_eligibility row still references the
--    anthropic slug (none at authoring time), clear it so the resolver
--    cannot resurrect anthropic as a viable route.
DELETE FROM public.task_type_route_eligibility
WHERE provider_slug = 'anthropic';

-- 7. Drop task_type_routing rows pointing at the direct anthropic slug.
--    These were the visible reason auto/architecture resolved to
--    anthropic/claude-opus-4-6 at rank=1. OpenRouter entries for
--    anthropic/claude-sonnet-4.6 stay — they reach Claude through a
--    working credential path.
DELETE FROM public.task_type_routing
WHERE provider_slug = 'anthropic';

COMMIT;

-- Verification (informational; run manually after commit):
--   SELECT COUNT(*) FROM provider_model_candidates WHERE provider_slug='anthropic'; -- expect 0
--   SELECT COUNT(*) FROM provider_cli_profiles WHERE provider_slug='anthropic';     -- expect 0
--   SELECT COUNT(*) FROM provider_lane_policy WHERE provider_slug='anthropic';      -- expect 0
--   SELECT COUNT(*) FROM provider_concurrency WHERE provider_slug='anthropic';      -- expect 0
--   SELECT COUNT(*) FROM provider_model_cost_rates WHERE provider_slug='anthropic'; -- expect 0
