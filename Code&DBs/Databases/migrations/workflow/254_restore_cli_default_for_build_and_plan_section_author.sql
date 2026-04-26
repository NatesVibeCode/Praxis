-- Migration 254: Restore cli_llm rank=1 for `build` and `plan_section_author`.
--
-- Rolls back the over-scoped api_llm rank-1 promotions from migration 243
-- (build) and migration 252 (plan_section_author). The compile task type
-- introduced by migration 245 is intentionally left alone — UI compile is
-- the explicit api exception per operator direction (2026-04-25).
--
-- Standing order being honored:
--   `architecture-policy::provider-routing::cli-default-api-exception`
--   (CLAUDE.md, added in commit 22b93427 — same commit that landed 243/252.
--   The migrations contradicted the standing order added alongside them;
--   this migration aligns the routing table back to the policy.)
--
-- Operator direction (2026-04-25): "deepseek was just for the UI compile
-- nothing else." Build and plan_section_author should never have been
-- swapped to api lanes.
--
-- Execution path chosen: cursor_local / composer-2 — the provider has a
-- cli_llm-only transport admission (no api ambiguity, no Claude Code OAuth
-- dependency that BUG-A7A940DF still blocks). Seeded by migration
-- 125_cursor_local_cli_provider_seed.sql.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Demote api_llm rank-1 entries for `build` (from migration 243) to
--    rank 99. Kept as fallback rows for transparency; the api circuit
--    breakers for openrouter/anthropic/google/openai are operator-pinned
--    force-open, so api fallbacks will not actually fire.
-- -----------------------------------------------------------------------
UPDATE task_type_routing
   SET rank = 99,
       rationale = 'Demoted by migration 254: api lane should not be primary for build per cli_default_api_exception standing order. Was rank=1 from migration 243.',
       updated_at = now()
 WHERE task_type = 'build'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

UPDATE task_type_routing
   SET rank = 99,
       rationale = 'Demoted by migration 254: api lane should not be primary for build per cli_default_api_exception standing order. Was rank=2 fallback from migration 243.',
       updated_at = now()
 WHERE task_type = 'build'
   AND provider_slug = 'openrouter'
   AND model_slug = 'anthropic/claude-sonnet-4.6';

-- -----------------------------------------------------------------------
-- 2. Pin cursor_local / composer-2 (cli_llm) at rank 1 for `build`.
--    cursor_local has only cli_llm transport admission (migration 125),
--    so this binding is unambiguously a CLI route.
-- -----------------------------------------------------------------------
INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES (
    'build', 'cursor_local', 'composer-2',
    TRUE, 1, 'high', 1, 'reasoning', 1,
    'Primary build engine restored to cli_llm per cli_default_api_exception standing order (migration 254). cursor_local has cli-only transport admission (migration 125); BUG-A7A940DF blocks anthropic CLI in worker container.',
    'explicit'
) ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source,
    updated_at = now();

-- -----------------------------------------------------------------------
-- 3. Demote api_llm rank-1/2 entries for `plan_section_author` (from
--    migrations 252 + 250) to rank 99. Same rationale as section 1.
-- -----------------------------------------------------------------------
UPDATE task_type_routing
   SET rank = 99,
       rationale = 'Demoted by migration 254: together direct api lane should not be primary for plan_section_author per cli_default_api_exception standing order. Was rank=1 from migration 252.',
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

UPDATE task_type_routing
   SET rank = 99,
       rationale = 'Demoted by migration 254: together direct api lane should not be primary for plan_section_author per cli_default_api_exception standing order. Was rank=2 fallback from migration 252.',
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

UPDATE task_type_routing
   SET rank = 99,
       rationale = 'Demoted by migration 254: openrouter api lane should not be primary for plan_section_author per cli_default_api_exception standing order.',
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'openrouter'
   AND model_slug IN ('deepseek/deepseek-v4-pro', 'deepseek/deepseek-v4-flash');

-- -----------------------------------------------------------------------
-- 4. Pin cursor_local / composer-2 (cli_llm) at rank 1 for
--    `plan_section_author`.
-- -----------------------------------------------------------------------
INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES (
    'plan_section_author', 'cursor_local', 'composer-2',
    TRUE, 1, 'high', 1, 'reasoning', 1,
    'Primary plan_section_author engine restored to cli_llm per cli_default_api_exception standing order (migration 254). cursor_local has cli-only transport admission (migration 125).',
    'explicit'
) ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source,
    updated_at = now();

COMMIT;

-- Verification (read-lane, no psql in operator workflow):
--   The api-server bootstrap re-validates the migration manifest on
--   restart. After this migration applies, rank=1 for `build` and
--   `plan_section_author` will be cursor_local/composer-2 (cli_llm).
--   `compile` task type rank=1 is intentionally NOT touched and
--   remains on its api binding per migration 245.
