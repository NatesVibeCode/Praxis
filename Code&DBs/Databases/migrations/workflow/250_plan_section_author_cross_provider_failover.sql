-- Migration 250: Cross-provider failover for plan_section_author.
--
-- Operator standing order (2026-04-25): when DeepSeek through OpenRouter
-- 429s upstream (Together pool throttle), don't jam through — fall over
-- to a different provider entirely. Section author retry_attempts=0 so
-- the route loop advances on the first 429.
--
-- Adds (rank order):
--   3. openai / gpt-5-codex          (high tier, US infra)
--   4. openai / gpt-5.1-codex        (high tier, US infra)
--   5. cursor / composer-2           (high tier, US infra)
--   6. google / gemini-3-flash-preview (high tier, US infra — flash speed)
--
-- Existing rows from migration 249 stay:
--   1. openrouter / deepseek-v4-pro   (high tier, primary)
--   2. openrouter / deepseek-v4-flash (medium tier, openrouter fallback)
--
-- All routes are US-hosted; no DeepSeek-API-China path is registered for
-- plan_section_author. Provider-routing standing order: this is the API
-- exception lane for compile/section-authoring; CLI is still default
-- everywhere else.

BEGIN;

INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES
    ('plan_section_author', 'openai', 'gpt-5-codex',
     TRUE, 3, 'high', 3, 'reasoning', 3,
     'Cross-provider failover when DeepSeek/OpenRouter 429s. OpenAI codex line, US-hosted.',
     'explicit'),
    ('plan_section_author', 'openai', 'gpt-5.1-codex',
     TRUE, 4, 'high', 4, 'reasoning', 4,
     'Second OpenAI codex tier; same lane as gpt-5-codex.',
     'explicit'),
    ('plan_section_author', 'cursor', 'composer-2',
     TRUE, 5, 'high', 5, 'reasoning', 5,
     'Cursor composer fallback — separate provider lane, US-hosted.',
     'explicit'),
    ('plan_section_author', 'google', 'gemini-3-flash-preview',
     TRUE, 6, 'high', 6, 'reasoning', 6,
     'Google Gemini 3 flash preview — fast US-hosted fallback when codex tier is also busy.',
     'explicit')
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
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

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug, route_tier, permitted
--   FROM task_type_routing WHERE task_type='plan_section_author' ORDER BY rank;
