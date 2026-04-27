-- Migration 252: Promote Together direct to plan_section_author primary.
--
-- Operator direction (2026-04-25): Together AI direct API onboarded with
-- our own key (migration 251 + onboarding wizard). Together hosts
-- DeepSeek-V4-Pro on US infra; routing direct (not via OpenRouter)
-- gives us our own rate-limit bucket and avoids the OpenRouter→Together
-- shared-pool 429 we saw earlier today.
--
-- New rank order for plan_section_author:
--   1. together     / deepseek-ai/DeepSeek-V4-Pro     (high tier, primary, US, our key)
--   2. together     / deepseek-ai/DeepSeek-V3.2       (high tier, fallback within Together)
--   3. openrouter   / deepseek/deepseek-v4-pro        (high tier, OpenRouter pool fallback)
--   4. openrouter   / deepseek/deepseek-v4-flash      (medium tier, OpenRouter cheap fallback)
--   5. openai       / gpt-5-codex                     (high tier, cross-provider US)
--   6. openai       / gpt-5.1-codex                   (high tier)
--   7. cursor       / composer-2                      (high tier, US)
--   8. google       / gemini-3-flash-preview          (high tier, US)
--
-- Existing OpenRouter rows are kept as fallbacks so a Together outage
-- doesn't block compile entirely.

BEGIN;

INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES
    ('plan_section_author', 'together', 'deepseek-ai/DeepSeek-V4-Pro',
     TRUE, 1, 'high', 1, 'reasoning', 1,
     'Direct Together API. Our own key (migration 251), US-hosted, no OpenRouter pool sharing. Primary for accuracy.',
     'explicit'),
    ('plan_section_author', 'together', 'deepseek-ai/DeepSeek-V3.2',
     TRUE, 2, 'high', 2, 'reasoning', 2,
     'Same provider lane (Together direct); fallback when V4-Pro is unavailable. Latest V3 series.',
     'explicit')
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source,
    updated_at = now();

-- Push existing OpenRouter rows to ranks 3-4 (were 1-2).
UPDATE task_type_routing
   SET rank = 3,
       rationale = 'OpenRouter pool fallback when Together direct is unavailable.',
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

UPDATE task_type_routing
   SET rank = 4,
       rationale = 'OpenRouter cheap pool fallback.',
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-flash';

-- Push existing cross-provider rows from migration 250 down by 2.
UPDATE task_type_routing SET rank = 5, updated_at = now()
 WHERE task_type='plan_section_author' AND provider_slug='openai' AND model_slug='gpt-5-codex';
UPDATE task_type_routing SET rank = 6, updated_at = now()
 WHERE task_type='plan_section_author' AND provider_slug='openai' AND model_slug='gpt-5.1-codex';
UPDATE task_type_routing SET rank = 7, updated_at = now()
 WHERE task_type='plan_section_author' AND provider_slug='cursor' AND model_slug='composer-2';
UPDATE task_type_routing SET rank = 8, updated_at = now()
 WHERE task_type='plan_section_author' AND provider_slug='google' AND model_slug='gemini-3-flash-preview';

COMMIT;

-- Verification:
--   SELECT rank, provider_slug, model_slug, route_tier, permitted
--   FROM task_type_routing WHERE task_type='plan_section_author' ORDER BY rank;
