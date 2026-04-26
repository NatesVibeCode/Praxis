-- Migration 262: Together as the compile-task-type primary
--
-- Operator direction (2026-04-26, nate): "use together you have the API in
-- the Keychain, but its all API not CLI so it should be easy".
--
-- Context: Migrations 243 + 246 set OpenRouter/deepseek-v4-pro and
-- OpenRouter/deepseek-v4-flash as the compile primaries (rank 1 + 2).
-- The UI "Describe it" path failed at runtime because the API server
-- container had no OPENROUTER_API_KEY. The operator's preferred lane is
-- Together — the V4-Pro model is already registered there
-- (provider_model_candidates row at priority=500) and Together's API
-- endpoint + key resolution are wired through provider_endpoint_bindings
-- (https://api.together.xyz/v1/chat/completions, TOGETHER_API_KEY).
--
-- This migration swaps task_type_routing for task_type='compile':
--   rank 1: together / deepseek-ai/DeepSeek-V4-Pro
--   rank 2: together / deepseek-ai/DeepSeek-V3.2 (fallback within the same
--           provider family — V3.2 is also Together-hosted)
-- The OpenRouter rows are demoted to fallback rank 5+ rather than deleted,
-- so they remain available if Together is unhealthy and the failover
-- chain has to walk.

BEGIN;

-- 1. Demote the OpenRouter compile primaries to fallback ranks.
UPDATE task_type_routing
   SET rank = 5,
       updated_at = now()
 WHERE task_type = 'compile'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-flash'
   AND route_source = 'explicit';

UPDATE task_type_routing
   SET rank = 6,
       updated_at = now()
 WHERE task_type = 'compile'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro'
   AND route_source = 'explicit';

-- 2. Insert Together as the new rank 1 + 2 compile routes.
INSERT INTO task_type_routing (
    task_type,
    rank,
    provider_slug,
    model_slug,
    route_source,
    permitted,
    updated_at
) VALUES
    ('compile', 1, 'together', 'deepseek-ai/DeepSeek-V4-Pro', 'explicit', TRUE, now()),
    ('compile', 2, 'together', 'deepseek-ai/DeepSeek-V3.2',  'explicit', TRUE, now())
ON CONFLICT (task_type, provider_slug, model_slug)
DO UPDATE SET
    rank = EXCLUDED.rank,
    route_source = EXCLUDED.route_source,
    permitted = EXCLUDED.permitted,
    updated_at = now();

-- 3. Tag together V4-Pro candidate with compile-primary capabilities so
--    auto-resolution, capability-tag filtering, and the planning-stack
--    matchers all see it as the compile primary, matching how migration
--    246 narrowed the OpenRouter v4-pro candidate to compile.
UPDATE provider_model_candidates
   SET capability_tags = '["compile","structured-output","workflow-definition","schema-normalization","long-context","primary-engine","api-only"]'::jsonb,
       task_affinities = '{
         "primary": ["compile","structured-output","workflow-definition","schema-normalization"],
         "secondary": [],
         "specialized": ["long-context","api-only"],
         "fallback": [],
         "avoid": ["cli","tool-use","agentic-coding"]
       }'::jsonb
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

COMMIT;
