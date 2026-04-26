-- Migration 264: plan_section_author primary = Together DeepSeek-V3.2 (Flash tier)
--
-- Operator direction (2026-04-26, nate): "use Flash for fork-outs only (keeps
-- Pro's decomposition quality, halves fork-out cost)" + "use Together you have
-- the API in the keychain".
--
-- Context: compose_plan_via_llm orchestrates synthesis (1 Pro call → packet
-- seeds) + fork-out (N parallel author calls). Today plan_section_author
-- rank 1 is cursor_local/composer-2 (CLI agent, wrong lane for the API-only
-- compile exception); Together V3.2 sits at rank 99. Synthesis already runs
-- Together V4-Pro at rank 1 from migration 262.
--
-- This migration:
--   - Demotes cursor_local at rank 1 (its existing rank stays for CLI lanes
--     elsewhere; we just promote Together above it for plan_section_author).
--   - Promotes Together DeepSeek-V3.2 to rank 1 (Flash tier — cheaper/faster
--     than V4-Pro, fine for well-scoped per-packet authoring).
--   - Promotes Together DeepSeek-V4-Pro to rank 2 as a quality fallback if
--     V3.2 falls over.
--   - OpenRouter v4-flash/v4-pro stay at rank 99 — available if Together
--     and the in-stack Codex routes all fail.

BEGIN;

-- 1. Demote the existing rank 1 / 2 fork-out routes to make room.
UPDATE task_type_routing
   SET rank = 10,
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'cursor_local'
   AND model_slug = 'composer-2'
   AND route_source = 'explicit';

-- 2. Promote Together V3.2 → rank 1 (Flash for fork-outs).
INSERT INTO task_type_routing (
    task_type,
    rank,
    provider_slug,
    model_slug,
    route_source,
    permitted,
    updated_at
) VALUES
    ('plan_section_author', 1, 'together', 'deepseek-ai/DeepSeek-V3.2',  'explicit', TRUE, now()),
    ('plan_section_author', 2, 'together', 'deepseek-ai/DeepSeek-V4-Pro', 'explicit', TRUE, now())
ON CONFLICT (task_type, provider_slug, model_slug)
DO UPDATE SET
    rank = EXCLUDED.rank,
    route_source = EXCLUDED.route_source,
    permitted = EXCLUDED.permitted,
    updated_at = now();

-- 3. Tag Together V3.2 candidate with fork-author capability tags so capability-
--    based matching in the planner sees it as the appropriate Flash route.
UPDATE provider_model_candidates
   SET capability_tags = '["plan_section_author","structured-output","schema-normalization","cheap","fast","api-only","flash-tier","compile"]'::jsonb,
       task_affinities = '{
         "primary": ["plan_section_author","structured-output","schema-normalization"],
         "secondary": ["compile"],
         "specialized": ["fork-out","cheap","fast","api-only"],
         "fallback": [],
         "avoid": ["cli","tool-use","agentic-coding","plan_synthesis"]
       }'::jsonb
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

COMMIT;
