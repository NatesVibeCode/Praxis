-- Migration 265: Drop DeepSeek V3.2 from plan_section_author primary
--
-- Operator direction (2026-04-26, nate): "No Deepseek V3.2 Right?" —
-- migration 264 promoted Together V3.2 to rank 1 for plan_section_author
-- as the v24 "Flash for fork-outs" route. Operator rejected that model.
--
-- Together's registered candidates today are only V3.2 and V4-Pro. Without
-- a different Together model named, this migration:
--   - Demotes V3.2 out of the primary fork-out lane (rank 99 — available
--     only as a desperate fallback)
--   - Promotes V4-Pro to rank 1 for plan_section_author (same engine as
--     synthesis; simpler — single Together model for both layers).
--
-- Cost trade: forfeits the v24 "halve fork-out cost" optimization until a
-- different Together model is registered (Llama / Qwen / Mistral / etc.).
-- Quality stays high (V4-Pro for both synthesis and forks).

BEGIN;

-- 1. Demote V3.2 to fallback rank (was rank 1 from migration 264).
UPDATE task_type_routing
   SET rank = 99,
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

-- 2. Promote V4-Pro to rank 1 (was rank 2 from migration 264).
UPDATE task_type_routing
   SET rank = 1,
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

-- 3. Strip flash-tier capability tags from V3.2 (it's no longer the
--    plan_section_author primary).
UPDATE provider_model_candidates
   SET capability_tags = '["api-only","fallback-only"]'::jsonb,
       task_affinities = '{
         "primary": [],
         "secondary": [],
         "specialized": ["api-only"],
         "fallback": ["plan_section_author"],
         "avoid": ["cli","tool-use","agentic-coding","plan_synthesis","compile"]
       }'::jsonb
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

COMMIT;
