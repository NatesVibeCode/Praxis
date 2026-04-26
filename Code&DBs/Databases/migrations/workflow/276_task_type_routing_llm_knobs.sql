-- Migration 276: Lift LLM call knobs (temperature, max_tokens) out of code
-- into task_type_routing so experiments can inherit them.
--
-- Operator decision: experiments are deltas on top of operator-authority
-- task_type rows. For "import the rules from another task type" to mean
-- something concrete, the rules have to LIVE in the row. Today temperature
-- (0.0 implicit via LLMRequest default) and max_tokens (4096 hardcoded in
-- plan_synthesis._call_synthesis_llm and plan_fork_author._call_fork_llm)
-- are baked into Python. After this migration:
--   - Every routing row may carry its own temperature + max_tokens.
--   - The call sites read from the resolved row when present, fall back
--     to the existing hardcoded defaults when NULL (backward-compat).
--   - The compose_experiment runner can resolve {provider, model,
--     temperature, max_tokens} from a base task_type and layer per-leg
--     overrides on top.
--
-- Backfills the three LLM-task rows that hardcoded these values today
-- (plan_synthesis, plan_fork_author, plan_section_author) so "import
-- from plan_synthesis" returns the values the runtime is already using.

BEGIN;

-- =====================================================================
-- Schema additions (NULLable, opt-in; pre-existing rows unaffected).
-- =====================================================================
ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS temperature NUMERIC(4,3),
    ADD COLUMN IF NOT EXISTS max_tokens INTEGER;

COMMENT ON COLUMN task_type_routing.temperature IS
    'LLM sampling temperature (0.0-2.0) applied to calls routed via this row. NULL = use the LLMRequest default (0.0). Lifted out of code by migration 276 so experiments can vary it without redeploys.';

COMMENT ON COLUMN task_type_routing.max_tokens IS
    'LLM max_tokens cap applied to calls routed via this row. NULL = use the per-task hardcoded fallback (4096 today for plan_* task types). Lifted out of code by migration 276 so experiments can vary it without redeploys.';

-- =====================================================================
-- Backfill rows that already use these values implicitly.
-- =====================================================================
-- plan_synthesis: synthesis call uses 4096 cap + temp=0.0 default today.
UPDATE task_type_routing
   SET temperature = 0.0,
       max_tokens = 4096,
       updated_at = now()
 WHERE task_type = 'plan_synthesis'
   AND temperature IS NULL
   AND max_tokens IS NULL;

-- plan_fork_author: each fan-out child uses 4096 cap + temp=0.0 default.
UPDATE task_type_routing
   SET temperature = 0.0,
       max_tokens = 4096,
       updated_at = now()
 WHERE task_type = 'plan_fork_author'
   AND temperature IS NULL
   AND max_tokens IS NULL;

-- plan_section_author: legacy section-author path; same defaults.
UPDATE task_type_routing
   SET temperature = 0.0,
       max_tokens = 4096,
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND temperature IS NULL
   AND max_tokens IS NULL;

COMMIT;

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug, temperature, max_tokens
--     FROM task_type_routing
--    WHERE task_type IN ('plan_synthesis', 'plan_fork_author', 'plan_section_author')
--    ORDER BY task_type, rank;
