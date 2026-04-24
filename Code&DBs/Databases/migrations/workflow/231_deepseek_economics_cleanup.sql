-- Migration 231: Clean up DeepSeek's provider_cli_profiles.adapter_economics.
--
-- Two pre-existing legacy gaps surfaced by migration 230's cache reload:
--
-- 1. DeepSeek has a `cli_llm` adapter_economics entry, but per the operator
--    policy "DeepSeek is API not CLI" (user memory + migration 159 lane
--    policy `{llm_task}` only), DeepSeek has no CLI binary. The cli_llm
--    entry is dead.
--
-- 2. Both DeepSeek adapter_economics entries (cli_llm and llm_task) are
--    sparse — they only set billing_mode, budget_bucket,
--    effective_marginal_cost. BUG-8DAA5468 rejects sparse rows because
--    paid-lane fallback policy cannot silently flip. Required fields:
--    allow_payg_fallback, prefer_prepaid.
--
-- This migration removes the dead cli_llm entry and fills in the missing
-- fallback fields on llm_task. After this, the registry loads cleanly and
-- workflow health no longer chokes on the sparse-row rejection.

BEGIN;

UPDATE public.provider_cli_profiles
SET adapter_economics = jsonb_build_object(
        'llm_task', jsonb_build_object(
            'billing_mode', 'metered_api',
            'budget_bucket', 'deepseek_api',
            'effective_marginal_cost', 0.27,
            'allow_payg_fallback', true,
            'prefer_prepaid', false
        )
    ),
    updated_at = now()
WHERE provider_slug = 'deepseek';

COMMIT;

-- Verification (run manually):
--   SELECT provider_slug, jsonb_object_keys(adapter_economics) AS adapter_type
--   FROM provider_cli_profiles WHERE provider_slug='deepseek';
--     -> expect only 'llm_task' (no 'cli_llm')
--   SELECT adapter_economics->'llm_task' FROM provider_cli_profiles
--   WHERE provider_slug='deepseek';
--     -> expect keys: allow_payg_fallback, billing_mode, budget_bucket,
--        effective_marginal_cost, prefer_prepaid
