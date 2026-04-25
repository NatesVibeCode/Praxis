-- Migration 251: Seed Together direct-API provider profile.
--
-- Operator direction (2026-04-25): Together AI key onboarded direct,
-- bypassing OpenRouter so we get our own Together rate-limit bucket
-- instead of sharing the OpenRouter pool.
--
-- Together's API is OpenAI-compatible (endpoint
-- https://api.together.xyz/v1/chat/completions, protocol family
-- openai_chat_completions). Provider lane is API; the operator
-- standing order limits API to the explicit compile/section-author
-- exception lane, so this row exists to support that lane and only
-- that lane. CLI remains default everywhere else.
--
-- Migration 252 will re-rank task_type_routing for plan_section_author
-- to put together at rank 1 (DeepSeek-V4-Pro hosted on Together's own
-- infra, US, with our key = no shared-pool 429s).

BEGIN;

INSERT INTO provider_cli_profiles (
    provider_slug,
    binary_name,
    api_endpoint,
    api_protocol_family,
    api_key_env_vars,
    status,
    output_format,
    output_envelope_key,
    base_flags,
    forbidden_flags,
    aliases,
    default_timeout,
    adapter_economics,
    prompt_mode
) VALUES (
    'together',
    'together-api',
    'https://api.together.xyz/v1/chat/completions',
    'openai_chat_completions',
    '["TOGETHER_API_KEY"]'::jsonb,
    'active',
    'json',
    'result',
    '[]'::jsonb,
    '[]'::jsonb,
    '[]'::jsonb,
    300,
    jsonb_build_object('llm_task', jsonb_build_object('allow_payg_fallback', true)),
    'stdin'
)
ON CONFLICT (provider_slug) DO UPDATE SET
    binary_name = EXCLUDED.binary_name,
    api_endpoint = EXCLUDED.api_endpoint,
    api_protocol_family = EXCLUDED.api_protocol_family,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    status = EXCLUDED.status,
    adapter_economics = EXCLUDED.adapter_economics,
    updated_at = now();

INSERT INTO provider_concurrency (
    provider_slug, max_concurrent, active_slots, cost_weight_default
) VALUES (
    'together', 8, 0, 1.0
)
ON CONFLICT (provider_slug) DO UPDATE SET
    max_concurrent = EXCLUDED.max_concurrent,
    cost_weight_default = EXCLUDED.cost_weight_default,
    updated_at = now();

COMMIT;

-- Verification:
--   SELECT provider_slug, api_endpoint, api_protocol_family, api_key_env_vars
--   FROM provider_cli_profiles WHERE provider_slug='together';
