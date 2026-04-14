BEGIN;

CREATE TABLE IF NOT EXISTS provider_cli_profiles (
    provider_slug TEXT PRIMARY KEY,
    binary_name TEXT NOT NULL,
    base_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_flag TEXT,
    system_prompt_flag TEXT,
    json_schema_flag TEXT,
    output_format TEXT NOT NULL DEFAULT 'json',
    output_envelope_key TEXT NOT NULL DEFAULT 'result',
    forbidden_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_timeout INT NOT NULL DEFAULT 300,
    aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL DEFAULT 'active',
    default_model TEXT,
    api_endpoint TEXT,
    api_protocol_family TEXT,
    api_key_env_vars JSONB NOT NULL DEFAULT '[]'::jsonb,
    adapter_economics JSONB NOT NULL DEFAULT '{}'::jsonb,
    prompt_mode TEXT NOT NULL DEFAULT 'stdin',
    mcp_config_style TEXT,
    mcp_args_template JSONB,
    sandbox_env_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
    exclude_from_rotation BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO provider_cli_profiles (
    provider_slug,
    binary_name,
    base_flags,
    model_flag,
    system_prompt_flag,
    json_schema_flag,
    output_format,
    output_envelope_key,
    forbidden_flags,
    default_timeout,
    aliases,
    status
) VALUES
(
    'anthropic',
    'claude',
    '["-p","--output-format","json"]'::jsonb,
    '--model',
    '--system-prompt',
    '--json-schema',
    'json',
    'result',
    '["--dangerously-skip-permissions","--allow-dangerously-skip-permissions","--add-dir"]'::jsonb,
    300,
    '[]'::jsonb,
    'active'
),
(
    'openai',
    'codex',
    '["exec","-","--json"]'::jsonb,
    '--model',
    NULL,
    NULL,
    'ndjson',
    'text',
    '["--full-auto"]'::jsonb,
    300,
    '[]'::jsonb,
    'active'
),
(
    'google',
    'gemini',
    '["-p",".","-o","json"]'::jsonb,
    '--model',
    NULL,
    NULL,
    'json',
    'response',
    '["--approval-mode","--yolo","-y"]'::jsonb,
    600,
    '["gemini-cli"]'::jsonb,
    'active'
)
ON CONFLICT (provider_slug) DO NOTHING;

ALTER TABLE provider_cli_profiles
    ADD COLUMN IF NOT EXISTS default_model TEXT,
    ADD COLUMN IF NOT EXISTS api_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS api_protocol_family TEXT,
    ADD COLUMN IF NOT EXISTS api_key_env_vars JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS adapter_economics JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_cli_profiles_api_key_env_vars_array_check'
    ) THEN
        ALTER TABLE provider_cli_profiles
            ADD CONSTRAINT provider_cli_profiles_api_key_env_vars_array_check
            CHECK (jsonb_typeof(api_key_env_vars) = 'array');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_cli_profiles_adapter_economics_object_check'
    ) THEN
        ALTER TABLE provider_cli_profiles
            ADD CONSTRAINT provider_cli_profiles_adapter_economics_object_check
            CHECK (jsonb_typeof(adapter_economics) = 'object');
    END IF;
END $$;

UPDATE provider_cli_profiles
SET default_model = 'claude-3-5-sonnet-latest',
    api_endpoint = 'https://api.anthropic.com/v1/messages',
    api_protocol_family = 'anthropic_messages',
    api_key_env_vars = '["ANTHROPIC_API_KEY"]'::jsonb,
    adapter_economics = '{
      "cli_llm": {
        "billing_mode": "subscription_included",
        "budget_bucket": "anthropic_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": true
      },
      "llm_task": {
        "billing_mode": "metered_api",
        "budget_bucket": "anthropic_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": false,
        "allow_payg_fallback": true
      }
    }'::jsonb
WHERE provider_slug = 'anthropic';

UPDATE provider_cli_profiles
SET default_model = 'gpt-4.1',
    api_endpoint = 'https://api.openai.com/v1/chat/completions',
    api_protocol_family = 'openai_chat_completions',
    api_key_env_vars = '["OPENAI_API_KEY"]'::jsonb,
    adapter_economics = '{
      "cli_llm": {
        "billing_mode": "subscription_included",
        "budget_bucket": "openai_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": true
      },
      "llm_task": {
        "billing_mode": "metered_api",
        "budget_bucket": "openai_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": false,
        "allow_payg_fallback": true
      }
    }'::jsonb
WHERE provider_slug = 'openai';

UPDATE provider_cli_profiles
SET default_model = 'gemini-2.5-flash',
    api_endpoint = 'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent',
    api_protocol_family = 'google_generate_content',
    api_key_env_vars = '["GEMINI_API_KEY","GOOGLE_API_KEY"]'::jsonb,
    adapter_economics = '{
      "cli_llm": {
        "billing_mode": "subscription_included",
        "budget_bucket": "google_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": true
      },
      "llm_task": {
        "billing_mode": "metered_api",
        "budget_bucket": "google_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": false,
        "allow_payg_fallback": true
      }
    }'::jsonb
WHERE provider_slug = 'google';

COMMIT;
