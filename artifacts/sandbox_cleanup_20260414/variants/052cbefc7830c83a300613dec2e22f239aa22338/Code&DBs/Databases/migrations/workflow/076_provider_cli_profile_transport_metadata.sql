BEGIN;

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
