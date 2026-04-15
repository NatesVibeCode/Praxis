-- Migration 111: Seed Cursor CLI provider authority for DB-native onboarding
--
-- Cursor is provisioned as a known provider in the canonical registry, but its
-- CLI lane starts disabled until the provider onboarding wizard verifies local
-- auth and prompt execution on the host machine.

BEGIN;

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
    status,
    default_model,
    api_key_env_vars,
    adapter_economics,
    prompt_mode,
    created_at,
    updated_at
) VALUES (
    'cursor',
    'cursor-agent',
    '["--trust","-p","--output-format","json","--sandbox","disabled"]'::jsonb,
    '--model',
    NULL,
    NULL,
    'json',
    'result',
    '["--cloud","--force","-f","--yolo","--workspace","-w","--worktree"]'::jsonb,
    900,
    '[]'::jsonb,
    'active',
    'composer-2',
    '["CURSOR_API_KEY"]'::jsonb,
    '{
      "cli_llm": {
        "billing_mode": "subscription_included",
        "budget_bucket": "cursor_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": false
      }
    }'::jsonb,
    'argv',
    now(),
    now()
) ON CONFLICT (provider_slug) DO UPDATE SET
    binary_name = EXCLUDED.binary_name,
    base_flags = EXCLUDED.base_flags,
    model_flag = EXCLUDED.model_flag,
    system_prompt_flag = EXCLUDED.system_prompt_flag,
    json_schema_flag = EXCLUDED.json_schema_flag,
    output_format = EXCLUDED.output_format,
    output_envelope_key = EXCLUDED.output_envelope_key,
    forbidden_flags = EXCLUDED.forbidden_flags,
    default_timeout = EXCLUDED.default_timeout,
    aliases = EXCLUDED.aliases,
    status = EXCLUDED.status,
    default_model = EXCLUDED.default_model,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    adapter_economics = EXCLUDED.adapter_economics,
    prompt_mode = EXCLUDED.prompt_mode,
    updated_at = now();

INSERT INTO provider_transport_admissions (
    provider_transport_admission_id,
    provider_slug,
    adapter_type,
    transport_kind,
    execution_topology,
    admitted_by_policy,
    policy_reason,
    lane_id,
    docs_urls,
    credential_sources,
    probe_contract,
    decision_ref,
    status,
    created_at,
    updated_at
) VALUES (
    'provider_transport_admission.cursor.cli_llm',
    'cursor',
    'cli_llm',
    'cli',
    'local_cli',
    false,
    'Provisioned by canonical migration; run provider onboarding to verify Cursor auth and admit the local CLI lane.',
    'cursor:cli_llm',
    '{}'::jsonb,
    '["CURSOR_API_KEY","ambient_cli_session"]'::jsonb,
    '{
      "seeded_by": "111_cursor_cli_provider_seed.sql",
      "wizard_required": true,
      "prompt_probe": {
        "status": "not_run",
        "strategy": "cli_headless_prompt",
        "prompt_mode": "argv"
      },
      "router_probe": {
        "selected_transport_supported": false
      }
    }'::jsonb,
    'migration.111.cursor_cli_provider_seed',
    'active',
    now(),
    now()
) ON CONFLICT (provider_slug, adapter_type) DO NOTHING;

COMMIT;
