-- Migration 125: Seed explicit local Cursor Agent CLI provider authority
--
-- `cursor` remains the Background Agents API provider. This migration adds a
-- separate repo-local CLI provider so local Cursor execution is explicit and
-- queryable instead of being hidden behind transport fallbacks.

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
    'cursor_local',
    'cursor-agent',
    '["-p","--output-format","json","--mode","ask","-f","--sandbox","disabled"]'::jsonb,
    '--model',
    NULL,
    NULL,
    'json',
    'result',
    '["--cloud","--workspace","-w","--worktree"]'::jsonb,
    900,
    '["cursor-cli"]'::jsonb,
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
    'stdin',
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
    'provider_transport_admission.cursor_local.cli_llm',
    'cursor_local',
    'cli_llm',
    'cli',
    'local_cli',
    true,
    'Admitted local Cursor Agent CLI lane for repo-local workflow execution.',
    'cursor_local:cli_llm',
    '{"authentication":"https://docs.cursor.com/en/cli/reference/authentication"}'::jsonb,
    '["CURSOR_API_KEY"]'::jsonb,
    '{
      "seeded_by": "125_cursor_local_cli_provider_seed.sql",
      "execution_mode": "stdin_json_cli",
      "noninteractive_auth": "cursor_api_key_required",
      "workspace_trust": "force_flag_required",
      "sandbox_mode": "disabled"
    }'::jsonb,
    'migration.125.cursor_local_cli_provider_seed',
    'active',
    now(),
    now()
) ON CONFLICT (provider_slug, adapter_type) DO UPDATE SET
    provider_transport_admission_id = EXCLUDED.provider_transport_admission_id,
    transport_kind = EXCLUDED.transport_kind,
    execution_topology = EXCLUDED.execution_topology,
    admitted_by_policy = EXCLUDED.admitted_by_policy,
    policy_reason = EXCLUDED.policy_reason,
    lane_id = EXCLUDED.lane_id,
    docs_urls = EXCLUDED.docs_urls,
    credential_sources = EXCLUDED.credential_sources,
    probe_contract = EXCLUDED.probe_contract,
    decision_ref = EXCLUDED.decision_ref,
    status = EXCLUDED.status,
    updated_at = now();

COMMIT;
