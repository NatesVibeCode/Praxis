-- Migration 182: Restore the anthropic CLI surface (subscription-only).
--
-- Migration 180 over-reached: it deleted the CLI profile, lane policy, and
-- concurrency rows alongside the API-path rows that caused BUG-AB6FF6D1.
-- Nate uses the `claude` CLI (OAuth/subscription, no API key). He does NOT
-- use a direct Anthropic API key. So:
--
--   - CLI surface: MUST WORK (claude binary, cli_llm adapter, lane-locked).
--   - API surface: MUST NOT EXIST (no direct api.anthropic.com call).
--
-- What 180 deleted that stays deleted (these were the API path):
--   * provider_model_candidates (85 rows) — the reason auto/* routed to
--     anthropic for rank=1 dispatch and hit 401.
--   * task_type_routing rows for provider_slug='anthropic' (33 rows) — the
--     explicit routes that made auto/architecture etc. pick anthropic.
--   * model_profile_candidate_bindings pointing at anthropic candidates.
--   * route_eligibility_states for anthropic candidates (auto-regenerates).
--   * provider_model_cost_rates (API billing, moot now).
--
-- What 180 deleted that this migration RESTORES (the CLI surface):
--   * provider_cli_profiles — the `claude` binary config.
--       Restored WITHOUT api_endpoint / api_key_env_vars (the API path
--       metadata). The CLI only needs binary + flags + auth forwarding.
--       adapter_economics retains only the `cli_llm` bucket.
--   * provider_lane_policy — `{cli_llm}`-only lock.
--   * provider_concurrency — 4-slot cap for CLI jobs.

BEGIN;

-- 1. provider_cli_profiles: restore with API metadata stripped.
INSERT INTO public.provider_cli_profiles (
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
    api_endpoint,
    api_protocol_family,
    api_key_env_vars,
    adapter_economics,
    prompt_mode,
    mcp_config_style,
    mcp_args_template,
    sandbox_env_overrides,
    exclude_from_rotation
) VALUES (
    'anthropic',
    'claude',
    '["-p", "--output-format", "json"]'::jsonb,
    '--model',
    '--system-prompt',
    '--json-schema',
    'json',
    'result',
    '["--dangerously-skip-permissions", "--allow-dangerously-skip-permissions", "--add-dir"]'::jsonb,
    300,
    '[]'::jsonb,
    'active',
    'claude-sonnet-4-6',   -- CLI default; subscription-billed
    NULL,                  -- api_endpoint REMOVED (was api.anthropic.com)
    NULL,                  -- api_protocol_family REMOVED
    '[]'::jsonb,           -- api_key_env_vars REMOVED (was ANTHROPIC_API_KEY)
    '{
      "cli_llm": {
        "billing_mode": "subscription_included",
        "budget_bucket": "anthropic_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": false
      }
    }'::jsonb,             -- llm_task economics bucket REMOVED
    'stdin',
    'claude_mcp_config',
    NULL,
    '{"strip": ["ANTHROPIC_API_KEY"]}'::jsonb,
    false
) ON CONFLICT (provider_slug) DO UPDATE SET
    binary_name           = EXCLUDED.binary_name,
    base_flags            = EXCLUDED.base_flags,
    model_flag            = EXCLUDED.model_flag,
    system_prompt_flag    = EXCLUDED.system_prompt_flag,
    json_schema_flag      = EXCLUDED.json_schema_flag,
    output_format         = EXCLUDED.output_format,
    output_envelope_key   = EXCLUDED.output_envelope_key,
    forbidden_flags       = EXCLUDED.forbidden_flags,
    default_timeout       = EXCLUDED.default_timeout,
    aliases               = EXCLUDED.aliases,
    status                = EXCLUDED.status,
    default_model         = EXCLUDED.default_model,
    api_endpoint          = EXCLUDED.api_endpoint,
    api_protocol_family   = EXCLUDED.api_protocol_family,
    api_key_env_vars      = EXCLUDED.api_key_env_vars,
    adapter_economics     = EXCLUDED.adapter_economics,
    prompt_mode           = EXCLUDED.prompt_mode,
    mcp_config_style      = EXCLUDED.mcp_config_style,
    mcp_args_template     = EXCLUDED.mcp_args_template,
    sandbox_env_overrides = EXCLUDED.sandbox_env_overrides,
    exclude_from_rotation = EXCLUDED.exclude_from_rotation,
    updated_at            = now();

-- 2. provider_lane_policy: restore the cli_llm-only lock. This is the
--    belt-and-suspenders for the API path removal — even if some future
--    code path tries to pick anthropic via llm_task, the admission gate
--    (runtime/routing_economics.admit_adapter_type) refuses.
INSERT INTO public.provider_lane_policy
    (provider_slug, allowed_adapter_types, overridable, decision_ref)
VALUES
    ('anthropic', ARRAY['cli_llm'], false, 'migration:181:anthropic_cli_only_restored')
ON CONFLICT (provider_slug) DO UPDATE SET
    allowed_adapter_types = EXCLUDED.allowed_adapter_types,
    overridable           = EXCLUDED.overridable,
    decision_ref          = EXCLUDED.decision_ref,
    effective_from        = now();

-- 3. provider_concurrency: restore 4-slot cap for CLI jobs.
INSERT INTO public.provider_concurrency
    (provider_slug, max_concurrent, active_slots, cost_weight_default)
VALUES
    ('anthropic', 4, 0, 1.0)
ON CONFLICT (provider_slug) DO UPDATE SET
    max_concurrent      = EXCLUDED.max_concurrent,
    cost_weight_default = EXCLUDED.cost_weight_default,
    updated_at          = now();

-- 4. Architecture decision record.
INSERT INTO public.operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref
) VALUES (
    'operator_decision.anthropic-cli-only-restored.2026-04-20',
    'decision.2026-04-20.anthropic-cli-only-restored',
    'architecture_policy',
    'decided',
    'Anthropic access is CLI-only (subscription, no API key)',
    'Nate has no direct Anthropic API key. The claude binary authenticates via OAuth ' ||
    '(CLAUDE_CODE_OAUTH_TOKEN) under the subscription plan. Direct calls to ' ||
    'api.anthropic.com via ANTHROPIC_API_KEY are forbidden. Migration 181 restores ' ||
    'the CLI profile + lane_policy (cli_llm-only) + concurrency rows that migration ' ||
    '180 over-zealously removed, but strips api_endpoint / api_key_env_vars from the ' ||
    'CLI profile so the DB row itself can never be used to reach the HTTP API. ' ||
    'Runtime code paths that tried to call api.anthropic.com directly ' ||
    '(task_assembler._call_haiku, provider_onboarding anthropic_models_list probe, ' ||
    'docker_runner ANTHROPIC_API_KEY env forwarding, provider_transport builtin ' ||
    'fallback api_endpoint) are removed or neutralized in the same change.',
    'nate',
    'claude_code',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'providers::anthropic'
) ON CONFLICT (decision_key) DO UPDATE SET
    decision_kind       = EXCLUDED.decision_kind,
    decision_status     = EXCLUDED.decision_status,
    title               = EXCLUDED.title,
    rationale           = EXCLUDED.rationale,
    decided_by          = EXCLUDED.decided_by,
    decision_source     = EXCLUDED.decision_source,
    effective_from      = EXCLUDED.effective_from,
    decided_at          = EXCLUDED.decided_at,
    updated_at          = now(),
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref  = EXCLUDED.decision_scope_ref;

COMMIT;

-- Verification (run manually):
--   SELECT provider_slug, api_endpoint, api_key_env_vars FROM provider_cli_profiles WHERE provider_slug='anthropic';
--     -> expect api_endpoint=NULL, api_key_env_vars='[]'
--   SELECT * FROM provider_lane_policy WHERE provider_slug='anthropic';
--     -> expect allowed_adapter_types={cli_llm}, overridable=false
--   SELECT * FROM provider_concurrency WHERE provider_slug='anthropic';
--     -> expect max_concurrent=4
--   SELECT COUNT(*) FROM provider_model_candidates WHERE provider_slug='anthropic';
--     -> expect 0 (intentionally NOT restored; auto/* routes through openrouter)
