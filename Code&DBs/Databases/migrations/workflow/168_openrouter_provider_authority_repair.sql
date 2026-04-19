-- Migration 168: Repair OpenRouter provider authority
--
-- Migration 091 incorrectly seeded provider_slug=openrouter with the direct
-- DeepSeek endpoint and credential source. OpenRouter is a broker/provider in
-- its own right: it uses the OpenAI-compatible OpenRouter API and its own key.

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
    api_endpoint,
    api_protocol_family,
    api_key_env_vars,
    adapter_economics,
    prompt_mode,
    created_at,
    updated_at
) VALUES (
    'openrouter',
    'openrouter-api',
    '[]'::jsonb,
    NULL,
    NULL,
    NULL,
    'json',
    'choices',
    '[]'::jsonb,
    900,
    '[]'::jsonb,
    'active',
    'openrouter/auto',
    'https://openrouter.ai/api/v1/chat/completions',
    'openai_chat_completions',
    '["OPENROUTER_API_KEY"]'::jsonb,
    '{
      "llm_task": {
        "billing_mode": "metered_api",
        "budget_bucket": "openrouter_api_payg",
        "effective_marginal_cost": 1.0,
        "pricing_model": "varies_by_routed_model",
        "prefer_prepaid": false,
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
    api_endpoint = EXCLUDED.api_endpoint,
    api_protocol_family = EXCLUDED.api_protocol_family,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    adapter_economics = EXCLUDED.adapter_economics,
    prompt_mode = EXCLUDED.prompt_mode,
    updated_at = now();

UPDATE provider_transport_admissions
   SET admitted_by_policy = false,
       policy_reason = 'OpenRouter has no authoritative local CLI lane; use the OpenRouter HTTP API lane.',
       decision_ref = 'migration.168.openrouter_provider_authority_repair',
       status = 'inactive',
       updated_at = now()
 WHERE provider_slug = 'openrouter'
   AND adapter_type = 'cli_llm';

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
    'provider_transport_admission.openrouter.llm_task',
    'openrouter',
    'llm_task',
    'http',
    'direct_http',
    true,
    'Admitted OpenRouter through its OpenAI-compatible HTTP API; DeepSeek direct API credentials belong only to provider_slug=deepseek.',
    'openrouter:llm_task',
    '{
      "authentication": "https://openrouter.ai/docs/api-reference/authentication",
      "chat_completion": "https://openrouter.ai/docs/api-reference/chat-completion",
      "models": "https://openrouter.ai/docs/api-reference/list-available-models",
      "auto_router": "https://openrouter.ai/docs/guides/routing/auto-model-selection"
    }'::jsonb,
    '["OPENROUTER_API_KEY"]'::jsonb,
    '{
      "api_endpoint": "https://openrouter.ai/api/v1/chat/completions",
      "api_protocol_family": "openai_chat_completions",
      "api_key_env_var": "OPENROUTER_API_KEY",
      "default_model": "openrouter/auto",
      "prompt_probe": {
        "strategy": "openai_chat_completion_auth_probe",
        "status": "seeded"
      },
      "model_discovery_probe": {
        "strategy": "openrouter_models_list",
        "status": "seeded"
      }
    }'::jsonb,
    'migration.168.openrouter_provider_authority_repair',
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

INSERT INTO provider_lane_policy
    (provider_slug, allowed_adapter_types, overridable, decision_ref, effective_from)
VALUES
    ('openrouter', ARRAY['llm_task'], false, 'migration:168:openrouter_api_only', now())
ON CONFLICT (provider_slug) DO UPDATE SET
    allowed_adapter_types = EXCLUDED.allowed_adapter_types,
    overridable = EXCLUDED.overridable,
    decision_ref = EXCLUDED.decision_ref,
    effective_from = EXCLUDED.effective_from;

INSERT INTO provider_model_candidates (
    candidate_ref,
    provider_ref,
    provider_name,
    provider_slug,
    model_slug,
    status,
    priority,
    balance_weight,
    capability_tags,
    default_parameters,
    effective_from,
    effective_to,
    decision_ref,
    created_at,
    cli_config,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    task_affinities,
    benchmark_profile
) VALUES (
    'candidate.openrouter.auto',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'openrouter/auto',
    'active',
    20,
    1,
    '["broker", "fallback", "model-router"]'::jsonb,
    '{
      "provider_slug": "openrouter",
      "model_slug": "openrouter/auto",
      "selected_transport": "api",
      "api_protocol_family": "openai_chat_completions",
      "context_window": 2000000,
      "pricing_model": "varies_by_routed_model",
      "catalog_source": "migration.168"
    }'::jsonb,
    now(),
    NULL,
    'migration.168.openrouter_provider_authority_repair',
    now(),
    '{}'::jsonb,
    'low',
    9,
    'instant',
    9,
    '{}'::jsonb,
    '{
      "primary": ["research"],
      "secondary": ["chat"],
      "specialized": ["brokered-routing"],
      "avoid": ["build", "architecture", "review", "test", "refactor"]
    }'::jsonb,
    '{
      "evidence_level": "vendor_positioning",
      "positioning": "OpenRouter auto router for explicit brokered API use.",
      "source_refs": ["openrouter_auto_router", "openrouter_chat_completion_api"]
    }'::jsonb
) ON CONFLICT (candidate_ref) DO UPDATE SET
    provider_ref = EXCLUDED.provider_ref,
    provider_name = EXCLUDED.provider_name,
    provider_slug = EXCLUDED.provider_slug,
    model_slug = EXCLUDED.model_slug,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    balance_weight = EXCLUDED.balance_weight,
    capability_tags = EXCLUDED.capability_tags,
    default_parameters = EXCLUDED.default_parameters,
    effective_to = EXCLUDED.effective_to,
    decision_ref = EXCLUDED.decision_ref,
    cli_config = EXCLUDED.cli_config,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    task_affinities = EXCLUDED.task_affinities,
    benchmark_profile = EXCLUDED.benchmark_profile;

UPDATE provider_model_candidates
   SET default_parameters = COALESCE(default_parameters, '{}'::jsonb)
        || '{
             "selected_transport": "api",
             "api_provider": "openrouter",
             "api_protocol_family": "openai_chat_completions"
           }'::jsonb,
       decision_ref = CASE
           WHEN decision_ref = 'decision.onboard.openrouter.deepseek.2026-04-11'
           THEN 'migration.168.openrouter_provider_authority_repair'
           ELSE decision_ref
       END
 WHERE provider_slug = 'openrouter';

COMMIT;
