-- Migration 093: Onboard DeepSeek as a direct API provider with R3 model
--
-- DeepSeek API uses the OpenAI chat completions protocol.
-- R3 is onboarded as a low-tier research model via direct API (not OpenRouter).

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Provider CLI profile
-- -----------------------------------------------------------------------
INSERT INTO provider_cli_profiles (
    provider_slug, binary_name, default_model,
    api_endpoint, api_protocol_family,
    api_key_env_vars, adapter_economics, created_at, updated_at
) VALUES (
    'deepseek',
    'deepseek',
    'deepseek-r3',
    'https://api.deepseek.com/v1/chat/completions',
    'openai_chat_completions',
    '["DEEPSEEK_API_KEY"]'::jsonb,
    '{
        "cli_llm": {
            "billing_mode": "metered_api",
            "budget_bucket": "deepseek_api_payg",
            "effective_marginal_cost": 0.55
        },
        "llm_task": {
            "billing_mode": "metered_api",
            "budget_bucket": "deepseek_api_payg",
            "effective_marginal_cost": 0.55
        }
    }'::jsonb,
    now(), now()
) ON CONFLICT (provider_slug) DO UPDATE SET
    default_model = EXCLUDED.default_model,
    api_endpoint = EXCLUDED.api_endpoint,
    api_protocol_family = EXCLUDED.api_protocol_family,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    adapter_economics = EXCLUDED.adapter_economics,
    updated_at = now();

-- -----------------------------------------------------------------------
-- 2. Model candidate: deepseek-r3 (low tier, research)
-- -----------------------------------------------------------------------
INSERT INTO provider_model_candidates (
    candidate_ref, provider_ref, provider_name, provider_slug,
    model_slug, status, priority, balance_weight,
    capability_tags, default_parameters,
    effective_from, effective_to, decision_ref, created_at,
    route_tier, route_tier_rank, latency_class, latency_rank,
    reasoning_control, task_affinities, benchmark_profile
) VALUES (
    'candidate.deepseek.deepseek-r3',
    'provider.deepseek',
    'DeepSeek',
    'deepseek',
    'deepseek-r3',
    'active',
    10,
    2,
    '["economy", "search", "research", "reasoning"]'::jsonb,
    '{
        "model_slug": "deepseek-r3",
        "provider_slug": "deepseek"
    }'::jsonb,
    now(), NULL,
    'decision.onboard.deepseek.r3.2026-04-11',
    now(),
    'low',
    3,
    'instant',
    3,
    '{}'::jsonb,
    '{
        "primary": ["research"],
        "secondary": [],
        "specialized": [],
        "avoid": ["debate", "build", "architecture", "review", "general-routing"]
    }'::jsonb,
    '{
        "evidence_level": "vendor_positioning",
        "positioning": "Low-cost research model via direct DeepSeek API.",
        "source_refs": ["deepseek_api_docs"],
        "benchmark_notes": ["Onboarded as economy research model."]
    }'::jsonb
) ON CONFLICT (candidate_ref) DO UPDATE SET
    status = EXCLUDED.status,
    model_slug = EXCLUDED.model_slug,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    task_affinities = EXCLUDED.task_affinities,
    capability_tags = EXCLUDED.capability_tags,
    benchmark_profile = EXCLUDED.benchmark_profile;

-- -----------------------------------------------------------------------
-- 3. Task type routing — permitted for research only
-- -----------------------------------------------------------------------
INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank,
    cost_per_m_tokens, rationale, reasoning_control
) VALUES
('research', 'deepseek', 'deepseek-r3',
 true, 5, 'low', 3, 'instant', 3, 0.55,
 'Low-cost DeepSeek R3 for research tasks via direct API', '{}'),
('build',        'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('architecture', 'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('debate',       'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('review',       'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('test',         'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('refactor',     'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('wiring',       'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('chat',         'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}'),
('planner',      'deepseek', 'deepseek-r3', false, 99, 'low', 99, 'instant', 99, 0, 'Research only', '{}')
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    cost_per_m_tokens = EXCLUDED.cost_per_m_tokens,
    rationale = EXCLUDED.rationale;

-- -----------------------------------------------------------------------
-- 4. Model profile (context window authority for AgentRegistry)
-- -----------------------------------------------------------------------
INSERT INTO model_profiles (
    model_profile_id, profile_name, provider_name, model_name,
    schema_version, status, budget_policy, routing_policy,
    default_parameters, effective_from, created_at
) VALUES (
    'model_profile.deepseek.r3',
    'DeepSeek R3',
    'deepseek',
    'deepseek-r3',
    1,
    'active',
    '{}'::jsonb,
    '{}'::jsonb,
    '{"context_window": 128000, "max_output_tokens": 8192}'::jsonb,
    now(),
    now()
) ON CONFLICT (model_profile_id) DO UPDATE SET
    status = EXCLUDED.status,
    default_parameters = EXCLUDED.default_parameters;

COMMIT;
