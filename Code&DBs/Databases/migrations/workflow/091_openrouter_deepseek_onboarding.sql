-- Migration 091: Onboard OpenRouter provider with DeepSeek models (low tier, research only)
--
-- OpenRouter uses the OpenAI-compatible chat completions protocol.
-- DeepSeek models are onboarded as low-tier, instant-latency candidates
-- permitted exclusively for research task type.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Provider CLI profile
-- -----------------------------------------------------------------------
INSERT INTO provider_cli_profiles (
    provider_slug, binary_name, api_endpoint, api_protocol_family,
    api_key_env_vars, adapter_economics, created_at, updated_at
) VALUES (
    'openrouter',
    'openrouter',
    'https://api.deepseek.com/v1/chat/completions',
    'openai_chat_completions',
    '["DEEPSEEK_API_KEY"]'::jsonb,
    '{
        "cli_llm": {
            "billing_mode": "metered_api",
            "budget_bucket": "openrouter_api_payg",
            "effective_marginal_cost": 0.55
        },
        "llm_task": {
            "billing_mode": "metered_api",
            "budget_bucket": "openrouter_api_payg",
            "effective_marginal_cost": 0.55
        }
    }'::jsonb,
    now(), now()
) ON CONFLICT (provider_slug) DO UPDATE SET
    api_endpoint = EXCLUDED.api_endpoint,
    api_protocol_family = EXCLUDED.api_protocol_family,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    adapter_economics = EXCLUDED.adapter_economics,
    updated_at = now();

-- -----------------------------------------------------------------------
-- 2. Model candidates (low tier, instant latency)
-- -----------------------------------------------------------------------
INSERT INTO provider_model_candidates (
    candidate_ref, provider_ref, provider_name, provider_slug,
    model_slug, status, priority, balance_weight,
    capability_tags, default_parameters,
    effective_from, effective_to, decision_ref, created_at,
    route_tier, route_tier_rank, latency_class, latency_rank,
    reasoning_control, task_affinities, benchmark_profile
) VALUES
(
    'candidate.openrouter.deepseek-r1',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'deepseek/deepseek-r1',
    'active',
    10,     -- low priority
    2,      -- balance weight
    '["economy", "search", "research", "reasoning"]'::jsonb,
    '{
        "model_slug": "deepseek/deepseek-r1",
        "provider_slug": "openrouter"
    }'::jsonb,
    now(), NULL,
    'decision.onboard.openrouter.deepseek.2026-04-11',
    now(),
    'low',          -- route_tier
    3,              -- route_tier_rank (after haiku, flash-lite)
    'instant',      -- latency_class
    3,              -- latency_rank
    '{}'::jsonb,    -- reasoning_control
    '{
        "primary": ["research"],
        "secondary": [],
        "specialized": [],
        "avoid": ["debate", "build", "architecture", "review"]
    }'::jsonb,
    '{
        "evidence_level": "vendor_positioning",
        "positioning": "Low-cost search and research model via OpenRouter.",
        "source_refs": ["openrouter_models"],
        "benchmark_notes": ["Onboarded as economy search model."]
    }'::jsonb
),
(
    'candidate.openrouter.deepseek-v3',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'deepseek/deepseek-chat-v3-0324',
    'active',
    10,
    2,
    '["economy", "search", "research"]'::jsonb,
    '{
        "model_slug": "deepseek/deepseek-chat-v3-0324",
        "provider_slug": "openrouter"
    }'::jsonb,
    now(), NULL,
    'decision.onboard.openrouter.deepseek.2026-04-11',
    now(),
    'low',
    4,
    'instant',
    4,
    '{}'::jsonb,
    '{
        "primary": ["research"],
        "secondary": [],
        "specialized": [],
        "avoid": ["debate", "build", "architecture", "review"]
    }'::jsonb,
    '{
        "evidence_level": "vendor_positioning",
        "positioning": "Low-cost search and research model via OpenRouter.",
        "source_refs": ["openrouter_models"],
        "benchmark_notes": ["Onboarded as economy search model."]
    }'::jsonb
)
ON CONFLICT (candidate_ref) DO UPDATE SET
    status = EXCLUDED.status,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
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
-- R1: permitted for research
('research', 'openrouter', 'deepseek/deepseek-r1',
 true, 8, 'low', 3, 'instant', 3, 0.55,
 'Low-cost DeepSeek R1 for search/research tasks via OpenRouter', '{}'),
-- V3: permitted for research
('research', 'openrouter', 'deepseek/deepseek-chat-v3-0324',
 true, 9, 'low', 4, 'instant', 4, 0.27,
 'Low-cost DeepSeek V3 for search/research tasks via OpenRouter', '{}'),
-- Explicitly block from all other task types
('build',        'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('build',        'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('architecture', 'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('architecture', 'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('debate',       'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('debate',       'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('review',       'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('review',       'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('test',         'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('test',         'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('refactor',     'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('refactor',     'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('wiring',       'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('wiring',       'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('chat',         'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('chat',         'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('planner',      'openrouter', 'deepseek/deepseek-r1',                false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}'),
('planner',      'openrouter', 'deepseek/deepseek-chat-v3-0324',     false, 99, 'low', 99, 'instant', 99, 0, 'Not permitted', '{}')
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    cost_per_m_tokens = EXCLUDED.cost_per_m_tokens,
    rationale = EXCLUDED.rationale;

COMMIT;
