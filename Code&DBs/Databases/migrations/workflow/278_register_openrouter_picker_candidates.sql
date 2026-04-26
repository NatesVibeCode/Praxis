-- Migration 278: Register the 10 OpenRouter picker candidates in
-- provider_model_candidates with priority=900 + price cards.
--
-- These rows make the override resolver in
-- runtime/compiler_llm._resolve_provider_for_model find each candidate
-- by model_slug, AND give compose_experiment._project_cost a price card
-- per row so cost_usd projections land on the matrix output.
--
-- Pricing values are pulled from OpenRouter's /v1/models endpoint as of
-- 2026-04-26 and stored as input_per_m_tokens / output_per_m_tokens (USD
-- per million tokens). All rows priority=900 — discoverable but won't
-- disturb existing rank-1 routing. Three rows are tagged 'picker-control'
-- to mark them as included for negative-result anchoring, not as
-- candidates the operator should promote.

BEGIN;

INSERT INTO provider_model_candidates (
    candidate_ref, provider_ref, provider_name, provider_slug,
    model_slug, status, priority, balance_weight,
    capability_tags, default_parameters,
    effective_from, effective_to, decision_ref, created_at,
    route_tier, route_tier_rank, latency_class, latency_rank,
    reasoning_control, task_affinities, benchmark_profile
) VALUES
    -- TARGET PICKS — instruct, non-reasoning
    ('candidate.openrouter.mistral-small-3.2-24b',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'mistralai/mistral-small-3.2-24b-instruct',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose']),
     jsonb_build_object('context_window', 128000,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.075, 'output_per_m_tokens', 0.200, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.qwen3-235b-a22b-2507',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'qwen/qwen3-235b-a22b-2507',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose','moe']),
     jsonb_build_object('context_window', 262144,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.071, 'output_per_m_tokens', 0.100, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.llama-3.3-70b-instruct',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'meta-llama/llama-3.3-70b-instruct',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose']),
     jsonb_build_object('context_window', 131072,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.100, 'output_per_m_tokens', 0.320, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.deepseek-v3.2',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'deepseek/deepseek-v3.2',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose']),
     jsonb_build_object('context_window', 131072,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.252, 'output_per_m_tokens', 0.378, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.mistral-medium-3.1',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'mistralai/mistral-medium-3.1',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose']),
     jsonb_build_object('context_window', 131072,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.400, 'output_per_m_tokens', 2.000, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.gemini-2.5-flash',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'google/gemini-2.5-flash',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose','json-mode']),
     jsonb_build_object('context_window', 1048576,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.300, 'output_per_m_tokens', 2.500, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.qwen3-max',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'qwen/qwen3-max',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-target','instruct','structured-output','compose']),
     jsonb_build_object('context_window', 262144,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.780, 'output_per_m_tokens', 3.900, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    -- NEGATIVE-RESULT CONTROLS
    ('candidate.openrouter.deepseek-v4-flash-picker',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'deepseek/deepseek-v4-flash',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-control','same-architecture-as-broken-default']),
     jsonb_build_object('context_window', 1048576,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.140, 'output_per_m_tokens', 0.280, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'instant', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.deepseek-r1-picker',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'deepseek/deepseek-r1',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-control','reasoning-model','expected-to-fail-this-task']),
     jsonb_build_object('context_window', 64000,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.700, 'output_per_m_tokens', 2.500, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'reasoning', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb),

    ('candidate.openrouter.qwen3-30b-a3b-thinking-2507',
     'provider.openrouter', 'OpenRouter', 'openrouter',
     'qwen/qwen3-30b-a3b-thinking-2507',
     'active', 900, 1,
     to_jsonb(ARRAY['picker-control','thinking-variant','expected-to-fail-this-task']),
     jsonb_build_object('context_window', 131072,
        'api_endpoint', 'https://openrouter.ai/api/v1/chat/completions',
        'api_protocol_family', 'openai_chat_completions',
        'pricing', jsonb_build_object('input_per_m_tokens', 0.080, 'output_per_m_tokens', 0.400, 'source', 'openrouter./v1/models 2026-04-26')),
     now(), NULL, 'decision.2026-04-26.compose-picker-matrix', now(),
     'low', 9, 'reasoning', 9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)

ON CONFLICT (candidate_ref) DO UPDATE SET
    capability_tags = EXCLUDED.capability_tags,
    default_parameters = EXCLUDED.default_parameters,
    decision_ref = EXCLUDED.decision_ref;

COMMIT;
