-- Migration 101: analysis route authority for classify / triage work
--
-- Promotes auto/classify from a support alias into a first-class analysis
-- lane backed by route profiles and explicit model routing authority.

BEGIN;

INSERT INTO task_type_route_profiles (
    task_type,
    affinity_labels,
    affinity_weights,
    task_rank_weights,
    benchmark_metric_weights,
    route_tier_preferences,
    latency_class_preferences,
    allow_unclassified_candidates,
    rationale
) VALUES (
    'analysis',
    '{"primary":["analysis","quick-analysis","score","triage","categorize"],"secondary":["review","research","chat"],"specialized":[],"fallback":["build"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
    '{"primary":1.0,"secondary":0.72,"specialized":0.45,"fallback":0.30,"unclassified":0.20,"avoid":0.0}'::jsonb,
    '{"affinity":0.58,"route_tier":0.24,"latency":0.18}'::jsonb,
    '{"artificial_analysis_intelligence_index":0.50,"artificial_analysis_math_index":0.20,"artificial_analysis_coding_index":0.15,"median_output_tokens_per_second":0.10,"price_1m_blended_3_to_1":0.05}'::jsonb,
    '["medium","high","low"]'::jsonb,
    '["instant","reasoning"]'::jsonb,
    true,
    'Analysis and classification work should prefer quick, grounded evaluators first, while still allowing stronger reasoning models when live route health or model quality justifies the spend.'
) ON CONFLICT (task_type) DO UPDATE SET
    affinity_labels = EXCLUDED.affinity_labels,
    affinity_weights = EXCLUDED.affinity_weights,
    task_rank_weights = EXCLUDED.task_rank_weights,
    benchmark_metric_weights = EXCLUDED.benchmark_metric_weights,
    route_tier_preferences = EXCLUDED.route_tier_preferences,
    latency_class_preferences = EXCLUDED.latency_class_preferences,
    allow_unclassified_candidates = EXCLUDED.allow_unclassified_candidates,
    rationale = EXCLUDED.rationale,
    updated_at = now();

DELETE FROM task_type_routing
WHERE task_type = 'analysis';

INSERT INTO task_type_routing (
    task_type, model_slug, provider_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank,
    reasoning_control, route_health_score,
    observed_completed_count, observed_execution_failure_count,
    observed_external_failure_count, observed_config_failure_count,
    observed_downstream_failure_count, observed_downstream_bug_count,
    consecutive_internal_failures, last_failure_category,
    last_failure_zone, route_source, recent_successes, recent_failures
) VALUES
    (
        'analysis', 'gpt-5.4', 'openai',
        true, 1, 'high', 1,
        'reasoning', 1,
        '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.72,
        0,0,0,0,0,0,0, '', '', 'explicit', 0,0
    ),
    (
        'analysis', 'gemini-3.1-pro-preview', 'google',
        true, 2, 'high', 2,
        'reasoning', 2,
        '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.68,
        0,0,0,0,0,0,0, '', '', 'explicit', 0,0
    ),
    (
        'analysis', 'claude-opus-4-6', 'anthropic',
        true, 3, 'high', 3,
        'reasoning', 3,
        '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.66,
        0,0,0,0,0,0,0, '', '', 'explicit', 0,0
    ),
    (
        'analysis', 'claude-sonnet-4-6', 'anthropic',
        true, 4, 'medium', 4,
        'reasoning', 4,
        '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.64,
        0,0,0,0,0,0,0, '', '', 'explicit', 0,0
    )
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    route_health_score = EXCLUDED.route_health_score,
    route_source = EXCLUDED.route_source,
    updated_at = now();

COMMIT;
