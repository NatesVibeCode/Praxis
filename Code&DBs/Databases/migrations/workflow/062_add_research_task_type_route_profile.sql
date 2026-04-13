INSERT INTO public.task_type_route_profiles (
    task_type,
    affinity_labels,
    affinity_weights,
    task_rank_weights,
    benchmark_metric_weights,
    route_tier_preferences,
    latency_class_preferences,
    allow_unclassified_candidates,
    rationale,
    created_at,
    updated_at
)
SELECT
    'research' AS task_type,
    jsonb_build_object(
        'avoid', architecture.affinity_labels -> 'avoid',
        'primary', '["research", "analysis", "architecture", "reasoning", "long-horizon"]'::jsonb,
        'fallback', '["review", "debug"]'::jsonb,
        'secondary', '["chat", "build", "general-agentic", "multimodal"]'::jsonb,
        'specialized', '[]'::jsonb
    ) AS affinity_labels,
    architecture.affinity_weights,
    architecture.task_rank_weights,
    architecture.benchmark_metric_weights,
    architecture.route_tier_preferences,
    architecture.latency_class_preferences,
    architecture.allow_unclassified_candidates,
    'Research and documentation work should prefer reasoning-capable models with explicit research or analysis affinity, while still allowing strong generalist fallbacks when live routing authority stays healthy.' AS rationale,
    now() AS created_at,
    now() AS updated_at
FROM public.task_type_route_profiles AS architecture
WHERE architecture.task_type = 'architecture'
ON CONFLICT (task_type) DO UPDATE
SET affinity_labels = EXCLUDED.affinity_labels,
    affinity_weights = EXCLUDED.affinity_weights,
    task_rank_weights = EXCLUDED.task_rank_weights,
    benchmark_metric_weights = EXCLUDED.benchmark_metric_weights,
    route_tier_preferences = EXCLUDED.route_tier_preferences,
    latency_class_preferences = EXCLUDED.latency_class_preferences,
    allow_unclassified_candidates = EXCLUDED.allow_unclassified_candidates,
    rationale = EXCLUDED.rationale,
    updated_at = now();
