-- Migration 249: Register the plan_section_author task type and route it to
-- DeepSeek v4 pro as the primary, with v4-flash as the fallback.
--
-- Operator standing order: accuracy is more important than speed for the
-- per-section authoring step. The compile task_type fronts UI compile,
-- where flash is acceptable; section authoring fills every menu-level
-- field of one PlanPacket against the data-dictionary sandbox, so a
-- higher-tier model is preferred and the route_tier='high' rows lead.
--
-- Behaviour: runtime.plan_section_author resolves routes via the new
-- ``plan_section_author`` task type. Order of preference is:
--
--   1. openrouter / deepseek/deepseek-v4-pro   (high tier — primary)
--   2. openrouter / deepseek/deepseek-v4-flash (medium tier — fallback)
--
-- Provider-routing standing orders still apply: this is API via
-- OpenRouter (the documented exception for UI compile), CLI is the
-- default everywhere else, and Anthropic/direct-Gemini are not in this
-- route list.

BEGIN;

INSERT INTO task_type_routing (
    task_type,
    provider_slug,
    model_slug,
    permitted,
    rank,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    rationale,
    route_source
) VALUES
    (
        'plan_section_author',
        'openrouter',
        'deepseek/deepseek-v4-pro',
        TRUE,
        1,
        'high',
        1,
        'reasoning',
        1,
        'Per-section LLM author — accuracy preferred over speed; v4-pro fronts the route list.',
        'explicit'
    ),
    (
        'plan_section_author',
        'openrouter',
        'deepseek/deepseek-v4-flash',
        TRUE,
        2,
        'medium',
        2,
        'reasoning',
        2,
        'Fallback when v4-pro is unavailable. Same provider lane (OpenRouter), faster but lower-accuracy variant.',
        'explicit'
    )
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT task_type, provider_slug, model_slug, rank, route_tier, permitted
--   FROM task_type_routing WHERE task_type='plan_section_author'
--   ORDER BY rank;
