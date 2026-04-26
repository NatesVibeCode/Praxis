-- Migration 253: Split synthesis and fork-author into separate task_types.
--
-- Synthesis (the one sequential LLM call) does pill audit + work-volume
-- decomposition — that's reasoning work; route to V4-Pro.
-- Fork-out authoring (N parallel calls) is bounded per-packet work — flash
-- is sufficient and ~13x cheaper.

BEGIN;

-- plan_synthesis: Pro primary, Flash fallback.
INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES
    ('plan_synthesis', 'together', 'deepseek-ai/DeepSeek-V4-Pro',
     TRUE, 1, 'high', 1, 'reasoning', 1,
     'Synthesis = pill audit + work-volume decomposition. Reasoning-heavy; pro primary.',
     'explicit'),
    ('plan_synthesis', 'openrouter', 'deepseek/deepseek-v4-pro',
     TRUE, 2, 'high', 2, 'reasoning', 2,
     'Pro fallback when Together direct rate-limits or fails.',
     'explicit')
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted, rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier, rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source, updated_at = now();

-- plan_fork_author: Flash primary (per-packet work is well-scoped), Pro fallback.
INSERT INTO task_type_routing (
    task_type, provider_slug, model_slug,
    permitted, rank, route_tier, route_tier_rank,
    latency_class, latency_rank, rationale, route_source
) VALUES
    ('plan_fork_author', 'openrouter', 'deepseek/deepseek-v4-flash',
     TRUE, 1, 'medium', 1, 'reasoning', 1,
     'Per-packet authoring is bounded; flash is sufficient and ~13x cheaper than pro.',
     'explicit'),
    ('plan_fork_author', 'together', 'deepseek-ai/DeepSeek-V4-Pro',
     TRUE, 2, 'high', 2, 'reasoning', 2,
     'Pro fallback when flash output fails to validate.',
     'explicit'),
    ('plan_fork_author', 'openrouter', 'deepseek/deepseek-v4-pro',
     TRUE, 3, 'high', 3, 'reasoning', 3,
     'Second pro fallback via OpenRouter pool.',
     'explicit')
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted, rank = EXCLUDED.rank,
    route_tier = EXCLUDED.route_tier, rationale = EXCLUDED.rationale,
    route_source = EXCLUDED.route_source, updated_at = now();

COMMIT;
