-- Migration 279: Promote compose_experiment picker-matrix winners to rank-1
-- in task_type_routing.
--
-- Decisions:
--   * plan_synthesis (decomposition) → openrouter/google/gemini-3-flash-preview
--   * plan_fork_author (per-packet authoring) → openrouter/google/gemini-3-flash-preview
--     (same family handles structured packet output too; we'll re-pick this
--     specifically once we have per-packet matrix data)
--   * plan_pill_match (NEW task_type) → openrouter/openai/gpt-5.4-mini
--     with reasoning_control={"effort":"medium"} (the verified discriminating
--     effort level — gpt-5.4-mini @ low/high are less consistent)
--
-- Decision provenance: empirical multi-round picker matrix with comprehensive
-- per-leg telemetry (tokens, wall, finish_reason, validation, pill audit
-- verdicts). Findings logged to operator_ideas.idea.compose-picker.empirical-findings.2026-04-26
-- and ~/.claude/projects/-Users-nate-Praxis/memory/findings_2026-04-26_compose_picker_models.md
--
-- Cross-intent contamination bug found AND FIXED (3 fixes shipped same day):
-- (1) softened synthesis prompt TASK clause to let LLM reject inappropriate
-- suggested_step labels; (2) pgvector semantic filter on suggested_step
-- retrieval (mirrors the pill fix); (3) tightened hardcoded `_suggest_steps`
-- conditions so app-integration-flavored labels only fire for app-integration
-- domain markers + added abstract domain-agnostic suggestions.
--
-- V4-Pro and V4-Flash kept in routing at rank 10+ as fallback (NOT deleted)
-- so they're available if the picker winners regress.

BEGIN;

-- ===================================================================
-- plan_synthesis — promote google/gemini-3-flash-preview to rank-1
-- ===================================================================

-- Demote existing rank-2 (openrouter/deepseek-v4-pro) to rank=10
UPDATE task_type_routing SET rank = 10, updated_at = now()
 WHERE task_type = 'plan_synthesis'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

-- Demote existing rank-5 (together/DeepSeek-V4-Pro) to rank=11
UPDATE task_type_routing SET rank = 11, updated_at = now()
 WHERE task_type = 'plan_synthesis'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

-- Insert/upsert the picker winner at rank=1
INSERT INTO task_type_routing (
    task_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control, route_source, updated_at
) VALUES (
    'plan_synthesis', 'google/gemini-3-flash-preview', 'openrouter', TRUE, 1,
    0.0, 4096, 'high', 1, 'instant', 1,
    '{"source": "decision.2026-04-26.compose-picker-matrix",
      "decision_evidence": "operator_ideas.idea.compose-picker.empirical-findings.2026-04-26"}'::jsonb,
    'explicit', now()
)
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    rank = 1, permitted = TRUE,
    temperature = EXCLUDED.temperature,
    max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    updated_at = now();


-- ===================================================================
-- plan_fork_author — promote google/gemini-3-flash-preview to rank-1
-- ===================================================================

UPDATE task_type_routing SET rank = 10, updated_at = now()
 WHERE task_type = 'plan_fork_author'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

UPDATE task_type_routing SET rank = 11, updated_at = now()
 WHERE task_type = 'plan_fork_author'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

INSERT INTO task_type_routing (
    task_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control, route_source, updated_at
) VALUES (
    'plan_fork_author', 'google/gemini-3-flash-preview', 'openrouter', TRUE, 1,
    0.0, 4096, 'high', 1, 'instant', 1,
    '{"source": "decision.2026-04-26.compose-picker-matrix",
      "note": "per-packet matrix not run yet; promoted from same-family decomposition winner. Re-pick when per-packet data lands."}'::jsonb,
    'explicit', now()
)
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    rank = 1, permitted = TRUE,
    temperature = EXCLUDED.temperature,
    max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    updated_at = now();


-- ===================================================================
-- plan_pill_match — register NEW task_type for pill scope filtering
-- (separates pill matching from compose end-to-end so the runner can
--  use a specialized model + reasoning effort for that skill)
-- ===================================================================

INSERT INTO task_type_routing (
    task_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control, route_source, updated_at
) VALUES (
    'plan_pill_match', 'openai/gpt-5.4-mini', 'openrouter', TRUE, 1,
    0.0, 4096, 'high', 1, 'instant', 1,
    '{"effort": "medium",
      "source": "decision.2026-04-26.compose-picker-matrix",
      "rationale": "5-intent consistency check confirmed gpt-5.4-mini @ medium effort is the only OpenAI mini variant that genuinely discriminates pills (cheaper minis are uniform-positive; high effort breaks parsing). Empirical findings: operator_ideas.idea.compose-picker.empirical-findings.2026-04-26"}'::jsonb,
    'explicit', now()
)
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    rank = 1, permitted = TRUE,
    temperature = EXCLUDED.temperature,
    max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    updated_at = now();


COMMIT;

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug, temperature, max_tokens, reasoning_control
--     FROM task_type_routing
--    WHERE task_type IN ('plan_synthesis', 'plan_fork_author', 'plan_pill_match')
--    ORDER BY task_type, rank;
