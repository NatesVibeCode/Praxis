-- Migration 303: Split compile into three independently-routed sub-tasks
--
-- Operator direction (2026-04-27, nate): mirror the compose split pattern
-- (plan_synthesis / plan_fork_author / plan_pill_match) for compile, so each
-- compile sub-stage can be routed to the model that won its concern. Today
-- compile is one monolithic LLM call against task_type='compile' (Together
-- V4-Pro per migration 262), which means the GPT-5.4-mini-medium pill-match
-- winner from the 2026-04-26 compose picker matrix can't be applied to
-- compile's matching work.
--
-- Sub-task decomposition + initial winners:
--   compile_synthesize → openrouter/google/gemini-3-flash-preview (synthesis
--     winner from compose picker matrix; fast structured-output decomposition)
--   compile_pill_match → openrouter/openai/gpt-5.4-mini @ medium (pill-match
--     winner from compose picker matrix; only OpenAI mini variant that
--     genuinely discriminates pills)
--   compile_author → together/deepseek-ai/DeepSeek-V4-Pro (current monolithic
--     compile primary; prose composition + structured JSON output is its
--     strength) + together/deepseek-ai/DeepSeek-V3.2 fallback at rank 2
--
-- Candidates already admitted by prior migrations:
--   - openrouter/google/gemini-3-flash-preview by migration 283
--   - openrouter/openai/gpt-5.4-mini by migration 283
--   - together/deepseek-ai/DeepSeek-V4-Pro by migration 262
--   - together/deepseek-ai/DeepSeek-V3.2 by migration 262
--
-- This migration only needs to:
--   (1) Insert task_type_routing rows for the three new task_types
--   (2) Allowlist them in private_provider_api_job_allowlist for both
--       active runtime profiles (praxis + scratch_agent)
--   (3) Refresh the job catalog + snapshot so the resolver picks them up
--
-- The legacy task_type='compile' rows are LEFT IN PLACE so any external
-- caller still resolving the old task_type continues to work as the
-- compile_author back-compat shim.
--
-- decision_ref: decision.2026-04-27.compile-sub-task-routing-split

BEGIN;

-- ===================================================================
-- 1. task_type_routing rows for the three new sub-tasks
-- ===================================================================

-- compile_synthesize: gemini-3-flash-preview rank 1, deepseek-v4-pro fallback rank 2
INSERT INTO task_type_routing (
    task_type, transport_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES
    (
        'compile_synthesize', 'API', 'google/gemini-3-flash-preview', 'openrouter', TRUE, 1,
        0.0, 4096, 'high', 1, 'instant', 1,
        '{"source": "decision.2026-04-27.compile-sub-task-routing-split",
          "decision_evidence": "Reuses the compose synthesis winner from the 2026-04-26 picker matrix — same shape of work (parse prose → structured skeleton)."}'::jsonb,
        'explicit',
        'Compile synthesize stage: parse operator prose into title + structural skeleton with un-resolved placeholders.',
        now()
    ),
    (
        'compile_synthesize', 'API', 'deepseek/deepseek-v4-pro', 'openrouter', TRUE, 2,
        0.0, 4096, 'high', 2, 'reasoning', 2,
        '{"source": "decision.2026-04-27.compile-sub-task-routing-split",
          "note": "Fallback within OpenRouter lane if Gemini is unhealthy."}'::jsonb,
        'explicit',
        'Compile synthesize stage fallback.',
        now()
    )
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();

-- compile_pill_match: gpt-5.4-mini @ medium rank 1
INSERT INTO task_type_routing (
    task_type, transport_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES
    (
        'compile_pill_match', 'API', 'openai/gpt-5.4-mini', 'openrouter', TRUE, 1,
        0.0, 4096, 'high', 1, 'instant', 1,
        '{"effort": "medium",
          "source": "decision.2026-04-27.compile-sub-task-routing-split",
          "rationale": "Reuses the plan_pill_match winner from the 2026-04-26 compose picker matrix. GPT-5.4-mini @ medium is the only OpenAI mini variant that genuinely discriminates pills (cheaper minis are uniform-positive; high effort breaks parsing). Compile pill matching is the same shape of work as compose pill matching — resolve raw token → typed catalog entry."}'::jsonb,
        'explicit',
        'Compile pill match stage: resolve raw @integration/action and #type/field placeholders against the catalog.',
        now()
    )
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();

-- compile_author: together/deepseek-v4-pro rank 1, deepseek-v3.2 rank 2 (mirrors current compile primary)
INSERT INTO task_type_routing (
    task_type, transport_type, model_slug, provider_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES
    (
        'compile_author', 'API', 'deepseek-ai/DeepSeek-V4-Pro', 'together', TRUE, 1,
        0.0, 4096, 'high', 1, 'reasoning', 1,
        '{"source": "decision.2026-04-27.compile-sub-task-routing-split",
          "carried_from": "decision.2026-04-26.together-compile-primary-api-exception",
          "rationale": "Author stage inherits the current monolithic compile primary — Together V4-Pro is proven on prose-with-bound-references composition."}'::jsonb,
        'explicit',
        'Compile author stage: weave bound references into final compiled prose with authority + SLA + capabilities.',
        now()
    ),
    (
        'compile_author', 'API', 'deepseek-ai/DeepSeek-V3.2', 'together', TRUE, 2,
        0.0, 4096, 'high', 2, 'reasoning', 2,
        '{"source": "decision.2026-04-27.compile-sub-task-routing-split",
          "note": "Same Together lane fallback as the current compile primary."}'::jsonb,
        'explicit',
        'Compile author stage fallback within the same Together direct API lane.',
        now()
    )
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();


-- ===================================================================
-- 2. private_provider_api_job_allowlist for both runtime profiles
--    (matches the migration 282 pattern — without these rows the
--    resolver silently drops the routing rows at the matrix JOIN)
-- ===================================================================

INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    -- compile_synthesize
    ('praxis',        'compile_synthesize', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('scratch_agent', 'compile_synthesize', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('praxis',        'compile_synthesize', 'llm_task',
     'openrouter',    'deepseek/deepseek-v4-pro',      TRUE,
     'compile_sub_task_split.2026_04_27.fallback',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('scratch_agent', 'compile_synthesize', 'llm_task',
     'openrouter',    'deepseek/deepseek-v4-pro',      TRUE,
     'compile_sub_task_split.2026_04_27.fallback',
     'decision.2026-04-27.compile-sub-task-routing-split'),

    -- compile_pill_match
    ('praxis',        'compile_pill_match', 'llm_task',
     'openrouter',    'openai/gpt-5.4-mini',           TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('scratch_agent', 'compile_pill_match', 'llm_task',
     'openrouter',    'openai/gpt-5.4-mini',           TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),

    -- compile_author
    ('praxis',        'compile_author',     'llm_task',
     'together',      'deepseek-ai/DeepSeek-V4-Pro',   TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('scratch_agent', 'compile_author',     'llm_task',
     'together',      'deepseek-ai/DeepSeek-V4-Pro',   TRUE,
     'compile_sub_task_split.2026_04_27',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('praxis',        'compile_author',     'llm_task',
     'together',      'deepseek-ai/DeepSeek-V3.2',     TRUE,
     'compile_sub_task_split.2026_04_27.fallback',
     'decision.2026-04-27.compile-sub-task-routing-split'),
    ('scratch_agent', 'compile_author',     'llm_task',
     'together',      'deepseek-ai/DeepSeek-V3.2',     TRUE,
     'compile_sub_task_split.2026_04_27.fallback',
     'decision.2026-04-27.compile-sub-task-routing-split')
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed      = EXCLUDED.allowed,
    reason_code  = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = NOW();


-- ===================================================================
-- 3. Refresh the catalog + snapshot so the resolver picks up the new
--    task_types immediately (matches migration 283 pattern)
-- ===================================================================

SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

COMMIT;

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug, temperature, max_tokens, reasoning_control
--     FROM task_type_routing
--    WHERE task_type IN ('compile_synthesize', 'compile_pill_match', 'compile_author')
--    ORDER BY task_type, rank;
--
--   SELECT * FROM task_type_routing_admission_audit
--    WHERE task_type IN ('compile_synthesize', 'compile_pill_match', 'compile_author')
--      AND admission_status <> 'admitted';
--   -- Expect zero rows.
