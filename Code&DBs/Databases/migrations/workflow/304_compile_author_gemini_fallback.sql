-- Migration 304: Gemini-3-Flash as compile_author fallback
--
-- Operator direction (2026-04-27, nate): the post-303 smoke surfaced two
-- problems on the compile_author chain — Together V4-Pro timed out at 12s
-- on the longer author prompt, and the V3.2 rank-2 fallback returned
-- "model_not_available" because the Together account does not have V3.2
-- as a serverless model. Add openrouter/google/gemini-3-flash-preview as
-- the actual usable fallback (already admitted as a candidate via
-- migration 283 — same model that wins compile_synthesize).
--
-- Changes:
--   * Insert openrouter/google/gemini-3-flash-preview at rank 2 for
--     compile_author (transport=API).
--   * Demote together/deepseek-ai/DeepSeek-V3.2 to rank 5 so a broken
--     route doesn't sit between V4-Pro and Gemini in the failover walk.
--   * Allowlist + catalog refresh for both runtime profiles.
--
-- decision_ref: decision.2026-04-27.compile-author-gemini-fallback

BEGIN;

-- Demote the broken Together V3.2 row out of the active fallback path.
UPDATE task_type_routing
   SET rank = 5,
       rationale = 'Demoted by migration 304: Together account does not have V3.2 as a serverless model — kept as deep fallback only.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

-- Insert Gemini-3-Flash as the new rank-2 fallback for compile_author.
INSERT INTO task_type_routing (
    task_type, transport_type, provider_slug, model_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES (
    'compile_author', 'API', 'openrouter', 'google/gemini-3-flash-preview', TRUE, 2,
    0.0, 4096, 'high', 2, 'instant', 2,
    '{"source": "decision.2026-04-27.compile-author-gemini-fallback",
      "rationale": "Replaces broken Together V3.2 fallback. Same model that wins compile_synthesize — proven on structured-output composition."}'::jsonb,
    'explicit',
    'Compile author stage fallback: Gemini-3-Flash via OpenRouter when Together V4-Pro is unavailable.',
    now()
)
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();

-- Allowlist the (compile_author, openrouter/gemini) admission for both profiles.
INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    ('praxis',        'compile_author', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compile_author_gemini_fallback.2026_04_27',
     'decision.2026-04-27.compile-author-gemini-fallback'),
    ('scratch_agent', 'compile_author', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compile_author_gemini_fallback.2026_04_27',
     'decision.2026-04-27.compile-author-gemini-fallback')
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed      = EXCLUDED.allowed,
    reason_code  = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = NOW();

-- Refresh catalog + snapshot.
SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

COMMIT;
