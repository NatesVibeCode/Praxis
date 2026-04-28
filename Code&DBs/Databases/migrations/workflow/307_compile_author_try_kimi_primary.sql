-- Migration 307: Promote Kimi K2.6 to compile_author primary.
--
-- Operator direction (2026-04-28, nate): "No try kimi : )"
--
-- Rationale:
--   Migration 306 cascade-flipped compile_author from V4-Pro (slow on
--   Together's serverless tier) to Gemini-3-Flash (proven 1.48s wall).
--   But Gemini-Flash has shown subtle quality losses on compile_author
--   (unsafe_step_collapse rejections in earlier smokes) and the operator
--   correctly flagged that "speed is not everything" — quality matters
--   for the build artifact.
--
--   Kimi K2.6 is the candidate middle path:
--     - Speed: 3.68s on a classification-shaped probe via OpenRouter
--       (much faster than V4-Pro's 30-50s, comparable to Gemini-Flash)
--     - Quality: II 54, MoE 1T/32B-active, #1 OpenRouter usage by token
--       volume — strongest open-weight model in production right now
--     - Not yet validated on compile_author prose-composition shape;
--       this migration is the validation experiment
--
--   Routing chain after this migration:
--     rank 1: openrouter/moonshotai/kimi-k2.6           (NEW primary)
--     rank 2: openrouter/google/gemini-3-flash-preview  (demoted from 1; still fast fallback)
--     rank 5: together/deepseek-ai/DeepSeek-V4-Pro      (deep fallback, slow)
--     rank 5: together/deepseek-ai/DeepSeek-V3.2        (deep fallback, broken)
--
--   If Kimi degrades quality (safety guard rejection rate climbs, or
--   downstream binding resolution accuracy drops), revert with one row
--   flip back to Gemini-Flash rank 1.
--
-- decision_ref: decision.2026-04-28.compile-author-try-kimi-primary

BEGIN;

-- Demote Gemini-Flash to rank 2.
UPDATE task_type_routing
   SET rank = 2,
       rationale = 'Demoted by migration 307: Kimi K2.6 promoted to primary on quality+speed grounds. Gemini stays as proven fast fallback.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'google/gemini-3-flash-preview';

-- Promote Kimi K2.6 to rank 1 (insert if missing, update if present).
INSERT INTO task_type_routing (
    task_type, transport_type, provider_slug, model_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES (
    'compile_author', 'API', 'openrouter', 'moonshotai/kimi-k2.6', TRUE, 1,
    0.0, 4096, 'high', 1, 'instant', 1,
    '{"source": "decision.2026-04-28.compile-author-try-kimi-primary",
      "rationale": "MoE 1T/32B-active, II 54, #1 OpenRouter usage by token volume — best open-weight balance of speed + quality. Validation experiment for compile_author prose composition."}'::jsonb,
    'explicit',
    'Compile author primary: Kimi K2.6 — best open-weight speed/quality balance, validation experiment.',
    now()
)
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)
DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();

-- Allowlist Kimi for compile_author (currently only allowlisted for compile_finalize).
INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    ('praxis',        'compile_author', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_author.try_kimi.2026_04_28', 'decision.2026-04-28.compile-author-try-kimi-primary'),
    ('scratch_agent', 'compile_author', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_author.try_kimi.2026_04_28', 'decision.2026-04-28.compile-author-try-kimi-primary')
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed      = EXCLUDED.allowed,
    reason_code  = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = NOW();

-- Refresh.
SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

COMMIT;
