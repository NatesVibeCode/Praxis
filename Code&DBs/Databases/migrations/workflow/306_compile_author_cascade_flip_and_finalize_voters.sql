-- Migration 306: Two acts on the 2026-04-28 findings.
--
-- Act 1 (cascade-flip on compile_author):
--   The current rank-1 (together/DeepSeek-V4-Pro) reliably times out on
--   compile-author-sized prompts because Together's serverless V4-Pro has
--   variable cold-start latency for reasoning models. Even when the
--   downstream LLMRequest retries 3× before giving up, the ~45s wall is
--   wasted before the rank-2 fallback (openrouter/gemini-3-flash-preview)
--   succeeds in ~4s. Flipping the cascade — Gemini-Flash rank 1 (proven
--   fast on prose composition), V4-Pro demoted to rank 5 — drops happy-path
--   compile latency from ~50s to ~4s. V4-Pro stays in the chain as a deep
--   fallback if Gemini fails.
--
-- Act 2 (register compile_finalize task_type with voting-pool admissions):
--   Compile currently stops at build_state='blocked' because binding gates
--   require human approval to flip from suggested/captured → accepted.
--   That violates the autonomous-first standing order. compile_finalize is
--   a new sub-task that takes the binding ledger, decides accept/reject per
--   gate, and seals a runnable spec. Voting-shaped (per-binding decision is
--   classification → vote across uncorrelated cheap models).
--
--   Voter pool admitted here (the picker resolves top-K at call time, NOT
--   pinned by rank in the migration — dogfood-correct):
--     - openrouter/google/gemini-3-flash-preview   (3.7s classification)
--     - openrouter/openai/gpt-5.4-mini             (proven pill_match winner)
--     - openrouter/x-ai/grok-4.1-fast              (133 t/s)
--     - openrouter/qwen/qwen3.6-plus               (1M ctx, mid-tier)
--     - openrouter/moonshotai/kimi-k2.6            (3.7s on classification, hot)
--
--   Note: Kimi K2.6 is admitted here for compile_finalize + (separately, in
--   future migration) compile_pill_match. NOT on compile_author — Kimi is
--   classification-fast, not yet validated on prose composition.
--
-- decision_ref: decision.2026-04-28.compile-author-cascade-and-finalize-voters

BEGIN;

-- ============================================================
-- Act 1: cascade-flip compile_author (Gemini-Flash to rank 1)
-- ============================================================

UPDATE task_type_routing
   SET rank = 5,
       rationale = 'Demoted by migration 306: Together V4-Pro times out reliably on compile-author prompts (Together serverless reasoning model latency). Kept as deep fallback only.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V4-Pro';

UPDATE task_type_routing
   SET rank = 1,
       rationale = 'Promoted to rank 1 by migration 306: proven ~4s wall on compile-author prompts. V4-Pro demoted to deep fallback because of timeout cascade.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'google/gemini-3-flash-preview';


-- ============================================================
-- Act 2: register compile_finalize task_type
-- ============================================================

INSERT INTO task_type_route_profiles (
    task_type, affinity_labels, affinity_weights, benchmark_metric_weights
) VALUES (
    'compile_finalize',
    '{"primary": ["classification","decision-making","json-mode","structured-output","reasoning"],
      "secondary": ["instruction-following","tool-use"],
      "specialized": ["binding-resolution","gate-decision"],
      "fallback": [],
      "avoid": ["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
    '{"classification": 1.0, "structured-output": 1.0, "json-mode": 0.8}'::jsonb,
    '{"artificial_analysis_intelligence_index": 0.30,
      "artificial_analysis_coding_index": 0.30,
      "median_output_tokens_per_second": 0.20,
      "price_1m_blended_3_to_1": 0.20}'::jsonb
)
ON CONFLICT (task_type) DO UPDATE SET
    affinity_labels = EXCLUDED.affinity_labels,
    affinity_weights = EXCLUDED.affinity_weights,
    benchmark_metric_weights = EXCLUDED.benchmark_metric_weights;


-- ============================================================
-- Act 2 cont'd: admit voter pool routes for compile_finalize
-- (picker re-ranks at call time; ranks here are just for tie-break ordering)
-- ============================================================

INSERT INTO task_type_routing (
    task_type, transport_type, provider_slug, model_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES
    ('compile_finalize', 'API', 'openrouter', 'google/gemini-3-flash-preview', TRUE, 1,
     0.0, 4096, 'high', 1, 'instant', 1,
     '{"source": "decision.2026-04-28.compile-author-cascade-and-finalize-voters", "role": "voter"}'::jsonb,
     'explicit',
     'Compile finalize voter: Gemini-3-Flash — fast structured-output classification.',
     now()),
    ('compile_finalize', 'API', 'openrouter', 'openai/gpt-5.4-mini', TRUE, 2,
     0.0, 4096, 'high', 2, 'instant', 2,
     '{"effort": "medium", "source": "decision.2026-04-28.compile-author-cascade-and-finalize-voters", "role": "voter"}'::jsonb,
     'explicit',
     'Compile finalize voter: GPT-5.4-mini @ medium — proven pill-discrimination winner.',
     now()),
    ('compile_finalize', 'API', 'openrouter', 'x-ai/grok-4.1-fast', TRUE, 3,
     0.0, 4096, 'high', 3, 'instant', 3,
     '{"source": "decision.2026-04-28.compile-author-cascade-and-finalize-voters", "role": "voter"}'::jsonb,
     'explicit',
     'Compile finalize voter: Grok-4.1-Fast — 133 t/s, cheap.',
     now()),
    ('compile_finalize', 'API', 'openrouter', 'qwen/qwen3.6-plus', TRUE, 4,
     0.0, 4096, 'high', 4, 'instant', 4,
     '{"source": "decision.2026-04-28.compile-author-cascade-and-finalize-voters", "role": "voter"}'::jsonb,
     'explicit',
     'Compile finalize voter: Qwen3.6-Plus — 1M ctx, instruction-following.',
     now()),
    ('compile_finalize', 'API', 'openrouter', 'moonshotai/kimi-k2.6', TRUE, 5,
     0.0, 4096, 'high', 5, 'instant', 5,
     '{"source": "decision.2026-04-28.compile-author-cascade-and-finalize-voters", "role": "tiebreaker"}'::jsonb,
     'explicit',
     'Compile finalize tiebreaker: Kimi K2.6 — proven 3.7s on classification, II 54.',
     now())
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)
DO UPDATE SET
    rank = EXCLUDED.rank, permitted = EXCLUDED.permitted,
    temperature = EXCLUDED.temperature, max_tokens = EXCLUDED.max_tokens,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    rationale = EXCLUDED.rationale,
    updated_at = now();


-- ============================================================
-- Allowlist voter pool for both runtime profiles
-- ============================================================

INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    ('praxis',        'compile_finalize', 'llm_task', 'openrouter', 'google/gemini-3-flash-preview', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('scratch_agent', 'compile_finalize', 'llm_task', 'openrouter', 'google/gemini-3-flash-preview', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('praxis',        'compile_finalize', 'llm_task', 'openrouter', 'openai/gpt-5.4-mini', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('scratch_agent', 'compile_finalize', 'llm_task', 'openrouter', 'openai/gpt-5.4-mini', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('praxis',        'compile_finalize', 'llm_task', 'openrouter', 'x-ai/grok-4.1-fast', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('scratch_agent', 'compile_finalize', 'llm_task', 'openrouter', 'x-ai/grok-4.1-fast', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('praxis',        'compile_finalize', 'llm_task', 'openrouter', 'qwen/qwen3.6-plus', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('scratch_agent', 'compile_finalize', 'llm_task', 'openrouter', 'qwen/qwen3.6-plus', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('praxis',        'compile_finalize', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters'),
    ('scratch_agent', 'compile_finalize', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_finalize.voter_pool.2026_04_28', 'decision.2026-04-28.compile-author-cascade-and-finalize-voters')
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed      = EXCLUDED.allowed,
    reason_code  = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = NOW();


-- ============================================================
-- Refresh catalog + snapshot
-- ============================================================

SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

COMMIT;
