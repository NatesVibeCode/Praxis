-- Migration 308: Revert Kimi from compile_author + try Kimi on compile_synthesize.
--
-- Operator direction (2026-04-28, nate): "Yes to 1 and 2" + "Try kimi for Synthesize"
--
-- Findings from migration 307 smoke (Kimi K2.6 as compile_author primary):
--   - Kimi K2.6 took 95.97s on a compile_author prose prompt (3-5K chars)
--   - Safety guard REJECTED the output (refinement: fallback)
--   - vs Gemini-3-Flash on the same shape: 1.48s, guard accepted, applied=True
--   - Theory: Kimi K2.6 is 1T MoE — short prompts route fast (3.68s on 200-char
--     classification probe), long prefill scales poorly + reasoning quirks
--   - Conclusion: Kimi is the wrong stage match for compile_author
--
-- Why try Kimi on compile_synthesize:
--   - Synthesize is short-prompt structured-output extraction (operator prose
--     in, JSON skeleton out). That matches Kimi's proven fast path (3.68s on
--     classification probe).
--   - Synthesize doesn't have the prose-quality safety guard that bit Kimi
--     on compile_author — the output is JSON-schema-shaped, not prose.
--   - Kimi tied for II 54 with Qwen 3.6, beats current Gemini-3-Flash-Preview
--     on coding/structured benchmarks per Artificial Analysis. If quality
--     gain is real and speed holds, this is a clean upgrade.
--
-- Two acts in one migration:
--   Act 1 (revert compile_author):
--     rank 1 ← openrouter/google/gemini-3-flash-preview  (was rank 2)
--     rank 2 ← openrouter/moonshotai/kimi-k2.6           (was rank 1)
--     V4-Pro and V3.2 stay at rank 5 deep fallback.
--
--   Act 2 (try Kimi on compile_synthesize):
--     rank 1 ← openrouter/moonshotai/kimi-k2.6           (NEW primary)
--     rank 2 ← openrouter/google/gemini-3-flash-preview  (was rank 1; proven fallback)
--     rank 3 ← openrouter/deepseek/deepseek-v4-pro       (was rank 2; kept further down)
--     Allowlist Kimi for compile_synthesize on both runtime profiles.
--
--   If Kimi degrades synthesize quality, one row flip back to Gemini-Flash rank 1.
--
-- decision_ref: decision.2026-04-28.compile-stage-rebalance-kimi-to-synthesize

BEGIN;

-- ============================================================
-- Act 1: revert compile_author primary to Gemini-3-Flash
-- ============================================================

UPDATE task_type_routing
   SET rank = 2,
       rationale = 'Demoted by migration 308 after the Kimi-as-author experiment in 307. Kimi is fast on short prompts but scales poorly on compile_author prose (95s wall vs Gemini 1.48s, plus guard rejection). Kept admitted as deep fallback.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'moonshotai/kimi-k2.6';

UPDATE task_type_routing
   SET rank = 1,
       rationale = 'Restored as compile_author primary by migration 308: 1.48s wall, safety guard accepted output, no degradation observed. Migration 307 Kimi experiment failed.',
       updated_at = now()
 WHERE task_type = 'compile_author'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'google/gemini-3-flash-preview';

-- ============================================================
-- Act 2: try Kimi K2.6 on compile_synthesize
-- ============================================================

-- Demote current synthesize rank-1 (Gemini-3-Flash) to rank 2.
UPDATE task_type_routing
   SET rank = 2,
       rationale = 'Demoted by migration 308: trying Kimi K2.6 as synthesize primary on the theory that short-prompt structured-output is Kimi''s strong shape. Kept as proven fallback.',
       updated_at = now()
 WHERE task_type = 'compile_synthesize'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'google/gemini-3-flash-preview';

-- Promote Kimi K2.6 to rank 1 for compile_synthesize.
INSERT INTO task_type_routing (
    task_type, transport_type, provider_slug, model_slug, permitted, rank,
    temperature, max_tokens, route_tier, route_tier_rank,
    latency_class, latency_rank, reasoning_control,
    route_source, rationale, updated_at
) VALUES (
    'compile_synthesize', 'API', 'openrouter', 'moonshotai/kimi-k2.6', TRUE, 1,
    0.0, 4096, 'high', 1, 'instant', 1,
    '{"source": "decision.2026-04-28.compile-stage-rebalance-kimi-to-synthesize",
      "rationale": "Kimi K2.6 proven 3.68s on classification probe; synthesize is short-prompt JSON-shaped output where Kimi''s strengths align. II 54 (ties Qwen 3.6, beats Gemini-3-Flash on coding/structured benchmarks)."}'::jsonb,
    'explicit',
    'Compile synthesize primary: Kimi K2.6 — short-prompt structured output is Kimi''s strong shape.',
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

-- Allowlist Kimi for compile_synthesize on both runtime profiles.
INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    ('praxis',        'compile_synthesize', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_synthesize.try_kimi.2026_04_28', 'decision.2026-04-28.compile-stage-rebalance-kimi-to-synthesize'),
    ('scratch_agent', 'compile_synthesize', 'llm_task', 'openrouter', 'moonshotai/kimi-k2.6', TRUE,
     'compile_synthesize.try_kimi.2026_04_28', 'decision.2026-04-28.compile-stage-rebalance-kimi-to-synthesize')
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
