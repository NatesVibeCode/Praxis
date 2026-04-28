-- Migration 309: Revert Kimi K2.6 off compile_synthesize.
--
-- Operator direction (2026-04-28, nate): "go ahead" + acknowledgment that
-- the prior smoke at 313.27s total was inconclusive — we don't actually
-- know if OpenRouter spike-deprioritization caused the slowdown vs other
-- causes (Kimi structurally slow on the synthesize prompt shape; voting
-- fan-out cost; build_authority_bundle running twice; etc).
--
-- Migration 308 promoted Kimi K2.6 to compile_synthesize rank 1 as an
-- experiment. The smoke that came back was inconclusive on per-stage
-- timings (didn't capture them) but the overall 5-minute total wasn't
-- acceptable either way. Reverting Kimi to keep the routing in a known-
-- working state until we have better instrumentation + per-stage timing.
--
-- After this migration:
--   compile_synthesize rank 1 ← openrouter/google/gemini-3-flash-preview  (proven prior winner)
--   compile_synthesize rank 2 ← openrouter/moonshotai/kimi-k2.6           (kept admitted, demoted)
--   compile_synthesize rank 3+ ← deepseek/deepseek-v4-pro etc.            (existing fallback chain)
--
-- decision_ref: decision.2026-04-28.revert-kimi-synthesize-await-instrumentation

BEGIN;

UPDATE task_type_routing
   SET rank = 2,
       rationale = 'Demoted by migration 309: Kimi-on-synthesize experiment from 308 was inconclusive (no per-stage timing captured; total wall 313s under sustained OpenRouter usage). Kept admitted as fallback until per-stage instrumentation lands.',
       updated_at = now()
 WHERE task_type = 'compile_synthesize'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'moonshotai/kimi-k2.6';

UPDATE task_type_routing
   SET rank = 1,
       rationale = 'Restored as compile_synthesize primary by migration 309: was rank 1 prior to 308 with proven ~1.7s wall on synthesis prompts.',
       updated_at = now()
 WHERE task_type = 'compile_synthesize'
   AND transport_type = 'API'
   AND provider_slug = 'openrouter'
   AND model_slug = 'google/gemini-3-flash-preview';

COMMIT;
