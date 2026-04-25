-- Migration 243: DeepSeek V4-Pro (via OpenRouter) as primary build engine
--
-- Operator direction (2026-04-25, nate): "update the api to be Deepseek from
-- OpenRouter its cheaper and better than sonnet" → "deepseek V4" → "Pro is
-- the one I trust please".
--
-- Standing-order reconciliation:
--   - architecture-policy::providers::openrouter (decision.2026-04-19) pins
--     OpenRouter as primary chat/compile broker. This migration honors that:
--     V4-Pro is still via the OpenRouter broker, so the broker policy is
--     preserved. Only the model slug behind the broker changes for the
--     `build` task type.
--   - user memory `feedback_deepseek_research_only` bans DeepSeek as a
--     *direct* API provider for anything besides research. This migration
--     does NOT register `deepseek` as a provider. The row lives under
--     provider_slug='openrouter' (broker) with model_slug='deepseek/
--     deepseek-v4-pro'. Direct `deepseek` provider remains research-only.
--
-- Pricing (probed from https://openrouter.ai/api/v1/models 2026-04-25):
--   deepseek/deepseek-v4-pro — prompt $1.74 / completion $3.48 per 1M tokens,
--                              context_length 1,048,576.
--   anthropic/claude-sonnet-4.6 (prior rank=1) —
--                              prompt $3.00 / completion $15.00 per 1M, 200K ctx.
-- Net: ~4.3× cheaper on output, 5× context window.
--
-- Scope: `build` task type only. Chat/review/analysis keep sonnet-4.6 at
-- rank=1 until the operator explicitly expands the swap. The compile path
-- (`_APP_COMPILE_TASK_ROUTE = "auto/build"`) hits this task type.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Register the DeepSeek V4-Pro candidate via OpenRouter.
-- -----------------------------------------------------------------------
INSERT INTO provider_model_candidates (
    candidate_ref,
    provider_ref,
    provider_name,
    provider_slug,
    model_slug,
    status,
    priority,
    balance_weight,
    capability_tags,
    default_parameters,
    effective_from,
    effective_to,
    decision_ref,
    created_at,
    cli_config,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    task_affinities,
    benchmark_profile,
    cap_language_high,
    cap_analysis_architecture_research,
    cap_build_high,
    cap_review,
    cap_tool_use,
    cap_build_med
) VALUES (
    'candidate.openrouter.deepseek-v4-pro',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'deepseek/deepseek-v4-pro',
    'active',
    1,
    10,
    '["build", "tool-use", "coding", "agentic-coding", "review", "analysis", "primary-engine", "long-context"]'::jsonb,
    '{
      "provider_slug": "openrouter",
      "model_slug": "deepseek/deepseek-v4-pro",
      "selected_transport": "api",
      "api_provider": "openrouter",
      "api_protocol_family": "openai_chat_completions",
      "context_window": 1048576,
      "pricing_model": "deepseek_via_openrouter",
      "pricing_prompt_per_mtok": 1.74,
      "pricing_completion_per_mtok": 3.48,
      "catalog_source": "migration.243"
    }'::jsonb,
    now(),
    NULL,
    'decision.2026-04-25.openrouter-deepseek-v4-pro-build-engine',
    now(),
    '{}'::jsonb,
    'high',
    3,
    'instant',
    9,
    '{}'::jsonb,
    '{
      "primary": ["build", "tool-use", "agentic-coding", "coding", "review", "analysis", "debug", "architecture"],
      "secondary": ["chat", "refactor", "test", "wiring"],
      "specialized": ["long-context", "brokered-routing"],
      "fallback": [],
      "avoid": []
    }'::jsonb,
    '{
      "evidence_level": "operator_evaluation",
      "positioning": "DeepSeek V4-Pro via OpenRouter — operator selected as primary build/compile engine 2026-04-25. Stronger coding + tool-use than Sonnet 4.6 at ~4.3x lower output cost and 5x context window. Broker stays OpenRouter per existing architecture policy.",
      "source_refs": ["openrouter_chat_completion_api", "operator_decision.deepseek-v4-pro.2026-04-25"]
    }'::jsonb,
    true,
    true,
    true,
    true,
    true,
    true
) ON CONFLICT (candidate_ref) DO UPDATE SET
    provider_ref = EXCLUDED.provider_ref,
    provider_name = EXCLUDED.provider_name,
    provider_slug = EXCLUDED.provider_slug,
    model_slug = EXCLUDED.model_slug,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    balance_weight = EXCLUDED.balance_weight,
    capability_tags = EXCLUDED.capability_tags,
    default_parameters = EXCLUDED.default_parameters,
    effective_to = EXCLUDED.effective_to,
    decision_ref = EXCLUDED.decision_ref,
    cli_config = EXCLUDED.cli_config,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    task_affinities = EXCLUDED.task_affinities,
    benchmark_profile = EXCLUDED.benchmark_profile,
    cap_language_high = EXCLUDED.cap_language_high,
    cap_analysis_architecture_research = EXCLUDED.cap_analysis_architecture_research,
    cap_build_high = EXCLUDED.cap_build_high,
    cap_review = EXCLUDED.cap_review,
    cap_tool_use = EXCLUDED.cap_tool_use,
    cap_build_med = EXCLUDED.cap_build_med;

-- -----------------------------------------------------------------------
-- 2. Demote the prior rank=1 sonnet row for build, then pin V4-Pro at 1.
--    rank is not unique on (task_type, ...) so the sonnet demote + deepseek
--    insert can coexist; rank=1 is settled by the explicit rows the resolver
--    picks in ascending order.
-- -----------------------------------------------------------------------
UPDATE task_type_routing
   SET rank = 2,
       rationale = 'Sonnet 4.6 demoted to fallback for build task — migration 243 promoted deepseek-v4-pro to rank=1 per operator decision 2026-04-25.',
       updated_at = now()
 WHERE task_type = 'build'
   AND provider_slug = 'openrouter'
   AND model_slug = 'anthropic/claude-sonnet-4.6'
   AND rank = 1;

INSERT INTO task_type_routing (
    task_type,
    provider_slug,
    model_slug,
    permitted,
    rank,
    rationale,
    updated_at,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    route_source
) VALUES (
    'build',
    'openrouter',
    'deepseek/deepseek-v4-pro',
    true,
    1,
    'Primary build/compile engine for Praxis app — migration 243. DeepSeek V4-Pro via OpenRouter broker. Cheaper + stronger code/tool-use than Sonnet 4.6 at 1M context.',
    now(),
    'high',
    3,
    'instant',
    9,
    'explicit'
) ON CONFLICT (task_type, model_slug, provider_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    rationale = EXCLUDED.rationale,
    updated_at = EXCLUDED.updated_at,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    route_source = EXCLUDED.route_source;

-- -----------------------------------------------------------------------
-- 3. File the architecture-policy decision.
--    Narrows decision.2026-04-19 for the `build` task type only; chat and
--    review/analysis remain on Sonnet 4.6 under the existing decision.
-- -----------------------------------------------------------------------
INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref
) VALUES (
    'operator_decision.openrouter-deepseek-v4-pro-build-engine.2026-04-25',
    'decision.2026-04-25.openrouter-deepseek-v4-pro-build-engine',
    'architecture_policy',
    'decided',
    'DeepSeek V4-Pro via OpenRouter is the primary build/compile engine',
    $DEC$
The Praxis compile path ("Describe it" → _APP_COMPILE_TASK_ROUTE="auto/build" → task_type=build) routes to DeepSeek V4-Pro via the OpenRouter broker.

Concretely:
- Rank=1 for task_type=build: `openrouter/deepseek/deepseek-v4-pro`.
- Rank=2 (fallback): `openrouter/anthropic/claude-sonnet-4.6` (prior rank=1; kept as fallback in case V4-Pro degrades or the operator reverses).
- OpenRouter remains the broker — architecture-policy::providers::openrouter (decision.2026-04-19) is NOT superseded; this decision narrows the MODEL behind the broker for the `build` task type.
- DeepSeek direct-API remains research-only under feedback_deepseek_research_only. This decision uses OpenRouter as the transport, not direct deepseek.com.

Rationale (operator, 2026-04-25):
- Pricing: V4-Pro is $1.74/$3.48 per 1M prompt/completion vs Sonnet 4.6 at $3.00/$15.00 — ~4.3× cheaper on output.
- Context: V4-Pro at 1,048,576 tokens vs Sonnet at 200,000 — 5× larger window. Moon compile emits large graph bundles; the 1M window removes a tail-of-distribution truncation risk.
- Capability: operator evaluation — "cheaper and better than sonnet" for build/tool-use workload. Evidence tier: operator_evaluation (not third-party benchmark).

Scope: `build` task_type. `chat`, `review`, `analysis` remain on Sonnet 4.6 at rank=1 per decision.2026-04-19 until operator explicitly expands this swap.

Rollback path: DELETE the rank=1 V4-Pro task_type_routing row; UPDATE sonnet-4.6 row rank 2→1.
    $DEC$,
    'nate',
    'claude_code',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'providers::openrouter::build-task'
) ON CONFLICT (decision_key) DO UPDATE SET
    decision_kind = EXCLUDED.decision_kind,
    decision_status = EXCLUDED.decision_status,
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    decided_by = EXCLUDED.decided_by,
    decision_source = EXCLUDED.decision_source,
    effective_from = EXCLUDED.effective_from,
    decided_at = EXCLUDED.decided_at,
    updated_at = now(),
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref = EXCLUDED.decision_scope_ref;

COMMIT;

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug FROM task_type_routing
--    WHERE task_type='build' ORDER BY rank LIMIT 5;
--     -> expect openrouter/deepseek/deepseek-v4-pro at rank=1,
--              openrouter/anthropic/claude-sonnet-4.6 at rank=2.
--
--   SELECT candidate_ref, provider_slug, model_slug, status FROM provider_model_candidates
--    WHERE model_slug='deepseek/deepseek-v4-pro';
--     -> expect one row, status='active'.
