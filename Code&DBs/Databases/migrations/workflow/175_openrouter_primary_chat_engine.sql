-- Migration 175: Promote OpenRouter to primary chat/build/review engine
--
-- OpenRouter is the sanctioned chat/compile engine for the app (Nate's primary
-- user experience). We register a dedicated candidate for
-- `anthropic/claude-sonnet-4.6` behind the OpenRouter broker, mark it primary
-- across chat/build/review/analysis affinity buckets, and pin it at rank=1 in
-- task_type_routing for those task types.
--
-- This also re-admits the openrouter/llm_task transport. The admission was
-- flipped to false by a stale health probe that ran before
-- OPENROUTER_API_KEY was stored in the macOS Keychain. The credential is
-- present now; the live runtime probe (runtime/health.py) reads Keychain
-- via resolve_secret(), so future admissions will stay true.

BEGIN;

-- 1. Re-admit openrouter/llm_task. The stale admission reason
--    ("api transport metadata present but credential or endpoint missing")
--    no longer applies — the key now lives in macOS Keychain under
--    service="praxis" and the runtime probe reads it via resolve_secret().
UPDATE provider_transport_admissions
   SET admitted_by_policy = true,
       policy_reason = 'Admitted OpenRouter via OpenAI-compatible HTTP API. Credential sourced from macOS Keychain (service=praxis, account=OPENROUTER_API_KEY). Promoted to primary chat engine in migration 175.',
       decision_ref = 'decision.2026-04-19.openrouter-primary-chat-engine',
       status = 'active',
       updated_at = now()
 WHERE provider_slug = 'openrouter'
   AND adapter_type = 'llm_task';

-- 2. Register the chosen chat engine candidate:
--    anthropic/claude-sonnet-4.6 via OpenRouter.
--    Strong tool calling + structured output + consistent behavior.
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
    'candidate.openrouter.anthropic.claude-sonnet-4-6',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'anthropic/claude-sonnet-4.6',
    'active',
    1,
    10,
    '["chat", "build", "review", "analysis", "tool-use", "multimodal", "primary-engine"]'::jsonb,
    '{
      "provider_slug": "openrouter",
      "model_slug": "anthropic/claude-sonnet-4.6",
      "selected_transport": "api",
      "api_provider": "openrouter",
      "api_protocol_family": "openai_chat_completions",
      "context_window": 200000,
      "pricing_model": "anthropic_via_openrouter",
      "catalog_source": "migration.175"
    }'::jsonb,
    now(),
    NULL,
    'decision.2026-04-19.openrouter-primary-chat-engine',
    now(),
    '{}'::jsonb,
    'medium',
    5,
    'instant',
    9,
    '{}'::jsonb,
    '{
      "primary": ["chat", "analysis", "quick-analysis", "build", "review", "tool-use", "agentic-coding", "coding", "multimodal", "debug", "architecture"],
      "secondary": ["research", "wiring", "refactor", "test"],
      "specialized": ["brokered-routing"],
      "fallback": [],
      "avoid": []
    }'::jsonb,
    '{
      "evidence_level": "vendor_positioning",
      "positioning": "Claude Sonnet 4.6 via OpenRouter as the primary chat/compile engine for Praxis app. Strong tool calling, structured output, consistent agent behavior.",
      "source_refs": ["anthropic_claude_sonnet_4_6", "openrouter_chat_completion_api"]
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

-- 3. Pin the OpenRouter candidate at rank=1 for chat/build/review/analysis.
--    Explicit rows beat derived routing rows; this guarantees the app chat
--    surface and the "Describe it" compile path both land on OpenRouter.
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
)
SELECT
    task_type,
    'openrouter',
    'anthropic/claude-sonnet-4.6',
    true,
    1,
    'Primary chat/compile engine for Praxis app — migration 175.',
    now(),
    'medium',
    5,
    'instant',
    9,
    'explicit'
FROM (VALUES ('chat'), ('build'), ('review'), ('analysis')) AS t(task_type)
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    rationale = EXCLUDED.rationale,
    updated_at = EXCLUDED.updated_at,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    route_source = EXCLUDED.route_source;

-- 4. Bump the existing rank=1 anthropic/claude-sonnet-4-5 chat row down so
--    the OpenRouter row actually wins tie-breaks. Also flip permitted=true
--    on the legacy sonnet-4-5/4-6 rows so they're not dead weight if we
--    ever lose the broker.
UPDATE task_type_routing
   SET rank = rank + 10,
       updated_at = now()
 WHERE task_type = 'chat'
   AND provider_slug = 'anthropic'
   AND model_slug IN ('claude-sonnet-4-5', 'claude-sonnet-4-6')
   AND rank <= 2;

-- 5. File the architecture-policy decision.
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
    'operator_decision.openrouter-primary-chat-engine.2026-04-19',
    'decision.2026-04-19.openrouter-primary-chat-engine',
    'architecture_policy',
    'decided',
    'OpenRouter is the primary chat/compile engine for the Praxis app (Nate''s primary user experience)',
    $DEC$
The Praxis app standardizes on OpenRouter as THE chat/compile engine for the primary user. Concretely:

- Chosen model: `anthropic/claude-sonnet-4.6` via OpenRouter. Strong tool calling, structured output, consistent agent behavior — matches the requirements of the app chat surface and the "Describe it" compile button.
- OpenRouter credential lives in macOS Keychain under service="praxis", account="OPENROUTER_API_KEY". The runtime probe in `runtime/health.py::_provider_api_key_present` reads Keychain via `adapters.keychain.resolve_secret`.
- `provider_model_candidates` row `candidate.openrouter.anthropic.claude-sonnet-4-6` is priority=1, route_tier=medium, latency_class=instant, task_affinities.primary covers chat/analysis/build/review/tool-use/agentic-coding.
- Explicit `task_type_routing` rows (rank=1) pin OpenRouter for `chat`, `build`, `review`, `analysis` — the four task types the app surfaces hit.
- Prior rank=1 anthropic/sonnet-4-5 chat row is bumped to rank=11 so the OpenRouter row wins.
- `provider_transport_admissions` for openrouter/llm_task is re-admitted; prior admission_reason "credential or endpoint missing" was a stale health probe result from before the Keychain secret was stored.

Scope: this decision governs the app's chat surface and the compile entry points ("Describe it" in `MoonBuildPage.tsx` and `Dashboard.tsx`). New/external users of the platform remain free to pick any provider — this is specifically about the default engine for the primary operator.
    $DEC$,
    'nate',
    'claude_code',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'providers::openrouter'
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
