-- Migration 245: Split app compile routing from build routing
--
-- Operator direction (2026-04-25, nate): "We should make a new task type for
-- compile, instead of just build... Do it".
--
-- Authority model:
--   CLI `build` remains implementation/execution work.
--   API `compile` is the Moon/app "Describe it" prose-to-definition route.
--
-- This removes the misleading `auto/build` compile route and gives the app
-- compiler its own task_type authority. DeepSeek V4-Pro stays the heavier
-- code/build API route. DeepSeek V4-Flash becomes the compile route because
-- compile is cheap prose/schema normalization, not code implementation.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Create a first-class compile task profile.
-- -----------------------------------------------------------------------
INSERT INTO task_type_route_profiles (
    task_type,
    affinity_labels,
    affinity_weights,
    task_rank_weights,
    benchmark_metric_weights,
    route_tier_preferences,
    latency_class_preferences,
    allow_unclassified_candidates,
    rationale
) VALUES (
    'compile',
    '{"primary":["compile","structured-output","tool-use","long-context","workflow-definition","schema-normalization"],"secondary":["analysis","coding","agentic-coding","review"],"specialized":["json-repair","data-extraction","classification"],"fallback":[],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
    '{"primary":1.0,"secondary":0.76,"specialized":0.62,"fallback":0.0,"unclassified":0.0,"avoid":0.0}'::jsonb,
    '{"affinity":0.65,"route_tier":0.20,"latency":0.15}'::jsonb,
    '{"artificial_analysis_coding_index":0.45,"artificial_analysis_intelligence_index":0.35,"median_output_tokens_per_second":0.10,"price_1m_blended_3_to_1":0.10}'::jsonb,
    '["high","medium","low"]'::jsonb,
    '["instant","reasoning"]'::jsonb,
    false,
    'Compile routing is not implementation. It turns operator prose into structured workflow definition and must use explicitly admitted compile-capable API models, while CLI build remains the default for real implementation work.'
) ON CONFLICT (task_type) DO UPDATE SET
    affinity_labels = EXCLUDED.affinity_labels,
    affinity_weights = EXCLUDED.affinity_weights,
    task_rank_weights = EXCLUDED.task_rank_weights,
    benchmark_metric_weights = EXCLUDED.benchmark_metric_weights,
    route_tier_preferences = EXCLUDED.route_tier_preferences,
    latency_class_preferences = EXCLUDED.latency_class_preferences,
    allow_unclassified_candidates = EXCLUDED.allow_unclassified_candidates,
    rationale = EXCLUDED.rationale,
    updated_at = now();

-- -----------------------------------------------------------------------
-- 2. Mark DeepSeek V4-Pro as compile-capable.
-- -----------------------------------------------------------------------
UPDATE provider_model_candidates
   SET capability_tags = '["compile","structured-output","workflow-definition","schema-normalization","build","tool-use","coding","agentic-coding","review","analysis","primary-engine","long-context"]'::jsonb,
       task_affinities = '{
         "primary": ["compile","structured-output","workflow-definition","build","tool-use","agentic-coding","coding","review","analysis","debug","architecture"],
         "secondary": ["chat","refactor","test","wiring"],
         "specialized": ["long-context","brokered-routing","schema-normalization"],
         "fallback": [],
         "avoid": []
       }'::jsonb
 WHERE provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

-- -----------------------------------------------------------------------
-- 3. Register DeepSeek V4-Flash as a cheaper compile-capable API candidate.
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
    'candidate.openrouter.deepseek-v4-flash',
    'provider.openrouter',
    'OpenRouter',
    'openrouter',
    'deepseek/deepseek-v4-flash',
    'active',
    2,
    6,
    '["compile","structured-output","schema-normalization","json-repair","data-extraction","classification","tool-use","long-context","cheap-lane","instant"]'::jsonb,
    '{
      "provider_slug": "openrouter",
      "model_slug": "deepseek/deepseek-v4-flash",
      "selected_transport": "api",
      "api_provider": "openrouter",
      "api_protocol_family": "openai_chat_completions",
      "context_window": 1048576,
      "pricing_model": "deepseek_via_openrouter",
      "pricing_prompt_per_mtok": 0.14,
      "pricing_completion_per_mtok": 0.28,
      "catalog_source": "migration.245"
    }'::jsonb,
    now(),
    NULL,
    'architecture-policy::provider-routing::compile-task-type',
    now(),
    '{}'::jsonb,
    'medium',
    4,
    'instant',
    3,
    '{}'::jsonb,
    '{
      "primary": ["compile","structured-output","schema-normalization","json-repair","data-extraction","classification"],
      "secondary": ["analysis","chat","review","tool-use"],
      "specialized": ["long-context","brokered-routing","cheap-lane"],
      "fallback": [],
      "avoid": ["final-build-authority"]
    }'::jsonb,
    '{
      "evidence_level": "catalog_pricing_plus_operator_design",
      "positioning": "DeepSeek V4-Flash via OpenRouter is the cheap compile-adjacent lane: classification, extraction, JSON repair, and secondary compile fallback. It is not the final build authority.",
      "source_refs": ["openrouter_models_api", "operator_conversation_2026_04_25"]
    }'::jsonb,
    true,
    true,
    false,
    false,
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
-- 4. Route compile explicitly through DeepSeek V4-Flash, with Pro fallback.
-- -----------------------------------------------------------------------
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
) VALUES
    (
        'compile',
        'openrouter',
        'deepseek/deepseek-v4-flash',
        true,
        1,
        'Primary API compile model for Moon Describe-it and prose-to-definition compilation. Flash is the cheap schema/prose normalization lane; split from code/build by migration 245.',
        now(),
        'medium',
        4,
        'instant',
        3,
        'explicit'
    ),
    (
        'compile',
        'openrouter',
        'deepseek/deepseek-v4-pro',
        true,
        2,
        'Secondary API compile fallback when Flash is insufficient or degraded. Pro remains the heavier code/build lane.',
        now(),
        'high',
        3,
        'instant',
        9,
        'explicit'
    )
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

-- -----------------------------------------------------------------------
-- 5. Keep the code/build API route distinct from compile.
-- -----------------------------------------------------------------------
UPDATE task_type_routing
   SET permitted = true,
       rank = 1,
       rationale = 'API code/build route: DeepSeek V4-Pro remains the heavier code lane. App compile is split to task_type=compile and uses DeepSeek V4-Flash.',
       updated_at = now()
 WHERE task_type = 'build'
   AND provider_slug = 'openrouter'
   AND model_slug = 'deepseek/deepseek-v4-pro';

UPDATE operator_decisions
   SET decision_status = 'superseded',
       effective_to = now(),
       updated_at = now(),
       rationale = rationale || E'\n\nSuperseded 2026-04-25 by architecture-policy::provider-routing::compile-task-type: compile has its own task_type and no longer hides behind build.'
 WHERE decision_key = 'decision.2026-04-25.openrouter-deepseek-v4-pro-build-engine'
   AND effective_to IS NULL;

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
    'operator_decision.provider-routing.compile-task-type.2026-04-25',
    'architecture-policy::provider-routing::compile-task-type',
    'architecture_policy',
    'decided',
    'Compile is a first-class API task type, separate from CLI build',
    'Compile means transforming operator prose into structured workflow definition. Code/build means implementation work. Moon/app compile routes through task_type=compile using OpenRouter API with DeepSeek V4-Flash as rank=1 and DeepSeek V4-Pro as heavier fallback. DeepSeek V4-Pro remains the heavier API code/build route. This prevents compile semantics from hiding inside task_type=build.',
    'nate',
    'conversation',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'provider_routing'
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

-- Verification:
--   runtime.compiler_llm._resolve_app_compile_route()
--     -> expect ('openrouter', 'deepseek/deepseek-v4-flash')
--
--   TaskTypeRouter(...).resolve_failover_chain('auto/compile')
--     -> expect OpenRouter DeepSeek V4-Flash then V4-Pro.
--
--   TaskTypeRouter(...).resolve_failover_chain('auto/build')
--     -> expect CLI candidates first for general work; DeepSeek V4-Pro is the
--        explicit API code/build route when an API lane is requested.
