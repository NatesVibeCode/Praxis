-- Migration 262: Together as the compile-task-type primary
--
-- Operator direction (2026-04-26, nate): "use together you have the API in
-- the Keychain, but its all API not CLI so it should be easy".
--
-- Context: Migrations 243 + 246 set OpenRouter/deepseek-v4-pro and
-- OpenRouter/deepseek-v4-flash as the compile primaries (rank 1 + 2).
-- The UI "Describe it" path failed at runtime because the API server
-- container had no OPENROUTER_API_KEY. The operator's preferred lane is
-- Together — the V4-Pro model is already registered there
-- (provider_model_candidates row at priority=500) and Together's API
-- endpoint + key resolution are wired through provider_endpoint_bindings
-- (https://api.together.xyz/v1/chat/completions, TOGETHER_API_KEY).
--
-- This migration swaps task_type_routing for task_type='compile':
--   rank 1: together / deepseek-ai/DeepSeek-V4-Pro
--   rank 2: together / deepseek-ai/DeepSeek-V3.2 (fallback within the same
--           provider family — V3.2 is also Together-hosted)
-- The OpenRouter rows are demoted to fallback rank 5+ rather than deleted,
-- so they remain available if Together is unhealthy and the failover
-- chain has to walk.

BEGIN;

-- 0a. Idempotency cleanup: an earlier apply of this migration created
--     model_profile_candidate_bindings rows via the AFTER INSERT trigger from
--     migration 095 (binding_id is built as 'binding.auto.' || candidate_ref,
--     so the PK is locked to the short-form candidate_ref this migration
--     declares). Subsequent operator workflows (provider onboarding /
--     candidate_ref normalization) renamed the underlying candidate rows to
--     the canonical longer form (candidate.together.deepseek-ai/DeepSeek-V4-Pro
--     etc.), which left the original short-form binding rows behind with their
--     candidate_ref column re-pointed at the renamed candidate. Re-running
--     this migration on a drifted DB then trips
--     model_profile_candidate_bindings_pkey when the trigger tries to recreate
--     the short-form binding for the freshly re-inserted short-form candidate.
--     Drop those orphans first so the trigger can generate clean bindings on
--     the candidate INSERT below. No FK references the bindings table, so
--     this is safe.
DELETE FROM model_profile_candidate_bindings
 WHERE model_profile_candidate_binding_id IN (
     'binding.auto.candidate.together.deepseek-v4-pro',
     'binding.auto.candidate.together.deepseek-v3.2'
 );

-- 0. Own the whole Together API authority chain for this private compile
--    exception. Ranking a task_type row is insufficient under the fail-closed
--    provider catalog: the route also needs lane policy, transport admission,
--    runtime-profile admission, key allowlist, and refreshed projections.
UPDATE provider_cli_profiles
   SET adapter_economics = jsonb_set(
           COALESCE(adapter_economics, '{}'::jsonb),
           '{llm_task}',
           COALESCE(adapter_economics -> 'llm_task', '{}'::jsonb)
           || jsonb_build_object(
               'allow_payg_fallback', true,
               'prefer_prepaid', false,
               'billing_mode', 'metered_api',
               'budget_bucket', 'together_api_payg',
               'effective_marginal_cost', 1.0
           ),
           true
       ),
       updated_at = now()
 WHERE provider_slug = 'together';

INSERT INTO provider_lane_policy
    (provider_slug, allowed_adapter_types, overridable, decision_ref, effective_from)
VALUES
    (
        'together',
        ARRAY['llm_task'],
        false,
        'decision.2026-04-26.together-compile-primary-api-exception',
        now()
    )
ON CONFLICT (provider_slug) DO UPDATE SET
    allowed_adapter_types = EXCLUDED.allowed_adapter_types,
    overridable = EXCLUDED.overridable,
    decision_ref = EXCLUDED.decision_ref,
    effective_from = EXCLUDED.effective_from;

INSERT INTO provider_transport_admissions (
    provider_transport_admission_id,
    provider_slug,
    adapter_type,
    transport_kind,
    execution_topology,
    admitted_by_policy,
    policy_reason,
    lane_id,
    docs_urls,
    credential_sources,
    probe_contract,
    decision_ref,
    status,
    created_at,
    updated_at
) VALUES (
    'provider_transport_admission.together.llm_task',
    'together',
    'llm_task',
    'http',
    'direct_http',
    true,
    'Private compile exception: Together direct API is admitted only through the llm_task lane and surfaced through the provider control plane catalog.',
    'together:llm_task',
    '{
      "chat_completion": "https://docs.together.ai/reference/chat-completions-1",
      "models": "https://docs.together.ai/docs/serverless-models"
    }'::jsonb,
    '["TOGETHER_API_KEY"]'::jsonb,
    '{
      "api_endpoint": "https://api.together.xyz/v1/chat/completions",
      "api_protocol_family": "openai_chat_completions",
      "api_key_env_var": "TOGETHER_API_KEY",
      "prompt_probe": {
        "strategy": "openai_chat_completion_auth_probe",
        "status": "seeded"
      }
    }'::jsonb,
    'decision.2026-04-26.together-compile-primary-api-exception',
    'active',
    now(),
    now()
) ON CONFLICT (provider_slug, adapter_type) DO UPDATE SET
    provider_transport_admission_id = EXCLUDED.provider_transport_admission_id,
    transport_kind = EXCLUDED.transport_kind,
    execution_topology = EXCLUDED.execution_topology,
    admitted_by_policy = EXCLUDED.admitted_by_policy,
    policy_reason = EXCLUDED.policy_reason,
    lane_id = EXCLUDED.lane_id,
    docs_urls = EXCLUDED.docs_urls,
    credential_sources = EXCLUDED.credential_sources,
    probe_contract = EXCLUDED.probe_contract,
    decision_ref = EXCLUDED.decision_ref,
    status = EXCLUDED.status,
    updated_at = now();

UPDATE registry_native_runtime_profile_authority AS profile
   SET provider_names = (
           SELECT jsonb_agg(provider_name ORDER BY provider_name)
           FROM (
               SELECT DISTINCT provider_name
               FROM jsonb_array_elements_text(
                   COALESCE(profile.provider_names, '[]'::jsonb) || '["together"]'::jsonb
               ) AS existing(provider_name)
           ) AS normalized
       ),
       allowed_models = (
           SELECT jsonb_agg(model_slug ORDER BY model_slug)
           FROM (
               SELECT DISTINCT model_slug
               FROM jsonb_array_elements_text(
                   COALESCE(profile.allowed_models, '[]'::jsonb)
                   || '["deepseek-ai/DeepSeek-V4-Pro", "deepseek-ai/DeepSeek-V3.2"]'::jsonb
               ) AS existing(model_slug)
           ) AS normalized
       ),
       recorded_at = now()
 WHERE profile.runtime_profile_ref IS NOT NULL;

UPDATE registry_sandbox_profile_authority AS sandbox
   SET secret_allowlist = (
           SELECT jsonb_agg(secret_name ORDER BY secret_name)
           FROM (
               SELECT DISTINCT secret_name
               FROM jsonb_array_elements_text(
                   COALESCE(sandbox.secret_allowlist, '[]'::jsonb) || '["TOGETHER_API_KEY"]'::jsonb
               ) AS existing(secret_name)
           ) AS normalized
       ),
       recorded_at = now()
 WHERE NOT (COALESCE(sandbox.secret_allowlist, '[]'::jsonb) ? 'TOGETHER_API_KEY');

-- 1. Register the concrete Together compile candidates. Migration 251 created
--    the provider profile, but the catalog reducer requires active candidate
--    rows for the exact provider/model pair before it can mark a job available.
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
) VALUES
    (
        'candidate.together.deepseek-v4-pro',
        'provider.together',
        'Together',
        'together',
        'deepseek-ai/DeepSeek-V4-Pro',
        'active',
        1,
        10,
        '["compile","structured-output","workflow-definition","schema-normalization","long-context","primary-engine","api-only"]'::jsonb,
        '{
          "provider_slug": "together",
          "model_slug": "deepseek-ai/DeepSeek-V4-Pro",
          "model_version": "DeepSeek-V4-Pro",
          "selected_transport": "api",
          "api_provider": "together",
          "api_protocol_family": "openai_chat_completions",
          "api_endpoint": "https://api.together.xyz/v1/chat/completions",
          "pricing_model": "together_direct_payg",
          "catalog_source": "migration.262"
        }'::jsonb,
        now(),
        NULL,
        'decision.2026-04-26.together-compile-primary-api-exception',
        now(),
        '{}'::jsonb,
        'high',
        1,
        'reasoning',
        1,
        '{}'::jsonb,
        '{
          "primary": ["compile","structured-output","workflow-definition","schema-normalization"],
          "secondary": [],
          "specialized": ["long-context","api-only"],
          "fallback": [],
          "avoid": ["cli","tool-use","agentic-coding"]
        }'::jsonb,
        '{
          "evidence_level": "operator_direction",
          "positioning": "Private Together direct API compile primary for the operator instance.",
          "source_refs": ["migration.251", "migration.262", "decision.2026-04-26.together-compile-primary-api-exception"]
        }'::jsonb,
        true,
        true,
        false,
        false,
        false,
        false
    ),
    (
        'candidate.together.deepseek-v3.2',
        'provider.together',
        'Together',
        'together',
        'deepseek-ai/DeepSeek-V3.2',
        'active',
        2,
        6,
        '["compile","structured-output","workflow-definition","schema-normalization","fallback","api-only"]'::jsonb,
        '{
          "provider_slug": "together",
          "model_slug": "deepseek-ai/DeepSeek-V3.2",
          "model_version": "DeepSeek-V3.2",
          "selected_transport": "api",
          "api_provider": "together",
          "api_protocol_family": "openai_chat_completions",
          "api_endpoint": "https://api.together.xyz/v1/chat/completions",
          "pricing_model": "together_direct_payg",
          "catalog_source": "migration.262"
        }'::jsonb,
        now(),
        NULL,
        'decision.2026-04-26.together-compile-primary-api-exception',
        now(),
        '{}'::jsonb,
        'high',
        2,
        'reasoning',
        2,
        '{}'::jsonb,
        '{
          "primary": ["compile","structured-output","workflow-definition","schema-normalization"],
          "secondary": [],
          "specialized": ["api-only"],
          "fallback": ["compile"],
          "avoid": ["cli","tool-use","agentic-coding"]
        }'::jsonb,
        '{
          "evidence_level": "operator_direction",
          "positioning": "Private Together direct API compile fallback within the same provider lane.",
          "source_refs": ["migration.251", "migration.262", "decision.2026-04-26.together-compile-primary-api-exception"]
        }'::jsonb,
        true,
        true,
        false,
        false,
        false,
        false
    )
ON CONFLICT (candidate_ref) DO UPDATE SET
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

-- 2. Admit the candidates into every native runtime profile so the private
--    job catalog can project them as available instead of configured-but-gone.
INSERT INTO runtime_profile_admitted_routes (
    runtime_profile_ref,
    model_profile_id,
    provider_policy_id,
    candidate_ref,
    provider_ref,
    provider_slug,
    model_slug,
    eligibility_status,
    reason_code,
    source_window_refs,
    projected_at,
    projection_ref
)
SELECT
    profile.runtime_profile_ref,
    route.model_profile_id,
    'provider_policy.together.llm_task',
    route.candidate_ref,
    'provider.together',
    'together',
    route.model_slug,
    'admitted',
    'together.compile.primary.api_exception',
    '["migration.262_together_compile_primary"]'::jsonb,
    now(),
    'projection.runtime_profile_admitted_routes'
FROM registry_native_runtime_profile_authority AS profile
CROSS JOIN (
    VALUES
        (
            'model_profile.together.compile.deepseek-v4-pro',
            'candidate.together.deepseek-v4-pro',
            'deepseek-ai/DeepSeek-V4-Pro'
        ),
        (
            'model_profile.together.compile.deepseek-v3.2',
            'candidate.together.deepseek-v3.2',
            'deepseek-ai/DeepSeek-V3.2'
        )
) AS route(model_profile_id, candidate_ref, model_slug)
ON CONFLICT (runtime_profile_ref, candidate_ref) DO UPDATE SET
    model_profile_id = EXCLUDED.model_profile_id,
    provider_policy_id = EXCLUDED.provider_policy_id,
    provider_ref = EXCLUDED.provider_ref,
    provider_slug = EXCLUDED.provider_slug,
    model_slug = EXCLUDED.model_slug,
    eligibility_status = EXCLUDED.eligibility_status,
    reason_code = EXCLUDED.reason_code,
    source_window_refs = EXCLUDED.source_window_refs,
    projected_at = EXCLUDED.projected_at,
    projection_ref = EXCLUDED.projection_ref;

-- 3. Demote the OpenRouter compile primaries to fallback ranks.
--
-- Re-apply note:
--   OpenRouter is HTTP/API-only. Historical routing rows were created before
--   transport_type was explicit and later inherited the task_type_routing
--   default transport_type='CLI'. Migration 378 correctly rejects
--   openrouter+CLI on UPDATE as well as INSERT, so this migration must prune
--   the impossible legacy rows before it touches OpenRouter fallback ranks.
DO $$
DECLARE
    has_transport_type BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'task_type_routing'
           AND column_name = 'transport_type'
    ) INTO has_transport_type;

    IF has_transport_type THEN
        DELETE FROM task_type_routing AS route
         WHERE route.provider_slug = 'openrouter'
           AND route.transport_type = 'CLI'
           AND NOT EXISTS (
               SELECT 1
                 FROM provider_transport_admissions AS admission
                WHERE admission.provider_slug = route.provider_slug
                  AND admission.transport_kind = 'cli'
                  AND admission.status = 'active'
           );

        UPDATE task_type_routing
           SET rank = 5,
               rationale = 'Demoted by migration 262: private compile primary moved to Together direct API; OpenRouter remains API fallback.',
               updated_at = now()
         WHERE task_type = 'compile'
           AND provider_slug = 'openrouter'
           AND model_slug = 'deepseek/deepseek-v4-flash'
           AND route_source = 'explicit'
           AND transport_type = 'API';

        UPDATE task_type_routing
           SET rank = 6,
               rationale = 'Demoted by migration 262: private compile primary moved to Together direct API; OpenRouter remains API fallback.',
               updated_at = now()
         WHERE task_type = 'compile'
           AND provider_slug = 'openrouter'
           AND model_slug = 'deepseek/deepseek-v4-pro'
           AND route_source = 'explicit'
           AND transport_type = 'API';
    ELSE
        UPDATE task_type_routing
           SET rank = 5,
               rationale = 'Demoted by migration 262: private compile primary moved to Together direct API; OpenRouter remains fallback.',
               updated_at = now()
         WHERE task_type = 'compile'
           AND provider_slug = 'openrouter'
           AND model_slug = 'deepseek/deepseek-v4-flash'
           AND route_source = 'explicit';

        UPDATE task_type_routing
           SET rank = 6,
               rationale = 'Demoted by migration 262: private compile primary moved to Together direct API; OpenRouter remains fallback.',
               updated_at = now()
         WHERE task_type = 'compile'
           AND provider_slug = 'openrouter'
           AND model_slug = 'deepseek/deepseek-v4-pro'
           AND route_source = 'explicit';
    END IF;
END $$;

-- 4. Insert Together as the new rank 1 + 2 compile routes.
--
-- Idempotency note: migration 333 later collapsed task_type_routing's primary
-- key from the transport-scoped shape to (task_type, sub_task_type,
-- provider_slug, model_slug). Re-running this older migration against a modern
-- DB cannot rely on an ON CONFLICT target that only existed in the old shape.
-- These two rows are wholly owned by this migration, so replace them directly.
DELETE FROM task_type_routing
 WHERE task_type = 'compile'
   AND sub_task_type = '*'
   AND provider_slug = 'together'
   AND model_slug IN (
       'deepseek-ai/DeepSeek-V4-Pro',
       'deepseek-ai/DeepSeek-V3.2'
   );

INSERT INTO task_type_routing (
    task_type,
    sub_task_type,
    transport_type,
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
        '*',
        'API',
        'together',
        'deepseek-ai/DeepSeek-V4-Pro',
        TRUE,
        1,
        'Private compile primary through Together direct API. This route is API-only and admitted by migration 262 provider-control-plane authority.',
        now(),
        'high',
        1,
        'reasoning',
        1,
        'explicit'
    ),
    (
        'compile',
        '*',
        'API',
        'together',
        'deepseek-ai/DeepSeek-V3.2',
        TRUE,
        2,
        'Private compile fallback through the same Together direct API lane.',
        now(),
        'high',
        2,
        'reasoning',
        2,
        'explicit'
    )
;

-- 5. Refresh the private CQRS catalog/snapshot after the authority rows land.
DO $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
    END LOOP;
END $$;

COMMIT;
