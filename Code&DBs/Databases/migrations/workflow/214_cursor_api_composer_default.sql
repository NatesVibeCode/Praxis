-- Migration 214: Repair Cursor API model authority
--
-- Cursor's background-agent API no longer reports "auto" in model discovery.
-- Keep the API lane admitted, but move its concrete default to the live
-- Cursor Composer model and retire the stale auto candidate.

BEGIN;

UPDATE provider_cli_profiles
   SET default_model = 'composer-2',
       updated_at = now()
 WHERE provider_slug = 'cursor'
   AND api_protocol_family = 'cursor_background_agent'
   AND default_model = 'auto';

INSERT INTO model_profiles (
    model_profile_id,
    profile_name,
    provider_name,
    model_name,
    schema_version,
    status,
    budget_policy,
    routing_policy,
    default_parameters,
    effective_from,
    effective_to,
    supersedes_model_profile_id,
    created_at
) VALUES (
    'model_profile.provider-onboarding.cursor.composer-2',
    'cursor.composer-2',
    'cursor',
    'composer-2',
    1,
    'active',
    '{"tier":"provider-onboarding","billing_mode":"subscription_included"}'::jsonb,
    '{"selection":"direct_candidate","transport":"api","route_tier":"high","latency_class":"reasoning"}'::jsonb,
    '{"context_window":128000,"provider_slug":"cursor","model_slug":"composer-2","selected_transport":"api"}'::jsonb,
    now(),
    NULL,
    NULL,
    now()
) ON CONFLICT (model_profile_id) DO UPDATE SET
    profile_name = EXCLUDED.profile_name,
    provider_name = EXCLUDED.provider_name,
    model_name = EXCLUDED.model_name,
    status = 'active',
    budget_policy = EXCLUDED.budget_policy,
    routing_policy = EXCLUDED.routing_policy,
    default_parameters = EXCLUDED.default_parameters,
    effective_to = NULL;

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
    benchmark_profile
) VALUES (
    'candidate.cursor.composer-2',
    'provider.cursor',
    'cursor',
    'cursor',
    'composer-2',
    'active',
    500,
    1,
    '["cursor","repo-agent","coding","provider-onboarding","composer-2"]'::jsonb,
    '{
      "provider_slug": "cursor",
      "model_slug": "composer-2",
      "selected_transport": "api",
      "context_window": 128000,
      "catalog_source": "migration.214"
    }'::jsonb,
    now(),
    NULL,
    'migration.214.cursor_api_composer_default',
    now(),
    '{}'::jsonb,
    'high',
    1,
    'reasoning',
    1,
    '{}'::jsonb,
    '{
      "primary": ["build", "review", "architecture"],
      "secondary": ["analysis", "research"],
      "specialized": ["repo-agent"],
      "avoid": ["general-routing"]
    }'::jsonb,
    '{
      "evidence_level": "live_provider_discovery",
      "positioning": "Cursor background-agent API Composer model for explicit repo-scoped execution.",
      "source_refs": ["cursor_background_agent_api", "provider_onboarding_probe_20260424"]
    }'::jsonb
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
    effective_from = EXCLUDED.effective_from,
    effective_to = EXCLUDED.effective_to,
    decision_ref = EXCLUDED.decision_ref,
    cli_config = EXCLUDED.cli_config,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    task_affinities = EXCLUDED.task_affinities,
    benchmark_profile = EXCLUDED.benchmark_profile;

INSERT INTO model_profile_candidate_bindings (
    model_profile_candidate_binding_id,
    model_profile_id,
    candidate_ref,
    binding_role,
    position_index,
    effective_from,
    effective_to,
    created_at
) VALUES (
    'binding.provider-onboarding.cursor.composer-2',
    'model_profile.provider-onboarding.cursor.composer-2',
    'candidate.cursor.composer-2',
    'primary',
    0,
    now(),
    NULL,
    now()
) ON CONFLICT (model_profile_candidate_binding_id) DO UPDATE SET
    model_profile_id = EXCLUDED.model_profile_id,
    candidate_ref = EXCLUDED.candidate_ref,
    binding_role = EXCLUDED.binding_role,
    position_index = EXCLUDED.position_index,
    effective_to = NULL;

UPDATE provider_model_candidates
   SET status = 'inactive',
       effective_to = COALESCE(effective_to, now()),
       decision_ref = 'migration.214.cursor_api_composer_default'
 WHERE provider_slug = 'cursor'
   AND model_slug = 'auto';

INSERT INTO task_type_routing (
    task_type,
    provider_slug,
    model_slug,
    permitted,
    rank,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    route_health_score,
    route_source,
    recent_successes,
    recent_failures,
    observed_completed_count,
    observed_execution_failure_count,
    observed_external_failure_count,
    observed_config_failure_count,
    observed_downstream_failure_count,
    observed_downstream_bug_count,
    consecutive_internal_failures,
    last_failure_category,
    last_failure_zone
)
SELECT task_type,
       'cursor',
       'composer-2',
       permitted,
       LEAST(rank, 500),
       'high',
       1,
       'reasoning',
       1,
       COALESCE(reasoning_control, '{}'::jsonb),
       route_health_score,
       route_source,
       recent_successes,
       recent_failures,
       observed_completed_count,
       observed_execution_failure_count,
       observed_external_failure_count,
       observed_config_failure_count,
       observed_downstream_failure_count,
       observed_downstream_bug_count,
       consecutive_internal_failures,
       last_failure_category,
       last_failure_zone
  FROM task_type_routing
 WHERE provider_slug = 'cursor'
   AND model_slug = 'auto'
ON CONFLICT (task_type, provider_slug, model_slug) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = LEAST(task_type_routing.rank, EXCLUDED.rank),
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    route_health_score = EXCLUDED.route_health_score,
    route_source = EXCLUDED.route_source;

UPDATE task_type_routing
   SET permitted = false,
       route_source = 'retired_by_migration_214'
 WHERE provider_slug = 'cursor'
   AND model_slug = 'auto';

COMMIT;
