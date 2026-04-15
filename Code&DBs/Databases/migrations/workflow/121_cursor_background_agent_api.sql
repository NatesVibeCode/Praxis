-- Migration 121: Convert Cursor to the background-agent API authority
--
-- Cursor is not a local prompt CLI in this architecture. The canonical
-- workflow lane is the repository-backed background-agent API.

BEGIN;

UPDATE provider_transport_admissions
   SET admitted_by_policy = false,
       policy_reason = 'Cursor CLI prompt execution is retired; use the background-agent API lane.',
       status = 'inactive',
       updated_at = now()
 WHERE provider_slug = 'cursor'
   AND adapter_type = 'cli_llm';

INSERT INTO provider_cli_profiles (
    provider_slug,
    binary_name,
    base_flags,
    model_flag,
    system_prompt_flag,
    json_schema_flag,
    output_format,
    output_envelope_key,
    forbidden_flags,
    default_timeout,
    aliases,
    status,
    default_model,
    api_endpoint,
    api_protocol_family,
    api_key_env_vars,
    adapter_economics,
    prompt_mode,
    updated_at,
    created_at
) VALUES (
    'cursor',
    'cursor-api',
    '[]'::jsonb,
    NULL,
    NULL,
    NULL,
    'text',
    'text',
    '[]'::jsonb,
    900,
    '[]'::jsonb,
    'active',
    'auto',
    'https://api.cursor.com/v0/agents',
    'cursor_background_agent',
    '["CURSOR_API_KEY"]'::jsonb,
    '{
      "llm_task": {
        "billing_mode": "subscription_included",
        "budget_bucket": "cursor_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": true,
        "allow_payg_fallback": false
      }
    }'::jsonb,
    'stdin',
    now(),
    now()
) ON CONFLICT (provider_slug) DO UPDATE SET
    binary_name = EXCLUDED.binary_name,
    base_flags = EXCLUDED.base_flags,
    model_flag = EXCLUDED.model_flag,
    system_prompt_flag = EXCLUDED.system_prompt_flag,
    json_schema_flag = EXCLUDED.json_schema_flag,
    output_format = EXCLUDED.output_format,
    output_envelope_key = EXCLUDED.output_envelope_key,
    forbidden_flags = EXCLUDED.forbidden_flags,
    default_timeout = EXCLUDED.default_timeout,
    aliases = EXCLUDED.aliases,
    status = EXCLUDED.status,
    default_model = EXCLUDED.default_model,
    api_endpoint = EXCLUDED.api_endpoint,
    api_protocol_family = EXCLUDED.api_protocol_family,
    api_key_env_vars = EXCLUDED.api_key_env_vars,
    adapter_economics = EXCLUDED.adapter_economics,
    prompt_mode = EXCLUDED.prompt_mode,
    updated_at = now();

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
    'provider_transport_admission.cursor.llm_task',
    'cursor',
    'llm_task',
    'http',
    'repo_agent_http',
    true,
    'Admitted Cursor background-agent API lane; runtime must provide a git-backed workdir with an origin remote.',
    'cursor:llm_task',
    '{"overview":"https://docs.cursor.com/background-agent/api/overview","launch_agent":"https://docs.cursor.com/en/background-agent/api/launch-an-agent","list_models":"https://docs.cursor.com/en/background-agent/api/list-models"}'::jsonb,
    '["CURSOR_API_KEY"]'::jsonb,
    '{
      "model_discovery_probe": {
        "strategy": "cursor_models_list",
        "status": "seeded"
      },
      "prompt_probe": {
        "strategy": "api_model_discovery_auth_probe",
        "status": "seeded"
      },
      "runtime_preconditions": {
        "git_workdir_required": true,
        "origin_remote_required": true,
        "named_branch_required": true
      }
    }'::jsonb,
    'migration.121.cursor_background_agent_api',
    'active',
    now(),
    now()
) ON CONFLICT (provider_slug, adapter_type) DO UPDATE SET
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
    'candidate.cursor.auto',
    'provider.cursor',
    'cursor',
    'cursor',
    'auto',
    'active',
    950,
    3,
    '["cursor","repo-agent","coding"]'::jsonb,
    '{
      "provider_slug": "cursor",
      "model_slug": "auto",
      "selected_transport": "api",
      "context_window": 128000,
      "catalog_source": "migration.121"
    }'::jsonb,
    now(),
    NULL,
    'migration.121.cursor_background_agent_api',
    now(),
    '{}'::jsonb,
    'low',
    1,
    'instant',
    1,
    '{}'::jsonb,
    '{
      "primary": ["agentic-coding"],
      "secondary": ["build","review"],
      "specialized": ["repo-agent"],
      "avoid": ["general-routing"]
    }'::jsonb,
    '{
      "evidence_level": "vendor_positioning",
      "positioning": "Cursor background-agent API default auto model for explicit repo-scoped execution.",
      "source_refs": ["cursor_background_agent_api"]
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

COMMIT;
