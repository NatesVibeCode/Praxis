-- Scratch agent lane for non-repo tool tasks.
--
-- This profile keeps model/tool execution in a low-authority Docker sandbox
-- without hydrating the Praxis workspace. Durable state remains in operator
-- and receipt surfaces; the container is execution only.

INSERT INTO registry_workspace_authority (
    workspace_ref,
    repo_root,
    workdir
) VALUES (
    'scratch_agent',
    '.',
    '.'
)
ON CONFLICT (workspace_ref) DO UPDATE
SET repo_root = EXCLUDED.repo_root,
    workdir = EXCLUDED.workdir,
    recorded_at = now();

INSERT INTO registry_sandbox_profile_authority (
    sandbox_profile_ref,
    sandbox_provider,
    docker_image,
    docker_cpus,
    docker_memory,
    network_policy,
    workspace_materialization,
    secret_allowlist,
    auth_mount_policy,
    timeout_profile
) VALUES (
    'sandbox_profile.scratch_agent.default',
    'docker_local',
    'praxis-worker:latest',
    '1',
    '2g',
    'enabled',
    'none',
    '["ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY"]'::jsonb,
    'provider_scoped',
    'scratch_agent'
)
ON CONFLICT (sandbox_profile_ref) DO UPDATE
SET sandbox_provider = EXCLUDED.sandbox_provider,
    docker_image = EXCLUDED.docker_image,
    docker_cpus = EXCLUDED.docker_cpus,
    docker_memory = EXCLUDED.docker_memory,
    network_policy = EXCLUDED.network_policy,
    workspace_materialization = EXCLUDED.workspace_materialization,
    secret_allowlist = EXCLUDED.secret_allowlist,
    auth_mount_policy = EXCLUDED.auth_mount_policy,
    timeout_profile = EXCLUDED.timeout_profile,
    recorded_at = now();

INSERT INTO registry_runtime_profile_authority (
    runtime_profile_ref,
    model_profile_id,
    provider_policy_id,
    sandbox_profile_ref
) VALUES (
    'scratch_agent',
    'model_profile.scratch_agent.default',
    'provider_policy.scratch_agent.default',
    'sandbox_profile.scratch_agent.default'
)
ON CONFLICT (runtime_profile_ref) DO UPDATE
SET model_profile_id = EXCLUDED.model_profile_id,
    provider_policy_id = EXCLUDED.provider_policy_id,
    sandbox_profile_ref = EXCLUDED.sandbox_profile_ref,
    recorded_at = now();

INSERT INTO registry_native_runtime_profile_authority (
    runtime_profile_ref,
    workspace_ref,
    instance_name,
    provider_name,
    provider_names,
    allowed_models,
    receipts_dir,
    topology_dir
) VALUES (
    'scratch_agent',
    'scratch_agent',
    'scratch_agent',
    'openai',
    '["openai", "anthropic", "google"]'::jsonb,
    '["gpt-5.4", "claude-opus-4-6", "claude-sonnet-4-6", "gemini-3.1-pro-preview", "gpt-5.4-mini", "gemini-3-flash-preview", "claude-haiku-4-5-20251001"]'::jsonb,
    'artifacts/runtime_receipts/scratch_agent',
    'artifacts/runtime_topology/scratch_agent'
)
ON CONFLICT (runtime_profile_ref) DO UPDATE
SET workspace_ref = EXCLUDED.workspace_ref,
    instance_name = EXCLUDED.instance_name,
    provider_name = EXCLUDED.provider_name,
    provider_names = EXCLUDED.provider_names,
    allowed_models = EXCLUDED.allowed_models,
    receipts_dir = EXCLUDED.receipts_dir,
    topology_dir = EXCLUDED.topology_dir,
    recorded_at = now();
