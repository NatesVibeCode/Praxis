-- Canonical DB-native authority for repo-local native runtime resolution.
--
-- The repo-local runtime no longer reads config/runtime_profiles.json as live
-- authority. Instead, Postgres owns:
--   - the default native runtime profile
--   - the native runtime profile metadata used for routing and instance setup
--   - the repo-local workspace/runtime/sandbox seed rows for the Praxis clone

CREATE TABLE IF NOT EXISTS registry_native_runtime_profile_authority (
    runtime_profile_ref text PRIMARY KEY,
    workspace_ref text NOT NULL,
    instance_name text NOT NULL,
    provider_name text NOT NULL,
    provider_names jsonb NOT NULL DEFAULT '[]'::jsonb,
    allowed_models jsonb NOT NULL DEFAULT '[]'::jsonb,
    receipts_dir text NOT NULL,
    topology_dir text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_native_runtime_profile_authority_runtime_profile_ref_nonblank
        CHECK (btrim(runtime_profile_ref) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_workspace_ref_nonblank
        CHECK (btrim(workspace_ref) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_instance_name_nonblank
        CHECK (btrim(instance_name) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_provider_name_nonblank
        CHECK (btrim(provider_name) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_receipts_dir_nonblank
        CHECK (btrim(receipts_dir) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_topology_dir_nonblank
        CHECK (btrim(topology_dir) <> ''),
    CONSTRAINT registry_native_runtime_profile_authority_provider_names_array
        CHECK (jsonb_typeof(provider_names) = 'array'),
    CONSTRAINT registry_native_runtime_profile_authority_allowed_models_array
        CHECK (jsonb_typeof(allowed_models) = 'array'),
    CONSTRAINT registry_native_runtime_profile_authority_runtime_profile_fkey
        FOREIGN KEY (runtime_profile_ref)
        REFERENCES registry_runtime_profile_authority(runtime_profile_ref)
        ON DELETE CASCADE,
    CONSTRAINT registry_native_runtime_profile_authority_workspace_fkey
        FOREIGN KEY (workspace_ref)
        REFERENCES registry_workspace_authority(workspace_ref)
        ON DELETE CASCADE
);

COMMENT ON TABLE registry_native_runtime_profile_authority IS 'Canonical native runtime-profile authority for repo-local runtime defaults and routing metadata.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.runtime_profile_ref IS 'Stable native runtime-profile ref resolved by repo-local native surfaces.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.workspace_ref IS 'Canonical workspace authority ref bound to the native runtime profile.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.instance_name IS 'Canonical native instance label surfaced in native contracts.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.provider_name IS 'Primary provider for the native runtime policy.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.provider_names IS 'Ordered provider allowlist for native runtime candidate projection.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.allowed_models IS 'Canonical model allowlist for native runtime candidate projection.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.receipts_dir IS 'Repo-relative or absolute receipts directory for the native runtime.';
COMMENT ON COLUMN registry_native_runtime_profile_authority.topology_dir IS 'Repo-relative or absolute topology directory for the native runtime.';

CREATE TABLE IF NOT EXISTS registry_native_runtime_defaults (
    authority_key text PRIMARY KEY,
    runtime_profile_ref text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_native_runtime_defaults_authority_key_nonblank
        CHECK (btrim(authority_key) <> ''),
    CONSTRAINT registry_native_runtime_defaults_authority_key_default_only
        CHECK (authority_key = 'default'),
    CONSTRAINT registry_native_runtime_defaults_runtime_profile_ref_nonblank
        CHECK (btrim(runtime_profile_ref) <> ''),
    CONSTRAINT registry_native_runtime_defaults_runtime_profile_fkey
        FOREIGN KEY (runtime_profile_ref)
        REFERENCES registry_native_runtime_profile_authority(runtime_profile_ref)
        ON DELETE CASCADE
);

COMMENT ON TABLE registry_native_runtime_defaults IS 'Singleton native runtime default selection. Owned by registry/.';
COMMENT ON COLUMN registry_native_runtime_defaults.authority_key IS 'Singleton selector key; currently only ''default'' is valid.';
COMMENT ON COLUMN registry_native_runtime_defaults.runtime_profile_ref IS 'Canonical default native runtime profile ref.';

INSERT INTO registry_workspace_authority (
    workspace_ref,
    repo_root,
    workdir
) VALUES (
    'praxis',
    '.',
    '.'
)
ON CONFLICT (workspace_ref) DO UPDATE
SET repo_root = EXCLUDED.repo_root,
    workdir = EXCLUDED.workdir,
    recorded_at = now();

INSERT INTO registry_runtime_profile_authority (
    runtime_profile_ref,
    model_profile_id,
    provider_policy_id,
    sandbox_profile_ref
) VALUES (
    'praxis',
    'model_profile.praxis.default',
    'provider_policy.praxis.default',
    'sandbox_profile.praxis.default'
)
ON CONFLICT (runtime_profile_ref) DO UPDATE
SET model_profile_id = EXCLUDED.model_profile_id,
    provider_policy_id = EXCLUDED.provider_policy_id,
    sandbox_profile_ref = EXCLUDED.sandbox_profile_ref,
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
    'sandbox_profile.praxis.default',
    'docker_local',
    'praxis-worker:latest',
    '2',
    '4g',
    'provider_only',
    'copy',
    '["ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY"]'::jsonb,
    'provider_scoped',
    'default'
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
    'praxis',
    'praxis',
    'praxis',
    'openai',
    '["openai", "anthropic", "google"]'::jsonb,
    '["gpt-5.4", "claude-opus-4-7", "claude-sonnet-4-6", "gemini-3.1-pro-preview", "gpt-5.4-mini", "gemini-3-flash-preview", "claude-haiku-4-5-20251001"]'::jsonb,
    'artifacts/runtime_receipts',
    'artifacts/runtime_topology'
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

INSERT INTO registry_native_runtime_defaults (
    authority_key,
    runtime_profile_ref
) VALUES (
    'default',
    'praxis'
)
ON CONFLICT (authority_key) DO UPDATE
SET runtime_profile_ref = EXCLUDED.runtime_profile_ref,
    recorded_at = now();
