-- Canonical sandbox-profile authority rows for runtime execution.
-- Runtime profiles point at one sandbox profile; execution consumes the
-- resolved sandbox contract instead of reconstructing it ad hoc.

CREATE TABLE IF NOT EXISTS registry_sandbox_profile_authority (
    sandbox_profile_ref text PRIMARY KEY,
    sandbox_provider text NOT NULL,
    docker_image text NULL,
    docker_cpus text NULL,
    docker_memory text NULL,
    network_policy text NOT NULL,
    workspace_materialization text NOT NULL,
    secret_allowlist jsonb NOT NULL DEFAULT '[]'::jsonb,
    auth_mount_policy text NOT NULL DEFAULT 'provider_scoped',
    timeout_profile text NOT NULL DEFAULT 'default',
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_sandbox_profile_authority_ref_nonblank
        CHECK (btrim(sandbox_profile_ref) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_provider_nonblank
        CHECK (btrim(sandbox_provider) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_network_policy_nonblank
        CHECK (btrim(network_policy) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_materialization_nonblank
        CHECK (btrim(workspace_materialization) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_auth_mount_policy_nonblank
        CHECK (btrim(auth_mount_policy) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_timeout_profile_nonblank
        CHECK (btrim(timeout_profile) <> ''),
    CONSTRAINT registry_sandbox_profile_authority_secret_allowlist_array
        CHECK (jsonb_typeof(secret_allowlist) = 'array')
);

COMMENT ON TABLE registry_sandbox_profile_authority IS 'Canonical sandbox execution profiles. Owned by registry/.';
COMMENT ON COLUMN registry_sandbox_profile_authority.sandbox_profile_ref IS 'Stable sandbox-profile authority ref bound from runtime profiles.';
COMMENT ON COLUMN registry_sandbox_profile_authority.sandbox_provider IS 'Canonical sandbox substrate (docker_local or cloudflare_remote).';
COMMENT ON COLUMN registry_sandbox_profile_authority.docker_image IS 'Pinned worker image ref for docker_local execution.';
COMMENT ON COLUMN registry_sandbox_profile_authority.docker_cpus IS 'Canonical CPU limit for docker_local execution.';
COMMENT ON COLUMN registry_sandbox_profile_authority.docker_memory IS 'Canonical memory limit for docker_local execution.';
COMMENT ON COLUMN registry_sandbox_profile_authority.network_policy IS 'Canonical network policy enforced by the sandbox.';
COMMENT ON COLUMN registry_sandbox_profile_authority.workspace_materialization IS 'Canonical workspace hydration mode for the sandbox.';
COMMENT ON COLUMN registry_sandbox_profile_authority.secret_allowlist IS 'Extra secret env names explicitly allowed into the sandbox.';
COMMENT ON COLUMN registry_sandbox_profile_authority.auth_mount_policy IS 'How CLI auth files may be mounted into docker sandboxes.';
COMMENT ON COLUMN registry_sandbox_profile_authority.timeout_profile IS 'Named timeout contract for the sandbox.';

ALTER TABLE registry_runtime_profile_authority
    ADD COLUMN IF NOT EXISTS sandbox_profile_ref text;

UPDATE registry_runtime_profile_authority
SET sandbox_profile_ref = runtime_profile_ref
WHERE sandbox_profile_ref IS NULL
   OR btrim(sandbox_profile_ref) = '';

ALTER TABLE registry_runtime_profile_authority
    ALTER COLUMN sandbox_profile_ref SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'registry_runtime_profile_authority_sandbox_profile_ref_nonblank'
          AND conrelid = 'registry_runtime_profile_authority'::regclass
    ) THEN
        ALTER TABLE registry_runtime_profile_authority
            ADD CONSTRAINT registry_runtime_profile_authority_sandbox_profile_ref_nonblank
            CHECK (btrim(sandbox_profile_ref) <> '');
    END IF;
END
$$;

COMMENT ON COLUMN registry_runtime_profile_authority.sandbox_profile_ref IS 'Canonical sandbox profile bound to the runtime profile.';
