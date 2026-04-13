-- Canonical registry authority rows for workspace and runtime-profile resolution.
-- These rows are authoritative input to registry/, not a derived projection.

CREATE TABLE IF NOT EXISTS registry_workspace_authority (
    workspace_ref text PRIMARY KEY,
    repo_root text NOT NULL,
    workdir text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_workspace_authority_workspace_ref_nonblank
        CHECK (btrim(workspace_ref) <> ''),
    CONSTRAINT registry_workspace_authority_repo_root_nonblank
        CHECK (btrim(repo_root) <> ''),
    CONSTRAINT registry_workspace_authority_workdir_nonblank
        CHECK (btrim(workdir) <> '')
);

COMMENT ON TABLE registry_workspace_authority IS 'Canonical workspace boundary authority. Owned by registry/.';
COMMENT ON COLUMN registry_workspace_authority.workspace_ref IS 'Stable workspace authority ref used by request intake.';
COMMENT ON COLUMN registry_workspace_authority.repo_root IS 'Canonical repository root for the workspace boundary.';
COMMENT ON COLUMN registry_workspace_authority.workdir IS 'Canonical execution workdir inside the workspace boundary.';

CREATE TABLE IF NOT EXISTS registry_runtime_profile_authority (
    runtime_profile_ref text PRIMARY KEY,
    model_profile_id text NOT NULL,
    provider_policy_id text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_runtime_profile_authority_runtime_profile_ref_nonblank
        CHECK (btrim(runtime_profile_ref) <> ''),
    CONSTRAINT registry_runtime_profile_authority_model_profile_id_nonblank
        CHECK (btrim(model_profile_id) <> ''),
    CONSTRAINT registry_runtime_profile_authority_provider_policy_id_nonblank
        CHECK (btrim(provider_policy_id) <> '')
);

COMMENT ON TABLE registry_runtime_profile_authority IS 'Canonical runtime-profile authority rows. Owned by registry/.';
COMMENT ON COLUMN registry_runtime_profile_authority.runtime_profile_ref IS 'Stable runtime-profile authority ref used by request intake.';
COMMENT ON COLUMN registry_runtime_profile_authority.model_profile_id IS 'Canonical model profile bound to the runtime profile.';
COMMENT ON COLUMN registry_runtime_profile_authority.provider_policy_id IS 'Canonical provider policy bound to the runtime profile.';
