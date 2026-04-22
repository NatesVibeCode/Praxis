-- Migration 199: workspace base-path authority.
--
-- Workspace identity is stable; filesystem roots are host-local. This keeps
-- registry_workspace_authority as the logical workspace authority while moving
-- host-local path choice behind an explicit base-path registry row.

BEGIN;

CREATE TABLE IF NOT EXISTS registry_workspace_base_path_authority (
    base_path_ref text PRIMARY KEY,
    workspace_ref text NOT NULL,
    base_path text NOT NULL,
    host_ref text NOT NULL DEFAULT 'default',
    is_active boolean NOT NULL DEFAULT true,
    priority integer NOT NULL DEFAULT 100,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT registry_workspace_base_path_ref_nonblank
        CHECK (btrim(base_path_ref) <> ''),
    CONSTRAINT registry_workspace_base_path_workspace_ref_nonblank
        CHECK (btrim(workspace_ref) <> ''),
    CONSTRAINT registry_workspace_base_path_base_path_nonblank
        CHECK (btrim(base_path) <> ''),
    CONSTRAINT registry_workspace_base_path_host_ref_nonblank
        CHECK (btrim(host_ref) <> ''),
    CONSTRAINT registry_workspace_base_path_workspace_fkey
        FOREIGN KEY (workspace_ref)
        REFERENCES registry_workspace_authority(workspace_ref)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS registry_workspace_base_path_active_idx
    ON registry_workspace_base_path_authority (workspace_ref, is_active, priority, recorded_at DESC);

ALTER TABLE registry_workspace_authority
    ADD COLUMN IF NOT EXISTS base_path_ref text,
    ADD COLUMN IF NOT EXISTS repo_root_path text,
    ADD COLUMN IF NOT EXISTS workdir_path text;

ALTER TABLE registry_workspace_authority
    DROP CONSTRAINT IF EXISTS registry_workspace_authority_repo_root_path_nonblank,
    DROP CONSTRAINT IF EXISTS registry_workspace_authority_workdir_path_nonblank,
    DROP CONSTRAINT IF EXISTS registry_workspace_authority_base_path_ref_fkey;

ALTER TABLE registry_workspace_authority
    ADD CONSTRAINT registry_workspace_authority_repo_root_path_nonblank
        CHECK (repo_root_path IS NULL OR btrim(repo_root_path) <> ''),
    ADD CONSTRAINT registry_workspace_authority_workdir_path_nonblank
        CHECK (workdir_path IS NULL OR btrim(workdir_path) <> '');

INSERT INTO registry_workspace_base_path_authority (
    base_path_ref,
    workspace_ref,
    base_path,
    host_ref,
    is_active,
    priority
) VALUES (
    'workspace_base.praxis.default',
    'praxis',
    '${PRAXIS_WORKSPACE_BASE_PATH}',
    'default',
    true,
    100
)
ON CONFLICT (base_path_ref) DO UPDATE
SET workspace_ref = EXCLUDED.workspace_ref,
    base_path = EXCLUDED.base_path,
    host_ref = EXCLUDED.host_ref,
    is_active = EXCLUDED.is_active,
    priority = EXCLUDED.priority,
    recorded_at = now();

UPDATE registry_workspace_authority
SET base_path_ref = COALESCE(base_path_ref, 'workspace_base.praxis.default'),
    repo_root_path = COALESCE(repo_root_path, '.'),
    workdir_path = COALESCE(workdir_path, '.'),
    recorded_at = now()
WHERE workspace_ref = 'praxis';

ALTER TABLE registry_workspace_authority
    ADD CONSTRAINT registry_workspace_authority_base_path_ref_fkey
    FOREIGN KEY (base_path_ref)
    REFERENCES registry_workspace_base_path_authority(base_path_ref)
    ON DELETE RESTRICT;

COMMENT ON TABLE registry_workspace_base_path_authority IS
    'Host-local base path authority for logical workspaces. Workspace rows store stable identity plus relative repo/workdir paths.';
COMMENT ON COLUMN registry_workspace_base_path_authority.base_path IS
    'Absolute base path or ${ENV_VAR} token resolved by the runtime on the executing host.';
COMMENT ON COLUMN registry_workspace_authority.base_path_ref IS
    'Optional base-path authority row used to resolve repo_root_path and workdir_path.';
COMMENT ON COLUMN registry_workspace_authority.repo_root_path IS
    'Repo path relative to base_path_ref when present. Legacy repo_root remains for compatibility.';
COMMENT ON COLUMN registry_workspace_authority.workdir_path IS
    'Workdir path relative to resolved repo root when base_path_ref is present. Legacy workdir remains for compatibility.';

COMMIT;
