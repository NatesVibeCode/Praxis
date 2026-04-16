ALTER TABLE fork_worktree_bindings
    ADD COLUMN IF NOT EXISTS materialized_repo_root text,
    ADD COLUMN IF NOT EXISTS materialized_workdir text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'fork_worktree_bindings_materialized_repo_root_nonblank'
    ) THEN
        ALTER TABLE fork_worktree_bindings
            ADD CONSTRAINT fork_worktree_bindings_materialized_repo_root_nonblank
            CHECK (
                materialized_repo_root IS NULL
                OR btrim(materialized_repo_root) <> ''
            );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'fork_worktree_bindings_materialized_workdir_nonblank'
    ) THEN
        ALTER TABLE fork_worktree_bindings
            ADD CONSTRAINT fork_worktree_bindings_materialized_workdir_nonblank
            CHECK (
                materialized_workdir IS NULL
                OR btrim(materialized_workdir) <> ''
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS fork_worktree_bindings_run_status_idx
    ON fork_worktree_bindings (workflow_run_id, binding_status, created_at DESC);

COMMENT ON COLUMN fork_worktree_bindings.materialized_repo_root IS 'Canonical host repo-root path for the active fork/worktree binding. Docker execution must hydrate from this authority instead of inferring shell state.';
COMMENT ON COLUMN fork_worktree_bindings.materialized_workdir IS 'Canonical host workdir path for the active fork/worktree binding. This is the workspace boundary hydrated into Docker for the bound run.';
