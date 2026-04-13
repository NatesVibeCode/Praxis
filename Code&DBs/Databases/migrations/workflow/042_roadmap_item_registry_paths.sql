-- Optional repo-path references for roadmap items.
--
-- These paths let roadmap authority point directly at the registry, migration,
-- and dispatch-plan files that define or validate one work item.

ALTER TABLE roadmap_items
    ADD COLUMN IF NOT EXISTS registry_paths jsonb;

COMMENT ON COLUMN roadmap_items.registry_paths IS
    'Optional JSON array of repo-relative registry or dispatch-reference paths for one roadmap item. NULL means no explicit path set.';
