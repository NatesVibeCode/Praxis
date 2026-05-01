-- Migration 404: Rename Compile→Materialize across event_type strings.
--
-- The operation_catalog_registry was migrated in 401 (operation rows). The
-- DB tables were migrated in 403. This migration is the third and final
-- layer of the public Compile→Materialize rename: event_type strings
-- emitted by command operations.
--
-- Renames:
--   event_type compile.materialized      → materialize.committed
--   event_type workflow_context.compiled → workflow_context.materialized
--
-- Pieces touched:
--   1. operation_catalog_registry.event_type — for the renamed operations
--      (materialize_commit, workflow_context_materialize).
--   2. authority_event_contracts — rename the row for workflow_context.compiled.
--      No row exists for compile.materialized today, so no contract row to
--      rename for materialize_commit; nothing to do there.
--   3. Historical authority_events rows retain their old event_type strings —
--      receipts and event-stream rows are immutable history.
--
-- Code-side: scripts/probes/compile_delivery_probe.py + the docstring in
-- runtime/operations/commands/materialize_commit.py still reference the
-- old strings; those are fixed in this same session via sed.

BEGIN;

UPDATE operation_catalog_registry
SET event_type = 'materialize.committed'
WHERE operation_name = 'materialize_commit'
  AND event_type = 'compile.materialized';

UPDATE operation_catalog_registry
SET event_type = 'workflow_context.materialized'
WHERE operation_name = 'workflow_context_materialize'
  AND event_type = 'workflow_context.compiled';

UPDATE authority_event_contracts
SET event_type = 'workflow_context.materialized'
WHERE event_type = 'workflow_context.compiled';

COMMIT;
