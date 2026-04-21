-- BUG-490C9C95: control_commands missing workflow_ prefix
-- -------------------------------------------------------------
-- Minimal backward-compatible fix: expose a workflow_control_commands
-- view that mirrors control_commands. Every other workflow-domain
-- table follows the workflow_ prefix per operator_decisions public
-- naming convention; this lets future readers/writers find the
-- ledger under the expected name without breaking the ~290 callsites
-- that reference control_commands today.
--
-- Full rename is deferred until a dedicated canonical-column migration
-- session (captured in BUG-CB443241 status-vocab drift).

BEGIN;

DROP VIEW IF EXISTS workflow_control_commands;

CREATE VIEW workflow_control_commands AS
SELECT *
FROM control_commands;

COMMENT ON VIEW workflow_control_commands IS
    'Backward-compat alias for control_commands under the workflow_ prefix '
    'per public-naming convention (BUG-490C9C95). Direct writes should still '
    'target control_commands; a future migration will rename the base table.';

COMMIT;
