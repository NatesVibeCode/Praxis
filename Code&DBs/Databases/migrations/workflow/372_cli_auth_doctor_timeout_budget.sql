-- Migration 372: Give cli_auth_doctor enough catalog timeout for Codex cold start.
--
-- The doctor probes claude, codex, and gemini sequentially. Codex can take
-- more than 15 seconds to cold-start inside the Docker worker while still
-- authenticating successfully. The operation catalog had a 15s timeout, so
-- the gateway wrapper reported operation.interactive_timeout even when the
-- underlying CLI auth was healthy.

BEGIN;

UPDATE operation_catalog_registry
   SET timeout_ms = 60000,
       binding_revision = 'binding.operation_catalog_registry.cli_auth_doctor.20260430.timeout60s',
       updated_at = now()
 WHERE operation_ref = 'cli-auth-doctor'
   AND operation_name = 'cli_auth_doctor';

COMMIT;

