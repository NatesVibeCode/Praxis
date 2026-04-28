-- Migration 314: Advertise the canonical compile materialize HTTP path.
--
-- `compile_materialize` was registered with `http_path='/api/compile_materialize'`,
-- which technically worked once the direct route existed, but it drifted from the
-- shared compile front door vocabulary (`/api/compile/preview`) and made API
-- discovery look like a legacy one-off instead of part of the compile family.
--
-- Canonicalize the catalog row to `/api/compile/materialize`. The REST surface
-- keeps `/api/compile_materialize` mounted as a compatibility alias so older
-- clients and stale receipts keep working while fresh discovery points callers at
-- the cleaner route.

BEGIN;

UPDATE operation_catalog_registry
   SET http_path = '/api/compile/materialize',
       binding_revision = 'binding.operation_catalog_registry.compile_materialize.20260428',
       updated_at = now()
 WHERE operation_ref = 'compile.materialize'
   AND operation_name = 'compile_materialize';

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, http_path
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'compile.materialize';
