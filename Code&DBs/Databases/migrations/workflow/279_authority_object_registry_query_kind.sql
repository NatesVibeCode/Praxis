-- Migration 279: Allow operation_kind='query' in authority_object_registry
-- and align existing search-op rows.
--
-- Closes BUG-36B00C79: data_dictionary_objects.category already accepts
-- 'query', but authority_object_registry.object_kind only accepts
-- {table, column, command, event, projection, service_bus_channel,
-- feedback_stream, definition, runtime_target}. That mismatch forces
-- every CQRS query operation to register itself as object_kind='command'
-- in authority_object_registry, contradicting operation_catalog_registry's
-- own operation_kind='query' value. Future migrations either trip the
-- constraint or perpetuate the mismatch.
--
-- This migration:
--   1. Drops + recreates authority_object_registry_object_kind_check with
--      'query' added.
--   2. Updates the 10 search.* rows registered by migration 278 from
--      object_kind='command' to 'query' so they align with their
--      operation_catalog_registry counterparts.
--   3. Updates the 10 search.* rows in data_dictionary_objects from
--      category='command' to 'query' for the same reason.

BEGIN;

ALTER TABLE authority_object_registry
    DROP CONSTRAINT IF EXISTS authority_object_registry_object_kind_check;

ALTER TABLE authority_object_registry
    ADD CONSTRAINT authority_object_registry_object_kind_check
    CHECK (object_kind = ANY (ARRAY[
        'table'::text,
        'column'::text,
        'command'::text,
        'query'::text,
        'event'::text,
        'projection'::text,
        'service_bus_channel'::text,
        'feedback_stream'::text,
        'definition'::text,
        'runtime_target'::text
    ]));

UPDATE authority_object_registry
   SET object_kind = 'query',
       updated_at = now()
 WHERE object_ref LIKE 'operation.search.%'
   AND object_kind = 'command';

UPDATE data_dictionary_objects
   SET category = 'query',
       updated_at = now()
 WHERE object_kind LIKE 'operation.search.%'
   AND category = 'command';

COMMIT;

-- Verification (run manually):
--   SELECT object_kind, COUNT(*) FROM authority_object_registry
--    WHERE object_ref LIKE 'operation.search.%' GROUP BY object_kind;
--   -- expected: ('query', 10)
--   SELECT category, COUNT(*) FROM data_dictionary_objects
--    WHERE object_kind LIKE 'operation.search.%' GROUP BY category;
--   -- expected: ('query', 10)
