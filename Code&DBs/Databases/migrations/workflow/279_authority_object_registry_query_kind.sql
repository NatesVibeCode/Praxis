-- Migration 279: Allow operation_kind='query' in authority_object_registry
-- and align operation rows from the catalog authority.
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
--   2. Upserts operation data-dictionary rows from operation_catalog_registry
--      using operation_kind as the category.
--   3. Upserts operation authority-object rows from operation_catalog_registry
--      using operation_kind as object_kind.

BEGIN;

DO $$
DECLARE
    constraint_body text;
BEGIN
    SELECT pg_get_constraintdef(oid)
      INTO constraint_body
      FROM pg_constraint
     WHERE conname = 'authority_object_registry_object_kind_check'
       AND conrelid = 'authority_object_registry'::regclass;

    IF constraint_body IS NULL OR constraint_body NOT LIKE '%query%' THEN
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
    END IF;
END $$;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    'operation.' || operation_name,
    operation_name,
    operation_kind,
    'Operation catalog entry owned by ' || authority_domain_ref,
    jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', operation_ref),
    jsonb_build_object(
        'authority_domain_ref', authority_domain_ref,
        'operation_kind', operation_kind,
        'source_kind', source_kind,
        'handler_ref', handler_ref
    )
FROM operation_catalog_registry
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = data_dictionary_objects.origin_ref || EXCLUDED.origin_ref,
    metadata = data_dictionary_objects.metadata || EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
)
SELECT
    'operation.' || operation_name,
    operation_kind,
    operation_name,
    NULL,
    authority_domain_ref,
    'operation.' || operation_name,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    CASE WHEN operation_kind = 'query' THEN 'read_model' ELSE 'command_model' END,
    'praxis.engine',
    decision_ref,
    jsonb_build_object(
        'operation_ref', operation_ref,
        'operation_kind', operation_kind,
        'source_kind', source_kind,
        'handler_ref', handler_ref
    )
FROM operation_catalog_registry
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = authority_object_registry.metadata || EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT operation_kind, COUNT(*) FROM operation_catalog_registry GROUP BY operation_kind;
--   SELECT object_kind, COUNT(*) FROM authority_object_registry
--    WHERE object_ref LIKE 'operation.%' GROUP BY object_kind;
--   SELECT category, COUNT(*) FROM data_dictionary_objects
--    WHERE object_kind LIKE 'operation.%' GROUP BY category;
