-- Migration 206: legacy schema authority backfill.
--
-- This is not modernization theater. It gives every existing public table and
-- column an explicit authority/data-dictionary home, while marking historical
-- objects as legacy until a real domain authority owns their command/event
-- lifecycle.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.legacy_schema',
    'praxis.engine',
    'stream.legacy_schema',
    'projection.legacy.schema_catalog',
    'praxis.primary_postgres',
    TRUE,
    'decision.cqrs_legacy_schema_backfill.20260422'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_projection_registry (
    projection_ref,
    authority_domain_ref,
    source_event_stream_ref,
    reducer_ref,
    storage_target_ref,
    freshness_policy_ref,
    enabled,
    decision_ref
) VALUES (
    'projection.legacy.schema_catalog',
    'authority.legacy_schema',
    'stream.legacy_schema',
    'runtime.authority_objects.list_authority_adoption',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.cqrs_legacy_schema_backfill.20260422'
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_event_stream_ref = EXCLUDED.source_event_stream_ref,
    reducer_ref = EXCLUDED.reducer_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
VALUES ('projection.legacy.schema_catalog', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'projection.legacy.schema_catalog',
        'Legacy schema catalog projection',
        'projection',
        'Read model for legacy schema adoption state.',
        '{"migration":"206_legacy_schema_authority_backfill.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.legacy_schema"}'::jsonb
    ),
    (
        'authority.objects.adoption',
        'List authority adoption state',
        'query',
        'Query operation for table and column CQRS adoption status.',
        '{"migration":"206_legacy_schema_authority_backfill.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.cqrs"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    tables.table_name,
    tables.table_name,
    'table',
    CASE
        WHEN registry.object_ref IS NULL
            THEN 'Legacy public table discovered from information_schema; registered for CQRS adoption tracking.'
        ELSE 'Public table bound to authority object registry.'
    END,
    jsonb_build_object(
        'source', 'information_schema.tables',
        'schema_name', tables.table_schema,
        'table_type', tables.table_type,
        'migration', '206_legacy_schema_authority_backfill.sql'
    ),
    jsonb_build_object(
        'authority_domain_ref', COALESCE(registry.authority_domain_ref, 'authority.legacy_schema'),
        'lifecycle_status', COALESCE(registry.lifecycle_status, 'legacy'),
        'legacy_backfill', registry.object_ref IS NULL
    )
FROM information_schema.tables tables
LEFT JOIN authority_object_registry registry
  ON registry.object_kind = 'table'
 AND registry.schema_name = tables.table_schema
 AND registry.object_name = tables.table_name
WHERE tables.table_schema = 'public'
  AND tables.table_type = 'BASE TABLE'
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = data_dictionary_objects.metadata || EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    default_value,
    valid_values,
    examples,
    deprecation_notes,
    display_order,
    origin_ref,
    metadata
)
SELECT
    columns.table_name,
    columns.column_name,
    'auto',
    CASE
        WHEN columns.data_type IN ('boolean') THEN 'boolean'
        WHEN columns.data_type IN (
            'smallint',
            'integer',
            'bigint',
            'decimal',
            'numeric',
            'real',
            'double precision',
            'smallserial',
            'serial',
            'bigserial'
        ) THEN 'number'
        WHEN columns.data_type IN ('json', 'jsonb') THEN 'json'
        WHEN columns.data_type = 'ARRAY' THEN 'array'
        WHEN columns.data_type = 'date' THEN 'date'
        WHEN columns.data_type LIKE 'timestamp%' THEN 'datetime'
        ELSE 'text'
    END,
    columns.column_name,
    'Column discovered from information_schema during legacy CQRS authority backfill.',
    columns.is_nullable = 'NO',
    CASE
        WHEN columns.column_default IS NULL THEN NULL
        ELSE to_jsonb(columns.column_default)
    END,
    '[]'::jsonb,
    '[]'::jsonb,
    '',
    columns.ordinal_position * 10,
    jsonb_build_object(
        'source', 'information_schema.columns',
        'schema_name', columns.table_schema,
        'table_name', columns.table_name,
        'column_name', columns.column_name,
        'migration', '206_legacy_schema_authority_backfill.sql'
    ),
    jsonb_build_object(
        'data_type', columns.data_type,
        'udt_name', columns.udt_name,
        'is_nullable', columns.is_nullable,
        'character_maximum_length', columns.character_maximum_length,
        'numeric_precision', columns.numeric_precision,
        'numeric_scale', columns.numeric_scale
    )
FROM information_schema.columns columns
JOIN information_schema.tables tables
  ON tables.table_schema = columns.table_schema
 AND tables.table_name = columns.table_name
 AND tables.table_type = 'BASE TABLE'
WHERE columns.table_schema = 'public'
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    default_value = EXCLUDED.default_value,
    valid_values = EXCLUDED.valid_values,
    examples = EXCLUDED.examples,
    deprecation_notes = EXCLUDED.deprecation_notes,
    display_order = EXCLUDED.display_order,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
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
    'table.' || tables.table_schema || '.' || tables.table_name,
    'table',
    tables.table_name,
    tables.table_schema,
    'authority.legacy_schema',
    tables.table_name,
    'legacy',
    'reference',
    'praxis.engine',
    'decision.cqrs_legacy_schema_backfill.20260422',
    jsonb_build_object(
        'source', 'information_schema.tables',
        'table_type', tables.table_type,
        'adoption_status', 'legacy_inventory',
        'modernization_required', true
    )
FROM information_schema.tables tables
WHERE tables.table_schema = 'public'
  AND tables.table_type = 'BASE TABLE'
  AND NOT EXISTS (
      SELECT 1
      FROM authority_object_registry registry
      WHERE registry.object_kind = 'table'
        AND registry.schema_name = tables.table_schema
        AND registry.object_name = tables.table_name
  )
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = authority_object_registry.metadata || EXCLUDED.metadata,
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
    'column.' || columns.table_schema || '.' || columns.table_name || '.' || columns.column_name,
    'column',
    columns.table_name || '.' || columns.column_name,
    columns.table_schema,
    table_registry.authority_domain_ref,
    columns.table_name,
    table_registry.lifecycle_status,
    'definition',
    table_registry.owner_ref,
    'decision.cqrs_legacy_schema_backfill.20260422',
    jsonb_build_object(
        'source', 'information_schema.columns',
        'table_name', columns.table_name,
        'column_name', columns.column_name,
        'ordinal_position', columns.ordinal_position,
        'data_type', columns.data_type,
        'udt_name', columns.udt_name,
        'is_nullable', columns.is_nullable,
        'column_default_present', columns.column_default IS NOT NULL
    )
FROM information_schema.columns columns
JOIN authority_object_registry table_registry
  ON table_registry.object_kind = 'table'
 AND table_registry.schema_name = columns.table_schema
 AND table_registry.object_name = columns.table_name
JOIN information_schema.tables tables
  ON tables.table_schema = columns.table_schema
 AND tables.table_name = columns.table_name
 AND tables.table_type = 'BASE TABLE'
WHERE columns.table_schema = 'public'
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
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
) VALUES
    (
        'projection.projection.legacy.schema_catalog',
        'projection',
        'projection.legacy.schema_catalog',
        NULL,
        'authority.legacy_schema',
        'projection.legacy.schema_catalog',
        'active',
        'projection',
        'praxis.engine',
        'decision.cqrs_legacy_schema_backfill.20260422',
        '{"source":"206_legacy_schema_authority_backfill.sql"}'::jsonb
    ),
    (
        'operation.authority.objects.adoption',
        'query',
        'authority.objects.adoption',
        NULL,
        'authority.cqrs',
        'authority.objects.adoption',
        'active',
        'read_model',
        'praxis.engine',
        'decision.cqrs_legacy_schema_backfill.20260422',
        '{"source":"206_legacy_schema_authority_backfill.sql"}'::jsonb
    )
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    authority_domain_ref,
    projection_ref,
    storage_target_ref,
    input_schema_ref,
    output_schema_ref,
    idempotency_key_fields,
    required_capabilities,
    allowed_callers,
    timeout_ms,
    receipt_required,
    event_required,
    event_type,
    projection_freshness_policy_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
) VALUES (
    'authority-objects-adoption',
    'authority.objects.adoption',
    'operation_query',
    'query',
    'GET',
    '/api/authority/objects/adoption',
    'runtime.authority_objects.ListAuthorityAdoptionCommand',
    'runtime.authority_objects.handle_list_authority_adoption',
    'authority.cqrs',
    'authority.cqrs',
    'projection.legacy.schema_catalog',
    'praxis.primary_postgres',
    'runtime.authority_objects.ListAuthorityAdoptionCommand',
    'authority.objects.adoption',
    '[]'::jsonb,
    '{}'::jsonb,
    '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
    15000,
    TRUE,
    FALSE,
    NULL,
    'projection_freshness.default',
    'observe',
    'read_only',
    TRUE,
    'binding.operation_catalog_registry.legacy_schema_backfill.20260422',
    'decision.cqrs_legacy_schema_backfill.20260422'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    projection_ref = EXCLUDED.projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    idempotency_key_fields = EXCLUDED.idempotency_key_fields,
    required_capabilities = EXCLUDED.required_capabilities,
    allowed_callers = EXCLUDED.allowed_callers,
    timeout_ms = EXCLUDED.timeout_ms,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    event_type = EXCLUDED.event_type,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_projection_contracts (
    projection_contract_ref,
    projection_ref,
    authority_domain_ref,
    source_ref_kind,
    source_ref,
    read_model_object_ref,
    freshness_policy_ref,
    last_event_required,
    last_receipt_required,
    failure_visibility_required,
    replay_supported,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'projection_contract.legacy.schema_catalog',
    'projection.legacy.schema_catalog',
    'authority.legacy_schema',
    'authority_view',
    'authority_schema_adoption_report',
    'projection.projection.legacy.schema_catalog',
    'projection_freshness.default',
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    TRUE,
    'decision.cqrs_legacy_schema_backfill.20260422',
    '{"source":"206_legacy_schema_authority_backfill.sql"}'::jsonb
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_ref_kind = EXCLUDED.source_ref_kind,
    source_ref = EXCLUDED.source_ref,
    read_model_object_ref = EXCLUDED.read_model_object_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    last_event_required = EXCLUDED.last_event_required,
    last_receipt_required = EXCLUDED.last_receipt_required,
    failure_visibility_required = EXCLUDED.failure_visibility_required,
    replay_supported = EXCLUDED.replay_supported,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

CREATE OR REPLACE VIEW authority_schema_adoption_report AS
WITH public_tables AS (
    SELECT tables.table_schema, tables.table_name
    FROM information_schema.tables tables
    WHERE tables.table_schema = 'public'
      AND tables.table_type = 'BASE TABLE'
),
table_registry AS (
    SELECT
        public_tables.table_schema,
        public_tables.table_name,
        registry.object_ref,
        registry.authority_domain_ref,
        registry.lifecycle_status,
        registry.write_model_kind,
        registry.owner_ref
    FROM public_tables
    LEFT JOIN authority_object_registry registry
      ON registry.object_kind = 'table'
     AND registry.schema_name = public_tables.table_schema
     AND registry.object_name = public_tables.table_name
),
column_counts AS (
    SELECT
        columns.table_schema,
        columns.table_name,
        count(*)::integer AS column_count
    FROM information_schema.columns columns
    WHERE columns.table_schema = 'public'
    GROUP BY columns.table_schema, columns.table_name
),
registered_column_counts AS (
    SELECT
        registry.schema_name AS table_schema,
        split_part(registry.object_name, '.', 1) AS table_name,
        count(*)::integer AS registered_column_count
    FROM authority_object_registry registry
    WHERE registry.object_kind = 'column'
      AND registry.schema_name = 'public'
    GROUP BY registry.schema_name, split_part(registry.object_name, '.', 1)
)
SELECT
    table_registry.table_schema,
    table_registry.table_name,
    table_registry.object_ref,
    table_registry.authority_domain_ref,
    table_registry.lifecycle_status,
    table_registry.write_model_kind,
    table_registry.owner_ref,
    COALESCE(column_counts.column_count, 0) AS column_count,
    COALESCE(registered_column_counts.registered_column_count, 0) AS registered_column_count,
    CASE
        WHEN table_registry.object_ref IS NULL THEN 'missing_authority_registry'
        WHEN table_registry.authority_domain_ref = 'authority.legacy_schema'
          OR table_registry.lifecycle_status = 'legacy'
            THEN 'legacy_inventory'
        ELSE 'cqrs_adopted'
    END AS adoption_status,
    jsonb_build_object(
        'column_registration_complete',
        COALESCE(column_counts.column_count, 0) = COALESCE(registered_column_counts.registered_column_count, 0),
        'needs_domain_authority',
        table_registry.authority_domain_ref = 'authority.legacy_schema'
          OR table_registry.lifecycle_status = 'legacy'
    ) AS adoption_metadata
FROM table_registry
LEFT JOIN column_counts
  ON column_counts.table_schema = table_registry.table_schema
 AND column_counts.table_name = table_registry.table_name
LEFT JOIN registered_column_counts
  ON registered_column_counts.table_schema = table_registry.table_schema
 AND registered_column_counts.table_name = table_registry.table_name;

CREATE OR REPLACE VIEW authority_legacy_backfill_summary AS
SELECT
    adoption_status,
    count(*)::integer AS table_count,
    sum(column_count)::integer AS column_count,
    sum(registered_column_count)::integer AS registered_column_count
FROM authority_schema_adoption_report
GROUP BY adoption_status;

COMMENT ON VIEW authority_schema_adoption_report IS
    'Per-table CQRS adoption state. legacy_inventory means cataloged and defined, but not yet owned by a modern command/event authority.';
COMMENT ON VIEW authority_legacy_backfill_summary IS
    'Summary of historical schema objects now visible to CQRS authority adoption.';

COMMIT;
