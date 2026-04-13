-- 037: Reference catalog for @reference autocomplete and validation.

BEGIN;

CREATE TABLE IF NOT EXISTS reference_catalog (
    slug TEXT PRIMARY KEY,
    ref_type TEXT NOT NULL CHECK (ref_type IN ('integration', 'object', 'variable', 'agent')),
    display_name TEXT NOT NULL,
    description TEXT,
    resolved_table TEXT,
    resolved_id TEXT,
    schema_def JSONB DEFAULT '{}',
    examples TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reference_catalog_type
    ON reference_catalog (ref_type);

CREATE OR REPLACE FUNCTION refresh_reference_catalog()
RETURNS integer
LANGUAGE sql
AS $$
WITH integration_rows AS (
    INSERT INTO reference_catalog (
        slug,
        ref_type,
        display_name,
        description,
        resolved_table,
        resolved_id
    )
    SELECT
        '@' || ir.id || '/' || cap->>'action' AS slug,
        'integration' AS ref_type,
        ir.name || ': ' || cap->>'action' AS display_name,
        cap->>'description' AS description,
        'integration_registry' AS resolved_table,
        ir.id AS resolved_id
    FROM integration_registry ir
    CROSS JOIN LATERAL jsonb_array_elements(ir.capabilities) AS cap
    WHERE cap->>'action' IS NOT NULL
    ON CONFLICT (slug) DO UPDATE SET
        ref_type = EXCLUDED.ref_type,
        display_name = EXCLUDED.display_name,
        description = EXCLUDED.description,
        resolved_table = EXCLUDED.resolved_table,
        resolved_id = EXCLUDED.resolved_id,
        updated_at = NOW()
    RETURNING 1
),
object_rows AS (
    INSERT INTO reference_catalog (
        slug,
        ref_type,
        display_name,
        description,
        resolved_table,
        resolved_id
    )
    SELECT
        '#' || ot.type_id AS slug,
        'object' AS ref_type,
        ot.name AS display_name,
        ot.description AS description,
        'object_types' AS resolved_table,
        ot.type_id AS resolved_id
    FROM object_types ot
    ON CONFLICT (slug) DO UPDATE SET
        ref_type = EXCLUDED.ref_type,
        display_name = EXCLUDED.display_name,
        description = EXCLUDED.description,
        resolved_table = EXCLUDED.resolved_table,
        resolved_id = EXCLUDED.resolved_id,
        updated_at = NOW()
    RETURNING 1
),
agent_rows AS (
    INSERT INTO reference_catalog (
        slug,
        ref_type,
        display_name,
        description,
        resolved_table,
        resolved_id
    )
    SELECT DISTINCT
        tr.task_type AS slug,
        'agent' AS ref_type,
        tr.task_type AS display_name,
        NULL::text AS description,
        'task_type_routing' AS resolved_table,
        NULL::text AS resolved_id
    FROM task_type_routing tr
    ON CONFLICT (slug) DO UPDATE SET
        ref_type = EXCLUDED.ref_type,
        display_name = EXCLUDED.display_name,
        description = EXCLUDED.description,
        resolved_table = EXCLUDED.resolved_table,
        resolved_id = EXCLUDED.resolved_id,
        updated_at = NOW()
    RETURNING 1
)
SELECT (
    COALESCE((SELECT COUNT(*) FROM integration_rows), 0) +
    COALESCE((SELECT COUNT(*) FROM object_rows), 0) +
    COALESCE((SELECT COUNT(*) FROM agent_rows), 0)
)::integer;
$$;

SELECT refresh_reference_catalog();

COMMIT;
