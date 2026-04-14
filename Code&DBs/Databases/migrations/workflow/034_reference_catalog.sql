-- Reference catalog: addressable slugs for the operating model prose language
-- @source/action for integrations, #type/field for objects, agent-name for agents

CREATE TABLE IF NOT EXISTS reference_catalog (
    slug TEXT PRIMARY KEY,
    ref_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    resolved_id TEXT,
    resolved_table TEXT,
    schema JSONB DEFAULT '{}',
    description TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ref_catalog_type ON reference_catalog (ref_type);

-- Auto-populate from integration_registry
INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, description)
SELECT
    '@' || ir.id || '/' || (cap.value->>'action') AS slug,
    'integration' AS ref_type,
    ir.name || ': ' || (cap.value->>'action') AS display_name,
    ir.id AS resolved_id,
    'integration_registry' AS resolved_table,
    cap.value->>'description' AS description
FROM integration_registry ir,
     jsonb_array_elements(CASE WHEN jsonb_typeof(ir.capabilities) = 'array' THEN ir.capabilities ELSE '[]'::jsonb END) AS cap(value)
WHERE cap.value->>'action' IS NOT NULL
ON CONFLICT (slug) DO NOTHING;

-- Also add integration-level references (without action)
INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, description)
SELECT
    '@' || id AS slug,
    'integration' AS ref_type,
    name AS display_name,
    id AS resolved_id,
    'integration_registry' AS resolved_table,
    description
FROM integration_registry
ON CONFLICT (slug) DO NOTHING;

-- Auto-populate from object_types
INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, description)
SELECT
    '#' || type_id AS slug,
    'object' AS ref_type,
    name AS display_name,
    type_id AS resolved_id,
    'object_types' AS resolved_table,
    description
FROM object_types
ON CONFLICT (slug) DO NOTHING;

-- Auto-populate object fields from property_definitions
INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, schema, description)
SELECT
    '#' || ot.type_id || '/' || (prop.value->>'name') AS slug,
    'object' AS ref_type,
    ot.name || '.' || (prop.value->>'name') AS display_name,
    ot.type_id AS resolved_id,
    'object_types' AS resolved_table,
    jsonb_build_object('field_type', prop.value->>'type', 'required', COALESCE(prop.value->>'required', 'false')) AS schema,
    COALESCE(prop.value->>'description', '') AS description
FROM object_types ot,
     jsonb_array_elements(CASE WHEN jsonb_typeof(ot.property_definitions) = 'array' THEN ot.property_definitions ELSE '[]'::jsonb END) AS prop(value)
WHERE prop.value->>'name' IS NOT NULL
ON CONFLICT (slug) DO NOTHING;

-- Auto-populate agent routes from task_type_routing.
-- Support both legacy rank-based routing and any future tier-based schema.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'task_type_routing'
          AND column_name = 'tier'
    ) THEN
        EXECUTE $sql$
            INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, description)
            SELECT DISTINCT
                task_type AS slug,
                'agent' AS ref_type,
                task_type || ' (' || provider_slug || '/' || model_slug || ')' AS display_name,
                provider_slug || '/' || model_slug AS resolved_id,
                'task_type_routing' AS resolved_table,
                'Agent route: ' || task_type
            FROM task_type_routing
            WHERE tier = 1
            ON CONFLICT (slug) DO NOTHING
        $sql$;
    ELSE
        EXECUTE $sql$
            INSERT INTO reference_catalog (slug, ref_type, display_name, resolved_id, resolved_table, description)
            SELECT DISTINCT
                task_type AS slug,
                'agent' AS ref_type,
                task_type || ' (' || provider_slug || '/' || model_slug || ')' AS display_name,
                provider_slug || '/' || model_slug AS resolved_id,
                'task_type_routing' AS resolved_table,
                'Agent route: ' || task_type
            FROM task_type_routing
            WHERE rank = 1
            ON CONFLICT (slug) DO NOTHING
        $sql$;
    END IF;
END $$;
