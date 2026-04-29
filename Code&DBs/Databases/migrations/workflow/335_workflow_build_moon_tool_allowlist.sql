BEGIN;

UPDATE capability_bundle_definitions
SET allowed_mcp_tools_json = (
    SELECT jsonb_agg(tool_name ORDER BY first_seen)
    FROM (
        SELECT tool_name, MIN(position) AS first_seen
        FROM (
            SELECT
                CASE
                    WHEN existing_tool.value = 'praxis_status' THEN 'praxis_status_snapshot'
                    ELSE existing_tool.value
                END AS tool_name,
                existing_tool.ordinality::int AS position
            FROM jsonb_array_elements_text(allowed_mcp_tools_json)
                 WITH ORDINALITY AS existing_tool(value, ordinality)

            UNION ALL

            SELECT 'praxis_moon', 100000
        ) AS normalized_tools
        WHERE btrim(tool_name) <> ''
        GROUP BY tool_name
    ) AS deduped_tools
)
WHERE status = 'active'
  AND (
      bundle_ref IN ('capability_bundle:email_triage', 'capability_bundle:invoice_processing')
      OR family IN ('support_triage', 'ap_invoice')
  );

INSERT INTO platform_config (
    config_key,
    config_value,
    value_type,
    category,
    description,
    min_value,
    max_value
) VALUES (
    'workflow_build.moon_tool_allowlist.v1',
    'praxis_moon',
    'string',
    'workflow_build',
    'Marker that active workflow-build capability bundles include praxis_moon and use praxis_status_snapshot.',
    NULL,
    NULL
)
ON CONFLICT (config_key) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    value_type = EXCLUDED.value_type,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    updated_at = now();

COMMIT;
