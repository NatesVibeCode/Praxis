-- Rename the "moon" surface to "canvas" across all registry tables.
-- Historical migrations that inserted 'moon' are not modified.

BEGIN;

UPDATE surface_catalog_registry
   SET surface_name = 'canvas'
 WHERE surface_name = 'moon';

UPDATE surface_review_decisions
   SET surface_name = 'canvas'
 WHERE surface_name = 'moon';

UPDATE atlas_area_registry
   SET surface_name = 'canvas'
 WHERE surface_name = 'moon';

-- Rename the MCP tool in capability bundle allowlists
UPDATE capability_bundle_tools
   SET tool_name = 'praxis_canvas'
 WHERE tool_name = 'praxis_moon';

-- Rename the config key
UPDATE workflow_config
   SET config_key   = 'workflow_build.canvas_tool_allowlist.v1',
       config_value = REPLACE(config_value, 'praxis_moon', 'praxis_canvas'),
       description  = REPLACE(description, 'praxis_moon', 'praxis_canvas')
 WHERE config_key = 'workflow_build.moon_tool_allowlist.v1';

-- Rename in operation_catalog_registry if registered there
UPDATE operation_catalog_registry
   SET operation_name = 'praxis_canvas'
 WHERE operation_name = 'praxis_moon';

COMMIT;
