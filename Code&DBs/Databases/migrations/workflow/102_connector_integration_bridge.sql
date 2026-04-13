-- Bridge connector_registry into integration_registry so built connectors
-- become callable through the standard execute_integration() path.

-- Link integration_registry rows to their generated connector code.
ALTER TABLE integration_registry
  ADD COLUMN IF NOT EXISTS connector_slug TEXT;

-- Allow webhook endpoints to target an integration action directly,
-- not just a workflow spec.  Mutually exclusive with target_workflow_id
-- at the application level.
ALTER TABLE webhook_endpoints
  ADD COLUMN IF NOT EXISTS target_integration_id TEXT,
  ADD COLUMN IF NOT EXISTS target_integration_action TEXT,
  ADD COLUMN IF NOT EXISTS target_integration_args JSONB DEFAULT '{}';
