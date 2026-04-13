-- Extend integration_registry to support manifest-sourced integrations.

ALTER TABLE integration_registry
  ADD COLUMN IF NOT EXISTS manifest_source text NOT NULL DEFAULT 'static',
  ADD COLUMN IF NOT EXISTS auth_shape jsonb NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS endpoint_templates jsonb NOT NULL DEFAULT '{}';

COMMENT ON COLUMN integration_registry.manifest_source IS 'Origin: static, manifest, mcp';
COMMENT ON COLUMN integration_registry.auth_shape IS 'Auth config: {kind: "env_var"|"oauth2"|"api_key", ...}';
COMMENT ON COLUMN integration_registry.endpoint_templates IS 'URL templates keyed by action';
