-- Migration 094: Add capability columns to provider_cli_profiles
--
-- Eliminates hardcoded provider-name switches in mcp_bridge, execution_backends,
-- execution_transport, task_type_router, and chat_orchestrator.

BEGIN;

ALTER TABLE provider_cli_profiles
  ADD COLUMN IF NOT EXISTS mcp_config_style TEXT,
  ADD COLUMN IF NOT EXISTS sandbox_env_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS exclude_from_rotation BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN provider_cli_profiles.mcp_config_style IS
  'MCP client config format: claude_mcp_config, codex_mcp_servers, or null (no MCP)';
COMMENT ON COLUMN provider_cli_profiles.sandbox_env_overrides IS
  'Env var mutations for sandbox: {"strip": ["KEY"], "set": {"K": "V"}, "set_home": true}';
COMMENT ON COLUMN provider_cli_profiles.exclude_from_rotation IS
  'Skip this provider in rotation-based dedup for debates/reviews';

UPDATE provider_cli_profiles SET mcp_config_style = 'claude_mcp_config'
  WHERE provider_slug = 'anthropic';
UPDATE provider_cli_profiles SET mcp_config_style = 'codex_mcp_servers'
  WHERE provider_slug = 'openai';
UPDATE provider_cli_profiles SET sandbox_env_overrides =
  '{"strip": ["ANTHROPIC_API_KEY"], "set_home": true}'::jsonb
  WHERE provider_slug = 'anthropic';
UPDATE provider_cli_profiles SET exclude_from_rotation = true
  WHERE provider_slug = 'cursor';

COMMIT;
