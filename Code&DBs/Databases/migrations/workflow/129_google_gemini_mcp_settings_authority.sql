BEGIN;

UPDATE provider_cli_profiles
SET mcp_config_style = 'gemini_project_settings',
    mcp_args_template = '["--allowed-mcp-server-names","dag-workflow"]'::jsonb,
    updated_at = now()
WHERE provider_slug = 'google';

COMMIT;
