BEGIN;

CREATE TABLE IF NOT EXISTS client_system_census (
    census_id text PRIMARY KEY,
    tenant_ref text NOT NULL,
    workspace_ref text NOT NULL,
    system_slug text NOT NULL,
    system_name text NOT NULL,
    discovery_source text NOT NULL,
    captured_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'captured',
    connector_count integer NOT NULL DEFAULT 0,
    evidence_hash text NOT NULL,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_system_census_tenant_captured
    ON client_system_census (tenant_ref, captured_at DESC);

CREATE TABLE IF NOT EXISTS client_connector_census (
    connector_census_id text PRIMARY KEY,
    census_id text NOT NULL REFERENCES client_system_census(census_id) ON DELETE CASCADE,
    integration_id text,
    connector_slug text NOT NULL,
    display_name text NOT NULL,
    provider text NOT NULL,
    auth_kind text NOT NULL DEFAULT 'unknown',
    auth_status text NOT NULL DEFAULT 'unknown',
    automation_classification text NOT NULL DEFAULT 'unknown',
    capability_count integer NOT NULL DEFAULT 0,
    object_surface_count integer NOT NULL DEFAULT 0,
    api_surface_count integer NOT NULL DEFAULT 0,
    event_surface_count integer NOT NULL DEFAULT 0,
    capabilities_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_connector_census_census
    ON client_connector_census (census_id, connector_slug);

CREATE INDEX IF NOT EXISTS idx_client_connector_census_search
    ON client_connector_census (provider, connector_slug);

CREATE TABLE IF NOT EXISTS client_connector_surface_evidence (
    evidence_id text PRIMARY KEY,
    census_id text NOT NULL REFERENCES client_system_census(census_id) ON DELETE CASCADE,
    connector_census_id text NOT NULL REFERENCES client_connector_census(connector_census_id) ON DELETE CASCADE,
    surface_kind text NOT NULL,
    surface_ref text NOT NULL,
    operation_name text,
    object_name text,
    http_method text,
    path_template text,
    event_name text,
    evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_connector_surface_kind
    ON client_connector_surface_evidence (connector_census_id, surface_kind);

CREATE TABLE IF NOT EXISTS client_connector_credential_health_refs (
    credential_health_ref_id text PRIMARY KEY,
    census_id text NOT NULL REFERENCES client_system_census(census_id) ON DELETE CASCADE,
    connector_census_id text NOT NULL REFERENCES client_connector_census(connector_census_id) ON DELETE CASCADE,
    integration_id text,
    credential_ref text,
    env_var_ref text,
    status text NOT NULL DEFAULT 'unknown',
    checked_at timestamptz,
    expires_at timestamptz,
    detail text,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_connector_credential_health_refs_connector
    ON client_connector_credential_health_refs (connector_census_id, status);

COMMIT;
