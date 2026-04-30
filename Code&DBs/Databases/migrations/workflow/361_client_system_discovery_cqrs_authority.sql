-- Migration 361: Client System Discovery CQRS authority.
--
-- Phase 1 originally landed as a typed runtime/repository substrate with a
-- direct MCP wrapper. This migration promotes it into the operation catalog:
-- the gateway owns writes, receipts, events, and HTTP route mounting.

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
    'authority.client_system_discovery',
    'praxis.engine',
    'stream.authority.client_system_discovery',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

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

ALTER TABLE client_system_census
    ADD COLUMN IF NOT EXISTS integration_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS category text NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS vendor text,
    ADD COLUMN IF NOT EXISTS deployment_model text NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS environment text NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS business_owner text,
    ADD COLUMN IF NOT EXISTS technical_owner text,
    ADD COLUMN IF NOT EXISTS criticality text NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS declared_purpose text,
    ADD COLUMN IF NOT EXISTS discovery_status text NOT NULL DEFAULT 'captured',
    ADD COLUMN IF NOT EXISTS last_verified_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_client_system_census_tenant_captured
    ON client_system_census (tenant_ref, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_client_system_census_status
    ON client_system_census (tenant_ref, discovery_status, captured_at DESC);

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

CREATE TABLE IF NOT EXISTS client_system_integration_evidence (
    integration_evidence_id text PRIMARY KEY,
    census_id text NOT NULL REFERENCES client_system_census(census_id) ON DELETE CASCADE,
    integration_id text NOT NULL,
    source_system_id text NOT NULL,
    target_system_id text NOT NULL,
    integration_type text NOT NULL DEFAULT 'unknown',
    transport text NOT NULL DEFAULT 'unknown',
    directionality text NOT NULL DEFAULT 'unknown',
    trigger_mode text NOT NULL DEFAULT 'unknown',
    integration_owner text,
    observed_status text NOT NULL DEFAULT 'unknown',
    evidence_ref text,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_system_integration_evidence_census
    ON client_system_integration_evidence (census_id, integration_id);

CREATE INDEX IF NOT EXISTS idx_client_system_integration_evidence_systems
    ON client_system_integration_evidence (source_system_id, target_system_id);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'client_system_census',
        'Client system census',
        'table',
        'Tenant/workspace scoped system census records captured during client-system discovery.',
        '{"migration":"361_client_system_discovery_cqrs_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.client_system_discovery"}'::jsonb
    ),
    (
        'client_connector_census',
        'Client connector census',
        'table',
        'Connector capability, auth, automation, and surface counts for one system census.',
        '{"migration":"361_client_system_discovery_cqrs_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.client_system_discovery"}'::jsonb
    ),
    (
        'client_connector_surface_evidence',
        'Client connector surface evidence',
        'table',
        'Object, API, event, and capability surface evidence for client connectors.',
        '{"migration":"361_client_system_discovery_cqrs_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.client_system_discovery"}'::jsonb
    ),
    (
        'client_connector_credential_health_refs',
        'Client connector credential health references',
        'table',
        'Redaction-safe credential health references for client connector census rows.',
        '{"migration":"361_client_system_discovery_cqrs_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.client_system_discovery"}'::jsonb
    ),
    (
        'client_system_integration_evidence',
        'Client system integration evidence',
        'table',
        'Observed or declared integration edges between systems in a client-system census.',
        '{"migration":"361_client_system_discovery_cqrs_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.client_system_discovery"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
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
) VALUES
    (
        'table.public.client_system_census',
        'table',
        'client_system_census',
        'public',
        'authority.client_system_discovery',
        'client_system_census',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.client_connector_census',
        'table',
        'client_connector_census',
        'public',
        'authority.client_system_discovery',
        'client_connector_census',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.client_connector_surface_evidence',
        'table',
        'client_connector_surface_evidence',
        'public',
        'authority.client_system_discovery',
        'client_connector_surface_evidence',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.client_connector_credential_health_refs',
        'table',
        'client_connector_credential_health_refs',
        'public',
        'authority.client_system_discovery',
        'client_connector_credential_health_refs',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
    ),
    (
        'table.public.client_system_integration_evidence',
        'table',
        'client_system_integration_evidence',
        'public',
        'authority.client_system_discovery',
        'client_system_integration_evidence',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{}'::jsonb
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

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES
    (
        'event_contract.client_system_discovery.census_recorded',
        'client_system_discovery.census_recorded',
        'authority.client_system_discovery',
        'data_dictionary.object.client_system_discovery_census_recorded_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{"expected_payload_fields":["census_id","tenant_ref","workspace_ref","system_slug","connector_count","integration_count","evidence_hash"]}'::jsonb
    ),
    (
        'event_contract.client_system_discovery.typed_gap_recorded',
        'client_system_discovery.typed_gap_recorded',
        'authority.client_system_discovery',
        'data_dictionary.object.client_system_discovery_typed_gap_recorded_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
        '{"expected_payload_fields":["gap_id","gap_kind","reason_code","source_ref","detail","severity","is_blocker"]}'::jsonb
    )
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'client-system-discovery-census-record',
    p_operation_name        := 'client_system_discovery_census_record',
    p_handler_ref           := 'runtime.operations.commands.client_system_discovery.handle_client_system_discovery_census_record',
    p_input_model_ref       := 'runtime.operations.commands.client_system_discovery.RecordClientSystemCensusCommand',
    p_authority_domain_ref  := 'authority.client_system_discovery',
    p_authority_ref         := 'authority.client_system_discovery',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator/client-system-discovery/census',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'client_system_discovery.census_recorded',
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_system_discovery_census_record.20260430',
    p_label                 := 'Client System Discovery Record Census',
    p_summary               := 'Persist one client-system census and connector evidence packet.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'client-system-discovery-census-read',
    p_operation_name        := 'client_system_discovery_census_read',
    p_handler_ref           := 'runtime.operations.queries.client_system_discovery.handle_client_system_discovery_census_read',
    p_input_model_ref       := 'runtime.operations.queries.client_system_discovery.QueryClientSystemDiscoveryCensusRead',
    p_authority_domain_ref  := 'authority.client_system_discovery',
    p_authority_ref         := 'authority.client_system_discovery',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/client-system-discovery/census',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_system_discovery_census_read.20260430',
    p_label                 := 'Client System Discovery Read Census',
    p_summary               := 'Read client-system census records by list, search, or describe.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'client-system-discovery-gap-record',
    p_operation_name        := 'client_system_discovery_gap_record',
    p_handler_ref           := 'runtime.operations.commands.client_system_discovery.handle_client_system_discovery_gap_record',
    p_input_model_ref       := 'runtime.operations.commands.client_system_discovery.RecordClientSystemDiscoveryGapCommand',
    p_authority_domain_ref  := 'authority.client_system_discovery',
    p_authority_ref         := 'authority.client_system_discovery',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator/client-system-discovery/gaps',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'client_system_discovery.typed_gap_recorded',
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_system_discovery_gap_record.20260430',
    p_label                 := 'Client System Discovery Record Gap',
    p_summary               := 'Record one typed client-system discovery gap through the gateway event ledger.'
);

COMMIT;
