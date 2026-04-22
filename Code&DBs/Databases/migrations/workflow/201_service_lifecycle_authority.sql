-- Migration 201: service lifecycle authority.
--
-- Service execution is declared as CQRS state: targets, service definitions,
-- desired states, events, and projections. Reconciler implementations can live
-- on any substrate; this schema deliberately avoids encoding host OS paths or
-- launch commands as authority.

BEGIN;

CREATE TABLE IF NOT EXISTS service_reconciler_registry (
    reconciler_ref TEXT PRIMARY KEY CHECK (btrim(reconciler_ref) <> ''),
    substrate_kind TEXT NOT NULL CHECK (
        substrate_kind IN (
            'browser',
            'mobile_device',
            'desktop_host',
            'home_box',
            'lan_node',
            'cloud_service',
            'saas_connector',
            'container',
            'managed_service',
            'unknown'
        )
    ),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    command_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(command_contract) = 'object'),
    capability_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(capability_contract) = 'object'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime_targets (
    runtime_target_ref TEXT PRIMARY KEY CHECK (btrim(runtime_target_ref) <> ''),
    target_scope TEXT NOT NULL DEFAULT 'service_lifecycle' CHECK (btrim(target_scope) <> ''),
    substrate_kind TEXT NOT NULL CHECK (
        substrate_kind IN (
            'browser',
            'mobile_device',
            'desktop_host',
            'home_box',
            'lan_node',
            'cloud_service',
            'saas_connector',
            'container',
            'managed_service',
            'unknown'
        )
    ),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    workspace_ref TEXT REFERENCES registry_workspace_authority (workspace_ref) ON DELETE SET NULL,
    base_path_ref TEXT REFERENCES registry_workspace_base_path_authority (base_path_ref) ON DELETE SET NULL,
    host_ref TEXT CHECK (host_ref IS NULL OR btrim(host_ref) <> ''),
    endpoint_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(endpoint_contract) = 'object'),
    capability_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(capability_contract) = 'object'),
    secret_provider_ref TEXT CHECK (secret_provider_ref IS NULL OR btrim(secret_provider_ref) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS runtime_targets_scope_enabled_idx
    ON runtime_targets (target_scope, enabled, runtime_target_ref);

CREATE INDEX IF NOT EXISTS runtime_targets_workspace_idx
    ON runtime_targets (workspace_ref, host_ref)
    WHERE workspace_ref IS NOT NULL;

CREATE TABLE IF NOT EXISTS service_definitions (
    service_ref TEXT PRIMARY KEY CHECK (btrim(service_ref) <> ''),
    service_kind TEXT NOT NULL CHECK (
        service_kind IN (
            'http_api',
            'web_app',
            'worker',
            'database',
            'connector',
            'automation',
            'managed_service',
            'other'
        )
    ),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    owner_ref TEXT NOT NULL DEFAULT 'praxis.engine' CHECK (btrim(owner_ref) <> ''),
    desired_state_schema JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(desired_state_schema) = 'object'),
    health_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(health_contract) = 'object'),
    default_reconciler_ref TEXT REFERENCES service_reconciler_registry (reconciler_ref) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS service_definitions_kind_enabled_idx
    ON service_definitions (service_kind, enabled, service_ref);

CREATE TABLE IF NOT EXISTS service_desired_states (
    desired_state_ref TEXT PRIMARY KEY CHECK (btrim(desired_state_ref) <> ''),
    service_ref TEXT NOT NULL REFERENCES service_definitions (service_ref) ON DELETE RESTRICT,
    runtime_target_ref TEXT NOT NULL REFERENCES runtime_targets (runtime_target_ref) ON DELETE RESTRICT,
    desired_status TEXT NOT NULL CHECK (desired_status IN ('running', 'stopped', 'paused', 'absent')),
    desired_config JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(desired_config) = 'object'),
    environment_refs JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(environment_refs) = 'object'),
    health_contract JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(health_contract) = 'object'),
    reconciler_ref TEXT REFERENCES service_reconciler_registry (reconciler_ref) ON DELETE SET NULL,
    declared_by TEXT NOT NULL CHECK (btrim(declared_by) <> ''),
    declaration_reason TEXT CHECK (declaration_reason IS NULL OR btrim(declaration_reason) <> ''),
    idempotency_key TEXT CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    supersedes_ref TEXT REFERENCES service_desired_states (desired_state_ref) ON DELETE SET NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    declared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS service_desired_states_active_unique_idx
    ON service_desired_states (service_ref, runtime_target_ref)
    WHERE active;

CREATE UNIQUE INDEX IF NOT EXISTS service_desired_states_idempotency_key_idx
    ON service_desired_states (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS service_desired_states_service_target_idx
    ON service_desired_states (service_ref, runtime_target_ref, declared_at DESC);

CREATE TABLE IF NOT EXISTS service_instance_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_sequence BIGSERIAL UNIQUE,
    service_ref TEXT NOT NULL REFERENCES service_definitions (service_ref) ON DELETE RESTRICT,
    runtime_target_ref TEXT NOT NULL REFERENCES runtime_targets (runtime_target_ref) ON DELETE RESTRICT,
    desired_state_ref TEXT REFERENCES service_desired_states (desired_state_ref) ON DELETE SET NULL,
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    observed_status TEXT CHECK (
        observed_status IS NULL
        OR observed_status IN (
            'unknown',
            'pending',
            'starting',
            'running',
            'healthy',
            'unhealthy',
            'stopping',
            'stopped',
            'failed',
            'absent'
        )
    ),
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_payload) = 'object'),
    event_status TEXT NOT NULL DEFAULT 'recorded' CHECK (event_status IN ('recorded', 'accepted', 'rejected', 'failed')),
    observed_by TEXT NOT NULL DEFAULT 'unknown' CHECK (btrim(observed_by) <> ''),
    operation_ref TEXT NOT NULL DEFAULT 'service.lifecycle.record_event' CHECK (btrim(operation_ref) <> ''),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS service_instance_events_service_target_seq_idx
    ON service_instance_events (service_ref, runtime_target_ref, event_sequence DESC);

CREATE INDEX IF NOT EXISTS service_instance_events_type_idx
    ON service_instance_events (event_type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS service_instance_projection (
    service_ref TEXT NOT NULL REFERENCES service_definitions (service_ref) ON DELETE RESTRICT,
    runtime_target_ref TEXT NOT NULL REFERENCES runtime_targets (runtime_target_ref) ON DELETE RESTRICT,
    active_desired_state_ref TEXT REFERENCES service_desired_states (desired_state_ref) ON DELETE SET NULL,
    desired_status TEXT NOT NULL DEFAULT 'unknown' CHECK (desired_status IN ('unknown', 'running', 'stopped', 'paused', 'absent')),
    observed_status TEXT NOT NULL DEFAULT 'unknown' CHECK (
        observed_status IN (
            'unknown',
            'pending',
            'starting',
            'running',
            'healthy',
            'unhealthy',
            'stopping',
            'stopped',
            'failed',
            'absent'
        )
    ),
    endpoint_refs JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(endpoint_refs) = 'object'),
    last_event_id UUID,
    last_event_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_event_sequence >= 0),
    last_checked_at TIMESTAMPTZ,
    last_healthy_at TIMESTAMPTZ,
    failure_reason TEXT,
    projection_revision INTEGER NOT NULL DEFAULT 1 CHECK (projection_revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service_ref, runtime_target_ref)
);

CREATE INDEX IF NOT EXISTS service_instance_projection_status_idx
    ON service_instance_projection (desired_status, observed_status, updated_at DESC);

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.service_lifecycle',
    'praxis.engine',
    'stream.service_lifecycle',
    'projection.service_lifecycle.instances',
    'praxis.primary_postgres',
    TRUE,
    'decision.service_lifecycle.runtime_target_neutrality.20260422'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_projection_registry (
    projection_ref,
    authority_domain_ref,
    source_event_stream_ref,
    reducer_ref,
    storage_target_ref,
    freshness_policy_ref,
    enabled,
    decision_ref
) VALUES (
    'projection.service_lifecycle.instances',
    'authority.service_lifecycle',
    'stream.service_lifecycle',
    'runtime.service_lifecycle.reduce_service_instance_events',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.service_lifecycle.runtime_target_neutrality.20260422'
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_event_stream_ref = EXCLUDED.source_event_stream_ref,
    reducer_ref = EXCLUDED.reducer_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO service_reconciler_registry (
    reconciler_ref,
    substrate_kind,
    display_name,
    command_contract,
    capability_contract,
    enabled,
    decision_ref
) VALUES (
    'service_reconciler.external_declarative',
    'unknown',
    'External declarative reconciler',
    jsonb_build_object(
        'contract', 'record events against declared desired state',
        'authority', 'service.lifecycle.record_event'
    ),
    jsonb_build_object(
        'requires_host_os', false,
        'requires_filesystem_path', false
    ),
    TRUE,
    'decision.service_lifecycle.runtime_target_neutrality.20260422'
)
ON CONFLICT (reconciler_ref) DO UPDATE SET
    substrate_kind = EXCLUDED.substrate_kind,
    display_name = EXCLUDED.display_name,
    command_contract = EXCLUDED.command_contract,
    capability_contract = EXCLUDED.capability_contract,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO service_definitions (
    service_ref,
    service_kind,
    display_name,
    owner_ref,
    desired_state_schema,
    health_contract,
    default_reconciler_ref,
    enabled,
    decision_ref
) VALUES
    (
        'praxis.workflow_api',
        'http_api',
        'Praxis Workflow API',
        'praxis.engine',
        jsonb_build_object(
            'required_fields', jsonb_build_array('api_url'),
            'path_authority', 'registry_workspace_base_path_authority'
        ),
        jsonb_build_object(
            'health_endpoint', '/api/health',
            'cache_policy', 'no-store'
        ),
        'service_reconciler.external_declarative',
        TRUE,
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'praxis.workflow_app',
        'web_app',
        'Praxis Workflow App',
        'praxis.engine',
        jsonb_build_object(
            'required_fields', jsonb_build_array('app_url'),
            'api_dependency', 'praxis.workflow_api'
        ),
        jsonb_build_object(
            'health_path', '/',
            'cache_policy', 'no-store_for_sensitive_mobile_routes'
        ),
        'service_reconciler.external_declarative',
        TRUE,
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    )
ON CONFLICT (service_ref) DO UPDATE SET
    service_kind = EXCLUDED.service_kind,
    display_name = EXCLUDED.display_name,
    owner_ref = EXCLUDED.owner_ref,
    desired_state_schema = EXCLUDED.desired_state_schema,
    health_contract = EXCLUDED.health_contract,
    default_reconciler_ref = EXCLUDED.default_reconciler_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    authority_domain_ref,
    projection_ref,
    storage_target_ref,
    input_schema_ref,
    output_schema_ref,
    idempotency_key_fields,
    required_capabilities,
    allowed_callers,
    timeout_ms,
    receipt_required,
    event_required,
    event_type,
    projection_freshness_policy_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
) VALUES
    (
        'service-lifecycle-register-target',
        'service.lifecycle.register_target',
        'operation_command',
        'command',
        'POST',
        '/api/service-lifecycle/targets',
        'runtime.service_lifecycle.RegisterRuntimeTargetCommand',
        'runtime.service_lifecycle.handle_register_runtime_target',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.RegisterRuntimeTargetCommand',
        'service.lifecycle.target',
        '["runtime_target_ref"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'service_lifecycle_target_registered',
        'projection_freshness.default',
        'operate',
        'idempotent',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'service-lifecycle-register-service',
        'service.lifecycle.register_service',
        'operation_command',
        'command',
        'POST',
        '/api/service-lifecycle/services',
        'runtime.service_lifecycle.RegisterServiceDefinitionCommand',
        'runtime.service_lifecycle.handle_register_service_definition',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.RegisterServiceDefinitionCommand',
        'service.lifecycle.service',
        '["service_ref"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'service_lifecycle_service_registered',
        'projection_freshness.default',
        'operate',
        'idempotent',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'service-lifecycle-declare-desired-state',
        'service.lifecycle.declare_desired_state',
        'operation_command',
        'command',
        'POST',
        '/api/service-lifecycle/desired-state',
        'runtime.service_lifecycle.DeclareServiceDesiredStateCommand',
        'runtime.service_lifecycle.handle_declare_service_desired_state',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.DeclareServiceDesiredStateCommand',
        'service.lifecycle.desired_state',
        '["idempotency_key"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'service_lifecycle_desired_state_declared',
        'projection_freshness.default',
        'operate',
        'idempotent',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'service-lifecycle-record-event',
        'service.lifecycle.record_event',
        'operation_command',
        'command',
        'POST',
        '/api/service-lifecycle/events',
        'runtime.service_lifecycle.RecordServiceLifecycleEventCommand',
        'runtime.service_lifecycle.handle_record_service_lifecycle_event',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.RecordServiceLifecycleEventCommand',
        'service.lifecycle.event',
        '[]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'service_lifecycle_event_recorded',
        'projection_freshness.default',
        'operate',
        'non_idempotent',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'service-lifecycle-get-projection',
        'service.lifecycle.get_projection',
        'operation_query',
        'query',
        'GET',
        '/api/service-lifecycle/projection/{service_ref}/{runtime_target_ref}',
        'runtime.service_lifecycle.QueryServiceProjectionCommand',
        'runtime.service_lifecycle.handle_query_service_projection',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.QueryServiceProjectionCommand',
        'service.lifecycle.projection',
        '["service_ref","runtime_target_ref"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        FALSE,
        NULL,
        'projection_freshness.default',
        'observe',
        'read_only',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    ),
    (
        'service-lifecycle-list-targets',
        'service.lifecycle.list_targets',
        'operation_query',
        'query',
        'GET',
        '/api/service-lifecycle/targets',
        'runtime.service_lifecycle.ListRuntimeTargetsCommand',
        'runtime.service_lifecycle.handle_list_runtime_targets',
        'authority.service_lifecycle',
        'authority.service_lifecycle',
        'projection.service_lifecycle.instances',
        'praxis.primary_postgres',
        'runtime.service_lifecycle.ListRuntimeTargetsCommand',
        'service.lifecycle.targets',
        '[]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        FALSE,
        NULL,
        'projection_freshness.default',
        'observe',
        'read_only',
        TRUE,
        'binding.operation_catalog_registry.service_lifecycle.20260422',
        'decision.service_lifecycle.runtime_target_neutrality.20260422'
    )
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    projection_ref = EXCLUDED.projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    idempotency_key_fields = EXCLUDED.idempotency_key_fields,
    required_capabilities = EXCLUDED.required_capabilities,
    allowed_callers = EXCLUDED.allowed_callers,
    timeout_ms = EXCLUDED.timeout_ms,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    event_type = EXCLUDED.event_type,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMENT ON TABLE runtime_targets IS
    'Runtime target declarations for service lifecycle. Targets are substrate-neutral and may represent browsers, SaaS, cloud services, home boxes, or connector runtimes.';
COMMENT ON COLUMN runtime_targets.base_path_ref IS
    'Optional link to workspace base-path authority for host-local targets. Browser and SaaS targets normally leave this null.';
COMMENT ON TABLE service_desired_states IS
    'Command-side desired state ledger for service lifecycle. The active row per service/target is the declarative source of truth.';
COMMENT ON TABLE service_instance_events IS
    'Append-only service lifecycle event stream observed by reconcilers, health checks, or operator surfaces.';
COMMENT ON TABLE service_instance_projection IS
    'Read-side projection of desired vs observed service state by service and runtime target.';

COMMIT;
