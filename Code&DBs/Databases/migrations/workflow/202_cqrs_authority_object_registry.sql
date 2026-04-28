-- Migration 202: CQRS authority object registry.
--
-- Authority is the durable ownership boundary for state. This registry makes
-- that boundary queryable across tables, commands, events, projections,
-- service-bus channels, feedback streams, definitions, and runtime targets.

BEGIN;

ALTER TABLE data_dictionary_objects
    DROP CONSTRAINT IF EXISTS data_dictionary_objects_category_check;

ALTER TABLE data_dictionary_objects
    ADD CONSTRAINT data_dictionary_objects_category_check
        CHECK (category IN (
            'table',
            'object_type',
            'integration',
            'dataset',
            'ingest',
            'decision',
            'receipt',
            'tool',
            'object',
            'command',
            'query',
            'event',
            'projection',
            'service_bus_channel',
            'feedback_stream',
            'definition',
            'runtime_target'
        ));

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES
    (
        'authority.cqrs',
        'praxis.engine',
        'stream.cqrs_authority',
        'projection.authority.objects',
        'praxis.primary_postgres',
        TRUE,
        'decision.cqrs_authority_unification.20260422'
    ),
    (
        'authority.service_bus',
        'praxis.engine',
        'stream.service_bus',
        'projection.service_bus.messages',
        'praxis.primary_postgres',
        TRUE,
        'decision.cqrs_authority_unification.20260422'
    )
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS authority_object_registry (
    object_ref TEXT PRIMARY KEY CHECK (btrim(object_ref) <> ''),
    object_kind TEXT NOT NULL CHECK (
        object_kind IN (
            'table',
            'column',
            'command',
            'query',
            'event',
            'projection',
            'service_bus_channel',
            'feedback_stream',
            'definition',
            'runtime_target'
        )
    ),
    object_name TEXT NOT NULL CHECK (btrim(object_name) <> ''),
    schema_name TEXT CHECK (schema_name IS NULL OR btrim(schema_name) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    data_dictionary_object_kind TEXT NOT NULL CHECK (btrim(data_dictionary_object_kind) <> ''),
    lifecycle_status TEXT NOT NULL DEFAULT 'active' CHECK (
        lifecycle_status IN ('draft', 'active', 'legacy', 'deprecated', 'dead')
    ),
    write_model_kind TEXT NOT NULL CHECK (
        write_model_kind IN (
            'command_model',
            'read_model',
            'event_stream',
            'transport',
            'feedback',
            'definition',
            'registry',
            'reference',
            'projection'
        )
    ),
    owner_ref TEXT NOT NULL DEFAULT 'praxis.engine' CHECK (btrim(owner_ref) <> ''),
    source_decision_ref TEXT CHECK (source_decision_ref IS NULL OR btrim(source_decision_ref) <> ''),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT authority_object_registry_source_check
        CHECK (source_decision_ref IS NOT NULL OR source_receipt_id IS NOT NULL),
    CONSTRAINT authority_object_registry_unique_domain_kind_name
        UNIQUE (authority_domain_ref, object_kind, object_name)
);

CREATE INDEX IF NOT EXISTS authority_object_registry_domain_kind_idx
    ON authority_object_registry (authority_domain_ref, object_kind, lifecycle_status);

CREATE INDEX IF NOT EXISTS authority_object_registry_dictionary_idx
    ON authority_object_registry (data_dictionary_object_kind);

CREATE OR REPLACE FUNCTION touch_authority_object_registry_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_authority_object_registry_touch ON authority_object_registry;
CREATE TRIGGER trg_authority_object_registry_touch
    BEFORE UPDATE ON authority_object_registry
    FOR EACH ROW EXECUTE FUNCTION touch_authority_object_registry_updated_at();

CREATE TABLE IF NOT EXISTS service_bus_channel_registry (
    channel_ref TEXT PRIMARY KEY CHECK (btrim(channel_ref) <> ''),
    channel_name TEXT NOT NULL UNIQUE CHECK (btrim(channel_name) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    transport_kind TEXT NOT NULL DEFAULT 'event_log' CHECK (transport_kind IN ('event_log', 'notify', 'external')),
    message_schema_ref TEXT NOT NULL DEFAULT 'service_bus.message.default' CHECK (btrim(message_schema_ref) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_bus_message_contracts (
    message_type_ref TEXT PRIMARY KEY CHECK (btrim(message_type_ref) <> ''),
    channel_ref TEXT NOT NULL REFERENCES service_bus_channel_registry (channel_ref) ON DELETE RESTRICT,
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    payload_schema_ref TEXT NOT NULL CHECK (btrim(payload_schema_ref) <> ''),
    correlation_policy_ref TEXT NOT NULL DEFAULT 'service_bus.correlation.default' CHECK (btrim(correlation_policy_ref) <> ''),
    receipt_policy_ref TEXT NOT NULL DEFAULT 'authority.receipt.required' CHECK (btrim(receipt_policy_ref) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_bus_message_ledger (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_ref TEXT NOT NULL REFERENCES service_bus_channel_registry (channel_ref) ON DELETE RESTRICT,
    message_type_ref TEXT NOT NULL REFERENCES service_bus_message_contracts (message_type_ref) ON DELETE RESTRICT,
    correlation_ref TEXT NOT NULL CHECK (btrim(correlation_ref) <> ''),
    command_ref TEXT CHECK (command_ref IS NULL OR btrim(command_ref) <> ''),
    receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    message_status TEXT NOT NULL DEFAULT 'published' CHECK (
        message_status IN ('queued', 'published', 'observed', 'failed')
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    recorded_by TEXT NOT NULL DEFAULT 'service_bus.authority' CHECK (btrim(recorded_by) <> ''),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS service_bus_message_ledger_channel_recorded_idx
    ON service_bus_message_ledger (channel_ref, recorded_at DESC);

CREATE INDEX IF NOT EXISTS service_bus_message_ledger_correlation_idx
    ON service_bus_message_ledger (correlation_ref, recorded_at DESC);

INSERT INTO authority_projection_registry (
    projection_ref,
    authority_domain_ref,
    source_event_stream_ref,
    reducer_ref,
    storage_target_ref,
    freshness_policy_ref,
    enabled,
    decision_ref
) VALUES
    (
        'projection.authority.objects',
        'authority.cqrs',
        'stream.cqrs_authority',
        'runtime.authority_objects.list_authority_objects',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.cqrs_authority_unification.20260422'
    ),
    (
        'projection.service_bus.messages',
        'authority.service_bus',
        'stream.service_bus',
        'runtime.service_bus_authority.list_service_bus_messages',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.cqrs_authority_unification.20260422'
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

INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
VALUES
    ('projection.authority.objects', 'fresh', now()),
    ('projection.service_bus.messages', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    updated_at = now();

INSERT INTO service_bus_channel_registry (
    channel_ref,
    channel_name,
    authority_domain_ref,
    transport_kind,
    message_schema_ref,
    enabled,
    decision_ref,
    metadata
) VALUES
    ('service_bus.channel.build_state', 'build_state', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.cache_invalidation', 'cache_invalidation', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.dataset_curation', 'dataset_curation', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.job_lifecycle', 'job_lifecycle', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.receipt', 'receipt', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.semantic_assertion', 'semantic_assertion', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.system', 'system', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.webhook', 'webhook', 'authority.service_bus', 'event_log', 'service_bus.event_log.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.event_log"}'::jsonb),
    ('service_bus.channel.workflow_command', 'workflow_command', 'authority.service_bus', 'event_log', 'service_bus.workflow_command.message', TRUE, 'decision.cqrs_authority_unification.20260422', '{"source":"runtime.control_commands"}'::jsonb)
ON CONFLICT (channel_ref) DO UPDATE SET
    channel_name = EXCLUDED.channel_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    transport_kind = EXCLUDED.transport_kind,
    message_schema_ref = EXCLUDED.message_schema_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO service_bus_message_contracts (
    message_type_ref,
    channel_ref,
    authority_domain_ref,
    payload_schema_ref,
    correlation_policy_ref,
    receipt_policy_ref,
    enabled,
    decision_ref,
    metadata
) VALUES
    ('service_bus.message.event_log.default', 'service_bus.channel.system', 'authority.service_bus', 'service_bus.event_log.message', 'service_bus.correlation.entity', 'authority.receipt.optional', TRUE, 'decision.cqrs_authority_unification.20260422', '{"covers":"generic event_log messages"}'::jsonb),
    ('service_bus.message.workflow_submit', 'service_bus.channel.workflow_command', 'authority.workflow_runs', 'workflow.command.submit', 'service_bus.correlation.command_id', 'authority.receipt.required', TRUE, 'decision.cqrs_authority_unification.20260422', '{"command_type":"workflow.submit"}'::jsonb),
    ('service_bus.message.workflow_spawn', 'service_bus.channel.workflow_command', 'authority.workflow_runs', 'workflow.command.spawn', 'service_bus.correlation.command_id', 'authority.receipt.required', TRUE, 'decision.cqrs_authority_unification.20260422', '{"command_type":"workflow.spawn"}'::jsonb),
    ('service_bus.message.workflow_chain_submit', 'service_bus.channel.workflow_command', 'authority.workflow_runs', 'workflow.command.chain_submit', 'service_bus.correlation.command_id', 'authority.receipt.required', TRUE, 'decision.cqrs_authority_unification.20260422', '{"command_type":"workflow.chain_submit"}'::jsonb)
ON CONFLICT (message_type_ref) DO UPDATE SET
    channel_ref = EXCLUDED.channel_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    correlation_policy_ref = EXCLUDED.correlation_policy_ref,
    receipt_policy_ref = EXCLUDED.receipt_policy_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
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
        'authority-objects-list',
        'authority.objects.list',
        'operation_query',
        'query',
        'GET',
        '/api/authority/objects',
        'runtime.authority_objects.ListAuthorityObjectsCommand',
        'runtime.authority_objects.handle_list_authority_objects',
        'authority.cqrs',
        'authority.cqrs',
        'projection.authority.objects',
        'praxis.primary_postgres',
        'runtime.authority_objects.ListAuthorityObjectsCommand',
        'authority.objects.list',
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
        'binding.operation_catalog_registry.cqrs_authority.20260422',
        'decision.cqrs_authority_unification.20260422'
    ),
    (
        'authority-objects-drift',
        'authority.objects.drift',
        'operation_query',
        'query',
        'GET',
        '/api/authority/objects/drift',
        'runtime.authority_objects.ListAuthorityDriftCommand',
        'runtime.authority_objects.handle_list_authority_drift',
        'authority.cqrs',
        'authority.cqrs',
        'projection.authority.objects',
        'praxis.primary_postgres',
        'runtime.authority_objects.ListAuthorityDriftCommand',
        'authority.objects.drift',
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
        'binding.operation_catalog_registry.cqrs_authority.20260422',
        'decision.cqrs_authority_unification.20260422'
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

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('authority_object_registry', 'Authority object registry', 'table', 'Canonical ownership registry for durable CQRS objects.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb),
    ('service_bus_channel_registry', 'Service bus channel registry', 'table', 'Registered transport channels. The service bus moves messages; authorities own truth.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.service_bus"}'::jsonb),
    ('service_bus_message_contracts', 'Service bus message contracts', 'table', 'Registered service bus message types and payload contracts.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.service_bus"}'::jsonb),
    ('service_bus_message_ledger', 'Service bus message ledger', 'table', 'Inspectable ledger of service bus transport envelopes.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.service_bus"}'::jsonb),
    ('authority.objects.list', 'List authority objects', 'command', 'Query operation for authority object ownership.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb),
    ('authority.objects.drift', 'List authority object drift', 'command', 'Query operation for unregistered or incomplete authority objects.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb),
    ('projection.authority.objects', 'Authority objects projection', 'projection', 'Read model over authority_object_registry and drift views.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb),
    ('projection.service_bus.messages', 'Service bus messages projection', 'projection', 'Read model over service bus message ledger.', '{"migration":"202_cqrs_authority_object_registry.sql"}'::jsonb, '{"authority_domain_ref":"authority.service_bus"}'::jsonb)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    'operation.' || operation_name,
    operation_name,
    operation_kind,
    'Operation catalog entry owned by ' || authority_domain_ref,
    jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', operation_ref),
    jsonb_build_object('authority_domain_ref', authority_domain_ref, 'operation_kind', operation_kind)
FROM operation_catalog_registry
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    projection_ref,
    projection_ref,
    'projection',
    'Authority projection owned by ' || authority_domain_ref,
    jsonb_build_object('source', 'authority_projection_registry'),
    jsonb_build_object('authority_domain_ref', authority_domain_ref, 'source_event_stream_ref', source_event_stream_ref)
FROM authority_projection_registry
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    'event.' || event_type,
    event_type,
    'event',
    'Authority event emitted by ' || authority_domain_ref,
    jsonb_build_object('source', 'operation_catalog_registry', 'operation_name', operation_name),
    jsonb_build_object('authority_domain_ref', authority_domain_ref, 'operation_ref', operation_ref)
FROM operation_catalog_registry
WHERE event_type IS NOT NULL AND btrim(event_type) <> ''
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    channel_ref,
    channel_name,
    'service_bus_channel',
    'Registered service bus channel; transport only, not state authority.',
    jsonb_build_object('source', 'service_bus_channel_registry'),
    jsonb_build_object('authority_domain_ref', authority_domain_ref, 'transport_kind', transport_kind)
FROM service_bus_channel_registry
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
    ('table.public.authority_storage_targets', 'table', 'authority_storage_targets', 'public', 'authority.cqrs', 'authority_storage_targets', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_domains', 'table', 'authority_domains', 'public', 'authority.cqrs', 'authority_domains', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_projection_registry', 'table', 'authority_projection_registry', 'public', 'authority.cqrs', 'authority_projection_registry', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_projection_state', 'table', 'authority_projection_state', 'public', 'authority.cqrs', 'authority_projection_state', 'active', 'read_model', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_operation_receipts', 'table', 'authority_operation_receipts', 'public', 'authority.cqrs', 'authority_operation_receipts', 'active', 'event_stream', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_events', 'table', 'authority_events', 'public', 'authority.cqrs', 'authority_events', 'active', 'event_stream', 'praxis.engine', 'decision.cqrs_authority_kernel.20260422', '{}'::jsonb),
    ('table.public.authority_object_registry', 'table', 'authority_object_registry', 'public', 'authority.cqrs', 'authority_object_registry', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.data_dictionary_objects', 'table', 'data_dictionary_objects', 'public', 'authority.object_schema', 'data_dictionary_objects', 'active', 'definition', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.data_dictionary_entries', 'table', 'data_dictionary_entries', 'public', 'authority.object_schema', 'data_dictionary_entries', 'active', 'definition', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.service_reconciler_registry', 'table', 'service_reconciler_registry', 'public', 'authority.service_lifecycle', 'service_reconciler_registry', 'active', 'registry', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.runtime_targets', 'table', 'runtime_targets', 'public', 'authority.service_lifecycle', 'runtime_targets', 'active', 'registry', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.service_definitions', 'table', 'service_definitions', 'public', 'authority.service_lifecycle', 'service_definitions', 'active', 'definition', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.service_desired_states', 'table', 'service_desired_states', 'public', 'authority.service_lifecycle', 'service_desired_states', 'active', 'command_model', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.service_instance_events', 'table', 'service_instance_events', 'public', 'authority.service_lifecycle', 'service_instance_events', 'active', 'event_stream', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.service_instance_projection', 'table', 'service_instance_projection', 'public', 'authority.service_lifecycle', 'service_instance_projection', 'active', 'read_model', 'praxis.engine', 'decision.service_lifecycle.runtime_target_neutrality.20260422', '{}'::jsonb),
    ('table.public.event_log', 'table', 'event_log', 'public', 'authority.service_bus', 'event_log', 'active', 'transport', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.event_log_cursors', 'table', 'event_log_cursors', 'public', 'authority.service_bus', 'event_log_cursors', 'active', 'transport', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.service_bus_channel_registry', 'table', 'service_bus_channel_registry', 'public', 'authority.service_bus', 'service_bus_channel_registry', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.service_bus_message_contracts', 'table', 'service_bus_message_contracts', 'public', 'authority.service_bus', 'service_bus_message_contracts', 'active', 'definition', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.service_bus_message_ledger', 'table', 'service_bus_message_ledger', 'public', 'authority.service_bus', 'service_bus_message_ledger', 'active', 'transport', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    schema_name = EXCLUDED.schema_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
)
SELECT
    'operation.' || operation_name,
    operation_kind,
    operation_name,
    NULL,
    authority_domain_ref,
    'operation.' || operation_name,
    'active',
    CASE WHEN operation_kind = 'query' THEN 'read_model' ELSE 'command_model' END,
    'praxis.engine',
    decision_ref,
    jsonb_build_object(
        'operation_ref', operation_ref,
        'operation_kind', operation_kind,
        'source_kind', source_kind,
        'handler_ref', handler_ref
    )
FROM operation_catalog_registry
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
)
SELECT
    'projection.' || projection_ref,
    'projection',
    projection_ref,
    NULL,
    authority_domain_ref,
    projection_ref,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    'projection',
    'praxis.engine',
    decision_ref,
    jsonb_build_object(
        'source_event_stream_ref', source_event_stream_ref,
        'reducer_ref', reducer_ref,
        'freshness_policy_ref', freshness_policy_ref
    )
FROM authority_projection_registry
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
)
SELECT
    'event.' || event_type,
    'event',
    event_type,
    NULL,
    authority_domain_ref,
    'event.' || event_type,
    'active',
    'event_stream',
    'praxis.engine',
    decision_ref,
    jsonb_build_object('operation_ref', operation_ref, 'operation_name', operation_name)
FROM operation_catalog_registry
WHERE event_type IS NOT NULL AND btrim(event_type) <> ''
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
)
SELECT
    channel_ref,
    'service_bus_channel',
    channel_name,
    NULL,
    authority_domain_ref,
    channel_ref,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    'transport',
    'praxis.engine',
    decision_ref,
    jsonb_build_object('transport_kind', transport_kind, 'message_schema_ref', message_schema_ref)
FROM service_bus_channel_registry
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
)
SELECT
    'runtime_target.' || runtime_target_ref,
    'runtime_target',
    runtime_target_ref,
    NULL,
    'authority.service_lifecycle',
    'runtime_target.' || runtime_target_ref,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    'registry',
    'praxis.engine',
    decision_ref,
    jsonb_build_object(
        'substrate_kind', substrate_kind,
        'workspace_ref', workspace_ref,
        'base_path_ref', base_path_ref,
        'host_ref', host_ref
    )
FROM runtime_targets
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    registry.data_dictionary_object_kind,
    registry.object_name,
    CASE
        WHEN registry.object_kind IN (
            'table',
            'command',
            'event',
            'projection',
            'service_bus_channel',
            'feedback_stream',
            'definition',
            'runtime_target'
        ) THEN registry.object_kind
        ELSE 'object'
    END,
    'Authority-bound ' || registry.object_kind || ' owned by ' || registry.authority_domain_ref,
    jsonb_build_object('source', 'authority_object_registry', 'object_ref', registry.object_ref),
    jsonb_build_object(
        'authority_domain_ref', registry.authority_domain_ref,
        'write_model_kind', registry.write_model_kind,
        'lifecycle_status', registry.lifecycle_status
    )
FROM authority_object_registry registry
WHERE NOT EXISTS (
    SELECT 1
    FROM data_dictionary_objects dictionary
    WHERE dictionary.object_kind = registry.data_dictionary_object_kind
)
ON CONFLICT (object_kind) DO NOTHING;

CREATE OR REPLACE VIEW authority_object_ownership AS
SELECT
    registry.object_ref,
    registry.object_kind,
    registry.object_name,
    registry.schema_name,
    registry.authority_domain_ref,
    domains.owner_ref AS authority_owner_ref,
    domains.event_stream_ref,
    domains.current_projection_ref,
    registry.data_dictionary_object_kind,
    dictionary.category AS data_dictionary_category,
    registry.lifecycle_status,
    registry.write_model_kind,
    registry.owner_ref,
    registry.source_decision_ref,
    registry.source_receipt_id,
    registry.metadata,
    registry.created_at,
    registry.updated_at
FROM authority_object_registry registry
JOIN authority_domains domains
  ON domains.authority_domain_ref = registry.authority_domain_ref
LEFT JOIN data_dictionary_objects dictionary
  ON dictionary.object_kind = registry.data_dictionary_object_kind;

CREATE OR REPLACE VIEW authority_object_table_drift AS
SELECT
    'table.public.' || tables.table_name AS object_ref,
    'table'::text AS object_kind,
    tables.table_name AS object_name,
    'public'::text AS schema_name,
    'missing_authority_object_registry'::text AS drift_kind,
    jsonb_build_object('table_type', tables.table_type) AS details
FROM information_schema.tables tables
WHERE tables.table_schema = 'public'
  AND tables.table_type = 'BASE TABLE'
  AND NOT EXISTS (
      SELECT 1
      FROM authority_object_registry registry
      WHERE registry.object_kind = 'table'
        AND registry.schema_name = 'public'
        AND registry.object_name = tables.table_name
  );

CREATE OR REPLACE VIEW authority_object_operation_drift AS
SELECT
    'operation.' || operations.operation_name AS object_ref,
    'command'::text AS object_kind,
    operations.operation_name AS object_name,
    NULL::text AS schema_name,
    'missing_authority_object_registry'::text AS drift_kind,
    jsonb_build_object(
        'operation_ref', operations.operation_ref,
        'operation_kind', operations.operation_kind,
        'authority_domain_ref', operations.authority_domain_ref
    ) AS details
FROM operation_catalog_registry operations
WHERE operations.enabled = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM authority_object_registry registry
      WHERE registry.object_kind = 'command'
        AND registry.object_ref = 'operation.' || operations.operation_name
  );

CREATE OR REPLACE VIEW authority_object_projection_drift AS
SELECT
    'projection.' || projections.projection_ref AS object_ref,
    'projection'::text AS object_kind,
    projections.projection_ref AS object_name,
    NULL::text AS schema_name,
    'missing_authority_object_registry'::text AS drift_kind,
    jsonb_build_object(
        'authority_domain_ref', projections.authority_domain_ref,
        'source_event_stream_ref', projections.source_event_stream_ref
    ) AS details
FROM authority_projection_registry projections
WHERE projections.enabled = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM authority_object_registry registry
      WHERE registry.object_kind = 'projection'
        AND registry.object_ref = 'projection.' || projections.projection_ref
  );

CREATE OR REPLACE VIEW authority_object_dictionary_drift AS
SELECT
    registry.object_ref,
    registry.object_kind,
    registry.object_name,
    registry.schema_name,
    'missing_data_dictionary_object'::text AS drift_kind,
    jsonb_build_object('data_dictionary_object_kind', registry.data_dictionary_object_kind) AS details
FROM authority_object_registry registry
WHERE NOT EXISTS (
    SELECT 1
    FROM data_dictionary_objects dictionary
    WHERE dictionary.object_kind = registry.data_dictionary_object_kind
);

CREATE OR REPLACE VIEW authority_object_drift_report AS
SELECT * FROM authority_object_table_drift
UNION ALL
SELECT * FROM authority_object_operation_drift
UNION ALL
SELECT * FROM authority_object_projection_drift
UNION ALL
SELECT * FROM authority_object_dictionary_drift;

COMMENT ON TABLE authority_object_registry IS
    'Canonical CQRS ownership registry. Durable objects without rows here are authority drift.';
COMMENT ON VIEW authority_object_drift_report IS
    'Machine-readable drift report for durable objects missing authority or data-dictionary bindings.';
COMMENT ON TABLE service_bus_message_ledger IS
    'Inspectable transport envelope ledger. The service bus coordinates messages; domain authorities own state.';

COMMIT;
