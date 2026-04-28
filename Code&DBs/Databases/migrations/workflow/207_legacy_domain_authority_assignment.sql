-- Migration 207: legacy domain authority assignment.
--
-- 206 made historical tables visible. This assigns those legacy objects to
-- real domain authorities so the remaining work is domain adoption, not a
-- generic "legacy schema" bucket. Lifecycle remains legacy until each domain
-- has command/event/projection ownership.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES
    ('authority.access_control', 'praxis.engine', 'stream.access_control', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.api_catalog', 'praxis.engine', 'stream.api_catalog', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.conversations', 'praxis.engine', 'stream.conversations', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.data_dictionary', 'praxis.engine', 'stream.data_dictionary', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.dataset_refinery', 'praxis.engine', 'stream.dataset_refinery', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.execution_control', 'praxis.engine', 'stream.execution_control', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.heartbeat', 'praxis.engine', 'stream.heartbeat', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.integrations', 'praxis.engine', 'stream.integrations', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.mobile_access', 'praxis.engine', 'stream.mobile_access', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.platform_runtime', 'praxis.engine', 'stream.platform_runtime', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.registry', 'praxis.engine', 'stream.registry', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.review_quality', 'praxis.engine', 'stream.review_quality', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.sandbox_runtime', 'praxis.engine', 'stream.sandbox_runtime', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.secrets', 'praxis.engine', 'stream.secrets', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('authority.surface_catalog', 'praxis.engine', 'stream.surface_catalog', NULL, 'praxis.primary_postgres', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422')
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS authority_legacy_domain_assignment_rules (
    rule_ref TEXT PRIMARY KEY CHECK (btrim(rule_ref) <> ''),
    priority INTEGER NOT NULL CHECK (priority > 0),
    table_pattern TEXT NOT NULL CHECK (btrim(table_pattern) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    assignment_reason TEXT NOT NULL CHECK (btrim(assignment_reason) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT authority_legacy_domain_assignment_rules_unique_pattern
        UNIQUE (table_pattern, authority_domain_ref)
);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'authority_legacy_domain_assignment_rules',
        'Legacy domain assignment rules',
        'table',
        'Rules that map historical public tables from authority.legacy_schema to their real owning authority domains.',
        '{"migration":"207_legacy_domain_authority_assignment.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.cqrs"}'::jsonb
    ),
    (
        'authority_legacy_domain_assignment_summary',
        'Legacy domain assignment summary',
        'projection',
        'Read model summarizing domain-owned legacy inventory.',
        '{"migration":"207_legacy_domain_authority_assignment.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.cqrs"}'::jsonb
    ),
    (
        'authority.objects.domain_summary',
        'List authority domain assignment summary',
        'query',
        'Query operation for domain-ranked legacy CQRS adoption state.',
        '{"migration":"207_legacy_domain_authority_assignment.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.cqrs"}'::jsonb
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
        'table.public.authority_legacy_domain_assignment_rules',
        'table',
        'authority_legacy_domain_assignment_rules',
        'public',
        'authority.cqrs',
        'authority_legacy_domain_assignment_rules',
        'active',
        'definition',
        'praxis.engine',
        'decision.cqrs_legacy_domain_assignment.20260422',
        '{}'::jsonb
    ),
    (
        'projection.authority_legacy_domain_assignment_summary',
        'projection',
        'authority_legacy_domain_assignment_summary',
        NULL,
        'authority.cqrs',
        'authority_legacy_domain_assignment_summary',
        'active',
        'projection',
        'praxis.engine',
        'decision.cqrs_legacy_domain_assignment.20260422',
        '{}'::jsonb
    ),
    (
        'operation.authority.objects.domain_summary',
        'query',
        'authority.objects.domain_summary',
        NULL,
        'authority.cqrs',
        'authority.objects.domain_summary',
        'active',
        'read_model',
        'praxis.engine',
        'decision.cqrs_legacy_domain_assignment.20260422',
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

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    default_value,
    valid_values,
    examples,
    deprecation_notes,
    display_order,
    origin_ref,
    metadata
)
SELECT
    columns.table_name,
    columns.column_name,
    'auto',
    CASE
        WHEN columns.data_type IN ('boolean') THEN 'boolean'
        WHEN columns.data_type IN (
            'smallint',
            'integer',
            'bigint',
            'decimal',
            'numeric',
            'real',
            'double precision',
            'smallserial',
            'serial',
            'bigserial'
        ) THEN 'number'
        WHEN columns.data_type IN ('json', 'jsonb') THEN 'json'
        WHEN columns.data_type = 'ARRAY' THEN 'array'
        WHEN columns.data_type = 'date' THEN 'date'
        WHEN columns.data_type LIKE 'timestamp%' THEN 'datetime'
        ELSE 'text'
    END,
    columns.column_name,
    'Column discovered from information_schema for the CQRS legacy domain assignment rules table.',
    columns.is_nullable = 'NO',
    CASE
        WHEN columns.column_default IS NULL THEN NULL
        ELSE to_jsonb(columns.column_default)
    END,
    '[]'::jsonb,
    '[]'::jsonb,
    '',
    columns.ordinal_position * 10,
    jsonb_build_object(
        'source', 'information_schema.columns',
        'schema_name', columns.table_schema,
        'table_name', columns.table_name,
        'column_name', columns.column_name,
        'migration', '207_legacy_domain_authority_assignment.sql'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.cqrs',
        'data_type', columns.data_type,
        'udt_name', columns.udt_name,
        'is_nullable', columns.is_nullable
    )
FROM information_schema.columns columns
WHERE columns.table_schema = 'public'
  AND columns.table_name = 'authority_legacy_domain_assignment_rules'
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    default_value = EXCLUDED.default_value,
    valid_values = EXCLUDED.valid_values,
    examples = EXCLUDED.examples,
    deprecation_notes = EXCLUDED.deprecation_notes,
    display_order = EXCLUDED.display_order,
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
)
SELECT
    'column.' || columns.table_schema || '.' || columns.table_name || '.' || columns.column_name,
    'column',
    columns.table_name || '.' || columns.column_name,
    columns.table_schema,
    'authority.cqrs',
    columns.table_name,
    'active',
    'definition',
    'praxis.engine',
    'decision.cqrs_legacy_domain_assignment.20260422',
    jsonb_build_object(
        'source', 'information_schema.columns',
        'table_name', columns.table_name,
        'column_name', columns.column_name,
        'ordinal_position', columns.ordinal_position,
        'data_type', columns.data_type,
        'udt_name', columns.udt_name,
        'is_nullable', columns.is_nullable,
        'column_default_present', columns.column_default IS NOT NULL
    )
FROM information_schema.columns columns
WHERE columns.table_schema = 'public'
  AND columns.table_name = 'authority_legacy_domain_assignment_rules'
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
) VALUES (
    'authority-objects-domain-summary',
    'authority.objects.domain_summary',
    'operation_query',
    'query',
    'GET',
    '/api/authority/objects/domain-summary',
    'runtime.authority_objects.ListAuthorityDomainSummaryCommand',
    'runtime.authority_objects.handle_list_authority_domain_summary',
    'authority.cqrs',
    'authority.cqrs',
    'projection.legacy.schema_catalog',
    'praxis.primary_postgres',
    'runtime.authority_objects.ListAuthorityDomainSummaryCommand',
    'authority.objects.domain_summary',
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
    'binding.operation_catalog_registry.legacy_domain_assignment.20260422',
    'decision.cqrs_legacy_domain_assignment.20260422'
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

INSERT INTO authority_legacy_domain_assignment_rules (
    rule_ref,
    priority,
    table_pattern,
    authority_domain_ref,
    assignment_reason,
    enabled,
    decision_ref
) VALUES
    ('legacy_domain.rule.schema_migrations', 90, 'schema_migrations', 'authority.cqrs', 'Migration receipts are CQRS/kernel adoption state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.operation_catalog', 91, 'operation_catalog%', 'authority.cqrs', 'Operation catalog rows are command-boundary authority state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.authority_kernel', 92, 'authority_%', 'authority.cqrs', 'Authority kernel tables belong to CQRS authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.bugs', 100, 'bugs', 'authority.bugs', 'Bug authority owns bug rows.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.bug_evidence_links', 101, 'bug_evidence_links', 'authority.bugs', 'Bug authority owns bug evidence links.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.issues', 110, 'issues', 'authority.operator_issues', 'Operator issue authority owns issue rows.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.roadmap', 120, 'roadmap_%', 'authority.roadmap_items', 'Roadmap authority owns roadmap tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.work_item', 130, 'work_item_%', 'authority.work_item_closeout', 'Work-item closeout authority owns closeout bindings.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.operator_decisions', 140, 'operator_decisions', 'authority.operator_decisions', 'Operator decisions table is decision authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.promotion_decisions', 141, 'promotion_decisions', 'authority.operator_decisions', 'Promotion decisions are operator decision rows.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.operator_ideas', 150, 'operator_idea%', 'authority.operator_ideas', 'Operator ideas authority owns idea tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.operator_object_relations', 160, 'operator_object_relations', 'authority.operator_object_relations', 'Operator object relation authority owns relation rows.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.semantic_assertions', 170, 'semantic%assertions', 'authority.semantic_assertions', 'Semantic assertion authority owns assertion tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.semantic_predicates', 171, 'semantic_predicates', 'authority.semantic_predicates', 'Semantic predicate authority owns predicate registry.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.memory', 180, 'memory_%', 'authority.memory_entities', 'Memory authority owns graph and evidence tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.context_bundles', 181, 'context_bundle%', 'authority.memory_entities', 'Context bundles are memory/context objects.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.persona', 182, 'persona_%', 'authority.memory_entities', 'Persona context is memory-scoped state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.workflow_build', 200, 'workflow_build_%', 'authority.workflow_build', 'Workflow build authority owns builder state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.workflow', 210, 'workflow_%', 'authority.workflow_runs', 'Workflow runtime authority owns workflow execution state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.workflows', 211, 'workflows', 'authority.workflow_runs', 'Workflow runtime authority owns workflow roots.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.run_nodes', 212, 'run_%', 'authority.workflow_runs', 'Run graph tables are workflow runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.execution_packets', 213, 'execution_packet%', 'authority.workflow_runs', 'Execution packets are workflow runtime provenance.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.execution_leases', 214, 'execution_leases', 'authority.workflow_runs', 'Execution leases are workflow runtime execution state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.schedule_definitions', 215, 'schedule_definitions', 'authority.workflow_runs', 'Schedules feed workflow runtime execution.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.recurring_windows', 216, 'recurring_run_windows', 'authority.workflow_runs', 'Recurring windows feed workflow runtime execution.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.control_commands', 230, 'control_commands', 'authority.execution_control', 'Control commands are execution control authority state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.idempotency', 231, 'idempotency_ledger', 'authority.execution_control', 'Idempotency is execution control state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.admission', 232, 'admission_decisions', 'authority.execution_control', 'Admission decisions control execution.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.maintenance', 233, 'maintenance_%', 'authority.execution_control', 'Maintenance policy/intents are execution control.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.autonomy', 234, 'autonomy_%', 'authority.execution_control', 'Autonomy ledger is execution control state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.mobile', 250, 'mobile_%', 'authority.mobile_access', 'Mobile authority owns mobile sessions and bootstrap state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.webauthn', 251, 'webauthn_%', 'authority.mobile_access', 'WebAuthn challenges are mobile access state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.device', 252, 'device_%', 'authority.mobile_access', 'Device enrollments are mobile access state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.session_blast_radius', 253, 'session_blast_radius%', 'authority.mobile_access', 'Session blast-radius state is access control for mobile/operator sessions.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.capability_grants', 260, 'capability_grants', 'authority.access_control', 'Capability grants are access-control state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.approvals', 261, 'approval_%', 'authority.access_control', 'Approvals are explicit access-control decisions.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.gates', 262, '%gate%', 'authority.access_control', 'Gate evaluations and cutover gates are access-control decisions.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.capability_outcomes', 259, 'capability_outcomes', 'authority.feedback', 'Capability outcomes are feedback evidence.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.capability_catalog', 270, 'capability_%', 'authority.capability_catalog', 'Capability catalog owns capability definitions.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.data_dictionary', 300, 'data_dictionary_%', 'authority.data_dictionary', 'Data dictionary authority owns dictionary tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.dataset', 310, 'dataset_%', 'authority.dataset_refinery', 'Dataset refinery owns dataset tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.provider_route_health', 319, 'provider_route_health_%', 'authority.task_route_eligibility', 'Route health is task-route eligibility state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.transport_admissions', 320, 'provider_transport_%', 'authority.transport_eligibility', 'Transport admission state is transport eligibility.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.provider', 321, 'provider_%', 'authority.provider_onboarding', 'Provider authority owns provider/model state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.model', 322, 'model_%', 'authority.provider_onboarding', 'Model profile state belongs to provider authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.market', 323, 'market_%', 'authority.provider_onboarding', 'Market model metadata belongs to provider authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.adapter', 324, 'adapter_%', 'authority.provider_onboarding', 'Adapter configuration is provider/runtime onboarding state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.compiler_route_hints', 325, 'compiler_route_hints', 'authority.provider_onboarding', 'Compiler routing hints select provider/model behavior.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.rate_limits', 326, 'rate_limit_configs', 'authority.provider_onboarding', 'Rate limits constrain provider usage.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.task_type', 331, 'task_type_%', 'authority.task_route_eligibility', 'Task-type routing state is task-route eligibility.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.route', 332, 'route_%', 'authority.task_route_eligibility', 'Route policy/eligibility state is task-route authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.receipts', 350, 'receipt%', 'authority.receipts', 'Receipt authority owns receipt tables.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.verification', 351, 'verification_%', 'authority.receipts', 'Verification rows are receipt/evidence proof state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.verify', 352, 'verify_%', 'authority.receipts', 'Verifier refs are receipt/evidence proof state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.heal', 353, 'heal%', 'authority.receipts', 'Healing rows are verifier/receipt proof state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.integration', 370, 'integration_%', 'authority.integrations', 'Integration authority owns integration registry rows.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.connector', 371, 'connector_%', 'authority.integrations', 'Connector registry rows are integration authority state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.webhook', 372, 'webhook_%', 'authority.integrations', 'Webhook endpoints/events are integration ingress state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.uploaded_files', 373, 'uploaded_files', 'authority.integrations', 'Uploaded files are integration/file ingress state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.app_manifest', 390, 'app_manifest%', 'authority.object_schema', 'App manifests are object/schema authority state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.object', 391, 'object%', 'authority.object_schema', 'Object registry tables belong to object schema authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.api', 400, 'api_%', 'authority.api_catalog', 'API catalog tables belong to API catalog authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.workflow_surface_usage', 199, 'workflow_surface_usage_%', 'authority.surface_catalog', 'Workflow surface usage belongs to surface catalog authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.surface', 410, 'surface_%', 'authority.surface_catalog', 'Surface catalog tables belong to surface catalog authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.registry', 420, 'registry_%', 'authority.registry', 'Registry tables belong to registry authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.sandbox', 440, 'sandbox_%', 'authority.sandbox_runtime', 'Sandbox tables are sandbox runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.fork', 441, 'fork_%', 'authority.sandbox_runtime', 'Fork/worktree state belongs to sandbox runtime.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.repo_snapshots', 442, 'repo_snapshots', 'authority.sandbox_runtime', 'Repo snapshots are sandbox/runtime provenance.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.heartbeat', 460, 'heartbeat_%', 'authority.heartbeat', 'Heartbeat tables belong to heartbeat authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.conversations', 480, 'conversation%', 'authority.conversations', 'Conversation tables belong to conversation authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.agent', 481, 'agent_%', 'authority.conversations', 'Agent profiles/sessions are conversation/runtime actor state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.review', 500, 'review_%', 'authority.review_quality', 'Review tables belong to review/quality authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.review_records', 501, 'review_records', 'authority.review_quality', 'Review records are review/quality feedback state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.debate', 502, 'debate_%', 'authority.review_quality', 'Debate metrics are review/quality state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.quality', 503, 'quality_%', 'authority.review_quality', 'Quality rollups are review/quality state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.secrets', 520, 'credential_%', 'authority.secrets', 'Credential token tables belong to secret authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.service_lifecycle', 540, 'service_%', 'authority.service_lifecycle', 'Service tables belong to service lifecycle authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.runtime_targets', 541, 'runtime_targets', 'authority.service_lifecycle', 'Runtime targets belong to service lifecycle authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.event_bus', 560, 'event_%', 'authority.service_bus', 'Event subscriptions/log-adjacent state belongs to service-bus authority.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.system_events', 561, 'system_events', 'authority.service_bus', 'System events are service-bus/event state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.functional_areas', 580, 'functional_areas', 'authority.functional_areas', 'Functional area registry owns functional areas.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.observability_failure', 600, 'failure_%', 'authority.observability_metrics', 'Failure catalog/metrics are observability state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.observability_friction', 601, 'friction_%', 'authority.observability_metrics', 'Friction events are observability state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.retrieval_metrics', 602, 'retrieval_%', 'authority.observability_metrics', 'Retrieval metrics are observability state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.reference_catalog', 620, 'reference_catalog', 'authority.registry', 'Reference catalog is registry authority state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.subscription_checkpoints', 640, 'subscription_%', 'authority.platform_runtime', 'Subscription checkpoints are platform runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.audit_exclusions', 641, 'audit_%', 'authority.platform_runtime', 'Audit exclusion policy is platform runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.policy_drift', 642, 'policy_%', 'authority.platform_runtime', 'Policy drift rows are platform runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.platform_config', 643, 'platform_%', 'authority.platform_runtime', 'Platform config is platform runtime state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.multimodal_ingest', 644, 'multimodal_%', 'authority.platform_runtime', 'Multimodal staging is platform runtime ingress state.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422'),
    ('legacy_domain.rule.fallback', 10000, '%', 'authority.platform_runtime', 'Fallback owner for residual historical tables until a narrower domain is created.', TRUE, 'decision.cqrs_legacy_domain_assignment.20260422')
ON CONFLICT (rule_ref) DO UPDATE SET
    priority = EXCLUDED.priority,
    table_pattern = EXCLUDED.table_pattern,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    assignment_reason = EXCLUDED.assignment_reason,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH matched_tables AS (
    SELECT
        registry.object_ref,
        registry.object_name AS table_name,
        registry.authority_domain_ref AS previous_authority_domain_ref,
        rules.rule_ref,
        rules.authority_domain_ref
    FROM authority_object_registry registry
    JOIN LATERAL (
        SELECT rule_ref, authority_domain_ref
        FROM authority_legacy_domain_assignment_rules rules
        WHERE rules.enabled = TRUE
          AND registry.object_name LIKE rules.table_pattern
        ORDER BY rules.priority ASC, rules.rule_ref ASC
        LIMIT 1
    ) rules ON TRUE
    WHERE registry.object_kind = 'table'
      AND registry.schema_name = 'public'
      AND registry.lifecycle_status = 'legacy'
)
UPDATE authority_object_registry registry
   SET authority_domain_ref = matched.authority_domain_ref,
       metadata = registry.metadata || jsonb_build_object(
           'domain_assignment_status', 'domain_assigned_legacy',
           'domain_assignment_rule_ref', matched.rule_ref,
           'previous_authority_domain_ref', matched.previous_authority_domain_ref
       ),
       updated_at = now()
  FROM matched_tables matched
 WHERE registry.object_ref = matched.object_ref;

WITH table_domains AS (
    SELECT
        registry.object_name AS table_name,
        registry.authority_domain_ref,
        registry.lifecycle_status,
        registry.owner_ref,
        registry.metadata ->> 'domain_assignment_rule_ref' AS domain_assignment_rule_ref
    FROM authority_object_registry registry
    WHERE registry.object_kind = 'table'
      AND registry.schema_name = 'public'
)
UPDATE authority_object_registry columns
   SET authority_domain_ref = table_domains.authority_domain_ref,
       lifecycle_status = table_domains.lifecycle_status,
       owner_ref = table_domains.owner_ref,
       metadata = columns.metadata || jsonb_build_object(
           'domain_assignment_status',
           CASE
               WHEN table_domains.lifecycle_status = 'legacy' THEN 'domain_assigned_legacy'
               ELSE 'cqrs_adopted'
           END,
           'domain_assignment_rule_ref', table_domains.domain_assignment_rule_ref
       ),
       updated_at = now()
  FROM table_domains
 WHERE columns.object_kind = 'column'
   AND columns.schema_name = 'public'
   AND columns.metadata ->> 'table_name' = table_domains.table_name;

WITH table_domains AS (
    SELECT
        registry.object_name AS table_name,
        registry.authority_domain_ref,
        registry.lifecycle_status,
        registry.metadata ->> 'domain_assignment_rule_ref' AS domain_assignment_rule_ref
    FROM authority_object_registry registry
    WHERE registry.object_kind = 'table'
      AND registry.schema_name = 'public'
)
UPDATE data_dictionary_objects dictionary
   SET metadata = dictionary.metadata || jsonb_build_object(
           'authority_domain_ref', table_domains.authority_domain_ref,
           'lifecycle_status', table_domains.lifecycle_status,
           'domain_assignment_rule_ref', table_domains.domain_assignment_rule_ref,
           'domain_assignment_status',
           CASE
               WHEN table_domains.lifecycle_status = 'legacy' THEN 'domain_assigned_legacy'
               ELSE 'cqrs_adopted'
           END
       ),
       updated_at = now()
  FROM table_domains
 WHERE dictionary.object_kind = table_domains.table_name;

CREATE OR REPLACE VIEW authority_schema_adoption_report AS
WITH public_tables AS (
    SELECT tables.table_schema, tables.table_name
    FROM information_schema.tables tables
    WHERE tables.table_schema = 'public'
      AND tables.table_type = 'BASE TABLE'
),
table_registry AS (
    SELECT
        public_tables.table_schema,
        public_tables.table_name,
        registry.object_ref,
        registry.authority_domain_ref,
        registry.lifecycle_status,
        registry.write_model_kind,
        registry.owner_ref,
        registry.metadata ->> 'domain_assignment_rule_ref' AS domain_assignment_rule_ref
    FROM public_tables
    LEFT JOIN authority_object_registry registry
      ON registry.object_kind = 'table'
     AND registry.schema_name = public_tables.table_schema
     AND registry.object_name = public_tables.table_name
),
column_counts AS (
    SELECT
        columns.table_schema,
        columns.table_name,
        count(*)::integer AS column_count
    FROM information_schema.columns columns
    WHERE columns.table_schema = 'public'
    GROUP BY columns.table_schema, columns.table_name
),
registered_column_counts AS (
    SELECT
        registry.schema_name AS table_schema,
        split_part(registry.object_name, '.', 1) AS table_name,
        count(*)::integer AS registered_column_count
    FROM authority_object_registry registry
    WHERE registry.object_kind = 'column'
      AND registry.schema_name = 'public'
    GROUP BY registry.schema_name, split_part(registry.object_name, '.', 1)
)
SELECT
    table_registry.table_schema,
    table_registry.table_name,
    table_registry.object_ref,
    table_registry.authority_domain_ref,
    table_registry.lifecycle_status,
    table_registry.write_model_kind,
    table_registry.owner_ref,
    COALESCE(column_counts.column_count, 0) AS column_count,
    COALESCE(registered_column_counts.registered_column_count, 0) AS registered_column_count,
    CASE
        WHEN table_registry.object_ref IS NULL THEN 'missing_authority_registry'
        WHEN table_registry.authority_domain_ref = 'authority.legacy_schema'
          OR table_registry.lifecycle_status = 'legacy'
           AND table_registry.domain_assignment_rule_ref IS NULL
            THEN 'legacy_inventory'
        WHEN table_registry.lifecycle_status = 'legacy'
            THEN 'domain_assigned_legacy'
        ELSE 'cqrs_adopted'
    END AS adoption_status,
    jsonb_build_object(
        'column_registration_complete',
        COALESCE(column_counts.column_count, 0) = COALESCE(registered_column_counts.registered_column_count, 0),
        'needs_domain_modernization',
        table_registry.lifecycle_status = 'legacy',
        'domain_assignment_rule_ref',
        table_registry.domain_assignment_rule_ref
    ) AS adoption_metadata,
    table_registry.domain_assignment_rule_ref
FROM table_registry
LEFT JOIN column_counts
  ON column_counts.table_schema = table_registry.table_schema
 AND column_counts.table_name = table_registry.table_name
LEFT JOIN registered_column_counts
  ON registered_column_counts.table_schema = table_registry.table_schema
 AND registered_column_counts.table_name = table_registry.table_name;

CREATE OR REPLACE VIEW authority_legacy_backfill_summary AS
SELECT
    adoption_status,
    count(*)::integer AS table_count,
    sum(column_count)::integer AS column_count,
    sum(registered_column_count)::integer AS registered_column_count
FROM authority_schema_adoption_report
GROUP BY adoption_status;

CREATE OR REPLACE VIEW authority_legacy_domain_assignment_summary AS
SELECT
    authority_domain_ref,
    adoption_status,
    count(*)::integer AS table_count,
    sum(column_count)::integer AS column_count,
    sum(registered_column_count)::integer AS registered_column_count
FROM authority_schema_adoption_report
GROUP BY authority_domain_ref, adoption_status;

COMMENT ON TABLE authority_legacy_domain_assignment_rules IS
    'Deterministic rules that assign legacy public tables to real domain authorities. These rules do not claim command/event modernization.';
COMMENT ON VIEW authority_schema_adoption_report IS
    'Per-table CQRS adoption state. domain_assigned_legacy means a real authority owns inventory, but the write model still needs modernization.';
COMMENT ON VIEW authority_legacy_domain_assignment_summary IS
    'Domain-ranked summary of legacy tables assigned to real authorities.';

COMMIT;
