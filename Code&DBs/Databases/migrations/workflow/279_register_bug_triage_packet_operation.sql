-- Migration 279: Register the LLM bug triage packet query.
--
-- Expected objects:
--   row:data_dictionary_objects.operation.operator.bug_triage_packet
--   row:authority_object_registry.operation.operator.bug_triage_packet
--   row:operation_catalog_registry.operator.bug_triage_packet

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'operation.operator.bug_triage_packet',
    'LLM bug triage packet',
    'query',
    'Read-only packet that classifies bugs for future LLM runs as live defects, evidence debt, stale projections, platform friction, fixed-pending-verification, or inactive.',
    jsonb_build_object(
        'source', 'migration.279_register_bug_triage_packet_operation',
        'operation_name', 'operator.bug_triage_packet',
        'operation_kind', 'query'
    ),
    jsonb_build_object(
        'operation_kind', 'query',
        'authority_domain_ref', 'authority.bugs',
        'projection_ref', 'projection.bugs',
        'handler_ref', 'runtime.operations.queries.operator_observability.handle_query_bug_triage_packet'
    )
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
) VALUES (
    'operation.operator.bug_triage_packet',
    'query',
    'operator.bug_triage_packet',
    NULL,
    'authority.bugs',
    'operation.operator.bug_triage_packet',
    'active',
    'read_model',
    'praxis.engine',
    'decision.operation_catalog_registry.bug_triage_packet.20260426',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.queries.operator_observability.handle_query_bug_triage_packet',
        'source_kind', 'operation_query'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind                  = EXCLUDED.object_kind,
    authority_domain_ref         = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind  = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status             = EXCLUDED.lifecycle_status,
    write_model_kind             = EXCLUDED.write_model_kind,
    owner_ref                    = EXCLUDED.owner_ref,
    source_decision_ref          = EXCLUDED.source_decision_ref,
    metadata                     = EXCLUDED.metadata,
    updated_at                   = now();

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
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref,
    input_schema_ref,
    output_schema_ref,
    storage_target_ref,
    receipt_required,
    event_required,
    projection_freshness_policy_ref
)
VALUES (
    'operator-bug-triage-packet',
    'operator.bug_triage_packet',
    'operation_query',
    'query',
    'GET',
    '/api/operator/bug-triage-packet',
    'runtime.operations.queries.operator_observability.QueryBugTriagePacket',
    'runtime.operations.queries.operator_observability.handle_query_bug_triage_packet',
    'authority.bugs',
    'authority.bugs',
    'projection.bugs',
    'observe',
    'read_only',
    TRUE,
    'binding.operation_catalog_registry.bug_triage_packet.20260426',
    'decision.operation_catalog_registry.bug_triage_packet.20260426',
    'runtime.operations.queries.operator_observability.QueryBugTriagePacket',
    'operation.output.default',
    'praxis.primary_postgres',
    TRUE,
    FALSE,
    'projection_freshness.default'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name      = EXCLUDED.operation_name,
    source_kind         = EXCLUDED.source_kind,
    operation_kind      = EXCLUDED.operation_kind,
    http_method         = EXCLUDED.http_method,
    http_path           = EXCLUDED.http_path,
    input_model_ref     = EXCLUDED.input_model_ref,
    handler_ref         = EXCLUDED.handler_ref,
    authority_ref       = EXCLUDED.authority_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    projection_ref      = EXCLUDED.projection_ref,
    posture             = EXCLUDED.posture,
    idempotency_policy  = EXCLUDED.idempotency_policy,
    enabled             = EXCLUDED.enabled,
    binding_revision    = EXCLUDED.binding_revision,
    decision_ref        = EXCLUDED.decision_ref,
    input_schema_ref    = EXCLUDED.input_schema_ref,
    output_schema_ref   = EXCLUDED.output_schema_ref,
    storage_target_ref  = EXCLUDED.storage_target_ref,
    receipt_required    = EXCLUDED.receipt_required,
    event_required      = EXCLUDED.event_required,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    updated_at          = now();

COMMIT;
