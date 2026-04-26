-- Migration 260: Register provider control-plane CQRS query.
--
-- One read-side front door for provider levers: full capability matrix,
-- effective job catalog, route explanation, transport admissions, route rows,
-- and circuit state.

BEGIN;

-- 1. data_dictionary_objects (category='command' is the catalog convention
-- for both commands and queries — matches migration 248's pattern, since
-- the category CHECK constraint admits 'command' but query-specific
-- semantics live in metadata.operation_kind).

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'operation.operator.provider_control_plane',
    'Provider control plane',
    'command',
    'CQRS read model that groups provider/model levers by job type, transport, provider, model, cost structure, version, effective availability, blocked reasons, route explanation, admissions, route rows, and circuit state.',
    jsonb_build_object(
        'source', 'migration.260_provider_control_plane_query',
        'operation_name', 'operator.provider_control_plane',
        'operation_kind', 'query'
    ),
    jsonb_build_object(
        'operation_kind', 'query',
        'authority_domain_ref', 'authority.provider_onboarding',
        'projection_ref', 'projection.private_provider_job_catalog',
        'handler_ref', 'runtime.operations.queries.circuits.handle_query_provider_control_plane'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- 2. authority_object_registry (required because operation_catalog_registry
-- has a constraint that any enabled operation has a matching object_ref row).

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
    'operation.operator.provider_control_plane',
    'command',
    'operator.provider_control_plane',
    NULL,
    'authority.provider_onboarding',
    'operation.operator.provider_control_plane',
    'active',
    'read_model',
    'praxis.engine',
    'decision.provider_control_plane.20260426',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.queries.circuits.handle_query_provider_control_plane',
        'source_kind', 'operation_query'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref         = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind  = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status             = EXCLUDED.lifecycle_status,
    write_model_kind             = EXCLUDED.write_model_kind,
    owner_ref                    = EXCLUDED.owner_ref,
    source_decision_ref          = EXCLUDED.source_decision_ref,
    metadata                     = EXCLUDED.metadata,
    updated_at                   = now();

-- 3. operation_catalog_registry.

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
) VALUES (
    'operator-provider-control-plane',
    'operator.provider_control_plane',
    'operation_query',
    'query',
    'GET',
    '/api/operator/provider-control-plane',
    'runtime.operations.queries.circuits.QueryProviderControlPlane',
    'runtime.operations.queries.circuits.handle_query_provider_control_plane',
    'authority.provider_onboarding',
    'authority.provider_onboarding',
    'projection.private_provider_job_catalog',
    'observe',
    'read_only',
    TRUE,
    'binding.operation_catalog_registry.provider_control_plane.20260426',
    'decision.provider_control_plane.20260426',
    'runtime.operations.queries.circuits.QueryProviderControlPlane',
    'operation.output.default',
    'praxis.primary_postgres',
    TRUE,
    FALSE,
    'projection_freshness.default'
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
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    updated_at = now();

COMMIT;
