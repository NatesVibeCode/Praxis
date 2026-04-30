-- Migration 370: Workspace surface migration authority.
--
-- Adds a receipt-backed preview/apply pair for migrating old blank workspace
-- grid surfaces to the compose surface. The migration tool is deliberately
-- small: preview is read-only, apply is idempotent, and the app_manifest
-- history table records the before/after snapshots.

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'workspace_surface_migrated_event',
    'workspace.surface_migrated event payload',
    'event',
    'Conceptual event emitted when an app manifest workspace surface migration is applied or idempotently observed as already applied.',
    '{"migration":"370_workspace_surface_migration_authority.sql"}'::jsonb,
    '{"event_type":"workspace.surface_migrated","payload_fields":["manifest_id","migration_ref","surface_id","tab_id","changed","from_hash","to_hash","changed_by","reject_reason"]}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
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
) VALUES (
    'event_contract.workspace.surface_migrated',
    'workspace.surface_migrated',
    'authority.surface_catalog',
    'data_dictionary.object.workspace_surface_migrated_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    '{"expected_payload_fields":["manifest_id","migration_ref","surface_id","tab_id","changed","from_hash","to_hash","changed_by","reject_reason"]}'::jsonb
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
    p_operation_ref         := 'workspace.query.surface_migration_preview',
    p_operation_name        := 'workspace.surface_migration.preview',
    p_handler_ref           := 'runtime.operations.queries.workspace_surface_migration.handle_workspace_surface_migration_preview',
    p_input_model_ref       := 'runtime.operations.queries.workspace_surface_migration.QueryWorkspaceSurfaceMigrationPreview',
    p_authority_domain_ref  := 'authority.surface_catalog',
    p_authority_ref         := 'authority.surface_catalog',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workspaces/{manifest_id}/surface-migration/preview',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.workspace_surface_migration_preview.20260430',
    p_label                 := 'Workspace Surface Migration Preview',
    p_summary               := 'Preview a workspace app-manifest surface migration without mutating app_manifests.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workspace.command.surface_migration_apply',
    p_operation_name        := 'workspace.surface_migration.apply',
    p_handler_ref           := 'runtime.operations.commands.workspace_surface_migration.handle_workspace_surface_migration_apply',
    p_input_model_ref       := 'runtime.operations.commands.workspace_surface_migration.ApplyWorkspaceSurfaceMigrationCommand',
    p_authority_domain_ref  := 'authority.surface_catalog',
    p_authority_ref         := 'authority.surface_catalog',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workspaces/{manifest_id}/surface-migration/apply',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workspace.surface_migrated',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.workspace_surface_migration_apply.20260430',
    p_label                 := 'Workspace Surface Migration Apply',
    p_summary               := 'Apply an idempotent app-manifest workspace surface migration and record before/after history.'
);

COMMIT;
