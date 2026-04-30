-- Migration 369: Manifest-run binding authority.

BEGIN;

CREATE TABLE IF NOT EXISTS manifest_run_bindings (
    manifest_id text NOT NULL REFERENCES app_manifests(id) ON DELETE CASCADE,
    workflow_id text NOT NULL,
    run_id text NOT NULL REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    operation_receipt_id uuid REFERENCES authority_operation_receipts(receipt_id) ON DELETE SET NULL,
    dispatched_at timestamptz NOT NULL DEFAULT now(),
    dispatched_by text NOT NULL DEFAULT 'workspace.compose',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (manifest_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_manifest_run_bindings_manifest_dispatched
    ON manifest_run_bindings (manifest_id, dispatched_at DESC);

CREATE INDEX IF NOT EXISTS idx_manifest_run_bindings_run
    ON manifest_run_bindings (run_id);

CREATE INDEX IF NOT EXISTS idx_manifest_run_bindings_workflow
    ON manifest_run_bindings (workflow_id, dispatched_at DESC);

COMMENT ON TABLE manifest_run_bindings IS
    'Durable workspace/app-manifest to workflow-run binding. Receipts tabs and workspace run views must read through this join.';
COMMENT ON COLUMN manifest_run_bindings.operation_receipt_id IS
    'Optional parent operation receipt that caused the dispatch; the binding command also receives its own authority_operation_receipts row.';

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('manifest_run_bindings', 'Manifest run bindings', 'table', 'Durable app_manifest to workflow_run bindings used to scope workspace receipts and run views.', '{"migration":"369_manifest_run_bindings_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.workflow_runs"}'::jsonb)
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
    ('table.public.manifest_run_bindings', 'table', 'manifest_run_bindings', 'public', 'authority.workflow_runs', 'manifest_run_bindings', 'active', 'registry', 'praxis.engine', 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus', '{}'::jsonb)
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
) VALUES (
    'event_contract.workspace.run_bound',
    'workspace.run_bound',
    'authority.workflow_runs',
    'data_dictionary.object.workspace_run_bound_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    '{"expected_payload_fields":["manifest_id","workflow_id","run_id","operation_receipt_id","dispatched_by"]}'::jsonb
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
    p_operation_ref         := 'workspace.command.run_binding_record',
    p_operation_name        := 'workspace.run_binding.record',
    p_handler_ref           := 'runtime.operations.commands.workspace_run_bindings.handle_workspace_run_binding_record',
    p_input_model_ref       := 'runtime.operations.commands.workspace_run_bindings.RecordWorkspaceRunBindingCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workspaces/{manifest_id}/run-bindings',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workspace.run_bound',
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.workspace_run_binding_record.20260430',
    p_label                 := 'Workspace Run Binding Record',
    p_summary               := 'Record the durable binding from an app manifest workspace to a dispatched workflow run.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workspace.query.runs_list',
    p_operation_name        := 'workspace.runs.list',
    p_handler_ref           := 'runtime.operations.queries.workspace_run_bindings.handle_workspace_runs_list',
    p_input_model_ref       := 'runtime.operations.queries.workspace_run_bindings.QueryWorkspaceRunsList',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workspaces/{manifest_id}/runs',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.workspace_runs_list.20260430',
    p_label                 := 'Workspace Runs List',
    p_summary               := 'List workflow runs bound to an app manifest workspace.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workspace.query.receipts_list',
    p_operation_name        := 'workspace.receipts.list',
    p_handler_ref           := 'runtime.operations.queries.workspace_run_bindings.handle_workspace_receipts_list',
    p_input_model_ref       := 'runtime.operations.queries.workspace_run_bindings.QueryWorkspaceReceiptsList',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workspaces/{manifest_id}/receipts',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.workspace_receipts_list.20260430',
    p_label                 := 'Workspace Receipts List',
    p_summary               := 'List workflow receipts scoped through manifest_run_bindings for one workspace.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'verifier.query.catalog_list',
    p_operation_name        := 'verifier.catalog.list',
    p_handler_ref           := 'runtime.operations.queries.verifier_catalog.handle_verifier_catalog_list',
    p_input_model_ref       := 'runtime.operations.queries.verifier_catalog.QueryVerifierCatalogList',
    p_authority_domain_ref  := 'authority.receipts',
    p_authority_ref         := 'authority.receipts',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/verifiers',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.verifier_catalog_list.20260430',
    p_label                 := 'Verifier Catalog List',
    p_summary               := 'List registered verifier authority refs for workspace compose outcome gates.'
);

COMMIT;
