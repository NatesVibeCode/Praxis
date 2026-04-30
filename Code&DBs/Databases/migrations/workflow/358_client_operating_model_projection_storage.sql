-- Migration 358: Client Operating Model operator-view snapshot storage.
--
-- Phase 13 keeps the request-time operator view read-only, then adds a
-- separate command/readback pair for durable historical snapshots.

BEGIN;

CREATE TABLE IF NOT EXISTS client_operating_model_operator_view_snapshots (
    snapshot_digest text PRIMARY KEY CHECK (btrim(snapshot_digest) <> ''),
    snapshot_ref text NOT NULL UNIQUE CHECK (btrim(snapshot_ref) <> ''),
    view_name text NOT NULL CHECK (btrim(view_name) <> ''),
    view_id text NOT NULL CHECK (btrim(view_id) <> ''),
    scope_ref text NOT NULL DEFAULT 'global' CHECK (btrim(scope_ref) <> ''),
    state text NOT NULL CHECK (btrim(state) <> ''),
    freshness_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    permission_scope_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    correlation_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    operator_view_json jsonb NOT NULL,
    observed_by_ref text NULL CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text NULL CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_client_operating_model_snapshots_view_scope
    ON client_operating_model_operator_view_snapshots (view_name, scope_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_client_operating_model_snapshots_view_id
    ON client_operating_model_operator_view_snapshots (view_id);

CREATE INDEX IF NOT EXISTS idx_client_operating_model_snapshots_state
    ON client_operating_model_operator_view_snapshots (state);

CREATE INDEX IF NOT EXISTS idx_client_operating_model_snapshots_payload_gin
    ON client_operating_model_operator_view_snapshots USING gin (operator_view_json);

CREATE OR REPLACE FUNCTION touch_client_operating_model_operator_view_snapshots_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_client_operating_model_operator_view_snapshots_touch
    ON client_operating_model_operator_view_snapshots;
CREATE TRIGGER trg_client_operating_model_operator_view_snapshots_touch
    BEFORE UPDATE ON client_operating_model_operator_view_snapshots
    FOR EACH ROW EXECUTE FUNCTION touch_client_operating_model_operator_view_snapshots_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'client_operating_model_operator_view_snapshots',
    'Client Operating Model operator-view snapshots',
    'table',
    'Durable historical snapshots of derived Client Operating Model operator views.',
    '{"migration":"358_client_operating_model_projection_storage.sql"}'::jsonb,
    '{"authority_domain_ref":"authority.client_operating_model"}'::jsonb
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
    'table.public.client_operating_model_operator_view_snapshots',
    'table',
    'client_operating_model_operator_view_snapshots',
    'public',
    'authority.client_operating_model',
    'client_operating_model_operator_view_snapshots',
    'active',
    'registry',
    'praxis.engine',
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"purpose":"historical operator-view snapshot authority"}'::jsonb
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
) VALUES (
    'event_contract.client_operating_model.operator_view_snapshot_stored',
    'client_operating_model.operator_view_snapshot_stored',
    'authority.client_operating_model',
    'data_dictionary.object.client_operating_model_operator_view_snapshot_stored_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["snapshot_digest","snapshot_ref","view_name","view_id","scope_ref","state"]}'::jsonb
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
    p_operation_ref         := 'client-operating-model-operator-view-snapshot-store',
    p_operation_name        := 'client_operating_model_operator_view_snapshot_store',
    p_handler_ref           := 'runtime.operations.commands.client_operating_model.handle_store_operator_view_snapshot',
    p_input_model_ref       := 'runtime.operations.commands.client_operating_model.StoreOperatorViewSnapshotCommand',
    p_authority_domain_ref  := 'authority.client_operating_model',
    p_authority_ref         := 'authority.client_operating_model',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator/client-operating-model/snapshots',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'client_operating_model.operator_view_snapshot_stored',
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_operating_model_operator_view_snapshot_store.20260430',
    p_label                 := 'Client Operating Model Store Operator View Snapshot',
    p_summary               := 'Persist one Client Operating Model operator-view snapshot for historical readback.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'client-operating-model-operator-view-snapshot-read',
    p_operation_name        := 'client_operating_model_operator_view_snapshot_read',
    p_handler_ref           := 'runtime.operations.queries.client_operating_model.handle_client_operating_model_snapshot_read',
    p_input_model_ref       := 'runtime.operations.queries.client_operating_model.QueryClientOperatingModelSnapshotRead',
    p_authority_domain_ref  := 'authority.client_operating_model',
    p_authority_ref         := 'authority.client_operating_model',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/operator/client-operating-model/snapshots',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.client_operating_model_operator_view_snapshot_read.20260430',
    p_label                 := 'Client Operating Model Read Operator View Snapshots',
    p_summary               := 'Read stored Client Operating Model operator-view snapshots by snapshot ref or latest view/scope.'
);

COMMIT;
