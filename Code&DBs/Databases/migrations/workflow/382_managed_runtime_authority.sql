-- Migration 382: Managed runtime accounting authority.
--
-- Phase 10 already owns the pure managed-runtime accounting contract under
-- runtime.managed_runtime. This migration adds the durable CQRS shell:
-- persisted runtime accounting records, queryable meter/health/audit facets,
-- pricing schedule references, and gateway operations for record/read.

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
    'authority.managed_runtime',
    'praxis.engine',
    'stream.authority.managed_runtime',
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

CREATE TABLE IF NOT EXISTS managed_runtime_pricing_schedule_versions (
    version_ref text PRIMARY KEY,
    schedule_ref text NOT NULL,
    effective_at timestamptz NOT NULL,
    currency text NOT NULL,
    cpu_core_second_rate numeric(18, 6) NOT NULL DEFAULT 0,
    memory_gib_second_rate numeric(18, 6) NOT NULL DEFAULT 0,
    accelerator_second_rate numeric(18, 6) NOT NULL DEFAULT 0,
    minimum_charge numeric(18, 6) NOT NULL DEFAULT 0,
    schedule_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_pricing_schedule
    ON managed_runtime_pricing_schedule_versions (schedule_ref, effective_at DESC);

CREATE TABLE IF NOT EXISTS managed_runtime_records (
    runtime_record_id text PRIMARY KEY,
    run_id text NOT NULL,
    receipt_id text NOT NULL,
    tenant_ref text NOT NULL,
    environment_ref text NOT NULL,
    workflow_ref text NOT NULL,
    workload_class text NOT NULL,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    configured_mode text NOT NULL CHECK (configured_mode IN ('managed', 'exported', 'hybrid')),
    execution_mode text NOT NULL CHECK (execution_mode IN ('managed', 'exported')),
    terminal_status text NOT NULL CHECK (terminal_status IN ('succeeded', 'failed', 'cancelled')),
    runtime_version_ref text NOT NULL,
    runtime_pool_ref text,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    duration_seconds numeric(18, 3) NOT NULL CHECK (duration_seconds >= 0),
    cost_status text NOT NULL CHECK (cost_status IN ('estimated', 'provisional', 'finalized', 'not_applicable')),
    cost_amount numeric(18, 6) NOT NULL DEFAULT 0 CHECK (cost_amount >= 0),
    currency text NOT NULL,
    pricing_schedule_version_ref text,
    policy_reason_code text NOT NULL,
    dispatch_allowed boolean,
    pool_health_state text CHECK (pool_health_state IS NULL OR pool_health_state IN ('healthy', 'degraded', 'stale', 'unavailable')),
    metered_event_count integer NOT NULL DEFAULT 0 CHECK (metered_event_count >= 0),
    duplicate_meter_event_count integer NOT NULL DEFAULT 0 CHECK (duplicate_meter_event_count >= 0),
    diagnostic_event_count integer NOT NULL DEFAULT 0 CHECK (diagnostic_event_count >= 0),
    receipt_json jsonb NOT NULL,
    usage_summary_json jsonb NOT NULL,
    mode_selection_json jsonb NOT NULL,
    customer_observability_json jsonb NOT NULL,
    internal_audit_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (pricing_schedule_version_ref)
        REFERENCES managed_runtime_pricing_schedule_versions(version_ref)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_records_identity
    ON managed_runtime_records (tenant_ref, environment_ref, workflow_ref, run_id);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_records_cost
    ON managed_runtime_records (execution_mode, cost_status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_records_health
    ON managed_runtime_records (pool_health_state, dispatch_allowed, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_records_receipt
    ON managed_runtime_records (receipt_id);

CREATE TABLE IF NOT EXISTS managed_runtime_meter_events (
    runtime_record_id text NOT NULL,
    event_id text NOT NULL,
    idempotency_key text NOT NULL,
    run_id text NOT NULL,
    tenant_ref text NOT NULL,
    environment_ref text NOT NULL,
    workflow_ref text NOT NULL,
    execution_mode text NOT NULL CHECK (execution_mode IN ('managed', 'exported')),
    runtime_version_ref text NOT NULL,
    occurred_at timestamptz NOT NULL,
    event_kind text NOT NULL CHECK (event_kind IN ('run_started', 'resource_usage', 'run_finished', 'diagnostic')),
    billable boolean NOT NULL DEFAULT TRUE,
    receipt_id text,
    source_event_ref text,
    event_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_record_id, event_id),
    UNIQUE (runtime_record_id, idempotency_key),
    FOREIGN KEY (runtime_record_id)
        REFERENCES managed_runtime_records(runtime_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_meter_events_run
    ON managed_runtime_meter_events (run_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_meter_events_kind
    ON managed_runtime_meter_events (event_kind, billable, occurred_at DESC);

CREATE TABLE IF NOT EXISTS managed_runtime_heartbeats (
    runtime_record_id text NOT NULL,
    worker_ref text NOT NULL,
    pool_ref text NOT NULL,
    tenant_ref text NOT NULL,
    environment_ref text NOT NULL,
    runtime_version_ref text NOT NULL,
    observed_at timestamptz NOT NULL,
    capacity_slots integer NOT NULL DEFAULT 0 CHECK (capacity_slots >= 0),
    active_runs integer NOT NULL DEFAULT 0 CHECK (active_runs >= 0),
    accepting_work boolean NOT NULL DEFAULT TRUE,
    last_error_code text,
    heartbeat_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_record_id, worker_ref, observed_at),
    FOREIGN KEY (runtime_record_id)
        REFERENCES managed_runtime_records(runtime_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_heartbeats_pool
    ON managed_runtime_heartbeats (pool_ref, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_heartbeats_tenant
    ON managed_runtime_heartbeats (tenant_ref, environment_ref, observed_at DESC);

CREATE TABLE IF NOT EXISTS managed_runtime_pool_health_snapshots (
    runtime_record_id text NOT NULL,
    pool_ref text NOT NULL,
    tenant_ref text NOT NULL,
    environment_ref text NOT NULL,
    state text NOT NULL CHECK (state IN ('healthy', 'degraded', 'stale', 'unavailable')),
    evaluated_at timestamptz NOT NULL,
    dispatch_allowed boolean NOT NULL,
    reason_codes_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    health_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_record_id, pool_ref, evaluated_at),
    FOREIGN KEY (runtime_record_id)
        REFERENCES managed_runtime_records(runtime_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_pool_health_state
    ON managed_runtime_pool_health_snapshots (state, dispatch_allowed, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_pool_health_tenant
    ON managed_runtime_pool_health_snapshots (tenant_ref, environment_ref, pool_ref, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS managed_runtime_audit_events (
    runtime_record_id text NOT NULL,
    audit_event_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    actor_ref text NOT NULL,
    action text NOT NULL,
    target_kind text NOT NULL,
    target_ref text NOT NULL,
    tenant_ref text NOT NULL,
    environment_ref text NOT NULL,
    reason_code text NOT NULL,
    run_id text,
    before_version_ref text,
    after_version_ref text,
    audit_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_record_id, audit_event_id),
    FOREIGN KEY (runtime_record_id)
        REFERENCES managed_runtime_records(runtime_record_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_audit_events_run
    ON managed_runtime_audit_events (run_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_managed_runtime_audit_events_target
    ON managed_runtime_audit_events (target_kind, target_ref, occurred_at DESC);

CREATE OR REPLACE FUNCTION touch_managed_runtime_records_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_managed_runtime_records_touch ON managed_runtime_records;
CREATE TRIGGER trg_managed_runtime_records_touch
    BEFORE UPDATE ON managed_runtime_records
    FOR EACH ROW EXECUTE FUNCTION touch_managed_runtime_records_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('managed_runtime_pricing_schedule_versions', 'Managed runtime pricing schedule versions', 'table', 'Versioned pricing schedule records used to explain managed runtime cost calculations without mutating historical receipts.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb),
    ('managed_runtime_records', 'Managed runtime records', 'table', 'Run-level managed/exported/hybrid runtime accounting records with receipts, cost status, health, and customer observability JSON.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb),
    ('managed_runtime_meter_events', 'Managed runtime meter events', 'table', 'Idempotent metering events for run starts, resource usage, diagnostics, and run finishes.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb),
    ('managed_runtime_heartbeats', 'Managed runtime heartbeats', 'table', 'Worker heartbeat evidence for managed runtime capacity and freshness.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb),
    ('managed_runtime_pool_health_snapshots', 'Managed runtime pool health snapshots', 'table', 'Derived pool health snapshots used to decide whether dispatch is safe and what customers may see.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb),
    ('managed_runtime_audit_events', 'Managed runtime audit events', 'table', 'Internal audit events tied to runtime routing, receipt creation, corrections, and operational overrides.', '{"migration":"382_managed_runtime_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.managed_runtime"}'::jsonb)
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
    ('table.public.managed_runtime_pricing_schedule_versions', 'table', 'managed_runtime_pricing_schedule_versions', 'public', 'authority.managed_runtime', 'managed_runtime_pricing_schedule_versions', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.managed_runtime_records', 'table', 'managed_runtime_records', 'public', 'authority.managed_runtime', 'managed_runtime_records', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.managed_runtime_meter_events', 'table', 'managed_runtime_meter_events', 'public', 'authority.managed_runtime', 'managed_runtime_meter_events', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.managed_runtime_heartbeats', 'table', 'managed_runtime_heartbeats', 'public', 'authority.managed_runtime', 'managed_runtime_heartbeats', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.managed_runtime_pool_health_snapshots', 'table', 'managed_runtime_pool_health_snapshots', 'public', 'authority.managed_runtime', 'managed_runtime_pool_health_snapshots', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.managed_runtime_audit_events', 'table', 'managed_runtime_audit_events', 'public', 'authority.managed_runtime', 'managed_runtime_audit_events', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
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
    'event_contract.managed_runtime.recorded',
    'managed_runtime.recorded',
    'authority.managed_runtime',
    'data_dictionary.object.managed_runtime_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["runtime_record_id","run_id","tenant_ref","environment_ref","workflow_ref","workload_class","configured_mode","execution_mode","terminal_status","receipt_id","cost_status","cost_amount","currency","pricing_schedule_version_ref","metered_event_count","duplicate_meter_event_count","pool_health_state","dispatch_allowed"]}'::jsonb
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
    p_operation_ref         := 'authority-managed-runtime-record',
    p_operation_name        := 'authority.managed_runtime.record',
    p_handler_ref           := 'runtime.operations.commands.managed_runtime.handle_record_managed_runtime',
    p_input_model_ref       := 'runtime.operations.commands.managed_runtime.RecordManagedRuntimeCommand',
    p_authority_domain_ref  := 'authority.managed_runtime',
    p_authority_ref         := 'authority.managed_runtime',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority/managed-runtime',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'managed_runtime.recorded',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.managed_runtime_record.20260430',
    p_label                 := 'Managed Runtime Record',
    p_summary               := 'Record optional managed/exported/hybrid runtime accounting snapshots, metering, run receipts, pricing refs, heartbeat health, audit context, and customer-safe observability through CQRS.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'authority-managed-runtime-read',
    p_operation_name        := 'authority.managed_runtime.read',
    p_handler_ref           := 'runtime.operations.queries.managed_runtime.handle_read_managed_runtime',
    p_input_model_ref       := 'runtime.operations.queries.managed_runtime.ReadManagedRuntimeQuery',
    p_authority_domain_ref  := 'authority.managed_runtime',
    p_authority_ref         := 'authority.managed_runtime',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/authority/managed-runtime',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.managed_runtime_read.20260430',
    p_label                 := 'Managed Runtime Read',
    p_summary               := 'Read persisted managed-runtime run receipts, metering, cost, heartbeat health, audit events, pricing schedules, and customer observability through CQRS.'
);

COMMIT;
