-- Migration 388: Synthetic Environment authority.
--
-- CQRS Forge build report:
-- - authority.synthetic_environment: receipt b7426fc3-8237-4d3b-bdb8-fe55ba3a1bd9.
-- - synthetic_environment_create: receipt 24159410-5c36-4ec0-b8b9-1ce173a9a8f5,
--   operation_ref synthetic-environment-create, command, event synthetic_environment.created.
-- - synthetic_environment_clear: receipt b063caee-5f11-49da-9b97-749cd9a8a541,
--   operation_ref synthetic-environment-clear, command, event synthetic_environment.cleared.
-- - synthetic_environment_reset: receipt dd7f5204-f755-4ed9-9f30-80d05c465284,
--   operation_ref synthetic-environment-reset, command, event synthetic_environment.reset.
-- - synthetic_environment_event_inject: receipt 56c661dc-c7d0-4d09-8b5d-55a2ac139b36,
--   operation_ref synthetic-environment-event-inject, command, event synthetic_environment.event_injected.
-- - synthetic_environment_clock_advance: receipt 304ef9ed-2684-4f29-8741-57bddcb9ad5a,
--   operation_ref synthetic-environment-clock-advance, command, event synthetic_environment.clock_advanced.
-- - synthetic_environment_read: receipt 4a496aaf-937d-4d88-a0eb-160da8a09b35,
--   operation_ref synthetic-environment-read, query, read_only.

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
    'authority.synthetic_environment',
    'praxis.engine',
    'stream.authority.synthetic_environment',
    'table.public.synthetic_environments',
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS synthetic_environments (
    environment_ref text PRIMARY KEY CHECK (btrim(environment_ref) <> ''),
    namespace text NOT NULL CHECK (btrim(namespace) <> ''),
    source_dataset_ref text NOT NULL REFERENCES synthetic_data_sets (dataset_ref) ON DELETE RESTRICT,
    seed text NOT NULL CHECK (btrim(seed) <> ''),
    lifecycle_state text NOT NULL CHECK (lifecycle_state IN ('active', 'cleared', 'retired', 'blocked')),
    clock_time timestamptz NOT NULL,
    seed_state_digest text NOT NULL CHECK (btrim(seed_state_digest) <> ''),
    current_state_digest text NOT NULL CHECK (btrim(current_state_digest) <> ''),
    record_count integer NOT NULL CHECK (record_count > 0),
    current_record_count integer NOT NULL CHECK (current_record_count >= 0),
    dirty_record_count integer NOT NULL CHECK (dirty_record_count >= 0),
    seed_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    current_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    permissions_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_by_ref text CHECK (observed_by_ref IS NULL OR btrim(observed_by_ref) <> ''),
    source_ref text CHECK (source_ref IS NULL OR btrim(source_ref) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_synthetic_environments_namespace
    ON synthetic_environments (namespace, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_environments_source_dataset
    ON synthetic_environments (source_dataset_ref, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_environments_lifecycle
    ON synthetic_environments (lifecycle_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_environments_current_digest
    ON synthetic_environments (current_state_digest);

CREATE INDEX IF NOT EXISTS idx_synthetic_environments_metadata_gin
    ON synthetic_environments USING gin (metadata_json);

CREATE TABLE IF NOT EXISTS synthetic_environment_effects (
    effect_ref text PRIMARY KEY CHECK (btrim(effect_ref) <> ''),
    environment_ref text NOT NULL REFERENCES synthetic_environments (environment_ref) ON DELETE CASCADE,
    sequence_number integer NOT NULL CHECK (sequence_number > 0),
    effect_type text NOT NULL CHECK (btrim(effect_type) <> ''),
    action text NOT NULL CHECK (action IN ('create', 'clear', 'reset', 'event_inject', 'clock_advance')),
    event_ref text CHECK (event_ref IS NULL OR btrim(event_ref) <> ''),
    actor_ref text NOT NULL CHECK (btrim(actor_ref) <> ''),
    target_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    before_state_digest text CHECK (before_state_digest IS NULL OR btrim(before_state_digest) <> ''),
    after_state_digest text NOT NULL CHECK (btrim(after_state_digest) <> ''),
    changed_record_count integer NOT NULL CHECK (changed_record_count >= 0),
    changed_fields_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    reversible boolean NOT NULL DEFAULT FALSE,
    receipt_ref text CHECK (receipt_ref IS NULL OR btrim(receipt_ref) <> ''),
    effect_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (environment_ref, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_synthetic_environment_effects_environment_seq
    ON synthetic_environment_effects (environment_ref, sequence_number DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_environment_effects_type
    ON synthetic_environment_effects (effect_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_synthetic_environment_effects_event_ref
    ON synthetic_environment_effects (event_ref)
    WHERE event_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_synthetic_environment_effects_json_gin
    ON synthetic_environment_effects USING gin (effect_json);

CREATE OR REPLACE FUNCTION touch_synthetic_environments_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_synthetic_environments_touch ON synthetic_environments;
CREATE TRIGGER trg_synthetic_environments_touch
    BEFORE UPDATE ON synthetic_environments
    FOR EACH ROW EXECUTE FUNCTION touch_synthetic_environments_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('synthetic_environments', 'Synthetic Environments', 'table', 'Mutable synthetic worlds seeded from Synthetic Data with current state, seed state, lifecycle, deterministic clock, and permissions.', '{"migration":"388_synthetic_environment_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.synthetic_environment"}'::jsonb),
    ('synthetic_environment_effects', 'Synthetic Environment effects', 'table', 'Append-only effect ledger for clears, resets, outside event injection, clock changes, and state digest transitions.', '{"migration":"388_synthetic_environment_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.synthetic_environment"}'::jsonb)
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
    ('table.public.synthetic_environments', 'table', 'synthetic_environments', 'public', 'authority.synthetic_environment', 'synthetic_environments', 'active', 'registry', 'praxis.engine', 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"purpose":"synthetic environment current projection"}'::jsonb),
    ('table.public.synthetic_environment_effects', 'table', 'synthetic_environment_effects', 'public', 'authority.synthetic_environment', 'synthetic_environment_effects', 'active', 'registry', 'praxis.engine', 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"purpose":"synthetic environment effect ledger"}'::jsonb)
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
    ('event_contract.synthetic_environment.created', 'synthetic_environment.created', 'authority.synthetic_environment', 'data_dictionary.object.synthetic_environment_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"aggregate_ref_field":"environment_ref","expected_payload_fields":["environment_ref","namespace","source_dataset_ref","lifecycle_state","clock_time","current_state_digest","record_count","current_record_count","dirty_record_count","effect_ref","effect_type","sequence_number","changed_record_count"]}'::jsonb),
    ('event_contract.synthetic_environment.cleared', 'synthetic_environment.cleared', 'authority.synthetic_environment', 'data_dictionary.object.synthetic_environment_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"aggregate_ref_field":"environment_ref","expected_payload_fields":["environment_ref","namespace","source_dataset_ref","lifecycle_state","clock_time","current_state_digest","record_count","current_record_count","dirty_record_count","effect_ref","effect_type","sequence_number","changed_record_count"]}'::jsonb),
    ('event_contract.synthetic_environment.reset', 'synthetic_environment.reset', 'authority.synthetic_environment', 'data_dictionary.object.synthetic_environment_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"aggregate_ref_field":"environment_ref","expected_payload_fields":["environment_ref","namespace","source_dataset_ref","lifecycle_state","clock_time","current_state_digest","record_count","current_record_count","dirty_record_count","effect_ref","effect_type","sequence_number","changed_record_count"]}'::jsonb),
    ('event_contract.synthetic_environment.event_injected', 'synthetic_environment.event_injected', 'authority.synthetic_environment', 'data_dictionary.object.synthetic_environment_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"aggregate_ref_field":"environment_ref","expected_payload_fields":["environment_ref","namespace","source_dataset_ref","lifecycle_state","clock_time","current_state_digest","record_count","current_record_count","dirty_record_count","effect_ref","effect_type","sequence_number","changed_record_count"]}'::jsonb),
    ('event_contract.synthetic_environment.clock_advanced', 'synthetic_environment.clock_advanced', 'authority.synthetic_environment', 'data_dictionary.object.synthetic_environment_event', 'custom', '[]'::jsonb, '[]'::jsonb, TRUE, 'replayable', TRUE, 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority', '{"aggregate_ref_field":"environment_ref","expected_payload_fields":["environment_ref","namespace","source_dataset_ref","lifecycle_state","clock_time","current_state_digest","record_count","current_record_count","dirty_record_count","effect_ref","effect_type","sequence_number","changed_record_count"]}'::jsonb)
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
    p_operation_ref         := 'synthetic-environment-create',
    p_operation_name        := 'synthetic_environment_create',
    p_handler_ref           := 'runtime.operations.commands.synthetic_environment.handle_synthetic_environment_create',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_environment.CreateSyntheticEnvironmentCommand',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-environments',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_environment.created',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_create.20260501',
    p_label                 := 'Synthetic Environment Create',
    p_summary               := 'Create a mutable Synthetic Environment seeded from one Synthetic Data dataset.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-environment-clear',
    p_operation_name        := 'synthetic_environment_clear',
    p_handler_ref           := 'runtime.operations.commands.synthetic_environment.handle_synthetic_environment_clear',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_environment.ClearSyntheticEnvironmentCommand',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-environments/clear',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_environment.cleared',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_clear.20260501',
    p_label                 := 'Synthetic Environment Clear',
    p_summary               := 'Clear current mutable records while preserving seed, receipts, and effect history.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-environment-reset',
    p_operation_name        := 'synthetic_environment_reset',
    p_handler_ref           := 'runtime.operations.commands.synthetic_environment.handle_synthetic_environment_reset',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_environment.ResetSyntheticEnvironmentCommand',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-environments/reset',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_environment.reset',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_reset.20260501',
    p_label                 := 'Synthetic Environment Reset',
    p_summary               := 'Reset a Synthetic Environment back to seed state with a recorded effect.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-environment-event-inject',
    p_operation_name        := 'synthetic_environment_event_inject',
    p_handler_ref           := 'runtime.operations.commands.synthetic_environment.handle_synthetic_environment_event_inject',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_environment.InjectSyntheticEnvironmentEventCommand',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-environments/events',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_environment.event_injected',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_event_inject.20260501',
    p_label                 := 'Synthetic Environment Event Inject',
    p_summary               := 'Inject a deterministic outside event and persist the resulting environment effect.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-environment-clock-advance',
    p_operation_name        := 'synthetic_environment_clock_advance',
    p_handler_ref           := 'runtime.operations.commands.synthetic_environment.handle_synthetic_environment_clock_advance',
    p_input_model_ref       := 'runtime.operations.commands.synthetic_environment.AdvanceSyntheticEnvironmentClockCommand',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/synthetic-environments/clock',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'synthetic_environment.clock_advanced',
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_clock_advance.20260501',
    p_label                 := 'Synthetic Environment Clock Advance',
    p_summary               := 'Advance or set a Synthetic Environment clock through a recorded effect.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'synthetic-environment-read',
    p_operation_name        := 'synthetic_environment_read',
    p_handler_ref           := 'runtime.operations.queries.synthetic_environment.handle_synthetic_environment_read',
    p_input_model_ref       := 'runtime.operations.queries.synthetic_environment.QuerySyntheticEnvironmentRead',
    p_authority_domain_ref  := 'authority.synthetic_environment',
    p_authority_ref         := 'authority.synthetic_environment',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/synthetic-environments',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'architecture-policy::synthetic-environment::synthetic-environment-mutable-world-authority',
    p_binding_revision      := 'binding.operation_catalog_registry.synthetic_environment_read.20260501',
    p_label                 := 'Synthetic Environment Read',
    p_summary               := 'Read Synthetic Environments, effects, current state, and diffs.'
);

COMMIT;
