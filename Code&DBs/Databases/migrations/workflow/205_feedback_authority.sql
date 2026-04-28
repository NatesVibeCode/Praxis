-- Migration 205: feedback authority.
--
-- Feedback is immutable intake plus events. Target domains can consume it
-- through explicit commands; feedback itself does not become a hidden writer
-- for routing, workflow, service, or capability state.

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
    'authority.feedback',
    'praxis.engine',
    'stream.feedback',
    'projection.feedback.events',
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

CREATE TABLE IF NOT EXISTS authority_feedback_streams (
    feedback_stream_ref TEXT PRIMARY KEY CHECK (btrim(feedback_stream_ref) <> ''),
    feedback_kind TEXT NOT NULL CHECK (
        feedback_kind IN (
            'capability_quality',
            'route_review',
            'manifest_refinement',
            'operator_review',
            'service_health',
            'workflow_closeout',
            'general'
        )
    ),
    target_authority_domain_ref TEXT REFERENCES authority_domains (authority_domain_ref) ON DELETE SET NULL,
    owner_ref TEXT NOT NULL DEFAULT 'praxis.engine' CHECK (btrim(owner_ref) <> ''),
    intake_schema_ref TEXT NOT NULL DEFAULT 'feedback.intake.default' CHECK (btrim(intake_schema_ref) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS authority_feedback_events (
    feedback_event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feedback_stream_ref TEXT NOT NULL REFERENCES authority_feedback_streams (feedback_stream_ref) ON DELETE RESTRICT,
    target_ref TEXT NOT NULL CHECK (btrim(target_ref) <> ''),
    source_ref TEXT NOT NULL DEFAULT 'unknown' CHECK (btrim(source_ref) <> ''),
    signal_kind TEXT NOT NULL CHECK (btrim(signal_kind) <> ''),
    signal_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(signal_payload) = 'object'),
    proposed_action JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(proposed_action) = 'object'),
    recorded_by TEXT NOT NULL DEFAULT 'feedback.authority' CHECK (btrim(recorded_by) <> ''),
    idempotency_key TEXT UNIQUE CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    authority_event_id UUID REFERENCES authority_events (event_id) ON DELETE SET NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS authority_feedback_events_stream_recorded_idx
    ON authority_feedback_events (feedback_stream_ref, recorded_at DESC);

CREATE INDEX IF NOT EXISTS authority_feedback_events_target_idx
    ON authority_feedback_events (target_ref, recorded_at DESC);

CREATE OR REPLACE VIEW authority_feedback_event_projection AS
SELECT
    events.feedback_event_id,
    events.feedback_stream_ref,
    streams.feedback_kind,
    streams.target_authority_domain_ref,
    events.target_ref,
    events.source_ref,
    events.signal_kind,
    events.signal_payload,
    events.proposed_action,
    events.recorded_by,
    events.idempotency_key,
    events.authority_event_id,
    events.recorded_at
FROM authority_feedback_events events
JOIN authority_feedback_streams streams
  ON streams.feedback_stream_ref = events.feedback_stream_ref;

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
    'projection.feedback.events',
    'authority.feedback',
    'stream.feedback',
    'runtime.feedback_authority.list_feedback_events',
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
VALUES ('projection.feedback.events', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    updated_at = now();

INSERT INTO authority_feedback_streams (
    feedback_stream_ref,
    feedback_kind,
    target_authority_domain_ref,
    owner_ref,
    intake_schema_ref,
    enabled,
    decision_ref,
    metadata
) VALUES
    ('feedback.capability_outcome', 'capability_quality', 'authority.capability_catalog', 'praxis.engine', 'feedback.capability_outcome', TRUE, 'decision.cqrs_authority_unification.20260422', '{"legacy_table":"capability_outcomes"}'::jsonb),
    ('feedback.route_review', 'route_review', 'authority.task_route_eligibility', 'praxis.engine', 'feedback.route_review', TRUE, 'decision.cqrs_authority_unification.20260422', '{"legacy_runtime":"runtime.task_type_router"}'::jsonb),
    ('feedback.manifest_refinement', 'manifest_refinement', 'authority.object_schema', 'praxis.engine', 'feedback.manifest_refinement', TRUE, 'decision.cqrs_authority_unification.20260422', '{"legacy_runtime":"runtime.manifest_generator"}'::jsonb),
    ('feedback.operator_review', 'operator_review', NULL, 'praxis.engine', 'feedback.operator_review', TRUE, 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('feedback.workflow_closeout', 'workflow_closeout', 'authority.workflow_runs', 'praxis.engine', 'feedback.workflow_closeout', TRUE, 'decision.cqrs_authority_unification.20260422', '{}'::jsonb)
ON CONFLICT (feedback_stream_ref) DO UPDATE SET
    feedback_kind = EXCLUDED.feedback_kind,
    target_authority_domain_ref = EXCLUDED.target_authority_domain_ref,
    owner_ref = EXCLUDED.owner_ref,
    intake_schema_ref = EXCLUDED.intake_schema_ref,
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
        'feedback-record',
        'feedback.record',
        'operation_command',
        'command',
        'POST',
        '/api/feedback/events',
        'runtime.feedback_authority.RecordAuthorityFeedbackCommand',
        'runtime.feedback_authority.handle_record_feedback',
        'authority.feedback',
        'authority.feedback',
        'projection.feedback.events',
        'praxis.primary_postgres',
        'runtime.feedback_authority.RecordAuthorityFeedbackCommand',
        'feedback.event',
        '["idempotency_key"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'feedback_recorded',
        'projection_freshness.default',
        'operate',
        'idempotent',
        TRUE,
        'binding.operation_catalog_registry.feedback.20260422',
        'decision.cqrs_authority_unification.20260422'
    ),
    (
        'feedback-list',
        'feedback.list',
        'operation_query',
        'query',
        'GET',
        '/api/feedback/events',
        'runtime.feedback_authority.ListAuthorityFeedbackCommand',
        'runtime.feedback_authority.handle_list_feedback_events',
        'authority.feedback',
        'authority.feedback',
        'projection.feedback.events',
        'praxis.primary_postgres',
        'runtime.feedback_authority.ListAuthorityFeedbackCommand',
        'feedback.events',
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
        'binding.operation_catalog_registry.feedback.20260422',
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
    ('authority_feedback_streams', 'Authority feedback streams', 'table', 'Registered feedback intake streams.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb),
    ('authority_feedback_events', 'Authority feedback events', 'table', 'Immutable feedback intake events.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb),
    ('feedback.record', 'Record feedback', 'command', 'Cataloged command for immutable feedback intake.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb),
    ('feedback.list', 'List feedback events', 'query', 'Cataloged query for feedback intake projection.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb),
    ('projection.feedback.events', 'Feedback event projection', 'projection', 'Read model over authority feedback events.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb),
    ('event.feedback_recorded', 'Feedback recorded event', 'event', 'Authority feedback event emitted when feedback is recorded.', '{"migration":"205_feedback_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.feedback"}'::jsonb)
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
    feedback_stream_ref,
    feedback_stream_ref,
    'feedback_stream',
    'Feedback stream for ' || feedback_kind,
    jsonb_build_object('source', 'authority_feedback_streams'),
    jsonb_build_object(
        'authority_domain_ref', 'authority.feedback',
        'target_authority_domain_ref', target_authority_domain_ref,
        'feedback_kind', feedback_kind
    )
FROM authority_feedback_streams
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
    ('table.public.authority_feedback_streams', 'table', 'authority_feedback_streams', 'public', 'authority.feedback', 'authority_feedback_streams', 'active', 'registry', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.authority_feedback_events', 'table', 'authority_feedback_events', 'public', 'authority.feedback', 'authority_feedback_events', 'active', 'feedback', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('operation.feedback.record', 'command', 'feedback.record', NULL, 'authority.feedback', 'feedback.record', 'active', 'command_model', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('operation.feedback.list', 'query', 'feedback.list', NULL, 'authority.feedback', 'feedback.list', 'active', 'read_model', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('projection.projection.feedback.events', 'projection', 'projection.feedback.events', NULL, 'authority.feedback', 'projection.feedback.events', 'active', 'projection', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('event.feedback_recorded', 'event', 'feedback_recorded', NULL, 'authority.feedback', 'event.feedback_recorded', 'active', 'event_stream', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb)
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
    feedback_stream_ref,
    'feedback_stream',
    feedback_stream_ref,
    NULL,
    'authority.feedback',
    feedback_stream_ref,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    'feedback',
    owner_ref,
    decision_ref,
    jsonb_build_object(
        'feedback_kind', feedback_kind,
        'target_authority_domain_ref', target_authority_domain_ref,
        'intake_schema_ref', intake_schema_ref
    )
FROM authority_feedback_streams
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
    'event_contract.feedback_recorded',
    'feedback_recorded',
    'authority.feedback',
    'feedback.event',
    'entity_ref',
    '["runtime.feedback_authority.list_feedback_events"]'::jsonb,
    '["projection.feedback.events"]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.cqrs_authority_unification.20260422',
    '{"source":"205_feedback_authority.sql"}'::jsonb
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs = EXCLUDED.reducer_refs,
    projection_refs = EXCLUDED.projection_refs,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_projection_contracts (
    projection_contract_ref,
    projection_ref,
    authority_domain_ref,
    source_ref_kind,
    source_ref,
    read_model_object_ref,
    freshness_policy_ref,
    last_event_required,
    last_receipt_required,
    failure_visibility_required,
    replay_supported,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'projection_contract.feedback.events',
    'projection.feedback.events',
    'authority.feedback',
    'event_stream',
    'stream.feedback',
    'projection.projection.feedback.events',
    'projection_freshness.default',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'decision.cqrs_authority_unification.20260422',
    '{"source":"205_feedback_authority.sql"}'::jsonb
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_ref_kind = EXCLUDED.source_ref_kind,
    source_ref = EXCLUDED.source_ref,
    read_model_object_ref = EXCLUDED.read_model_object_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    last_event_required = EXCLUDED.last_event_required,
    last_receipt_required = EXCLUDED.last_receipt_required,
    failure_visibility_required = EXCLUDED.failure_visibility_required,
    replay_supported = EXCLUDED.replay_supported,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMENT ON TABLE authority_feedback_streams IS
    'Registered feedback intake streams. Feedback is evidence, not direct mutation authority for target domains.';
COMMENT ON TABLE authority_feedback_events IS
    'Immutable feedback events. Domain authorities may consume these through explicit cataloged commands.';
COMMENT ON VIEW authority_feedback_event_projection IS
    'Read projection for feedback intake streams and events.';

COMMIT;
