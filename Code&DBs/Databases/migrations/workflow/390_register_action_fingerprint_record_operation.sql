-- Migration 390: Register receipt-backed raw action fingerprint recording.
--
-- Problem: migration 383 created the action_fingerprints authority, but only
-- the receipt trigger actually wrote rows. That meant gateway operations were
-- recorded while raw shell/edit/write/read tool calls from Claude/Codex/Gemini
-- had no canonical write surface, so tool_opportunities_pending stayed empty
-- after migration 385 filtered out gateway_op rows.
--
-- Fix: add a dedicated command operation the per-harness hooks can call on
-- every raw tool invocation. The handler receives raw tool payload, derives a
-- shape-only fingerprint server-side, and inserts one action_fingerprints row
-- through the gateway path. No script-only side channel; one registered
-- authority write.

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
    'authority.action_fingerprints',
    'praxis.engine',
    'stream.authority.action_fingerprints',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
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
    'event_contract.action_fingerprint.recorded',
    'action_fingerprint.recorded',
    'authority.action_fingerprints',
    'data_dictionary.object.action_fingerprint_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.390_register_action_fingerprint_record_operation',
        'expected_payload_fields', jsonb_build_array(
            'source_surface',
            'action_kind',
            'shape_hash',
            'normalized_command',
            'path_shape',
            'session_ref',
            'recorded'
        )
    )
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'action_fingerprint_recorded_event',
    'action_fingerprint.recorded event payload',
    'event',
    'Conceptual event emitted when one raw shell/edit/write/read action is fingerprinted and persisted.',
    jsonb_build_object('source', 'migration.390'),
    jsonb_build_object(
        'event_type', 'action_fingerprint.recorded',
        'payload_fields', jsonb_build_array(
            'source_surface',
            'action_kind',
            'shape_hash',
            'normalized_command',
            'path_shape',
            'session_ref',
            'recorded'
        )
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'action-fingerprint-record',
    p_operation_name        := 'action_fingerprint_record',
    p_handler_ref           := 'runtime.operations.commands.action_fingerprint_record.handle_action_fingerprint_record',
    p_input_model_ref       := 'runtime.operations.commands.action_fingerprint_record.ActionFingerprintRecordInput',
    p_authority_domain_ref  := 'authority.action_fingerprints',
    p_authority_ref         := 'authority.action_fingerprints',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/action_fingerprint_record',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'action_fingerprint.recorded',
    p_timeout_ms            := 10000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    p_binding_revision      := 'binding.operation_catalog_registry.action_fingerprint_record.20260501',
    p_label                 := 'Action Fingerprint Record',
    p_summary               := 'Record one raw shell/edit/write/read tool invocation into action_fingerprints using server-side shape normalization.'
);

COMMIT;
