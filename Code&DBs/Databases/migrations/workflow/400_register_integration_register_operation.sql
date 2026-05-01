-- Migration 400: Register integration.register as a CQRS-visible command.
--
-- Closes the leak where every integration_registry write happened through
-- migration seeds (051, 084, 165, 180, ...) or the bare upsert_integration
-- helper — no gateway, no receipt, no authority_events row. Per
-- feedback_authority_layer_tighten the canonical path is: input model
-- validated by operation_catalog_gateway, handler runs the upsert, gateway
-- writes one authority_operation_receipts row and (because event_required
-- is TRUE) one authority_events row with event_type='integration.registered'.
--
-- After this migration lands, registering a new integration goes through
-- the gateway:
--
--   praxis workflow tools call praxis_register_operation ...   -- one-shot, only for adding new ops
--   praxis workflow tools call integration_register \
--     --input-json '{...}' --yes                                -- per-integration registration
--
-- authority.integrations was registered in migration 207 (legacy domain
-- assignment); we just attach the new operation to it.
--
-- Forge preview verified the path before this migration was written
-- (architecture-policy::agent-behavior::cqrs-wizard-before-cqrs-edits):
--   praxis_operation_forge → ok_to_register=true, register_operation_payload
--   matches this migration's register_operation_atomic call.

BEGIN;

-- =====================================================================
-- Authority domain — already registered in migration 207, but keep the
-- upsert here so the chain is self-contained and idempotent.
-- =====================================================================
INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.integrations',
    'praxis.engine',
    'stream.authority.integrations',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref           = EXCLUDED.owner_ref,
    event_stream_ref    = EXCLUDED.event_stream_ref,
    storage_target_ref  = EXCLUDED.storage_target_ref,
    enabled             = EXCLUDED.enabled,
    decision_ref        = EXCLUDED.decision_ref,
    updated_at          = now();

-- =====================================================================
-- Conceptual event contract — integration.registered.
-- =====================================================================
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
    'event_contract.integration.registered',
    'integration.registered',
    'authority.integrations',
    'data_dictionary.object.integration_registered_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.400_register_integration_register_operation',
        'note', 'One event per integration_registry row registered through the catalog gateway. Replaces direct migration seeds and the bare upsert_integration helper.',
        'expected_payload_fields', jsonb_build_array(
            'integration_id',
            'name',
            'provider',
            'auth_status',
            'catalog_dispatch',
            'manifest_source',
            'mcp_server_id',
            'actions',
            'action_count',
            'decision_ref'
        )
    )
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    receipt_required   = EXCLUDED.receipt_required,
    replay_policy      = EXCLUDED.replay_policy,
    enabled            = EXCLUDED.enabled,
    decision_ref       = EXCLUDED.decision_ref,
    metadata           = EXCLUDED.metadata,
    updated_at         = now();

-- =====================================================================
-- data_dictionary_objects — the conceptual event payload entry. The
-- operation row itself is added by register_operation_atomic below.
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'integration_registered_event',
    'integration.registered event payload',
    'event',
    'Conceptual event emitted when an integration_registry row is registered through the catalog gateway.',
    jsonb_build_object('source', 'migration.400'),
    jsonb_build_object(
        'event_type', 'integration.registered',
        'payload_fields', jsonb_build_array(
            'integration_id',
            'name',
            'provider',
            'auth_status',
            'catalog_dispatch',
            'manifest_source',
            'mcp_server_id',
            'actions',
            'action_count',
            'decision_ref'
        )
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label      = EXCLUDED.label,
    category   = EXCLUDED.category,
    summary    = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata   = EXCLUDED.metadata,
    updated_at = now();

-- =====================================================================
-- operation_catalog_registry chain — three-row landing via helper.
-- idempotent: re-registering the same integration with the same payload
-- is a gateway no-op so an operator can run it freely.
-- =====================================================================
SELECT register_operation_atomic(
    p_operation_ref         := 'integration-register',
    p_operation_name        := 'integration_register',
    p_handler_ref           := 'runtime.operations.commands.integration_register.handle_integration_register',
    p_input_model_ref       := 'runtime.operations.commands.integration_register.IntegrationRegisterCommand',
    p_authority_domain_ref  := 'authority.integrations',
    p_authority_ref         := 'authority.integrations',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/integration_register',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'integration.registered',
    p_timeout_ms            := 10000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    p_binding_revision      := 'binding.operation_catalog_registry.integration_register.20260501',
    p_label                 := 'Integration Register',
    p_summary               := 'Register one integration_registry row through the catalog gateway with receipt and integration.registered event.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, event_type, idempotency_policy
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'integration-register';
--
--   SELECT event_contract_ref, event_type, authority_domain_ref
--     FROM authority_event_contracts
--    WHERE event_type = 'integration.registered';
