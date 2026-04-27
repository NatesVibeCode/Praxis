-- Migration 285: Register the runtime CQRS-registration wizard.
--
-- Path (b) of the "make it easy to register a new CQRS op" build.
-- After migrations 239 + 240 + 284, register_operation_atomic is the
-- canonical three-table register helper, and is correct for both command
-- and query ops. But it's only callable from inside SQL migrations.
--
-- This migration registers it as a runtime catalog operation
--   catalog.operation.register     (kind=command, idempotent,
--                                   event=catalog.operation.registered)
--   catalog.operation.retire       (kind=command, idempotent,
--                                   event=catalog.operation.retired)
--
-- so an agent (or operator) can register / retire ops from CLI / MCP /
-- HTTP without authoring a migration. Idempotent: same payload returns
-- the cached gateway receipt; the helper's ON CONFLICT DO UPDATE makes
-- re-registration a no-op anyway.
--
-- The Python handler validates that handler_ref + input_model_ref import-
-- resolve in the server process BEFORE calling register_operation_atomic,
-- so the historical "API startup degrades when operation catalog bindings
-- point at missing runtime exports" foot-gun is caught at registration
-- time instead of next restart.
--
-- Authority domain: authority.cqrs (the CQRS framework owns its own
-- meta-registration ops).

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- (1) Event contracts for the two new lifecycle events.
-- ──────────────────────────────────────────────────────────────────────────
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
(
    'event_contract.catalog.operation.registered',
    'catalog.operation.registered',
    'authority.cqrs',
    'data_dictionary.object.catalog_operation_registered_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Emitted by catalog.operation.register on completed receipts. Payload mirrors the registered row identity (operation_ref, operation_name, operation_kind, authority_domain_ref) plus the import-resolved handler_ref / input_model_ref.',
        'expected_payload_fields', jsonb_build_array(
            'operation_ref',
            'operation_name',
            'operation_kind',
            'authority_domain_ref',
            'handler_ref',
            'input_model_ref'
        )
    )
),
(
    'event_contract.catalog.operation.retired',
    'catalog.operation.retired',
    'authority.cqrs',
    'data_dictionary.object.catalog_operation_retired_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'note', 'Emitted by catalog.operation.retire on completed receipts. Soft-retire only — operation_catalog_registry.enabled flips to FALSE and authority_object_registry.lifecycle_status flips to deprecated. Receipts/events still resolve the row by operation_ref.',
        'expected_payload_fields', jsonb_build_array(
            'operation_ref',
            'operation_name',
            'reason_code',
            'operator_message'
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

-- ──────────────────────────────────────────────────────────────────────────
-- (2) Register catalog.operation.register via the now-fixed helper.
-- ──────────────────────────────────────────────────────────────────────────
SELECT register_operation_atomic(
    p_operation_ref         := 'catalog.operation.register',
    p_operation_name        := 'catalog_operation_register',
    p_handler_ref           := 'runtime.operations.commands.catalog_operation_register.handle_register_operation',
    p_input_model_ref       := 'runtime.operations.commands.catalog_operation_register.RegisterOperationCommand',
    p_authority_domain_ref  := 'authority.cqrs',
    p_operation_kind        := 'command',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_type            := 'catalog.operation.registered',
    p_event_required        := TRUE,
    p_label                 := 'Operation: catalog.operation.register',
    p_summary               := 'Runtime CQRS-registration wizard. Lands a new operation_catalog_registry + authority_object_registry + data_dictionary_objects row triple via register_operation_atomic, after import-resolving handler_ref + input_model_ref to refuse fabricated bindings before they degrade API startup. Idempotent on (operation_ref) — same payload returns the cached receipt and the helper''s ON CONFLICT DO UPDATE makes re-registration a no-op.'
);

-- ──────────────────────────────────────────────────────────────────────────
-- (3) Register catalog.operation.retire via the now-fixed helper.
-- ──────────────────────────────────────────────────────────────────────────
SELECT register_operation_atomic(
    p_operation_ref         := 'catalog.operation.retire',
    p_operation_name        := 'catalog_operation_retire',
    p_handler_ref           := 'runtime.operations.commands.catalog_operation_register.handle_retire_operation',
    p_input_model_ref       := 'runtime.operations.commands.catalog_operation_register.RetireOperationCommand',
    p_authority_domain_ref  := 'authority.cqrs',
    p_operation_kind        := 'command',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_type            := 'catalog.operation.retired',
    p_event_required        := TRUE,
    p_label                 := 'Operation: catalog.operation.retire',
    p_summary               := 'Soft-retire a catalog operation. Sets operation_catalog_registry.enabled=FALSE so the gateway stops binding it, and flips the matching authority_object_registry row''s lifecycle_status to deprecated. Physical deletion is intentionally not supported — receipts and events still resolve the row by operation_ref.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, idempotency_policy, event_type, enabled
--     FROM operation_catalog_registry
--    WHERE operation_ref IN ('catalog.operation.register', 'catalog.operation.retire');
--
--   curl -sS -X POST http://localhost:8420/api/operate -H 'Content-Type: application/json' \
--        -d '{"operation":"catalog_operation_register","input":{"operation_ref":"smoke.test.x","operation_name":"smoke_test_x","handler_ref":"runtime.operations.commands.catalog_operation_register.handle_register_operation","input_model_ref":"runtime.operations.commands.catalog_operation_register.RegisterOperationCommand","authority_domain_ref":"authority.cqrs","operation_kind":"command","event_type":"catalog.operation.registered"}}' | jq .result
