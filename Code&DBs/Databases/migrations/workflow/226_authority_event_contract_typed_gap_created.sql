-- Migration 226: Register typed_gap.created conceptual event contract.
--
-- Context: Phase 1.6 of the public beta ramp (decision
-- decision.2026-04-24.public-beta-ramp-master-plan). Fail-closed surfaces
-- across Phase 1 (source_refs, stage vocab, write scope, verification
-- gaps, type_contract slug resolution) already produce structured gap
-- objects — the missing piece is a durable event that fires when one is
-- created, so observers (Moon, operator console, future projections) can
-- react without polling the write sites.
--
-- Per architecture-policy::platform-architecture::conceptual-events-
-- register-through-operation-catalog-registry, this migration registers
-- the event type. Follow-up wires up emission from the places that
-- already emit structured gaps (runtime/catalog_type_contract_validation,
-- spec_compiler.UnresolvedSourceRefError / UnresolvedStageError /
-- UnresolvedWriteScopeError, _compute_verification_gaps).
--
-- Payload keys declared in metadata.payload_keys (see below) so consumers
-- can rely on the shape without reading emitter source.

BEGIN;

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
    'event_contract.typed_gap.created',
    'typed_gap.created',
    'authority.workflow_runs',
    'data_dictionary.object.typed_gap_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.226_authority_event_contract_typed_gap_created',
        'emission_path_queued', 'runtime.system_events.emit_system_event',
        'note', 'Phase 1.6 registration. Emitters: catalog_type_contract_validation findings, spec_compiler Unresolved* errors, _compute_verification_gaps. Emission wiring ships with follow-up packets.',
        'payload_keys', jsonb_build_array(
            'gap_id',
            'gap_kind',
            'missing_type',
            'reason_code',
            'legal_repair_actions',
            'source_ref',
            'context'
        )
    )
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

COMMIT;
