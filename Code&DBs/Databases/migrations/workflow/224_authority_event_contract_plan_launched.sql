-- Migration 224: Register plan.launched conceptual event contract.
--
-- Context: Phase 1.1.d of the public beta ramp (decision
-- decision.2026-04-24.public-beta-ramp-master-plan) emits plan.launched via
-- emit_system_event → direct INSERT INTO system_events. That is the
-- observability sidecar half of the event story. Per architecture-policy::
-- platform-architecture::conceptual-events-register-through-operation-
-- catalog-registry, conceptual events must also register in
-- authority_event_contracts so the event type is CQRS-visible and agents
-- can reason about it through the same substrate as every other event.
--
-- This migration adds the contract row. The event is now a named
-- authority type with: receipt_required, replayable policy, and a payload
-- schema ref naming the expected fields (run_id, workflow_id, spec_name,
-- total_jobs, source_refs, packet_labels).
--
-- Follow-up (tracked in architecture-policy::platform-architecture::
-- conceptual-events-register-through-operation-catalog-registry):
-- register a launch_plan (or plan-aggregate) operation in
-- operation_catalog_registry declaring event_type='plan.launched' so
-- emission flows through operation_receipt.event_ids instead of the
-- system_events sidecar. Until then, system_events writes remain the
-- emission path but the type is authoritative.

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
    'event_contract.plan.launched',
    'plan.launched',
    'authority.workflow_runs',
    'data_dictionary.object.plan_launched_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    jsonb_build_object(
        'source', 'migration.224_authority_event_contract_plan_launched',
        'emission_path', 'runtime.system_events.emit_system_event',
        'note', 'Phase 1.1.d CQRS registration. Emission through operation_receipt.event_ids queued as follow-up: register launch_plan operation in operation_catalog_registry so submit_workflow_command''s parent plan event flows through the receipt path.',
        'payload_keys', jsonb_build_array(
            'run_id',
            'workflow_id',
            'spec_name',
            'total_jobs',
            'source_refs',
            'packet_labels'
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
