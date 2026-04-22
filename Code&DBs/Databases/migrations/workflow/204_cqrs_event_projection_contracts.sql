-- Migration 204: CQRS event and projection contracts.
--
-- Events and projections become explicit replayable contracts instead of
-- incidental rows. Projection state also links back to receipts so read-model
-- freshness can be audited from command outcome to reducer state.

BEGIN;

CREATE TABLE IF NOT EXISTS authority_event_contracts (
    event_contract_ref TEXT PRIMARY KEY CHECK (btrim(event_contract_ref) <> ''),
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    payload_schema_ref TEXT NOT NULL CHECK (btrim(payload_schema_ref) <> ''),
    aggregate_ref_policy TEXT NOT NULL CHECK (
        aggregate_ref_policy IN ('operation_ref', 'domain_ref', 'entity_ref', 'custom')
    ),
    reducer_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reducer_refs) = 'array'),
    projection_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(projection_refs) = 'array'),
    receipt_required BOOLEAN NOT NULL DEFAULT TRUE,
    replay_policy TEXT NOT NULL DEFAULT 'replayable' CHECK (
        replay_policy IN ('replayable', 'snapshot_only', 'not_replayable')
    ),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT authority_event_contracts_unique_domain_event
        UNIQUE (authority_domain_ref, event_type)
);

CREATE TABLE IF NOT EXISTS authority_projection_contracts (
    projection_contract_ref TEXT PRIMARY KEY CHECK (btrim(projection_contract_ref) <> ''),
    projection_ref TEXT NOT NULL REFERENCES authority_projection_registry (projection_ref) ON DELETE CASCADE,
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    source_ref_kind TEXT NOT NULL CHECK (
        source_ref_kind IN ('event_stream', 'table', 'authority_view', 'service_bus_channel')
    ),
    source_ref TEXT NOT NULL CHECK (btrim(source_ref) <> ''),
    read_model_object_ref TEXT NOT NULL CHECK (btrim(read_model_object_ref) <> ''),
    freshness_policy_ref TEXT NOT NULL DEFAULT 'projection_freshness.default' CHECK (btrim(freshness_policy_ref) <> ''),
    last_event_required BOOLEAN NOT NULL DEFAULT TRUE,
    last_receipt_required BOOLEAN NOT NULL DEFAULT TRUE,
    failure_visibility_required BOOLEAN NOT NULL DEFAULT TRUE,
    replay_supported BOOLEAN NOT NULL DEFAULT TRUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT authority_projection_contracts_unique_projection
        UNIQUE (projection_ref)
);

ALTER TABLE authority_projection_state
    ADD COLUMN IF NOT EXISTS last_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMPTZ;

INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
SELECT projection_ref, 'unknown', NULL
FROM authority_projection_registry
ON CONFLICT (projection_ref) DO NOTHING;

CREATE INDEX IF NOT EXISTS authority_event_contracts_domain_event_idx
    ON authority_event_contracts (authority_domain_ref, event_type)
    WHERE enabled;

CREATE INDEX IF NOT EXISTS authority_projection_contracts_domain_idx
    ON authority_projection_contracts (authority_domain_ref, projection_ref)
    WHERE enabled;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('authority_event_contracts', 'Authority event contracts', 'table', 'Event contracts owned by CQRS authority domains.', '{"migration":"204_cqrs_event_projection_contracts.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb),
    ('authority_projection_contracts', 'Authority projection contracts', 'table', 'Projection contracts connecting read models to source authority state.', '{"migration":"204_cqrs_event_projection_contracts.sql"}'::jsonb, '{"authority_domain_ref":"authority.cqrs"}'::jsonb)
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
    ('table.public.authority_event_contracts', 'table', 'authority_event_contracts', 'public', 'authority.cqrs', 'authority_event_contracts', 'active', 'definition', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb),
    ('table.public.authority_projection_contracts', 'table', 'authority_projection_contracts', 'public', 'authority.cqrs', 'authority_projection_contracts', 'active', 'definition', 'praxis.engine', 'decision.cqrs_authority_unification.20260422', '{}'::jsonb)
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
)
SELECT
    'event_contract.' || operation.event_type,
    operation.event_type,
    operation.authority_domain_ref,
    operation.output_schema_ref,
    'operation_ref',
    CASE
        WHEN operation.projection_ref IS NULL THEN '[]'::jsonb
        ELSE jsonb_build_array(COALESCE(projection.reducer_ref, operation.projection_ref))
    END,
    CASE
        WHEN operation.projection_ref IS NULL THEN '[]'::jsonb
        ELSE jsonb_build_array(operation.projection_ref)
    END,
    TRUE,
    'replayable',
    TRUE,
    'decision.cqrs_authority_unification.20260422',
    jsonb_build_object(
        'source', 'operation_catalog_registry',
        'operation_ref', operation.operation_ref,
        'operation_name', operation.operation_name
    )
FROM operation_catalog_registry operation
LEFT JOIN authority_projection_registry projection
  ON projection.projection_ref = operation.projection_ref
WHERE operation.event_type IS NOT NULL
  AND btrim(operation.event_type) <> ''
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
)
SELECT
    'projection_contract.' || projection.projection_ref,
    projection.projection_ref,
    projection.authority_domain_ref,
    'event_stream',
    projection.source_event_stream_ref,
    registry.object_ref,
    projection.freshness_policy_ref,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    projection.enabled,
    'decision.cqrs_authority_unification.20260422',
    jsonb_build_object(
        'reducer_ref', projection.reducer_ref,
        'storage_target_ref', projection.storage_target_ref
    )
FROM authority_projection_registry projection
LEFT JOIN authority_object_registry registry
  ON registry.object_kind = 'projection'
 AND registry.object_ref = 'projection.' || projection.projection_ref
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

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
SELECT
    'event.' || event_type,
    event_type,
    'event',
    'Event contract owned by ' || authority_domain_ref,
    jsonb_build_object('source', 'authority_event_contracts', 'event_contract_ref', event_contract_ref),
    jsonb_build_object('authority_domain_ref', authority_domain_ref, 'payload_schema_ref', payload_schema_ref)
FROM authority_event_contracts
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
)
SELECT
    'event.' || event_type,
    'event',
    event_type,
    NULL,
    authority_domain_ref,
    'event.' || event_type,
    CASE WHEN enabled THEN 'active' ELSE 'deprecated' END,
    'event_stream',
    'praxis.engine',
    decision_ref,
    jsonb_build_object(
        'event_contract_ref', event_contract_ref,
        'payload_schema_ref', payload_schema_ref,
        'replay_policy', replay_policy,
        'projection_refs', projection_refs
    )
FROM authority_event_contracts
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

CREATE OR REPLACE VIEW authority_event_projection_contract_report AS
SELECT
    contracts.event_contract_ref AS contract_ref,
    'event'::text AS contract_kind,
    contracts.authority_domain_ref,
    contracts.event_type AS object_name,
    CASE
        WHEN jsonb_typeof(contracts.reducer_refs) <> 'array' THEN 'invalid_reducer_refs'
        WHEN jsonb_typeof(contracts.projection_refs) <> 'array' THEN 'invalid_projection_refs'
        WHEN contracts.receipt_required IS DISTINCT FROM TRUE THEN 'receipt_not_required'
        ELSE 'ok'
    END AS validation_status,
    jsonb_build_object(
        'payload_schema_ref', contracts.payload_schema_ref,
        'projection_refs', contracts.projection_refs,
        'replay_policy', contracts.replay_policy
    ) AS details
FROM authority_event_contracts contracts
WHERE contracts.enabled = TRUE
UNION ALL
SELECT
    contracts.projection_contract_ref AS contract_ref,
    'projection'::text AS contract_kind,
    contracts.authority_domain_ref,
    contracts.projection_ref AS object_name,
    CASE
        WHEN state.projection_ref IS NULL THEN 'missing_projection_state'
        WHEN contracts.failure_visibility_required
             AND NOT EXISTS (
                 SELECT 1
                 FROM information_schema.columns columns
                 WHERE columns.table_schema = 'public'
                   AND columns.table_name = 'authority_projection_state'
                   AND columns.column_name IN ('error_code', 'error_detail', 'last_failure_at')
             ) THEN 'missing_failure_visibility'
        ELSE 'ok'
    END AS validation_status,
    jsonb_build_object(
        'source_ref_kind', contracts.source_ref_kind,
        'source_ref', contracts.source_ref,
        'read_model_object_ref', contracts.read_model_object_ref,
        'freshness_status', state.freshness_status,
        'last_event_id', state.last_event_id,
        'last_receipt_id', state.last_receipt_id
    ) AS details
FROM authority_projection_contracts contracts
LEFT JOIN authority_projection_state state
  ON state.projection_ref = contracts.projection_ref
WHERE contracts.enabled = TRUE;

COMMENT ON TABLE authority_event_contracts IS
    'Per-domain event contracts. Events are replayable authority facts, not incidental log rows.';
COMMENT ON TABLE authority_projection_contracts IS
    'Projection contracts linking read models to source authority streams and freshness rules.';
COMMENT ON VIEW authority_event_projection_contract_report IS
    'Machine-readable validation report for event and projection CQRS contracts.';

COMMIT;
