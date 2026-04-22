BEGIN;

CREATE TABLE IF NOT EXISTS authority_storage_targets (
    storage_target_ref TEXT PRIMARY KEY CHECK (btrim(storage_target_ref) <> ''),
    storage_kind TEXT NOT NULL CHECK (storage_kind IN ('postgres', 'postgres_saas')),
    connection_ref TEXT NOT NULL CHECK (btrim(connection_ref) <> ''),
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(capabilities) = 'object'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO authority_storage_targets (
    storage_target_ref,
    storage_kind,
    connection_ref,
    capabilities,
    enabled,
    decision_ref
) VALUES (
    'praxis.primary_postgres',
    'postgres',
    'WORKFLOW_DATABASE_URL',
    jsonb_build_object(
        'jsonb', true,
        'advisory_locks', true,
        'listen_notify', true,
        'serializable', true,
        'pgvector', true,
        'max_statement_timeout_ms', 15000
    ),
    TRUE,
    'decision.cqrs_authority_kernel.20260422'
)
ON CONFLICT (storage_target_ref) DO UPDATE SET
    storage_kind = EXCLUDED.storage_kind,
    connection_ref = EXCLUDED.connection_ref,
    capabilities = EXCLUDED.capabilities,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS authority_domains (
    authority_domain_ref TEXT PRIMARY KEY CHECK (btrim(authority_domain_ref) <> ''),
    owner_ref TEXT NOT NULL DEFAULT 'praxis.engine' CHECK (btrim(owner_ref) <> ''),
    event_stream_ref TEXT NOT NULL CHECK (btrim(event_stream_ref) <> ''),
    current_projection_ref TEXT CHECK (current_projection_ref IS NULL OR btrim(current_projection_ref) <> ''),
    storage_target_ref TEXT NOT NULL REFERENCES authority_storage_targets (storage_target_ref) ON DELETE RESTRICT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
)
SELECT
    authority_ref,
    'praxis.engine',
    'stream.' || authority_ref,
    MIN(NULLIF(btrim(projection_ref), '')),
    'praxis.primary_postgres',
    TRUE,
    'decision.cqrs_authority_kernel.20260422'
FROM operation_catalog_registry
WHERE authority_ref IS NOT NULL
  AND btrim(authority_ref) <> ''
GROUP BY authority_ref
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    current_projection_ref = COALESCE(authority_domains.current_projection_ref, EXCLUDED.current_projection_ref),
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

ALTER TABLE operation_catalog_registry
    ADD COLUMN IF NOT EXISTS authority_domain_ref TEXT,
    ADD COLUMN IF NOT EXISTS storage_target_ref TEXT NOT NULL DEFAULT 'praxis.primary_postgres',
    ADD COLUMN IF NOT EXISTS input_schema_ref TEXT,
    ADD COLUMN IF NOT EXISTS output_schema_ref TEXT NOT NULL DEFAULT 'operation.output.default',
    ADD COLUMN IF NOT EXISTS idempotency_key_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS required_capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS allowed_callers JSONB NOT NULL DEFAULT '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
    ADD COLUMN IF NOT EXISTS timeout_ms INTEGER NOT NULL DEFAULT 15000,
    ADD COLUMN IF NOT EXISTS receipt_required BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS event_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS event_type TEXT,
    ADD COLUMN IF NOT EXISTS projection_freshness_policy_ref TEXT;

UPDATE operation_catalog_registry
   SET authority_domain_ref = COALESCE(NULLIF(btrim(authority_domain_ref), ''), authority_ref),
       input_schema_ref = COALESCE(NULLIF(btrim(input_schema_ref), ''), input_model_ref),
       event_required = CASE WHEN operation_kind = 'command' THEN TRUE ELSE event_required END,
       event_type = COALESCE(NULLIF(btrim(event_type), ''), replace(operation_name, '.', '_')),
       projection_freshness_policy_ref = CASE
           WHEN operation_kind = 'query'
                AND projection_ref IS NOT NULL
                AND btrim(projection_ref) <> ''
               THEN COALESCE(NULLIF(btrim(projection_freshness_policy_ref), ''), 'projection_freshness.default')
           ELSE projection_freshness_policy_ref
       END
 WHERE TRUE;

ALTER TABLE operation_catalog_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_authority_domain_nonblank_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_storage_target_nonblank_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_input_schema_nonblank_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_output_schema_nonblank_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_json_policy_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_timeout_positive_check,
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_event_type_nonblank_check;

ALTER TABLE operation_catalog_registry
    ADD CONSTRAINT operation_catalog_registry_authority_domain_nonblank_check
        CHECK (authority_domain_ref IS NOT NULL AND btrim(authority_domain_ref) <> '') NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_storage_target_nonblank_check
        CHECK (btrim(storage_target_ref) <> '') NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_input_schema_nonblank_check
        CHECK (input_schema_ref IS NOT NULL AND btrim(input_schema_ref) <> '') NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_output_schema_nonblank_check
        CHECK (btrim(output_schema_ref) <> '') NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_json_policy_check
        CHECK (
            jsonb_typeof(idempotency_key_fields) = 'array'
            AND jsonb_typeof(required_capabilities) = 'object'
            AND jsonb_typeof(allowed_callers) = 'array'
        ) NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_timeout_positive_check
        CHECK (timeout_ms > 0) NOT VALID,
    ADD CONSTRAINT operation_catalog_registry_event_type_nonblank_check
        CHECK (event_type IS NULL OR btrim(event_type) <> '') NOT VALID;

CREATE INDEX IF NOT EXISTS operation_catalog_registry_authority_domain_idx
    ON operation_catalog_registry (authority_domain_ref, enabled, operation_name);

CREATE INDEX IF NOT EXISTS operation_catalog_registry_storage_target_idx
    ON operation_catalog_registry (storage_target_ref, enabled, operation_name);

CREATE TABLE IF NOT EXISTS authority_projection_registry (
    projection_ref TEXT PRIMARY KEY CHECK (btrim(projection_ref) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT,
    source_event_stream_ref TEXT NOT NULL CHECK (btrim(source_event_stream_ref) <> ''),
    reducer_ref TEXT NOT NULL CHECK (btrim(reducer_ref) <> ''),
    storage_target_ref TEXT NOT NULL REFERENCES authority_storage_targets (storage_target_ref) ON DELETE RESTRICT,
    freshness_policy_ref TEXT NOT NULL DEFAULT 'projection_freshness.default' CHECK (btrim(freshness_policy_ref) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS authority_projection_state (
    projection_ref TEXT PRIMARY KEY REFERENCES authority_projection_registry (projection_ref) ON DELETE CASCADE,
    last_event_id UUID,
    last_event_sequence BIGINT NOT NULL DEFAULT 0,
    lag_events BIGINT NOT NULL DEFAULT 0 CHECK (lag_events >= 0),
    last_refreshed_at TIMESTAMPTZ,
    freshness_status TEXT NOT NULL DEFAULT 'unknown' CHECK (
        freshness_status IN ('fresh', 'warning', 'critical', 'unknown')
    ),
    error_code TEXT,
    error_detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS authority_operation_receipts (
    receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    operation_ref TEXT NOT NULL,
    operation_name TEXT NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('command', 'query')),
    authority_domain_ref TEXT NOT NULL,
    authority_ref TEXT NOT NULL,
    projection_ref TEXT,
    storage_target_ref TEXT NOT NULL,
    input_hash TEXT NOT NULL CHECK (btrim(input_hash) <> ''),
    output_hash TEXT,
    idempotency_key TEXT,
    caller_ref TEXT NOT NULL DEFAULT 'unknown',
    execution_status TEXT NOT NULL CHECK (execution_status IN ('completed', 'failed', 'replayed')),
    result_status TEXT,
    error_code TEXT,
    error_detail TEXT,
    event_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(event_ids) = 'array'),
    projection_freshness JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(projection_freshness) = 'object'),
    result_payload JSONB,
    duration_ms INTEGER NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    binding_revision TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS authority_operation_receipts_idempotency_success_idx
    ON authority_operation_receipts (operation_ref, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND execution_status IN ('completed', 'replayed');

CREATE INDEX IF NOT EXISTS authority_operation_receipts_operation_created_idx
    ON authority_operation_receipts (operation_name, created_at DESC);

CREATE INDEX IF NOT EXISTS authority_operation_receipts_authority_created_idx
    ON authority_operation_receipts (authority_domain_ref, created_at DESC);

CREATE TABLE IF NOT EXISTS authority_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_sequence BIGSERIAL UNIQUE,
    authority_domain_ref TEXT NOT NULL,
    aggregate_ref TEXT NOT NULL CHECK (btrim(aggregate_ref) <> ''),
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_payload) = 'object'),
    idempotency_key TEXT,
    operation_ref TEXT NOT NULL,
    receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    emitted_by TEXT NOT NULL DEFAULT 'authority_gateway',
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS authority_events_domain_sequence_idx
    ON authority_events (authority_domain_ref, event_sequence);

CREATE INDEX IF NOT EXISTS authority_events_aggregate_sequence_idx
    ON authority_events (authority_domain_ref, aggregate_ref, event_sequence);

CREATE INDEX IF NOT EXISTS authority_events_operation_idx
    ON authority_events (operation_ref, emitted_at DESC);

COMMIT;
