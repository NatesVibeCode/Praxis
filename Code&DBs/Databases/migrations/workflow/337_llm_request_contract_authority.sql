-- Migration 337: LLM request contract authority.
--
-- Provider/model request shape is durable platform authority, not adapter
-- folklore. This creates the contract table/read model and adds the remaining
-- request-shape policy columns to task_type_routing without making token
-- telemetry a launch-admission gate.

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
    'authority.llm_request_contracts',
    'praxis.engine',
    'stream.authority.llm_request_contracts',
    'projection.effective_llm_request_contracts',
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::provider-routing::reasoning-effort-is-first-class-route-dimension'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS request_contract_ref TEXT,
    ADD COLUMN IF NOT EXISTS cache_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS structured_output_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS streaming_policy JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE task_type_routing
    DROP CONSTRAINT IF EXISTS task_type_routing_cache_policy_object_check,
    ADD CONSTRAINT task_type_routing_cache_policy_object_check
        CHECK (jsonb_typeof(cache_policy) = 'object'),
    DROP CONSTRAINT IF EXISTS task_type_routing_structured_output_policy_object_check,
    ADD CONSTRAINT task_type_routing_structured_output_policy_object_check
        CHECK (jsonb_typeof(structured_output_policy) = 'object'),
    DROP CONSTRAINT IF EXISTS task_type_routing_streaming_policy_object_check,
    ADD CONSTRAINT task_type_routing_streaming_policy_object_check
        CHECK (jsonb_typeof(streaming_policy) = 'object');

COMMENT ON COLUMN task_type_routing.request_contract_ref IS
    'Optional llm_request_contracts row selected for this task/provider/model route. NULL means resolve by provider/model/transport/task/runtime scope.';
COMMENT ON COLUMN task_type_routing.cache_policy IS
    'Prompt-cache request policy for this route. Object; empty means inherit from llm_request_contracts.';
COMMENT ON COLUMN task_type_routing.structured_output_policy IS
    'Structured-output request policy for this route. Object; empty means inherit from llm_request_contracts.';
COMMENT ON COLUMN task_type_routing.streaming_policy IS
    'Streaming/background request policy for this route. Object; empty means inherit from llm_request_contracts.';

CREATE TABLE IF NOT EXISTS llm_request_contracts (
    llm_request_contract_id TEXT PRIMARY KEY CHECK (btrim(llm_request_contract_id) <> ''),
    provider_slug TEXT NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL CHECK (btrim(model_slug) <> ''),
    transport_type TEXT NOT NULL DEFAULT 'API' CHECK (transport_type IN ('API', 'CLI')),
    protocol_family TEXT,
    task_type TEXT NOT NULL DEFAULT '*' CHECK (btrim(task_type) <> ''),
    runtime_profile_ref TEXT NOT NULL DEFAULT '*' CHECK (btrim(runtime_profile_ref) <> ''),
    context_window_tokens INTEGER CHECK (context_window_tokens IS NULL OR context_window_tokens > 0),
    max_output_tokens INTEGER CHECK (max_output_tokens IS NULL OR max_output_tokens > 0),
    supported_parameters JSONB NOT NULL DEFAULT '[]'::jsonb,
    forbidden_parameters JSONB NOT NULL DEFAULT '[]'::jsonb,
    unsupported_parameter_policy TEXT NOT NULL DEFAULT 'omit'
        CHECK (unsupported_parameter_policy IN ('omit', 'fail')),
    sampling_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    reasoning_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    cache_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    structured_output_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    tool_call_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    truncation_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    telemetry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    tokenizer_ref TEXT,
    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence TEXT NOT NULL DEFAULT 'operator_seed'
        CHECK (confidence IN ('provider_api', 'provider_doc', 'operator_seed', 'observed', 'unknown')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT llm_request_contracts_supported_parameters_array_check
        CHECK (jsonb_typeof(supported_parameters) = 'array'),
    CONSTRAINT llm_request_contracts_forbidden_parameters_array_check
        CHECK (jsonb_typeof(forbidden_parameters) = 'array'),
    CONSTRAINT llm_request_contracts_sampling_policy_object_check
        CHECK (jsonb_typeof(sampling_policy) = 'object'),
    CONSTRAINT llm_request_contracts_reasoning_policy_object_check
        CHECK (jsonb_typeof(reasoning_policy) = 'object'),
    CONSTRAINT llm_request_contracts_cache_policy_object_check
        CHECK (jsonb_typeof(cache_policy) = 'object'),
    CONSTRAINT llm_request_contracts_structured_output_policy_object_check
        CHECK (jsonb_typeof(structured_output_policy) = 'object'),
    CONSTRAINT llm_request_contracts_tool_call_policy_object_check
        CHECK (jsonb_typeof(tool_call_policy) = 'object'),
    CONSTRAINT llm_request_contracts_truncation_policy_object_check
        CHECK (jsonb_typeof(truncation_policy) = 'object'),
    CONSTRAINT llm_request_contracts_telemetry_policy_object_check
        CHECK (jsonb_typeof(telemetry_policy) = 'object'),
    CONSTRAINT llm_request_contracts_provenance_object_check
        CHECK (jsonb_typeof(provenance) = 'object'),
    CONSTRAINT llm_request_contracts_effective_window_check
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS llm_request_contracts_scope_effective_idx
    ON llm_request_contracts (
        provider_slug,
        model_slug,
        transport_type,
        task_type,
        runtime_profile_ref,
        effective_from
    );

CREATE INDEX IF NOT EXISTS llm_request_contracts_active_lookup_idx
    ON llm_request_contracts (
        provider_slug,
        model_slug,
        transport_type,
        task_type,
        runtime_profile_ref,
        effective_from DESC
    )
    WHERE active = TRUE;

CREATE OR REPLACE FUNCTION touch_llm_request_contracts_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_llm_request_contracts_touch ON llm_request_contracts;
CREATE TRIGGER trg_llm_request_contracts_touch
    BEFORE UPDATE ON llm_request_contracts
    FOR EACH ROW EXECUTE FUNCTION touch_llm_request_contracts_updated_at();

CREATE OR REPLACE VIEW effective_llm_request_contracts AS
SELECT DISTINCT ON (
    provider_slug,
    model_slug,
    transport_type,
    task_type,
    runtime_profile_ref
)
    llm_request_contract_id,
    provider_slug,
    model_slug,
    transport_type,
    protocol_family,
    task_type,
    runtime_profile_ref,
    context_window_tokens,
    max_output_tokens,
    supported_parameters,
    forbidden_parameters,
    unsupported_parameter_policy,
    sampling_policy,
    reasoning_policy,
    cache_policy,
    structured_output_policy,
    tool_call_policy,
    truncation_policy,
    telemetry_policy,
    tokenizer_ref,
    provenance,
    confidence,
    active,
    effective_from,
    effective_to,
    created_at,
    updated_at
FROM llm_request_contracts
WHERE active = TRUE
  AND effective_from <= now()
  AND (effective_to IS NULL OR effective_to > now())
ORDER BY
    provider_slug,
    model_slug,
    transport_type,
    task_type,
    runtime_profile_ref,
    effective_from DESC,
    updated_at DESC,
    llm_request_contract_id DESC;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'llm_request_contracts',
        'LLM request contracts',
        'table',
        'Provider/model request-shape authority: context and output limits, sampling, reasoning, cache, structured output, truncation, tokenizer, and telemetry policy.',
        '{"migration":"337_llm_request_contract_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.llm_request_contracts"}'::jsonb
    ),
    (
        'table:effective_llm_request_contracts',
        'Effective LLM request contracts',
        'projection',
        'Current active provider/model request contracts by provider, model, transport, task type, and runtime profile.',
        '{"migration":"337_llm_request_contract_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.llm_request_contracts"}'::jsonb
    )
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
    (
        'table.public.llm_request_contracts',
        'table',
        'llm_request_contracts',
        'public',
        'authority.llm_request_contracts',
        'llm_request_contracts',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::provider-routing::reasoning-effort-is-first-class-route-dimension',
        '{"purpose":"provider request contract authority"}'::jsonb
    ),
    (
        'view.public.effective_llm_request_contracts',
        'projection',
        'effective_llm_request_contracts',
        'public',
        'authority.llm_request_contracts',
        'table:effective_llm_request_contracts',
        'active',
        'read_model',
        'praxis.engine',
        'architecture-policy::provider-routing::reasoning-effort-is-first-class-route-dimension',
        '{"purpose":"active request contract read model"}'::jsonb
    )
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    schema_name = EXCLUDED.schema_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
