BEGIN;

CREATE TABLE IF NOT EXISTS provider_transport_admissions (
    provider_transport_admission_id TEXT PRIMARY KEY,
    provider_slug TEXT NOT NULL,
    adapter_type TEXT NOT NULL CHECK (adapter_type IN ('cli_llm', 'llm_task')),
    transport_kind TEXT NOT NULL CHECK (transport_kind IN ('cli', 'http')),
    execution_topology TEXT NOT NULL,
    admitted_by_policy BOOLEAN NOT NULL DEFAULT false,
    policy_reason TEXT NOT NULL DEFAULT '',
    lane_id TEXT NOT NULL,
    docs_urls JSONB NOT NULL DEFAULT '{}'::jsonb,
    credential_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    probe_contract JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision_ref TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_slug, adapter_type),
    UNIQUE (lane_id),
    CONSTRAINT provider_transport_admissions_docs_urls_object_check
        CHECK (jsonb_typeof(docs_urls) = 'object'),
    CONSTRAINT provider_transport_admissions_credential_sources_array_check
        CHECK (jsonb_typeof(credential_sources) = 'array'),
    CONSTRAINT provider_transport_admissions_probe_contract_object_check
        CHECK (jsonb_typeof(probe_contract) = 'object')
);

CREATE INDEX IF NOT EXISTS provider_transport_admissions_provider_status_idx
    ON provider_transport_admissions (provider_slug, status);

CREATE INDEX IF NOT EXISTS provider_transport_admissions_adapter_status_idx
    ON provider_transport_admissions (adapter_type, status);

CREATE TABLE IF NOT EXISTS provider_transport_probe_receipts (
    provider_transport_probe_receipt_id TEXT PRIMARY KEY,
    provider_slug TEXT NOT NULL,
    adapter_type TEXT NOT NULL CHECK (adapter_type IN ('cli_llm', 'llm_task')),
    decision_ref TEXT NOT NULL,
    probe_step TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT provider_transport_probe_receipts_details_object_check
        CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS provider_transport_probe_receipts_provider_step_idx
    ON provider_transport_probe_receipts (provider_slug, adapter_type, probe_step, recorded_at DESC);

CREATE INDEX IF NOT EXISTS provider_transport_probe_receipts_decision_idx
    ON provider_transport_probe_receipts (decision_ref, recorded_at DESC);

INSERT INTO provider_transport_admissions (
    provider_transport_admission_id,
    provider_slug,
    adapter_type,
    transport_kind,
    execution_topology,
    admitted_by_policy,
    policy_reason,
    lane_id,
    docs_urls,
    credential_sources,
    probe_contract,
    decision_ref,
    status
)
SELECT
    'provider_transport_admission.' || provider_slug || '.cli_llm',
    provider_slug,
    'cli_llm',
    'cli',
    'local_cli',
    true,
    'Admitted via legacy provider_cli_profiles backfill.',
    provider_slug || ':cli_llm',
    '{}'::jsonb,
    COALESCE(api_key_env_vars, '[]'::jsonb),
    jsonb_build_object(
        'backfilled_from', 'provider_cli_profiles',
        'prompt_mode', COALESCE(prompt_mode, 'stdin')
    ),
    'migration.078.provider_transport_admissions.backfill',
    'active'
FROM provider_cli_profiles
WHERE status = 'active'
  AND binary_name IS NOT NULL
  AND trim(binary_name) <> ''
  AND adapter_economics ? 'cli_llm'
ON CONFLICT (provider_slug, adapter_type) DO NOTHING;

INSERT INTO provider_transport_admissions (
    provider_transport_admission_id,
    provider_slug,
    adapter_type,
    transport_kind,
    execution_topology,
    admitted_by_policy,
    policy_reason,
    lane_id,
    docs_urls,
    credential_sources,
    probe_contract,
    decision_ref,
    status
)
SELECT
    'provider_transport_admission.' || provider_slug || '.llm_task',
    provider_slug,
    'llm_task',
    'http',
    'direct_http',
    true,
    'Admitted via legacy provider_cli_profiles backfill.',
    provider_slug || ':llm_task',
    '{}'::jsonb,
    COALESCE(api_key_env_vars, '[]'::jsonb),
    jsonb_build_object(
        'backfilled_from', 'provider_cli_profiles',
        'api_endpoint', api_endpoint,
        'api_protocol_family', api_protocol_family
    ),
    'migration.078.provider_transport_admissions.backfill',
    'active'
FROM provider_cli_profiles
WHERE status = 'active'
  AND api_endpoint IS NOT NULL
  AND trim(api_endpoint) <> ''
  AND api_protocol_family IS NOT NULL
  AND trim(api_protocol_family) <> ''
  AND adapter_economics ? 'llm_task'
ON CONFLICT (provider_slug, adapter_type) DO NOTHING;

COMMIT;
