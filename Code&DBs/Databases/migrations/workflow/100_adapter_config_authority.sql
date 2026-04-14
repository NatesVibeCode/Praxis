-- Migration 100: Adapter config authority
--
-- Moves HTTP transport defaults (timeout, retries, backoff, retryable codes,
-- user-agent, expected status) and adapter failure code mappings from
-- hardcoded Python constants into Postgres.

BEGIN;

-- Typed runtime config authority used by config_registry and adaptive params.
CREATE TABLE IF NOT EXISTS platform_config (
    config_key   TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    value_type   TEXT NOT NULL,
    category     TEXT NOT NULL DEFAULT 'general',
    description  TEXT NOT NULL DEFAULT '',
    min_value    DOUBLE PRECISION,
    max_value    DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Generic key-value config table for adapter runtime parameters.
-- config_value is JSONB so scalars (int, string), arrays, and objects all fit.
CREATE TABLE IF NOT EXISTS adapter_config (
    config_key   TEXT PRIMARY KEY,
    config_value JSONB NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Maps raw transport failure codes to the adapter's canonical failure namespace.
-- transport_kind: 'cli' | 'http'
CREATE TABLE IF NOT EXISTS adapter_failure_mappings (
    id             SERIAL PRIMARY KEY,
    transport_kind TEXT NOT NULL,
    failure_code   TEXT NOT NULL,
    mapped_code    TEXT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (transport_kind, failure_code)
);

-- LLM HTTP transport defaults
INSERT INTO adapter_config (config_key, config_value, description) VALUES
    ('llm_http.timeout_seconds',
     '120',
     'Default HTTP timeout (seconds) for LLM API calls'),
    ('llm_http.retry_attempts',
     '2',
     'Number of retry attempts for transient LLM HTTP failures'),
    ('llm_http.retry_backoff_seconds',
     '[2, 5]',
     'Per-attempt backoff delays (seconds) between LLM HTTP retries'),
    ('llm_http.retryable_status_codes',
     '[408, 429, 500, 502, 503, 504]',
     'HTTP status codes that trigger an automatic retry for LLM calls'),
    ('llm_http.max_tokens_default',
     '4096',
     'Default max_tokens for LLM API requests when not specified by caller'),
    -- API task adapter defaults
    ('api_task.timeout_seconds',
     '30',
     'Default HTTP timeout (seconds) for API task adapter calls'),
    ('api_task.expected_status',
     '200',
     'Default expected HTTP response status for API task adapter'),
    ('api_task.user_agent',
     '"DAG-APITaskAdapter/1.0"',
     'User-Agent header sent by the API task adapter')
ON CONFLICT (config_key) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    description  = EXCLUDED.description,
    updated_at   = now();

-- CLI adapter failure code mappings
INSERT INTO adapter_failure_mappings (transport_kind, failure_code, mapped_code) VALUES
    ('cli', 'cli_adapter.timeout',      'cli_adapter.timeout'),
    ('cli', 'cli_adapter.nonzero_exit', 'cli_adapter.nonzero_exit'),
    ('cli', 'cli_adapter.exec_error',   'cli_adapter.exec_error'),
    -- HTTP adapter failure code mappings
    ('http', 'llm_client.http_error',           'adapter.http_error'),
    ('http', 'llm_client.network_error',        'adapter.network_error'),
    ('http', 'llm_client.timeout',              'adapter.timeout'),
    ('http', 'llm_client.response_parse_error', 'adapter.response_parse_error')
ON CONFLICT (transport_kind, failure_code) DO UPDATE SET
    mapped_code = EXCLUDED.mapped_code,
    updated_at  = now();

-- Core platform config defaults required by runtime config_registry users.
INSERT INTO platform_config (
    config_key,
    config_value,
    value_type,
    category,
    description,
    min_value,
    max_value
) VALUES
    ('context.budget_ratio', '0.6', 'float', 'context', 'Fraction of context window reserved for pipeline context.', 0.30, 0.85),
    ('breaker.failure_threshold', '5', 'int', 'routing', 'Consecutive failures before opening the circuit breaker.', 2, 15),
    ('breaker.recovery_timeout_s', '300', 'float', 'routing', 'Seconds to wait in OPEN state before probing recovery.', 30, 1800),
    ('health.max_consecutive_failures', '3', 'int', 'routing', 'Consecutive route failures before marking unhealthy.', 1, 10),
    ('context.preview_chars', '2000', 'int', 'context', 'Max chars kept in upstream context previews.', 500, 5000)
ON CONFLICT (config_key) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    value_type = EXCLUDED.value_type,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    updated_at = now();

COMMIT;
