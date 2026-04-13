BEGIN;

CREATE TABLE IF NOT EXISTS market_benchmark_source_registry (
    source_slug TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    api_url TEXT NOT NULL,
    api_key_env_var TEXT NOT NULL,
    modality TEXT NOT NULL DEFAULT 'llm',
    request_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    common_metric_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
    creator_slug_aliases JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT market_benchmark_source_registry_request_headers_object_check
        CHECK (jsonb_typeof(request_headers) = 'object'),
    CONSTRAINT market_benchmark_source_registry_common_metric_paths_object_check
        CHECK (jsonb_typeof(common_metric_paths) = 'object'),
    CONSTRAINT market_benchmark_source_registry_creator_slug_aliases_object_check
        CHECK (jsonb_typeof(creator_slug_aliases) = 'object')
);

INSERT INTO market_benchmark_source_registry (
    source_slug,
    display_name,
    api_url,
    api_key_env_var,
    modality,
    request_headers,
    common_metric_paths,
    creator_slug_aliases,
    enabled,
    decision_ref
) VALUES (
    'artificial_analysis',
    'Artificial Analysis',
    'https://artificialanalysis.ai/api/v2/data/llms/models',
    'ARTIFICIAL_ANALYSIS_API_KEY',
    'llm',
    '{"Accept":"application/json","x-api-key":"{api_key}"}'::jsonb,
    '{
      "artificial_analysis_intelligence_index":"evaluations.artificial_analysis_intelligence_index",
      "artificial_analysis_coding_index":"evaluations.artificial_analysis_coding_index",
      "artificial_analysis_math_index":"evaluations.artificial_analysis_math_index",
      "price_1m_blended_3_to_1":"pricing.price_1m_blended_3_to_1",
      "price_1m_input_tokens":"pricing.price_1m_input_tokens",
      "price_1m_output_tokens":"pricing.price_1m_output_tokens",
      "median_output_tokens_per_second":"median_output_tokens_per_second",
      "median_time_to_first_token_seconds":"median_time_to_first_token_seconds",
      "median_time_to_first_answer_token":"median_time_to_first_answer_token"
    }'::jsonb,
    '{
      "anthropic":"anthropic",
      "google":"google",
      "google-deepmind":"google",
      "openai":"openai"
    }'::jsonb,
    TRUE,
    'decision.market_benchmark_source_registry.bootstrap.20260408'
)
ON CONFLICT (source_slug) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    api_url = EXCLUDED.api_url,
    api_key_env_var = EXCLUDED.api_key_env_var,
    modality = EXCLUDED.modality,
    request_headers = EXCLUDED.request_headers,
    common_metric_paths = EXCLUDED.common_metric_paths,
    creator_slug_aliases = EXCLUDED.creator_slug_aliases,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
