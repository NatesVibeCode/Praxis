CREATE TABLE IF NOT EXISTS market_model_registry (
    market_model_ref text PRIMARY KEY,
    source_slug text NOT NULL,
    modality text NOT NULL,
    source_model_id text NOT NULL,
    source_model_slug text NOT NULL,
    model_name text NOT NULL,
    creator_id text,
    creator_slug text NOT NULL,
    creator_name text NOT NULL,
    evaluations jsonb NOT NULL,
    pricing jsonb NOT NULL,
    speed_metrics jsonb NOT NULL,
    prompt_options jsonb NOT NULL,
    raw_payload jsonb NOT NULL,
    decision_ref text NOT NULL,
    first_seen_at timestamptz NOT NULL,
    last_synced_at timestamptz NOT NULL,
    CONSTRAINT market_model_registry_source_identity_key
        UNIQUE (source_slug, modality, source_model_id),
    CONSTRAINT market_model_registry_evaluations_object_check
        CHECK (jsonb_typeof(evaluations) = 'object'),
    CONSTRAINT market_model_registry_pricing_object_check
        CHECK (jsonb_typeof(pricing) = 'object'),
    CONSTRAINT market_model_registry_speed_metrics_object_check
        CHECK (jsonb_typeof(speed_metrics) = 'object'),
    CONSTRAINT market_model_registry_prompt_options_object_check
        CHECK (jsonb_typeof(prompt_options) = 'object'),
    CONSTRAINT market_model_registry_raw_payload_object_check
        CHECK (jsonb_typeof(raw_payload) = 'object')
);

CREATE INDEX IF NOT EXISTS market_model_registry_source_slug_idx
    ON market_model_registry (source_slug, modality, creator_slug, source_model_slug);

CREATE INDEX IF NOT EXISTS market_model_registry_creator_slug_idx
    ON market_model_registry (creator_slug, source_model_slug);

CREATE TABLE IF NOT EXISTS provider_model_market_bindings (
    provider_model_market_binding_id text PRIMARY KEY,
    candidate_ref text NOT NULL,
    market_model_ref text NOT NULL,
    binding_kind text NOT NULL,
    binding_confidence numeric(4,3) NOT NULL
        CHECK (binding_confidence >= 0 AND binding_confidence <= 1),
    decision_ref text NOT NULL,
    bound_at timestamptz NOT NULL,
    CONSTRAINT provider_model_market_bindings_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE CASCADE,
    CONSTRAINT provider_model_market_bindings_market_model_fkey
        FOREIGN KEY (market_model_ref)
        REFERENCES market_model_registry (market_model_ref)
        ON DELETE CASCADE,
    CONSTRAINT provider_model_market_bindings_unique_binding
        UNIQUE (candidate_ref, market_model_ref)
);

CREATE INDEX IF NOT EXISTS provider_model_market_bindings_candidate_idx
    ON provider_model_market_bindings (candidate_ref, bound_at DESC);

CREATE INDEX IF NOT EXISTS provider_model_market_bindings_market_model_idx
    ON provider_model_market_bindings (market_model_ref, bound_at DESC);

COMMENT ON TABLE market_model_registry IS 'External market benchmark registry for model comparison data. This is not the executable routing catalog.';
COMMENT ON TABLE provider_model_market_bindings IS 'Bindings from executable provider/model candidates to external market benchmark registry rows.';
COMMENT ON COLUMN market_model_registry.source_slug IS 'External benchmark source, e.g. artificial_analysis.';
COMMENT ON COLUMN market_model_registry.evaluations IS 'Comparable benchmark scores from the external source.';
COMMENT ON COLUMN market_model_registry.pricing IS 'Comparable price metrics from the external source.';
COMMENT ON COLUMN market_model_registry.speed_metrics IS 'Comparable speed and latency metrics from the external source.';
