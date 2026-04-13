BEGIN;

CREATE TABLE IF NOT EXISTS market_benchmark_metric_registry (
    metric_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    metric_group TEXT NOT NULL,
    higher_is_better BOOLEAN NOT NULL,
    value_unit TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO market_benchmark_metric_registry (
    metric_key,
    display_name,
    metric_group,
    higher_is_better,
    value_unit,
    enabled
) VALUES
    ('artificial_analysis_coding_index', 'Artificial Analysis Coding Index', 'quality', true, 'score', true),
    ('artificial_analysis_intelligence_index', 'Artificial Analysis Intelligence Index', 'quality', true, 'score', true),
    ('artificial_analysis_math_index', 'Artificial Analysis Math Index', 'quality', true, 'score', true),
    ('median_output_tokens_per_second', 'Median Output Tokens / Second', 'latency', true, 'tokens_per_second', true),
    ('median_time_to_first_answer_token', 'Median Time To First Answer Token', 'latency', false, 'seconds', true),
    ('median_time_to_first_token_seconds', 'Median Time To First Token', 'latency', false, 'seconds', true),
    ('price_1m_blended_3_to_1', 'Blended Price Per 1M Tokens (3:1)', 'cost', false, 'usd', true),
    ('price_1m_input_tokens', 'Input Price Per 1M Tokens', 'cost', false, 'usd', true),
    ('price_1m_output_tokens', 'Output Price Per 1M Tokens', 'cost', false, 'usd', true)
ON CONFLICT (metric_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    metric_group = EXCLUDED.metric_group,
    higher_is_better = EXCLUDED.higher_is_better,
    value_unit = EXCLUDED.value_unit,
    enabled = EXCLUDED.enabled,
    updated_at = now();

CREATE TABLE IF NOT EXISTS task_type_route_profiles (
    task_type TEXT PRIMARY KEY,
    affinity_labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    affinity_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
    task_rank_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
    benchmark_metric_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_tier_preferences JSONB NOT NULL DEFAULT '[]'::jsonb,
    latency_class_preferences JSONB NOT NULL DEFAULT '[]'::jsonb,
    allow_unclassified_candidates BOOLEAN NOT NULL DEFAULT true,
    rationale TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT task_type_route_profiles_affinity_labels_object_check
        CHECK (jsonb_typeof(affinity_labels) = 'object'),
    CONSTRAINT task_type_route_profiles_affinity_weights_object_check
        CHECK (jsonb_typeof(affinity_weights) = 'object'),
    CONSTRAINT task_type_route_profiles_task_rank_weights_object_check
        CHECK (jsonb_typeof(task_rank_weights) = 'object'),
    CONSTRAINT task_type_route_profiles_benchmark_metric_weights_object_check
        CHECK (jsonb_typeof(benchmark_metric_weights) = 'object'),
    CONSTRAINT task_type_route_profiles_route_tier_preferences_array_check
        CHECK (jsonb_typeof(route_tier_preferences) = 'array'),
    CONSTRAINT task_type_route_profiles_latency_class_preferences_array_check
        CHECK (jsonb_typeof(latency_class_preferences) = 'array')
);

INSERT INTO task_type_route_profiles (
    task_type,
    affinity_labels,
    affinity_weights,
    task_rank_weights,
    benchmark_metric_weights,
    route_tier_preferences,
    latency_class_preferences,
    allow_unclassified_candidates,
    rationale
) VALUES
    (
        'architecture',
        '{"primary":["architecture","research","analysis","long-horizon","reasoning"],"secondary":["review","build","general-agentic","multimodal","chat"],"specialized":[],"fallback":["debug"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.75,"specialized":0.55,"fallback":0.35,"unclassified":0.15,"avoid":0.0}'::jsonb,
        '{"affinity":0.60,"route_tier":0.25,"latency":0.15}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.70,"artificial_analysis_math_index":0.20,"artificial_analysis_coding_index":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Long-horizon design work should prefer reasoning-first frontier models, but still admit strong secondary generalist models when the market and live health data support them.'
    ),
    (
        'build',
        '{"primary":["build","agentic-coding","coding","tool-use","debug"],"secondary":["review","analysis","research","architecture","wiring","fast-build","light-build","quick-fix","subagents","computer-use"],"specialized":[],"fallback":["chat","multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.78,"specialized":0.55,"fallback":0.40,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.60,"route_tier":0.25,"latency":0.15}'::jsonb,
        '{"artificial_analysis_coding_index":0.70,"artificial_analysis_intelligence_index":0.20,"median_output_tokens_per_second":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Build routing should start from coding capability, then use intelligence and live route health to break ties instead of trusting any single benchmark family.'
    ),
    (
        'chat',
        '{"primary":["chat","analysis","quick-analysis","multimodal"],"secondary":["research","review","build","batch"],"specialized":[],"fallback":["wiring"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.72,"specialized":0.40,"fallback":0.45,"unclassified":0.25,"avoid":0.0}'::jsonb,
        '{"affinity":0.45,"route_tier":0.20,"latency":0.35}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.45,"median_output_tokens_per_second":0.25,"median_time_to_first_token_seconds":0.20,"artificial_analysis_coding_index":0.10}'::jsonb,
        '["medium","low","high"]'::jsonb,
        '["instant","reasoning"]'::jsonb,
        true,
        'Chat defaults should stay fast and affordable while still preferring stronger general conversation and analysis models over specialized media lanes.'
    ),
    (
        'research',
        '{"primary":["research","analysis","documentation","long-horizon","reasoning"],"secondary":["architecture","review","chat","build"],"specialized":[],"fallback":["multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.74,"specialized":0.45,"fallback":0.30,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.62,"route_tier":0.23,"latency":0.15}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.65,"artificial_analysis_math_index":0.15,"median_output_tokens_per_second":0.10,"artificial_analysis_coding_index":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Research routing should prefer documentation-aware analysis and reasoning lanes, while still allowing healthy generalist models to cover broad repo and product discovery work.'
    ),
    (
        'debate',
        '{"primary":["debate","research","analysis","architecture","long-horizon","reasoning"],"secondary":["review","chat","general-agentic","build"],"specialized":[],"fallback":["multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.74,"specialized":0.45,"fallback":0.30,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.65,"route_tier":0.25,"latency":0.10}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.75,"artificial_analysis_math_index":0.15,"median_output_tokens_per_second":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Debate and adversarial analysis should heavily favor reasoning-aligned models, with throughput only acting as a tie-breaker.'
    ),
    (
        'planner',
        '{"primary":["architecture","research","analysis","long-horizon","general-agentic","reasoning"],"secondary":["review","chat","build"],"specialized":[],"fallback":["multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.74,"specialized":0.45,"fallback":0.35,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.60,"route_tier":0.25,"latency":0.15}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.60,"artificial_analysis_math_index":0.20,"artificial_analysis_coding_index":0.10,"median_output_tokens_per_second":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Planner routes should reward analysis, research, and long-horizon synthesis over raw speed while keeping healthy medium-tier fallbacks in play.'
    ),
    (
        'refactor',
        '{"primary":["refactor","build","coding","debug","wiring"],"secondary":["review","analysis","architecture","tool-use"],"specialized":[],"fallback":["chat","multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.78,"specialized":0.50,"fallback":0.35,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.60,"route_tier":0.25,"latency":0.15}'::jsonb,
        '{"artificial_analysis_coding_index":0.60,"artificial_analysis_intelligence_index":0.25,"median_output_tokens_per_second":0.15}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["instant","reasoning"]'::jsonb,
        true,
        'Refactor work should prefer strong coding models with healthy fast-edit loops, while keeping broader reasoning models for more invasive restructures.'
    ),
    (
        'review',
        '{"primary":["review","analysis","build","research","coding"],"secondary":["architecture","chat","tool-use","debug"],"specialized":[],"fallback":["multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.76,"specialized":0.45,"fallback":0.35,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.60,"route_tier":0.25,"latency":0.15}'::jsonb,
        '{"artificial_analysis_coding_index":0.50,"artificial_analysis_intelligence_index":0.35,"artificial_analysis_math_index":0.15}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["reasoning","instant"]'::jsonb,
        true,
        'Review routes should balance code understanding and general reasoning, then let live downstream bug feedback keep unsafe models from staying on top.'
    ),
    (
        'test',
        '{"primary":["test","build","debug","tool-use","coding"],"secondary":["review","analysis","wiring","fast-build","quick-fix","research"],"specialized":[],"fallback":["chat","multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.78,"specialized":0.45,"fallback":0.35,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.55,"route_tier":0.20,"latency":0.25}'::jsonb,
        '{"artificial_analysis_coding_index":0.55,"artificial_analysis_intelligence_index":0.20,"median_output_tokens_per_second":0.15,"median_time_to_first_token_seconds":0.10}'::jsonb,
        '["high","medium","low"]'::jsonb,
        '["instant","reasoning"]'::jsonb,
        true,
        'Test work should reward tool-using coding competence and iteration speed while still letting healthy frontier models win when quality gains are material.'
    ),
    (
        'support',
        '{"primary":["support","chat","analysis","quick-analysis"],"secondary":["review","research","build","wiring"],"specialized":[],"fallback":["multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.72,"specialized":0.35,"fallback":0.30,"unclassified":0.25,"avoid":0.0}'::jsonb,
        '{"affinity":0.45,"route_tier":0.15,"latency":0.40}'::jsonb,
        '{"artificial_analysis_intelligence_index":0.35,"median_output_tokens_per_second":0.25,"median_time_to_first_token_seconds":0.20,"artificial_analysis_coding_index":0.10,"price_1m_blended_3_to_1":0.10}'::jsonb,
        '["medium","low","high"]'::jsonb,
        '["instant","reasoning"]'::jsonb,
        true,
        'Support routing should stay fast, calm, and reliable for operator-facing reply and triage work while still preserving enough analysis depth to avoid shallow or unsafe answers.'
    ),
    (
        'wiring',
        '{"primary":["wiring","fast-build","light-build","quick-fix","build"],"secondary":["debug","chat","quick-analysis","batch","computer-use","subagents"],"specialized":[],"fallback":["analysis","multimodal"],"avoid":["tts","voice-agent","audio","image","image-generation","image-editing","live-audio"]}'::jsonb,
        '{"primary":1.0,"secondary":0.76,"specialized":0.40,"fallback":0.30,"unclassified":0.20,"avoid":0.0}'::jsonb,
        '{"affinity":0.45,"route_tier":0.25,"latency":0.30}'::jsonb,
        '{"artificial_analysis_coding_index":0.45,"median_output_tokens_per_second":0.30,"price_1m_blended_3_to_1":0.15,"median_time_to_first_token_seconds":0.10}'::jsonb,
        '["medium","low","high"]'::jsonb,
        '["instant","reasoning"]'::jsonb,
        true,
        'Wiring should prefer cheaper, faster implementation lanes unless live route health or coding quality clearly says a heavier model is worth the extra spend.'
    )
ON CONFLICT (task_type) DO UPDATE SET
    affinity_labels = EXCLUDED.affinity_labels,
    affinity_weights = EXCLUDED.affinity_weights,
    task_rank_weights = EXCLUDED.task_rank_weights,
    benchmark_metric_weights = EXCLUDED.benchmark_metric_weights,
    route_tier_preferences = EXCLUDED.route_tier_preferences,
    latency_class_preferences = EXCLUDED.latency_class_preferences,
    allow_unclassified_candidates = EXCLUDED.allow_unclassified_candidates,
    rationale = EXCLUDED.rationale,
    updated_at = now();

CREATE INDEX IF NOT EXISTS task_type_route_profiles_route_type_idx
    ON task_type_route_profiles (task_type);

ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS route_source TEXT NOT NULL DEFAULT 'explicit';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'task_type_routing_route_source_check'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_route_source_check
            CHECK (route_source IN ('explicit', 'derived'));
    END IF;
END $$;

DELETE FROM task_type_routing AS auto_row
USING task_type_routing AS normalized_row
WHERE auto_row.task_type LIKE 'auto/%'
  AND split_part(auto_row.task_type, '/', 2) = normalized_row.task_type
  AND auto_row.provider_slug = normalized_row.provider_slug
  AND auto_row.model_slug = normalized_row.model_slug;

UPDATE task_type_routing
SET task_type = split_part(task_type, '/', 2)
WHERE task_type LIKE 'auto/%';

ALTER TABLE task_type_routing
    DROP CONSTRAINT IF EXISTS task_type_routing_pkey;

ALTER TABLE task_type_routing
    ADD CONSTRAINT task_type_routing_pkey
    PRIMARY KEY (task_type, provider_slug, model_slug);

COMMIT;
