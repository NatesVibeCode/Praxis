CREATE TABLE IF NOT EXISTS provider_model_market_match_rules (
    provider_model_market_match_rule_id text PRIMARY KEY,
    source_slug text NOT NULL
        REFERENCES market_benchmark_source_registry (source_slug)
        ON DELETE CASCADE,
    provider_slug text NOT NULL,
    candidate_model_slug text NOT NULL,
    target_creator_slug text NOT NULL,
    target_source_model_slug text,
    match_kind text NOT NULL
        CHECK (
            match_kind = ANY (
                ARRAY[
                    'exact_source_slug',
                    'normalized_slug_alias',
                    'dated_release_alias',
                    'family_proxy',
                    'source_unavailable'
                ]
            )
        ),
    binding_confidence numeric(4,3) NOT NULL
        CHECK (binding_confidence >= 0 AND binding_confidence <= 1),
    selection_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(selection_metadata) = 'object'),
    enabled boolean NOT NULL DEFAULT true,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_model_market_match_rules_candidate_key
        UNIQUE (source_slug, provider_slug, candidate_model_slug)
);

CREATE INDEX IF NOT EXISTS provider_model_market_match_rules_target_idx
    ON provider_model_market_match_rules (
        source_slug,
        target_creator_slug,
        target_source_model_slug
    );

COMMENT ON TABLE provider_model_market_match_rules IS 'Explicit authority mapping from executable provider/model candidates to external market benchmark rows or explicit coverage gaps.';
COMMENT ON COLUMN provider_model_market_match_rules.match_kind IS 'Binding semantics for this candidate. source_unavailable means the external source does not publish a directly comparable row.';
COMMENT ON COLUMN provider_model_market_match_rules.selection_metadata IS 'Authority notes explaining why the selected market row (or gap) is the right long-term match for this candidate.';

WITH seed (
    provider_model_market_match_rule_id,
    source_slug,
    provider_slug,
    candidate_model_slug,
    target_creator_slug,
    target_source_model_slug,
    match_kind,
    binding_confidence,
    selection_metadata,
    decision_ref
) AS (
    VALUES
        (
            'provider_model_market_match_rule.artificial_analysis.anthropic.claude-haiku-4-5-20251001',
            'artificial_analysis',
            'anthropic',
            'claude-haiku-4-5-20251001',
            'anthropic',
            'claude-4-5-haiku',
            'dated_release_alias',
            0.980,
            jsonb_build_object(
                'reason',
                'Anthropic publishes a dated runtime slug while Artificial Analysis tracks the stable Claude 4.5 Haiku benchmark family.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.anthropic.claude-opus-4-7',
            'artificial_analysis',
            'anthropic',
            'claude-opus-4-7',
            'anthropic',
            'claude-opus-4-7',
            'exact_source_slug',
            1.000,
            jsonb_build_object(
                'reason',
                'The executable Anthropic slug matches the Artificial Analysis benchmark slug directly.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.anthropic.claude-sonnet-4-6',
            'artificial_analysis',
            'anthropic',
            'claude-sonnet-4-6',
            'anthropic',
            'claude-sonnet-4-6',
            'exact_source_slug',
            1.000,
            jsonb_build_object(
                'reason',
                'The executable Anthropic slug matches the Artificial Analysis benchmark slug directly.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-1.5-pro-002',
            'artificial_analysis',
            'google',
            'gemini-1.5-pro-002',
            'google',
            'gemini-1-5-pro',
            'dated_release_alias',
            0.970,
            jsonb_build_object(
                'reason',
                'Vertex exposes the 002 release slug while Artificial Analysis benchmarks the stable Gemini 1.5 Pro family row.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.0-flash',
            'artificial_analysis',
            'google',
            'gemini-2.0-flash',
            'google',
            'gemini-2-0-flash',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.0-flash-001',
            'artificial_analysis',
            'google',
            'gemini-2.0-flash-001',
            'google',
            'gemini-2-0-flash',
            'dated_release_alias',
            0.950,
            jsonb_build_object(
                'reason',
                'The 001 runtime release maps to the main Gemini 2.0 Flash benchmark row.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.0-flash-lite-001',
            'artificial_analysis',
            'google',
            'gemini-2.0-flash-lite-001',
            'google',
            'gemini-2-0-flash-lite-001',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-flash',
            'artificial_analysis',
            'google',
            'gemini-2.5-flash',
            'google',
            'gemini-2-5-flash',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-flash-lite',
            'artificial_analysis',
            'google',
            'gemini-2.5-flash-lite',
            'google',
            'gemini-2-5-flash-lite',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-flash-preview-04-17',
            'artificial_analysis',
            'google',
            'gemini-2.5-flash-preview-04-17',
            'google',
            'gemini-2-5-flash-04-2025',
            'dated_release_alias',
            0.930,
            jsonb_build_object(
                'reason',
                'The runtime preview date aligns with the April 2025 Artificial Analysis preview benchmark row.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-flash-tts',
            'artificial_analysis',
            'google',
            'gemini-2.5-flash-tts',
            'google',
            'gemini-2-5-flash',
            'family_proxy',
            0.650,
            jsonb_build_object(
                'reason',
                'Artificial Analysis publishes the text Gemini 2.5 Flash row, not a dedicated TTS variant, so this is a family proxy only.',
                'coverage_scope',
                'text_benchmark',
                'surface_gap',
                'tts'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-pro',
            'artificial_analysis',
            'google',
            'gemini-2.5-pro',
            'google',
            'gemini-2-5-pro',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-pro-exp-03-25',
            'artificial_analysis',
            'google',
            'gemini-2.5-pro-exp-03-25',
            'google',
            'gemini-2-5-pro-03-25',
            'dated_release_alias',
            0.980,
            jsonb_build_object(
                'reason',
                'The experimental runtime slug maps to the March 2025 preview benchmark row.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-2.5-pro-tts',
            'artificial_analysis',
            'google',
            'gemini-2.5-pro-tts',
            'google',
            'gemini-2-5-pro',
            'family_proxy',
            0.650,
            jsonb_build_object(
                'reason',
                'Artificial Analysis publishes the text Gemini 2.5 Pro row, not a dedicated TTS variant, so this is a family proxy only.',
                'coverage_scope',
                'text_benchmark',
                'surface_gap',
                'tts'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-3-flash-preview',
            'artificial_analysis',
            'google',
            'gemini-3-flash-preview',
            'google',
            'gemini-3-flash',
            'dated_release_alias',
            0.980,
            jsonb_build_object(
                'reason',
                'Artificial Analysis tracks the stable Gemini 3 Flash benchmark row while the runtime keeps a preview suffix.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-3.1-flash-image-preview',
            'artificial_analysis',
            'google',
            'gemini-3.1-flash-image-preview',
            'google',
            NULL,
            'source_unavailable',
            0.000,
            jsonb_build_object(
                'reason',
                'Artificial Analysis does not publish a directly comparable text benchmark row for the image-preview surface.',
                'coverage_scope',
                'gap',
                'surface_gap',
                'image_preview'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-3.1-flash-lite-preview',
            'artificial_analysis',
            'google',
            'gemini-3.1-flash-lite-preview',
            'google',
            'gemini-3-1-flash-lite-preview',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-3.1-pro-preview',
            'artificial_analysis',
            'google',
            'gemini-3.1-pro-preview',
            'google',
            'gemini-3-1-pro-preview',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.google.gemini-live-2.5-flash-native-audio',
            'artificial_analysis',
            'google',
            'gemini-live-2.5-flash-native-audio',
            'google',
            NULL,
            'source_unavailable',
            0.000,
            jsonb_build_object(
                'reason',
                'Artificial Analysis does not publish a directly comparable benchmark row for the live native-audio surface.',
                'coverage_scope',
                'gap',
                'surface_gap',
                'live_native_audio'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5',
            'artificial_analysis',
            'openai',
            'gpt-5',
            'openai',
            'gpt-5',
            'exact_source_slug',
            1.000,
            jsonb_build_object(
                'reason',
                'The executable OpenAI slug matches the Artificial Analysis benchmark slug directly.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5-codex',
            'artificial_analysis',
            'openai',
            'gpt-5-codex',
            'openai',
            'gpt-5-codex',
            'exact_source_slug',
            1.000,
            jsonb_build_object(
                'reason',
                'The executable OpenAI slug matches the Artificial Analysis benchmark slug directly.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5-codex-mini',
            'artificial_analysis',
            'openai',
            'gpt-5-codex-mini',
            'openai',
            'gpt-5-codex',
            'family_proxy',
            0.580,
            jsonb_build_object(
                'reason',
                'Artificial Analysis does not publish a dedicated GPT-5 Codex mini row yet, so this uses the base GPT-5 Codex family as a coarse proxy.',
                'coverage_scope',
                'text_benchmark',
                'surface_gap',
                'mini_variant'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.1',
            'artificial_analysis',
            'openai',
            'gpt-5.1',
            'openai',
            'gpt-5-1',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.1-codex',
            'artificial_analysis',
            'openai',
            'gpt-5.1-codex',
            'openai',
            'gpt-5-1-codex',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.1-codex-max',
            'artificial_analysis',
            'openai',
            'gpt-5.1-codex-max',
            'openai',
            'gpt-5-1-codex',
            'family_proxy',
            0.740,
            jsonb_build_object(
                'reason',
                'Artificial Analysis does not separate the codex-max service tier, so this uses the GPT-5.1 Codex family row as a proxy.',
                'coverage_scope',
                'text_benchmark',
                'surface_gap',
                'max_service_tier'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.1-codex-mini',
            'artificial_analysis',
            'openai',
            'gpt-5.1-codex-mini',
            'openai',
            'gpt-5-1-codex-mini',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.2',
            'artificial_analysis',
            'openai',
            'gpt-5.2',
            'openai',
            'gpt-5-2',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.2-codex',
            'artificial_analysis',
            'openai',
            'gpt-5.2-codex',
            'openai',
            'gpt-5-2-codex',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.3-codex',
            'artificial_analysis',
            'openai',
            'gpt-5.3-codex',
            'openai',
            'gpt-5-3-codex',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.3-codex-spark',
            'artificial_analysis',
            'openai',
            'gpt-5.3-codex-spark',
            'openai',
            'gpt-5-3-codex',
            'family_proxy',
            0.700,
            jsonb_build_object(
                'reason',
                'Artificial Analysis does not publish a dedicated Spark row, so this uses the GPT-5.3 Codex family row as a proxy.',
                'coverage_scope',
                'text_benchmark',
                'surface_gap',
                'spark_service_tier'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.4',
            'artificial_analysis',
            'openai',
            'gpt-5.4',
            'openai',
            'gpt-5-4',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        ),
        (
            'provider_model_market_match_rule.artificial_analysis.openai.gpt-5.4-mini',
            'artificial_analysis',
            'openai',
            'gpt-5.4-mini',
            'openai',
            'gpt-5-4-mini',
            'normalized_slug_alias',
            0.990,
            jsonb_build_object(
                'reason',
                'Only punctuation differs between the executable slug and the benchmark slug.',
                'coverage_scope',
                'text_benchmark'
            ),
            'decision.market-model-match-rules.2026-04-08'
        )
)
INSERT INTO provider_model_market_match_rules (
    provider_model_market_match_rule_id,
    source_slug,
    provider_slug,
    candidate_model_slug,
    target_creator_slug,
    target_source_model_slug,
    match_kind,
    binding_confidence,
    selection_metadata,
    decision_ref
)
SELECT
    provider_model_market_match_rule_id,
    source_slug,
    provider_slug,
    candidate_model_slug,
    target_creator_slug,
    target_source_model_slug,
    match_kind,
    binding_confidence,
    selection_metadata,
    decision_ref
FROM seed
ON CONFLICT (source_slug, provider_slug, candidate_model_slug) DO UPDATE SET
    provider_model_market_match_rule_id = EXCLUDED.provider_model_market_match_rule_id,
    target_creator_slug = EXCLUDED.target_creator_slug,
    target_source_model_slug = EXCLUDED.target_source_model_slug,
    match_kind = EXCLUDED.match_kind,
    binding_confidence = EXCLUDED.binding_confidence,
    selection_metadata = EXCLUDED.selection_metadata,
    enabled = true,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();
