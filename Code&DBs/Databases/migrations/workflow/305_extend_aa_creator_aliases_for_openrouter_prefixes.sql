-- Extend Artificial Analysis creator_slug_aliases to cover aggregator
-- prefixes (OpenRouter `<creator>/<model>`, Together `<creator>/<model>`).
-- The post-onboarding sync planner uses the first slug segment as the AA
-- creator key; these mappings let aggregator candidates resolve cleanly
-- instead of falling through to `source_unavailable`.

UPDATE market_benchmark_source_registry
SET creator_slug_aliases = creator_slug_aliases || jsonb_build_object(
        'deepseek-ai', 'deepseek',
        'meta-llama',  'meta',
        'mistralai',   'mistral',
        'moonshotai',  'kimi',
        'qwen',        'alibaba',
        'x-ai',        'xai',
        'z-ai',        'zai'
    )
WHERE source_slug = 'artificial_analysis';
