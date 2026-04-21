ALTER TABLE provider_model_candidates
    ADD COLUMN IF NOT EXISTS cli_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS route_tier text,
    ADD COLUMN IF NOT EXISTS route_tier_rank integer,
    ADD COLUMN IF NOT EXISTS latency_class text,
    ADD COLUMN IF NOT EXISTS latency_rank integer,
    ADD COLUMN IF NOT EXISTS reasoning_control jsonb,
    ADD COLUMN IF NOT EXISTS task_affinities jsonb,
    ADD COLUMN IF NOT EXISTS benchmark_profile jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_route_tier_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_route_tier_check
            CHECK (route_tier IN ('high', 'medium', 'low'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_route_tier_rank_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_route_tier_rank_check
            CHECK (route_tier_rank >= 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_latency_class_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_latency_class_check
            CHECK (latency_class IN ('reasoning', 'instant'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_latency_rank_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_latency_rank_check
            CHECK (latency_rank >= 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_reasoning_control_object_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_reasoning_control_object_check
            CHECK (jsonb_typeof(reasoning_control) = 'object');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_task_affinities_object_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_task_affinities_object_check
            CHECK (jsonb_typeof(task_affinities) = 'object');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_model_candidates_benchmark_profile_object_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_benchmark_profile_object_check
            CHECK (jsonb_typeof(benchmark_profile) = 'object');
    END IF;
END $$;

WITH profile_rows AS (
    SELECT * FROM (VALUES
        ('anthropic', 'claude-haiku-4-5-20251001', 'low', 1, 'instant', 1, '{"default":"adaptive","kind":"anthropic_thinking","supported_values":["adaptive"]}'::jsonb, '{"avoid":[],"primary":["chat","quick-analysis","batch"],"secondary":["wiring","light-build"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Vendor docs position Haiku as the speed route, even with strong intelligence for its size."],"evidence_level":"vendor_positioning","positioning":"Fastest Claude route for cheaper, higher-throughput work.","source_refs":["anthropic_models","anthropic_choosing"]}'::jsonb, '["economy","instant","chat","quick-analysis","batch","wiring","light-build"]'::jsonb),
        ('anthropic', 'claude-opus-4-7', 'high', 2, 'reasoning', 4, '{"default":"adaptive","kind":"anthropic_thinking","supported_values":["adaptive","extended"]}'::jsonb, '{"avoid":[],"primary":["architecture","review","research","long-horizon"],"secondary":["build","debate"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Vendor docs and release material position Opus as the most capable Claude model.","Use as a high-tier reasoning route, especially when long-horizon synthesis matters."],"evidence_level":"vendor_plus_secondary","positioning":"Anthropic flagship for the most capable and agentic work.","source_refs":["anthropic_models","anthropic_choosing","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","architecture","review","research","long-horizon","build","debate"]'::jsonb),
        ('anthropic', 'claude-sonnet-4-6', 'medium', 1, 'instant', 3, '{"default":"adaptive","kind":"anthropic_thinking","supported_values":["adaptive","extended"]}'::jsonb, '{"avoid":[],"primary":["review","build","chat","analysis"],"secondary":["research","architecture"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Vendor docs describe Sonnet as the best speed-intelligence balance.","Keep it medium-tier instant at the catalog level even though task-specific routing may elevate it for some work."],"evidence_level":"vendor_plus_secondary","positioning":"Balanced Claude route with the best blend of speed and intelligence.","source_refs":["anthropic_models","anthropic_choosing","aa_home","aa_api"]}'::jsonb, '["mid","instant","review","build","chat","analysis","research","architecture"]'::jsonb),
        ('google', 'gemini-1.5-pro-002', 'high', 5, 'reasoning', 4, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","high"]}'::jsonb, '{"avoid":[],"primary":["research","analysis","multimodal"],"secondary":["build","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Keep these in the high reasoning bucket because they remain pro-family models, but lower rank them because they are older or preview-specific."],"evidence_level":"vendor_positioning","positioning":"Earlier or preview pro reasoning routes that still belong in the high bucket but trail the current lead pro models.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["frontier","reasoning","research","analysis","multimodal","build","review"]'::jsonb),
        ('google', 'gemini-2.0-flash', 'medium', 2, 'instant', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","medium"]}'::jsonb, '{"avoid":[],"primary":["chat","build","analysis","multimodal"],"secondary":["research","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google positions Flash as the price-performance and lower-latency family.","2.5 Flash stays instant at the catalog level even though it can handle reasoning-heavy tasks."],"evidence_level":"vendor_positioning","positioning":"Balanced flash family for lower-latency general work, with 2.5 Flash adding more reasoning depth than older flash lines.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["mid","instant","chat","build","analysis","multimodal","research","review"]'::jsonb),
        ('google', 'gemini-2.0-flash-001', 'medium', 2, 'instant', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","medium"]}'::jsonb, '{"avoid":[],"primary":["chat","build","analysis","multimodal"],"secondary":["research","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google positions Flash as the price-performance and lower-latency family.","2.5 Flash stays instant at the catalog level even though it can handle reasoning-heavy tasks."],"evidence_level":"vendor_positioning","positioning":"Balanced flash family for lower-latency general work, with 2.5 Flash adding more reasoning depth than older flash lines.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["mid","instant","chat","build","analysis","multimodal","research","review"]'::jsonb),
        ('google', 'gemini-2.0-flash-lite-001', 'low', 2, 'instant', 1, '{"default":"low","kind":"google_thinking_budget","supported_values":["low"]}'::jsonb, '{"avoid":[],"primary":["chat","batch","quick-analysis"],"secondary":["wiring"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Keep standard flash-lite routes in the low bucket."],"evidence_level":"vendor_positioning","positioning":"Low-cost flash-lite family optimized for speed and scale over depth.","source_refs":["google_models"]}'::jsonb, '["economy","instant","chat","batch","quick-analysis","wiring"]'::jsonb),
        ('google', 'gemini-2.5-flash', 'medium', 2, 'instant', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","medium"]}'::jsonb, '{"avoid":[],"primary":["chat","build","analysis","multimodal"],"secondary":["research","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google positions Flash as the price-performance and lower-latency family.","2.5 Flash stays instant at the catalog level even though it can handle reasoning-heavy tasks."],"evidence_level":"vendor_positioning","positioning":"Balanced flash family for lower-latency general work, with 2.5 Flash adding more reasoning depth than older flash lines.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["mid","instant","chat","build","analysis","multimodal","research","review"]'::jsonb),
        ('google', 'gemini-2.5-flash-lite', 'low', 2, 'instant', 1, '{"default":"low","kind":"google_thinking_budget","supported_values":["low"]}'::jsonb, '{"avoid":[],"primary":["chat","batch","quick-analysis"],"secondary":["wiring"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Keep standard flash-lite routes in the low bucket."],"evidence_level":"vendor_positioning","positioning":"Low-cost flash-lite family optimized for speed and scale over depth.","source_refs":["google_models"]}'::jsonb, '["economy","instant","chat","batch","quick-analysis","wiring"]'::jsonb),
        ('google', 'gemini-2.5-flash-preview-04-17', 'medium', 2, 'instant', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","medium"]}'::jsonb, '{"avoid":[],"primary":["chat","build","analysis","multimodal"],"secondary":["research","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google positions Flash as the price-performance and lower-latency family.","2.5 Flash stays instant at the catalog level even though it can handle reasoning-heavy tasks."],"evidence_level":"vendor_positioning","positioning":"Balanced flash family for lower-latency general work, with 2.5 Flash adding more reasoning depth than older flash lines.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["mid","instant","chat","build","analysis","multimodal","research","review"]'::jsonb),
        ('google', 'gemini-2.5-flash-tts', 'low', 3, 'instant', 1, '{"default":"standard","kind":"google_tts_generation","supported_values":["standard"]}'::jsonb, '{"avoid":["general-routing"],"primary":["tts"],"secondary":["voice-agent"],"specialized":["audio"]}'::jsonb, '{"benchmark_notes":["Treat as specialized low-tier instant audio infrastructure, not a general model."],"evidence_level":"vendor_positioning","positioning":"Fast and cost-efficient TTS route.","source_refs":["google_models"]}'::jsonb, '["economy","instant","tts","voice-agent","audio"]'::jsonb),
        ('google', 'gemini-2.5-pro', 'high', 3, 'reasoning', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","high"]}'::jsonb, '{"avoid":[],"primary":["research","architecture","build","multimodal"],"secondary":["review","analysis"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Vendor materials position Gemini Pro routes as the top reasoning family.","Artificial Analysis is used only as a cross-vendor comparator for intelligence and coding standing."],"evidence_level":"vendor_plus_secondary","positioning":"Google''s flagship pro reasoning family for highest-quality agentic and multimodal work.","source_refs":["google_models","google_gemini3_blog","google_gemini3_developers","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","research","architecture","build","multimodal","review","analysis"]'::jsonb),
        ('google', 'gemini-2.5-pro-exp-03-25', 'high', 5, 'reasoning', 4, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","high"]}'::jsonb, '{"avoid":[],"primary":["research","analysis","multimodal"],"secondary":["build","review"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Keep these in the high reasoning bucket because they remain pro-family models, but lower rank them because they are older or preview-specific."],"evidence_level":"vendor_positioning","positioning":"Earlier or preview pro reasoning routes that still belong in the high bucket but trail the current lead pro models.","source_refs":["google_models","google_gemini3_blog"]}'::jsonb, '["frontier","reasoning","research","analysis","multimodal","build","review"]'::jsonb),
        ('google', 'gemini-2.5-pro-tts', 'medium', 5, 'instant', 2, '{"default":"high-fidelity","kind":"google_tts_generation","supported_values":["high-fidelity"]}'::jsonb, '{"avoid":["general-routing"],"primary":["tts"],"secondary":["voice-agent"],"specialized":["audio"]}'::jsonb, '{"benchmark_notes":["Keep it medium-tier because Google positions it as the quality TTS option."],"evidence_level":"vendor_positioning","positioning":"Higher-quality TTS route optimized for output fidelity over lowest cost.","source_refs":["google_models"]}'::jsonb, '["mid","instant","tts","voice-agent","audio"]'::jsonb),
        ('google', 'gemini-3-flash-preview', 'high', 4, 'instant', 2, '{"default":"auto","kind":"google_thinking_level","supported_values":["auto","elevated"]}'::jsonb, '{"avoid":[],"primary":["build","agentic-coding","chat","multimodal"],"secondary":["review","research"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google positions Gemini 3 Flash as frontier-class at a fraction of the cost.","This is intentionally high-tier instant rather than medium because its official benchmark claims are materially stronger than ordinary flash models."],"evidence_level":"vendor_plus_secondary","positioning":"High-end low-latency Gemini route with frontier-class coding and webdev performance.","source_refs":["google_gemini3_blog","google_gemini3_developers","aa_home","aa_api"]}'::jsonb, '["frontier","instant","build","agentic-coding","chat","multimodal","review","research"]'::jsonb),
        ('google', 'gemini-3.1-flash-image-preview', 'medium', 5, 'instant', 2, '{"default":"preview","kind":"google_image_generation","supported_values":["preview"]}'::jsonb, '{"avoid":["general-routing"],"primary":["image-generation","image-editing","multimodal"],"secondary":["chat"],"specialized":["image"]}'::jsonb, '{"benchmark_notes":["Specialized image preview route; do not treat as a default general text model."],"evidence_level":"vendor_positioning","positioning":"Preview flash image route for image-oriented multimodal work.","source_refs":["google_models"]}'::jsonb, '["mid","instant","image-generation","image-editing","multimodal","chat","image"]'::jsonb),
        ('google', 'gemini-3.1-flash-lite-preview', 'medium', 4, 'instant', 1, '{"default":"light","kind":"google_thinking_level","supported_values":["auto","light"]}'::jsonb, '{"avoid":[],"primary":["chat","multimodal","fast-build"],"secondary":["analysis"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Google explicitly positions Gemini 3.1 Flash-Lite as frontier-class for its cost envelope, so it sits in medium rather than low."],"evidence_level":"vendor_positioning","positioning":"Frontier-class lite variant that lands above ordinary flash-lite routes.","source_refs":["google_models","google_gemini3_blog","google_gemini3_developers"]}'::jsonb, '["mid","instant","chat","multimodal","fast-build","analysis"]'::jsonb),
        ('google', 'gemini-3.1-pro-preview', 'high', 3, 'reasoning', 2, '{"default":"dynamic","kind":"google_thinking_budget","supported_values":["dynamic","high"]}'::jsonb, '{"avoid":[],"primary":["research","architecture","build","multimodal"],"secondary":["review","analysis"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Vendor materials position Gemini Pro routes as the top reasoning family.","Artificial Analysis is used only as a cross-vendor comparator for intelligence and coding standing."],"evidence_level":"vendor_plus_secondary","positioning":"Google''s flagship pro reasoning family for highest-quality agentic and multimodal work.","source_refs":["google_models","google_gemini3_blog","google_gemini3_developers","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","research","architecture","build","multimodal","review","analysis"]'::jsonb),
        ('google', 'gemini-live-2.5-flash-native-audio', 'medium', 5, 'instant', 1, '{"default":"streaming","kind":"google_live_native_audio","supported_values":["streaming"]}'::jsonb, '{"avoid":["general-routing"],"primary":["live-audio","voice-agent"],"secondary":["multimodal"],"specialized":["audio"]}'::jsonb, '{"benchmark_notes":["This is not a general text-routing default even though it is active in the catalog."],"evidence_level":"vendor_positioning","positioning":"Specialized live native-audio model for low-latency voice interaction.","source_refs":["google_models"]}'::jsonb, '["mid","instant","live-audio","voice-agent","multimodal","audio"]'::jsonb),
        ('openai', 'gpt-5', 'high', 4, 'reasoning', 4, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high"]}'::jsonb, '{"avoid":[],"primary":["analysis","research","general-agentic"],"secondary":["build","review","chat"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Official docs mark these as earlier family members relative to GPT-5.4.","Keep them in the high bucket because they remain full-capability reasoning models."],"evidence_level":"vendor_positioning","positioning":"Earlier full-size GPT-5 family models remain high-tier reasoning models but are no longer the lead frontier pick.","source_refs":["openai_all","openai_gpt54"]}'::jsonb, '["frontier","reasoning","analysis","research","general-agentic","build","review","chat"]'::jsonb),
        ('openai', 'gpt-5-codex', 'high', 2, 'reasoning', 2, '{"default":"high","kind":"openai_reasoning_effort","supported_values":["low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","review","debug","tool-use"],"secondary":["architecture","research"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor docs position Codex family models for professional software tasks.","Artificial Analysis coding-style cross-vendor benchmarks are used only as secondary ordering evidence."],"evidence_level":"vendor_plus_secondary","positioning":"Full-size Codex lineage optimized for coding and agentic software work.","source_refs":["openai_gpt52_codex","openai_gpt53_codex","openai_all","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","build","review","debug","tool-use","architecture","research","coding"]'::jsonb),
        ('openai', 'gpt-5-codex-mini', 'medium', 3, 'instant', 2, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high"]}'::jsonb, '{"avoid":[],"primary":["build","wiring","debug"],"secondary":["review","chat"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Treat as medium-tier instant coding specialists rather than general frontier models."],"evidence_level":"vendor_positioning","positioning":"Compact coding-specialized Codex routes for cheaper and faster implementation work.","source_refs":["openai_all","openai_gpt52_codex"]}'::jsonb, '["mid","instant","build","wiring","debug","review","chat","coding"]'::jsonb),
        ('openai', 'gpt-5.1', 'high', 4, 'reasoning', 4, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high"]}'::jsonb, '{"avoid":[],"primary":["analysis","research","general-agentic"],"secondary":["build","review","chat"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Official docs mark these as earlier family members relative to GPT-5.4.","Keep them in the high bucket because they remain full-capability reasoning models."],"evidence_level":"vendor_positioning","positioning":"Earlier full-size GPT-5 family models remain high-tier reasoning models but are no longer the lead frontier pick.","source_refs":["openai_all","openai_gpt54"]}'::jsonb, '["frontier","reasoning","analysis","research","general-agentic","build","review","chat"]'::jsonb),
        ('openai', 'gpt-5.1-codex', 'high', 2, 'reasoning', 2, '{"default":"high","kind":"openai_reasoning_effort","supported_values":["low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","review","debug","tool-use"],"secondary":["architecture","research"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor docs position Codex family models for professional software tasks.","Artificial Analysis coding-style cross-vendor benchmarks are used only as secondary ordering evidence."],"evidence_level":"vendor_plus_secondary","positioning":"Full-size Codex lineage optimized for coding and agentic software work.","source_refs":["openai_gpt52_codex","openai_gpt53_codex","openai_all","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","build","review","debug","tool-use","architecture","research","coding"]'::jsonb),
        ('openai', 'gpt-5.1-codex-max', 'high', 1, 'reasoning', 5, '{"default":"xhigh","kind":"openai_reasoning_effort","supported_values":["high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","debug","review","agentic-coding"],"secondary":["architecture"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor release positions Codex Max above standard coding routes for difficult software tasks.","Keep it high-tier but slower because it is explicitly the heavier reasoning coding mode."],"evidence_level":"vendor_positioning","positioning":"Most deliberate OpenAI coding route in the current active catalog.","source_refs":["openai_gpt51_codex_max","openai_all"]}'::jsonb, '["frontier","reasoning","build","debug","review","agentic-coding","architecture","coding"]'::jsonb),
        ('openai', 'gpt-5.1-codex-mini', 'medium', 3, 'instant', 2, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high"]}'::jsonb, '{"avoid":[],"primary":["build","wiring","debug"],"secondary":["review","chat"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Treat as medium-tier instant coding specialists rather than general frontier models."],"evidence_level":"vendor_positioning","positioning":"Compact coding-specialized Codex routes for cheaper and faster implementation work.","source_refs":["openai_all","openai_gpt52_codex"]}'::jsonb, '["mid","instant","build","wiring","debug","review","chat","coding"]'::jsonb),
        ('openai', 'gpt-5.2', 'high', 4, 'reasoning', 4, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high"]}'::jsonb, '{"avoid":[],"primary":["analysis","research","general-agentic"],"secondary":["build","review","chat"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Official docs mark these as earlier family members relative to GPT-5.4.","Keep them in the high bucket because they remain full-capability reasoning models."],"evidence_level":"vendor_positioning","positioning":"Earlier full-size GPT-5 family models remain high-tier reasoning models but are no longer the lead frontier pick.","source_refs":["openai_all","openai_gpt54"]}'::jsonb, '["frontier","reasoning","analysis","research","general-agentic","build","review","chat"]'::jsonb),
        ('openai', 'gpt-5.2-codex', 'high', 2, 'reasoning', 2, '{"default":"high","kind":"openai_reasoning_effort","supported_values":["low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","review","debug","tool-use"],"secondary":["architecture","research"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor docs position Codex family models for professional software tasks.","Artificial Analysis coding-style cross-vendor benchmarks are used only as secondary ordering evidence."],"evidence_level":"vendor_plus_secondary","positioning":"Full-size Codex lineage optimized for coding and agentic software work.","source_refs":["openai_gpt52_codex","openai_gpt53_codex","openai_all","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","build","review","debug","tool-use","architecture","research","coding"]'::jsonb),
        ('openai', 'gpt-5.3-codex', 'high', 2, 'reasoning', 2, '{"default":"high","kind":"openai_reasoning_effort","supported_values":["low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","review","debug","tool-use"],"secondary":["architecture","research"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor docs position Codex family models for professional software tasks.","Artificial Analysis coding-style cross-vendor benchmarks are used only as secondary ordering evidence."],"evidence_level":"vendor_plus_secondary","positioning":"Full-size Codex lineage optimized for coding and agentic software work.","source_refs":["openai_gpt52_codex","openai_gpt53_codex","openai_all","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","build","review","debug","tool-use","architecture","research","coding"]'::jsonb),
        ('openai', 'gpt-5.3-codex-spark', 'medium', 2, 'instant', 1, '{"default":"low","kind":"openai_reasoning_effort","supported_values":["none","low","medium"]}'::jsonb, '{"avoid":[],"primary":["wiring","fast-build","quick-fix"],"secondary":["build","debug"],"specialized":["coding"]}'::jsonb, '{"benchmark_notes":["Vendor release positions Spark around speed and responsiveness rather than max depth."],"evidence_level":"vendor_positioning","positioning":"Small, fast inference coding route for real-time or high-throughput software tasks.","source_refs":["openai_gpt53_codex_spark","openai_all"]}'::jsonb, '["mid","instant","wiring","fast-build","quick-fix","build","debug","coding"]'::jsonb),
        ('openai', 'gpt-5.4', 'high', 1, 'reasoning', 3, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["none","low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","review","architecture","research"],"secondary":["chat","analysis","multimodal"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Official docs position GPT-5.4 as the best intelligence-at-scale GPT-5 family model.","Artificial Analysis is used as a secondary cross-check for cross-vendor intelligence and coding standing."],"evidence_level":"vendor_plus_secondary","positioning":"Highest-intelligence OpenAI flagship for complex multi-step work.","source_refs":["openai_all","openai_gpt54","aa_home","aa_api"]}'::jsonb, '["frontier","reasoning","build","review","architecture","research","chat","analysis","multimodal"]'::jsonb),
        ('openai', 'gpt-5.4-mini', 'medium', 1, 'instant', 2, '{"default":"medium","kind":"openai_reasoning_effort","supported_values":["low","medium","high","xhigh"]}'::jsonb, '{"avoid":[],"primary":["build","wiring","subagents","computer-use"],"secondary":["review","chat","analysis"],"specialized":[]}'::jsonb, '{"benchmark_notes":["Official release positions GPT-5.4-mini as the strongest mini in the family.","Treat as a high-competence instant model, not a low-effort equivalent of full GPT-5.4."],"evidence_level":"vendor_plus_secondary","positioning":"OpenAI''s strongest mini model for coding, computer use, and subagents.","source_refs":["openai_gpt54_mini_release","openai_all","aa_home","aa_api"]}'::jsonb, '["mid","instant","build","wiring","subagents","computer-use","review","chat","analysis"]'::jsonb)
    ) AS p(provider_slug, model_slug, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, task_affinities, benchmark_profile, capability_tags)
)
INSERT INTO provider_model_candidates (
    candidate_ref,
    provider_ref,
    provider_name,
    provider_slug,
    model_slug,
    status,
    priority,
    balance_weight,
    capability_tags,
    default_parameters,
    effective_from,
    effective_to,
    decision_ref,
    created_at,
    cli_config,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    task_affinities,
    benchmark_profile
)
SELECT
    'candidate.' || p.provider_slug || '.' || p.model_slug AS candidate_ref,
    'provider.' || p.provider_slug AS provider_ref,
    p.provider_slug AS provider_name,
    p.provider_slug,
    p.model_slug,
    'active' AS status,
    CASE p.route_tier
        WHEN 'high' THEN 500
        WHEN 'medium' THEN 700
        ELSE 900
    END + p.route_tier_rank AS priority,
    CASE p.route_tier
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        ELSE 3
    END AS balance_weight,
    p.capability_tags,
    jsonb_build_object(
        'provider_slug', p.provider_slug,
        'model_slug', p.model_slug,
        'catalog_source', 'migration.046'
    ) AS default_parameters,
    now() AS effective_from,
    NULL::timestamptz AS effective_to,
    'migration.046.provider_model_candidate_profiles' AS decision_ref,
    now() AS created_at,
    CASE p.provider_slug
        WHEN 'anthropic' THEN '{"prompt_mode":"stdin","cmd_template":["claude","-p","--output-format","json","--model","{model}"],"envelope_key":"result","output_format":"json"}'::jsonb
        WHEN 'openai' THEN '{"prompt_mode":"stdin","cmd_template":["codex","exec","-","--json","--model","{model}"],"envelope_key":"text","output_format":"ndjson"}'::jsonb
        WHEN 'google' THEN '{"prompt_mode":"stdin","cmd_template":["gemini","-p",".","-o","json","--model","{model}"],"envelope_key":"response","output_format":"json"}'::jsonb
        ELSE '{}'::jsonb
    END AS cli_config,
    p.route_tier,
    p.route_tier_rank,
    p.latency_class,
    p.latency_rank,
    p.reasoning_control,
    p.task_affinities,
    p.benchmark_profile
FROM profile_rows AS p
ON CONFLICT (candidate_ref) DO UPDATE SET
    provider_ref = EXCLUDED.provider_ref,
    provider_name = EXCLUDED.provider_name,
    provider_slug = EXCLUDED.provider_slug,
    model_slug = EXCLUDED.model_slug,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    balance_weight = EXCLUDED.balance_weight,
    capability_tags = EXCLUDED.capability_tags,
    default_parameters = EXCLUDED.default_parameters,
    effective_to = EXCLUDED.effective_to,
    decision_ref = EXCLUDED.decision_ref,
    cli_config = EXCLUDED.cli_config,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    task_affinities = EXCLUDED.task_affinities,
    benchmark_profile = EXCLUDED.benchmark_profile;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_model_candidates
        WHERE route_tier IS NULL
           OR route_tier_rank IS NULL
           OR latency_class IS NULL
           OR latency_rank IS NULL
           OR reasoning_control IS NULL
           OR task_affinities IS NULL
           OR benchmark_profile IS NULL
    ) THEN
        RAISE EXCEPTION
            'provider_model_candidates profile backfill incomplete; every row must carry route_tier, latency_class, reasoning_control, task_affinities, and benchmark_profile';
    END IF;
END $$;

ALTER TABLE provider_model_candidates
    ALTER COLUMN route_tier SET NOT NULL,
    ALTER COLUMN route_tier_rank SET NOT NULL,
    ALTER COLUMN latency_class SET NOT NULL,
    ALTER COLUMN latency_rank SET NOT NULL,
    ALTER COLUMN reasoning_control SET NOT NULL,
    ALTER COLUMN task_affinities SET NOT NULL,
    ALTER COLUMN benchmark_profile SET NOT NULL;

CREATE INDEX IF NOT EXISTS provider_model_candidates_route_tier_idx
    ON provider_model_candidates (route_tier, route_tier_rank, provider_slug, model_slug);

CREATE INDEX IF NOT EXISTS provider_model_candidates_latency_class_idx
    ON provider_model_candidates (latency_class, latency_rank, provider_slug, model_slug);

COMMENT ON COLUMN provider_model_candidates.route_tier IS 'Catalog-wide route class for the concrete provider/model row. Values: high, medium, low.';
COMMENT ON COLUMN provider_model_candidates.route_tier_rank IS 'Relative rank inside the route_tier bucket. Lower is preferred.';
COMMENT ON COLUMN provider_model_candidates.latency_class IS 'Catalog-wide operating posture. Values: reasoning or instant.';
COMMENT ON COLUMN provider_model_candidates.latency_rank IS 'Relative preference inside the latency_class bucket. Lower is preferred.';
COMMENT ON COLUMN provider_model_candidates.reasoning_control IS 'Provider-specific reasoning/thinking controls for this model. Stored as explicit JSON authority, not inferred from code.';
COMMENT ON COLUMN provider_model_candidates.task_affinities IS 'Task affinity hints for future routing. Includes primary, secondary, specialized, and avoid buckets.';
COMMENT ON COLUMN provider_model_candidates.benchmark_profile IS 'Captured benchmark and positioning notes with source refs for this model row.';
