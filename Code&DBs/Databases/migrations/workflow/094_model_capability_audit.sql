-- Migration 094: Model capability audit
-- Replaces vendor-marketing task_affinities with user-approved capability matrix.
-- Disables dead models instead of deleting them.

-- ============================================================
-- 0. Add typed capability columns (used by router)
-- ============================================================

ALTER TABLE provider_model_candidates
    ADD COLUMN IF NOT EXISTS cap_language_high BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_analysis_architecture_research BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_build_high BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_review BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_tool_use BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_build_med BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_language_low BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_build_low BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_research_fan BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cap_image BOOLEAN NOT NULL DEFAULT false;

-- ============================================================
-- 1. Disable dead/unreachable models (keep rows, mark inactive)
-- ============================================================

UPDATE provider_model_candidates SET status = 'inactive'
WHERE model_slug IN (
    'gemini-1.5-pro-002',
    'gemini-2.0-flash',
    'gemini-2.0-flash-001',
    'gemini-2.0-flash-lite-001',
    'gemini-live-2.5-flash-native-audio'
) AND status = 'active';

-- ============================================================
-- 2. Rewrite task_affinities with audited capability matrix
--    Categories: language_high, analysis_architecture_research,
--    build_high, review, tool_use, build_med, language_low,
--    build_low, research_fan, image
-- ============================================================

-- Anthropic
UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_low", "build_low", "research_fan"],
    "primary": ["language_low", "build_low"],
    "secondary": ["research_fan"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "image"]
}'::jsonb
WHERE model_slug = 'claude-haiku-4-5-20251001' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_high", "build_high", "tool_use", "build_med", "language_low", "build_low"],
    "primary": ["language_high", "build_high", "tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["analysis_architecture_research", "review", "image"]
}'::jsonb
WHERE model_slug = 'claude-sonnet-4-6' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "language_low", "build_low"],
    "primary": ["language_high", "analysis_architecture_research", "build_high", "review"],
    "secondary": ["tool_use", "build_med", "language_low", "build_low"],
    "avoid": ["image"]
}'::jsonb
WHERE model_slug = 'claude-opus-4-6' AND status = 'active';

-- Google (active models only)
UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_low", "research_fan"],
    "primary": ["language_low", "research_fan"],
    "secondary": [],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "build_low", "image"]
}'::jsonb
WHERE model_slug IN ('gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.5-flash-preview-04-17', 'gemini-2.5-flash-tts')
AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_low"],
    "primary": ["language_low"],
    "secondary": [],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "build_low", "research_fan", "image"]
}'::jsonb
WHERE model_slug IN ('gemini-2.5-pro', 'gemini-2.5-pro-exp-03-25', 'gemini-2.5-pro-tts')
AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["tool_use", "build_med", "language_low", "build_low"],
    "primary": ["tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gemini-3-flash-preview' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["image"],
    "primary": ["image"],
    "secondary": [],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "language_low", "build_low", "research_fan"]
}'::jsonb
WHERE model_slug = 'gemini-3.1-flash-image-preview' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["review", "tool_use", "build_med", "language_low", "build_low"],
    "primary": ["review", "tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gemini-3.1-flash-lite-preview' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "language_low", "build_low"],
    "primary": ["language_high", "analysis_architecture_research", "build_high", "review"],
    "secondary": ["tool_use", "build_med", "language_low", "build_low"],
    "avoid": ["research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gemini-3.1-pro-preview' AND status = 'active';

-- OpenAI
UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_low", "build_low"],
    "primary": ["language_low", "build_low"],
    "secondary": [],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "research_fan", "image"]
}'::jsonb
WHERE model_slug IN ('gpt-5', 'gpt-5-codex', 'gpt-5.1', 'gpt-5.1-codex-max')
AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_low", "build_low", "research_fan"],
    "primary": ["language_low", "build_low", "research_fan"],
    "secondary": [],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "image"]
}'::jsonb
WHERE model_slug IN ('gpt-5-codex-mini', 'gpt-5.1-codex-mini')
AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["build_med", "language_low", "build_low"],
    "primary": ["build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gpt-5.1-codex' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["tool_use", "build_med", "language_low", "build_low"],
    "primary": ["tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "research_fan", "image"]
}'::jsonb
WHERE model_slug IN ('gpt-5.2', 'gpt-5.2-codex')
AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["tool_use", "build_med", "language_low", "build_low"],
    "primary": ["tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gpt-5.3-codex' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["tool_use", "language_low", "build_low"],
    "primary": ["tool_use"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "build_med", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gpt-5.3-codex-spark' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["language_high", "analysis_architecture_research", "build_high", "review", "tool_use", "build_med", "language_low", "build_low"],
    "primary": ["language_high", "analysis_architecture_research", "build_high", "review"],
    "secondary": ["tool_use", "build_med", "language_low", "build_low"],
    "avoid": ["image"]
}'::jsonb
WHERE model_slug = 'gpt-5.4' AND status = 'active';

UPDATE provider_model_candidates
SET task_affinities = '{
    "capabilities": ["tool_use", "build_med", "language_low", "build_low"],
    "primary": ["tool_use", "build_med"],
    "secondary": ["language_low", "build_low"],
    "avoid": ["language_high", "analysis_architecture_research", "build_high", "review", "research_fan", "image"]
}'::jsonb
WHERE model_slug = 'gpt-5.4-mini' AND status = 'active';

-- ============================================================
-- 2b. Populate capability columns from audit
-- ============================================================

-- Anthropic
UPDATE provider_model_candidates SET cap_language_low=true, cap_build_low=true, cap_research_fan=true WHERE model_slug='claude-haiku-4-5-20251001' AND status='active';
UPDATE provider_model_candidates SET cap_language_high=true, cap_build_high=true, cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='claude-sonnet-4-6' AND status='active';
UPDATE provider_model_candidates SET cap_language_high=true, cap_build_high=true, cap_review=true, cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='claude-opus-4-6' AND status='active';

-- Google
UPDATE provider_model_candidates SET cap_language_low=true, cap_research_fan=true WHERE model_slug IN ('gemini-2.5-flash','gemini-2.5-flash-lite','gemini-2.5-flash-preview-04-17','gemini-2.5-flash-tts') AND status='active';
UPDATE provider_model_candidates SET cap_language_low=true WHERE model_slug IN ('gemini-2.5-pro','gemini-2.5-pro-exp-03-25','gemini-2.5-pro-tts') AND status='active';
UPDATE provider_model_candidates SET cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gemini-3-flash-preview' AND status='active';
UPDATE provider_model_candidates SET cap_image=true WHERE model_slug='gemini-3.1-flash-image-preview' AND status='active';
UPDATE provider_model_candidates SET cap_review=true, cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gemini-3.1-flash-lite-preview' AND status='active';
UPDATE provider_model_candidates SET cap_language_high=true, cap_analysis_architecture_research=true, cap_build_high=true, cap_review=true, cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gemini-3.1-pro-preview' AND status='active';

-- OpenAI
UPDATE provider_model_candidates SET cap_language_low=true, cap_build_low=true WHERE model_slug IN ('gpt-5','gpt-5-codex','gpt-5.1','gpt-5.1-codex-max') AND status='active';
UPDATE provider_model_candidates SET cap_language_low=true, cap_build_low=true, cap_research_fan=true WHERE model_slug IN ('gpt-5-codex-mini','gpt-5.1-codex-mini') AND status='active';
UPDATE provider_model_candidates SET cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gpt-5.1-codex' AND status='active';
UPDATE provider_model_candidates SET cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug IN ('gpt-5.2','gpt-5.2-codex','gpt-5.3-codex') AND status='active';
UPDATE provider_model_candidates SET cap_tool_use=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gpt-5.3-codex-spark' AND status='active';
UPDATE provider_model_candidates SET cap_language_high=true, cap_analysis_architecture_research=true, cap_build_high=true, cap_review=true, cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gpt-5.4' AND status='active';
UPDATE provider_model_candidates SET cap_tool_use=true, cap_build_med=true, cap_language_low=true, cap_build_low=true WHERE model_slug='gpt-5.4-mini' AND status='active';

-- Trigger: setting a model to inactive auto-removes it from all routes
CREATE OR REPLACE FUNCTION cascade_inactive_to_routes()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'inactive' AND (OLD.status IS DISTINCT FROM 'inactive') THEN
        DELETE FROM task_type_routing WHERE model_slug = NEW.model_slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cascade_inactive_to_routes ON provider_model_candidates;
CREATE TRIGGER trg_cascade_inactive_to_routes
    AFTER UPDATE OF status ON provider_model_candidates
    FOR EACH ROW
    EXECUTE FUNCTION cascade_inactive_to_routes();

-- Remove inactive models from ALL routes (not just audited ones)
DELETE FROM task_type_routing
WHERE model_slug IN (
    SELECT model_slug FROM provider_model_candidates WHERE status = 'inactive'
);

-- ============================================================
-- 3. Rebuild auto/ route table from capability matrix
--    Rank = preference order within each route
--    Reset observed counters so live health starts fresh
-- ============================================================

-- Clear stale route entries
DELETE FROM task_type_routing WHERE task_type IN (
    'build', 'architecture', 'wiring', 'test', 'review', 'debate', 'refactor'
);

-- auto/build → models with build_high capability
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('build', 'claude-opus-4-6',        'anthropic', true, 1, 'high',   1, 'reasoning', 1, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('build', 'gpt-5.4',               'openai',    true, 2, 'high',   2, 'reasoning', 2, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('build', 'gemini-3.1-pro-preview', 'google',    true, 3, 'high',   3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('build', 'claude-sonnet-4-6',      'anthropic', true, 4, 'medium', 4, 'reasoning', 4, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/architecture → analysis_architecture_research models
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('architecture', 'claude-opus-4-6',        'anthropic', true, 1, 'high', 1, 'reasoning', 1, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('architecture', 'gpt-5.4',               'openai',    true, 2, 'high', 2, 'reasoning', 2, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('architecture', 'gemini-3.1-pro-preview', 'google',    true, 3, 'high', 3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/wiring → build_low/build_med (cheap + fast)
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('wiring', 'gpt-5.4-mini',              'openai', true, 1, 'medium', 1, 'instant', 1, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('wiring', 'gemini-3-flash-preview',     'google', true, 2, 'medium', 2, 'instant', 2, '{}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('wiring', 'gemini-3.1-flash-lite-preview','google',true, 3, 'medium', 3, 'instant', 3, '{}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('wiring', 'gpt-5.3-codex',             'openai', true, 4, 'medium', 4, 'instant', 4, '{}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/test → tool_use + build capability
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('test', 'gpt-5.4',               'openai',    true, 1, 'high',   1, 'reasoning', 1, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('test', 'claude-opus-4-6',        'anthropic', true, 2, 'high',   2, 'reasoning', 2, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('test', 'gemini-3.1-pro-preview', 'google',    true, 3, 'high',   3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('test', 'claude-sonnet-4-6',      'anthropic', true, 4, 'medium', 4, 'reasoning', 4, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/review → models with review capability
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('review', 'claude-opus-4-6',              'anthropic', true, 1, 'high',   1, 'reasoning', 1, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('review', 'gpt-5.4',                     'openai',    true, 2, 'high',   2, 'reasoning', 2, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('review', 'gemini-3.1-pro-preview',       'google',    true, 3, 'high',   3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('review', 'gemini-3.1-flash-lite-preview','google',    true, 4, 'medium', 4, 'instant',   4, '{}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/debate → frontier models only (one per provider)
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('debate', 'claude-opus-4-6',        'anthropic', true, 1, 'high', 1, 'reasoning', 1, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('debate', 'gpt-5.4',               'openai',    true, 2, 'high', 2, 'reasoning', 2, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('debate', 'gemini-3.1-pro-preview', 'google',    true, 3, 'high', 3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);

-- auto/refactor → same as build
INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, route_tier, route_tier_rank, latency_class, latency_rank, reasoning_control, route_health_score, observed_completed_count, observed_execution_failure_count, observed_external_failure_count, observed_config_failure_count, observed_downstream_failure_count, observed_downstream_bug_count, consecutive_internal_failures, last_failure_category, last_failure_zone, route_source, recent_successes, recent_failures) VALUES
    ('refactor', 'claude-opus-4-6',        'anthropic', true, 1, 'high',   1, 'reasoning', 1, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('refactor', 'gpt-5.4',               'openai',    true, 2, 'high',   2, 'reasoning', 2, '{"kind":"discrete","parameter":"reasoning.effort","default_level":"none","supported_levels":["none","low","medium","high","xhigh"]}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('refactor', 'gemini-3.1-pro-preview', 'google',    true, 3, 'high',   3, 'reasoning', 3, '{"kind":"discrete","parameter":"thinking_level","turn_off_supported":false}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0),
    ('refactor', 'claude-sonnet-4-6',      'anthropic', true, 4, 'medium', 4, 'reasoning', 4, '{"kind":"budgeted","parameter":"thinking.budget_tokens","adaptive_thinking":true,"extended_thinking":true}'::jsonb, 0.65, 0,0,0,0,0,0,0, '', '', 'explicit', 0,0);
