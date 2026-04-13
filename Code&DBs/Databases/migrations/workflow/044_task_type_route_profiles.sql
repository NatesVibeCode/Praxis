-- Migration 044: explicit route profiles beyond raw ranking metrics
--
-- Adds first-class authority for:
--   * route_tier       -> high | medium | low
--   * latency_class    -> reasoning | instant
--   * reasoning_control -> provider-specific effort/budget metadata
--
-- This keeps "how strong is this route family?" separate from
-- "how much internal thinking can the provider apply?"

ALTER TABLE task_type_routing
  ADD COLUMN IF NOT EXISTS route_tier TEXT NOT NULL DEFAULT 'medium'
    CHECK (route_tier IN ('high', 'medium', 'low')),
  ADD COLUMN IF NOT EXISTS route_tier_rank INTEGER NOT NULL DEFAULT 99
    CHECK (route_tier_rank >= 1),
  ADD COLUMN IF NOT EXISTS latency_class TEXT NOT NULL DEFAULT 'reasoning'
    CHECK (latency_class IN ('reasoning', 'instant')),
  ADD COLUMN IF NOT EXISTS latency_rank INTEGER NOT NULL DEFAULT 99
    CHECK (latency_rank >= 1),
  ADD COLUMN IF NOT EXISTS reasoning_control JSONB NOT NULL DEFAULT '{}'::jsonb;

WITH latest_candidates AS (
    SELECT DISTINCT ON (provider_slug, model_slug)
        provider_slug,
        model_slug,
        capability_tags
    FROM provider_model_candidates
    WHERE status = 'active'
    ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
)
UPDATE task_type_routing route
SET
    route_tier = CASE
        WHEN route.provider_slug = 'openai' AND route.model_slug LIKE '%-pro' THEN 'high'
        WHEN route.provider_slug = 'openai' AND route.model_slug NOT LIKE '%-mini' AND route.model_slug NOT LIKE '%-nano' THEN 'high'
        WHEN route.provider_slug = 'anthropic' AND route.model_slug LIKE 'claude-opus-%' THEN 'high'
        WHEN route.provider_slug = 'google' AND route.model_slug LIKE '%-pro%' THEN 'high'
        WHEN route.provider_slug = 'openai' AND route.model_slug LIKE '%-mini' THEN 'medium'
        WHEN route.provider_slug = 'anthropic' AND route.model_slug LIKE 'claude-sonnet-%' THEN 'medium'
        WHEN route.provider_slug = 'google' AND route.model_slug LIKE '%flash%' THEN 'medium'
        WHEN route.provider_slug = 'openai' AND route.model_slug LIKE '%-nano' THEN 'low'
        WHEN route.provider_slug = 'anthropic' AND route.model_slug LIKE 'claude-haiku-%' THEN 'low'
        WHEN route.provider_slug = 'google' AND route.model_slug LIKE '%flash-lite%' THEN 'low'
        WHEN latest_candidates.capability_tags ? 'frontier' THEN 'high'
        WHEN latest_candidates.capability_tags ? 'economy' THEN 'low'
        ELSE 'medium'
    END,
    route_tier_rank = GREATEST(COALESCE(route.rank, 99), 1),
    latency_class = CASE
        WHEN route.provider_slug = 'openai'
            AND (route.model_slug LIKE '%-mini' OR route.model_slug LIKE '%-nano') THEN 'instant'
        WHEN route.provider_slug = 'anthropic' AND route.model_slug LIKE 'claude-haiku-%' THEN 'instant'
        WHEN route.provider_slug = 'google'
            AND (route.model_slug LIKE '%flash%' OR route.model_slug LIKE '%flash-lite%') THEN 'instant'
        WHEN latest_candidates.capability_tags ? 'latency' THEN 'instant'
        WHEN route.task_type IN ('chat', 'auto/chat', 'wiring') THEN 'instant'
        ELSE 'reasoning'
    END,
    latency_rank = GREATEST(COALESCE(route.rank, 99), 1),
    reasoning_control = CASE
        WHEN route.provider_slug = 'openai' AND route.model_slug LIKE 'gpt-5.4%' THEN jsonb_build_object(
            'kind', 'discrete',
            'parameter', 'reasoning.effort',
            'supported_levels', jsonb_build_array('none', 'low', 'medium', 'high', 'xhigh'),
            'default_level', 'none'
        )
        WHEN route.provider_slug = 'openai' AND route.model_slug LIKE 'gpt-5%' THEN jsonb_build_object(
            'kind', 'discrete',
            'parameter', 'reasoning.effort',
            'supported_levels', jsonb_build_array('minimal', 'low', 'medium', 'high'),
            'default_level', 'minimal'
        )
        WHEN route.provider_slug = 'anthropic' THEN jsonb_build_object(
            'kind', 'budgeted',
            'parameter', 'thinking.budget_tokens',
            'extended_thinking', true,
            'adaptive_thinking', CASE
                WHEN route.model_slug LIKE 'claude-haiku-%' THEN false
                ELSE true
            END
        )
        WHEN route.provider_slug = 'google' AND route.model_slug LIKE 'gemini-3%' THEN jsonb_build_object(
            'kind', 'discrete',
            'parameter', 'thinking_level',
            'turn_off_supported', CASE
                WHEN route.model_slug LIKE '%pro%' THEN false
                ELSE true
            END
        )
        WHEN route.provider_slug = 'google' AND route.model_slug LIKE 'gemini-2.5%' THEN jsonb_build_object(
            'kind', 'budgeted',
            'parameter', 'thinking_budget',
            'turn_off_supported', CASE
                WHEN route.model_slug LIKE '%pro%' THEN false
                ELSE true
            END
        )
        ELSE COALESCE(route.reasoning_control, '{}'::jsonb)
    END
FROM latest_candidates
WHERE latest_candidates.provider_slug = route.provider_slug
  AND latest_candidates.model_slug = route.model_slug;

CREATE INDEX IF NOT EXISTS task_type_routing_route_tier_idx
    ON task_type_routing (route_tier, route_tier_rank, rank)
    WHERE permitted = TRUE;

CREATE INDEX IF NOT EXISTS task_type_routing_latency_class_idx
    ON task_type_routing (latency_class, latency_rank, rank)
    WHERE permitted = TRUE;
