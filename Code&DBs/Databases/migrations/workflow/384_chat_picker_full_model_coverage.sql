-- Migration 384: Surface every routed model as a chat candidate.
--
-- Why:
--   The chat picker drawer (StrategyConsole + chat.routing_options.list) shows
--   only rows where task_type='chat'. Today most providers/models have routing
--   rows for build/architecture/analysis/research/etc. but NOT for chat, so
--   they're invisible in the picker. Operator wants all known models available
--   as chat participants.
--
-- What it does:
--   For every distinct (provider_slug, model_slug) that has at least one
--   permitted routing row in some other task_type AND lacks a chat row:
--     * insert a chat row with sub_task_type='*'
--     * pick the transport from the source row with the lowest rank
--       (operator's current preferred transport for that model)
--     * inherit route_tier / latency_class / route_health_score / temperature /
--       max_tokens / cost / benchmark_score from the source so quality signal
--       carries forward
--     * rank = 50 (below today's curated chat rank 1-47, sorts by health
--       within that bucket per the picker query handler)
--     * permitted = TRUE
--     * route_source = 'derived' (auto-promotion, not an explicit operator
--       per-row decision; the operator decision is "all models in chat")
--
-- Trigger 378 (task_type_routing_transport_admission_check) will reject any
-- (provider, transport) combo without a matching active provider_transport_admissions
-- row. The cleanup migration 375 already removed those, so the source rows
-- selected here are by construction valid transport combos.
--
-- Idempotent: NOT EXISTS guard means re-runs add only newly-introduced models.

BEGIN;

WITH best_transport_per_model AS (
    SELECT DISTINCT ON (provider_slug, model_slug)
        provider_slug,
        model_slug,
        transport_type,
        route_tier,
        latency_class,
        route_health_score,
        benchmark_score,
        cost_per_m_tokens,
        temperature,
        max_tokens
      FROM task_type_routing
     WHERE permitted = TRUE
       AND task_type <> 'chat'
     ORDER BY provider_slug, model_slug, rank ASC, transport_type
)
INSERT INTO task_type_routing (
    task_type,
    sub_task_type,
    provider_slug,
    model_slug,
    transport_type,
    rank,
    permitted,
    route_source,
    route_tier,
    latency_class,
    route_health_score,
    benchmark_score,
    cost_per_m_tokens,
    temperature,
    max_tokens,
    updated_at
)
SELECT
    'chat'                                  AS task_type,
    '*'                                     AS sub_task_type,
    btpm.provider_slug,
    btpm.model_slug,
    btpm.transport_type,
    50                                      AS rank,
    TRUE                                    AS permitted,
    'derived'                               AS route_source,
    COALESCE(btpm.route_tier, 'medium')     AS route_tier,
    COALESCE(btpm.latency_class, 'reasoning') AS latency_class,
    COALESCE(btpm.route_health_score, 0.65) AS route_health_score,
    btpm.benchmark_score,
    btpm.cost_per_m_tokens,
    btpm.temperature,
    btpm.max_tokens,
    now()                                   AS updated_at
  FROM best_transport_per_model btpm
 WHERE NOT EXISTS (
     SELECT 1
       FROM task_type_routing existing_chat
      WHERE existing_chat.task_type     = 'chat'
        AND existing_chat.sub_task_type = '*'
        AND existing_chat.provider_slug = btpm.provider_slug
        AND existing_chat.model_slug    = btpm.model_slug
 );

COMMIT;
