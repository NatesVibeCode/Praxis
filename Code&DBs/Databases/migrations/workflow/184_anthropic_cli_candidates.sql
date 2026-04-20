-- Migration 184: Restore a minimal anthropic CLI candidate pair.
--
-- Migration 182 restored the anthropic CLI profile + lane policy +
-- concurrency, but left provider_model_candidates empty. Without at least
-- one candidate row, worker-dispatched CLI jobs targeting anthropic can't
-- resolve — _get_cli_config (task_assembler, manifest_generator) reads
-- cli_config off a provider_model_candidates row.
--
-- The interactive `claude` binary at the shell is unaffected (it doesn't
-- touch the DB). But if any spec or operator path ever invokes the CLI via
-- a worker (provider=anthropic, adapter=cli_llm), it needs a candidate.
--
-- This migration adds exactly TWO candidate rows:
--   * anthropic/claude-sonnet-4-6  (default CLI model)
--   * anthropic/claude-opus-4-6    (heavier model, same CLI binary)
--
-- Both rows are CLI-only:
--   - cli_config populated with the claude binary invocation template.
--   - default_parameters has NO selected_transport=api, NO api endpoint.
--   - task_affinities.primary = [] so auto/* does not rank these.
--   - No task_type_routing row is added (auto/* cannot resolve anthropic).
--   - No model_profile_candidate_bindings row is added (profile resolver
--     will not pull these in).
--
-- Effect: a spec that explicitly names anthropic/claude-sonnet-4-6 with
-- adapter_type=cli_llm will resolve and dispatch the claude binary. No
-- other path can land on anthropic.

BEGIN;

INSERT INTO public.provider_model_candidates (
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
    cli_config,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    task_affinities,
    benchmark_profile,
    decision_ref,
    effective_from,
    created_at,
    cap_language_high,
    cap_analysis_architecture_research,
    cap_build_high,
    cap_review,
    cap_tool_use,
    cap_build_med
) VALUES
(
    'candidate.anthropic.cli.claude-sonnet-4-6',
    'provider.anthropic',
    'Anthropic (CLI)',
    'anthropic',
    'claude-sonnet-4-6',
    'active',
    10,   -- intentionally low priority so auto/* never ranks this ahead of openrouter/openai
    1,
    '["cli", "subscription", "local-cli"]'::jsonb,
    '{
      "provider_slug": "anthropic",
      "model_slug": "claude-sonnet-4-6",
      "adapter_type": "cli_llm",
      "billing_model": "subscription_included",
      "catalog_source": "migration.184"
    }'::jsonb,
    '{
      "binary_name": "claude",
      "model_slug": "claude-sonnet-4-6",
      "provider_slug": "anthropic",
      "prompt_mode": "stdin",
      "cmd_template": ["claude", "-p", "--output-format", "json", "--model", "{model}"],
      "envelope_key": "result",
      "output_format": "json",
      "mcp_config_style": "claude_mcp_config"
    }'::jsonb,
    'low',
    99,
    'instant',
    8,
    '{}'::jsonb,
    '{
      "primary": [],
      "secondary": [],
      "specialized": ["interactive-cli"],
      "fallback": [],
      "avoid": []
    }'::jsonb,
    '{
      "evidence_level": "operator_positioning",
      "positioning": "Anthropic CLI (claude binary) subscription lane. Reachable only by explicit spec-level provider=anthropic adapter_type=cli_llm requests. auto/* does not rank this.",
      "source_refs": ["decision.2026-04-20.anthropic-cli-only-restored"]
    }'::jsonb,
    'decision.2026-04-20.anthropic-cli-only-restored',
    now(),
    now(),
    true,
    true,
    true,
    true,
    true,
    true
),
(
    'candidate.anthropic.cli.claude-opus-4-6',
    'provider.anthropic',
    'Anthropic (CLI)',
    'anthropic',
    'claude-opus-4-6',
    'active',
    10,
    1,
    '["cli", "subscription", "local-cli", "heavy"]'::jsonb,
    '{
      "provider_slug": "anthropic",
      "model_slug": "claude-opus-4-6",
      "adapter_type": "cli_llm",
      "billing_model": "subscription_included",
      "catalog_source": "migration.184"
    }'::jsonb,
    '{
      "binary_name": "claude",
      "model_slug": "claude-opus-4-6",
      "provider_slug": "anthropic",
      "prompt_mode": "stdin",
      "cmd_template": ["claude", "-p", "--output-format", "json", "--model", "{model}"],
      "envelope_key": "result",
      "output_format": "json",
      "mcp_config_style": "claude_mcp_config"
    }'::jsonb,
    'low',
    99,
    'reasoning',
    6,
    '{}'::jsonb,
    '{
      "primary": [],
      "secondary": [],
      "specialized": ["interactive-cli", "heavy-reasoning"],
      "fallback": [],
      "avoid": []
    }'::jsonb,
    '{
      "evidence_level": "operator_positioning",
      "positioning": "Anthropic CLI opus variant. Heavier reasoning; same CLI binary and subscription lane as sonnet. Reachable only by explicit request.",
      "source_refs": ["decision.2026-04-20.anthropic-cli-only-restored"]
    }'::jsonb,
    'decision.2026-04-20.anthropic-cli-only-restored',
    now(),
    now(),
    true,
    true,
    true,
    true,
    true,
    true
)
ON CONFLICT (candidate_ref) DO UPDATE SET
    provider_ref       = EXCLUDED.provider_ref,
    provider_name      = EXCLUDED.provider_name,
    provider_slug      = EXCLUDED.provider_slug,
    model_slug         = EXCLUDED.model_slug,
    status             = EXCLUDED.status,
    priority           = EXCLUDED.priority,
    balance_weight     = EXCLUDED.balance_weight,
    capability_tags    = EXCLUDED.capability_tags,
    default_parameters = EXCLUDED.default_parameters,
    cli_config         = EXCLUDED.cli_config,
    route_tier         = EXCLUDED.route_tier,
    route_tier_rank    = EXCLUDED.route_tier_rank,
    latency_class      = EXCLUDED.latency_class,
    latency_rank       = EXCLUDED.latency_rank,
    task_affinities    = EXCLUDED.task_affinities,
    benchmark_profile  = EXCLUDED.benchmark_profile,
    decision_ref       = EXCLUDED.decision_ref,
    effective_from     = EXCLUDED.effective_from;

COMMIT;

-- Verification:
--   SELECT candidate_ref, model_slug, status, priority FROM provider_model_candidates
--     WHERE provider_slug='anthropic';
--   SELECT COUNT(*) FROM task_type_routing WHERE provider_slug='anthropic';  -- expect 0
--   SELECT COUNT(*) FROM model_profile_candidate_bindings
--     WHERE candidate_ref LIKE 'candidate.anthropic.%';  -- expect 0
