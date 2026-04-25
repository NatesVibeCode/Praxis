-- Migration 244: Open the OpenRouter API paid lane for explicit operator routes
--
-- Operator direction (2026-04-25, nate): "fix it and open the router".
--
-- Context:
--   Migration 243 registered DeepSeek V4-Pro through OpenRouter and pinned it
--   as task_type='build' rank=1. The OpenRouter API product itself works, but
--   TaskTypeRouter rejects the route before the compile path can use it because
--   provider_cli_profiles.adapter_economics->'llm_task' declares
--   allow_payg_fallback=false. runtime.lane_policy treats that as a hard
--   paid-lane refusal: lane.rejected.payg_fallback_disabled.
--
-- Scope:
--   API only: provider_slug='openrouter', adapter_type='llm_task'.
--   CLI remains the default provider execution lane under
--   architecture-policy::provider-routing::cli-default-api-exception. This
--   migration does not add, demote, or alter any CLI route.

BEGIN;

UPDATE provider_cli_profiles
   SET adapter_economics = jsonb_set(
           adapter_economics,
           '{llm_task,allow_payg_fallback}',
           'true'::jsonb,
           true
       ),
       updated_at = now()
 WHERE provider_slug = 'openrouter';

INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref
) VALUES (
    'operator_decision.openrouter-api-payg-lane-open.2026-04-25',
    'architecture-policy::provider-routing::openrouter-api-payg-lane-open',
    'architecture_policy',
    'decided',
    'OpenRouter API paid lane is open when explicitly routed',
    'Operator explicitly authorized opening the OpenRouter API lane after DeepSeek V4-Pro via OpenRouter tested successfully but the compile resolver rejected it with lane.rejected.payg_fallback_disabled. This decision opens provider_slug=openrouter adapter_type=llm_task by setting allow_payg_fallback=true. Scope is API/OpenRouter only. CLI remains the default provider execution lane for every use case unless explicitly overridden.',
    'nate',
    'conversation',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'provider_routing'
) ON CONFLICT (decision_key) DO UPDATE SET
    decision_kind = EXCLUDED.decision_kind,
    decision_status = EXCLUDED.decision_status,
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    decided_by = EXCLUDED.decided_by,
    decision_source = EXCLUDED.decision_source,
    effective_from = EXCLUDED.effective_from,
    decided_at = EXCLUDED.decided_at,
    updated_at = now(),
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref = EXCLUDED.decision_scope_ref;

COMMIT;

-- Verification:
--   SELECT adapter_economics->'llm_task'->>'allow_payg_fallback'
--     FROM provider_cli_profiles
--    WHERE provider_slug='openrouter';
--     -> expect true
--
--   runtime.compiler_llm._resolve_app_compile_route()
--     -> expect ('openrouter', 'deepseek/deepseek-v4-pro')
