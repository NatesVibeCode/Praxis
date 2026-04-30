-- Migration 373: Reclassify deterministic compose_plan as interactive.
--
-- Migration 353 grouped compose_plan with compile_preview and launch_plan as
-- background + kickoff_required. That was too broad: compose_plan is the
-- deterministic, explicit-step-marker path that returns a ProposedPlan inline
-- for agent approval. The LLM compose path remains compose_plan_via_llm and
-- keeps the background/kickoff contract.

BEGIN;

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
    effective_to,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref,
    scope_clamp
) VALUES (
    'operator_decision.architecture_policy.concurrency.deterministic_compose_plan_interactive',
    'architecture-policy::concurrency::deterministic-compose-plan-is-interactive',
    'architecture_policy',
    'decided',
    'Deterministic compose_plan is interactive; LLM compose remains kickoff-first',
    'compose_plan only accepts explicit step markers and returns a ProposedPlan for approval; marking it kickoff_required breaks the public agent front door before compose code runs. LLM-bound synthesis remains compose_plan_via_llm and is still background + kickoff_required.',
    'operator:nate',
    'conversation:2026-04-30-compose-function-auth-public-beta',
    NOW(),
    NULL,
    NOW(),
    NOW(),
    NOW(),
    'authority_domain',
    'concurrency::gateway_operations',
    '{"applies_to":["operation_catalog_registry.compose_plan","gateway.dispatch","praxis_compose_plan"],"does_not_apply_to":["operation_catalog_registry.compose_plan_via_llm","operation_catalog_registry.launch_plan","operation_catalog_registry.compile_preview"]}'::jsonb
)
ON CONFLICT (decision_key) DO UPDATE SET
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    scope_clamp = EXCLUDED.scope_clamp,
    updated_at = now();

UPDATE operation_catalog_registry
   SET execution_lane = 'interactive',
       kickoff_required = FALSE,
       timeout_ms = 35000,
       binding_revision = 'binding.operation_catalog_registry.compose_plan.gateway.20260424.lane.20260430.interactive',
       decision_ref = 'architecture-policy::concurrency::deterministic-compose-plan-is-interactive',
       updated_at = now()
 WHERE operation_ref = 'compose-plan'
   AND operation_name = 'compose_plan'
   AND (
        execution_lane IS DISTINCT FROM 'interactive'
        OR kickoff_required IS DISTINCT FROM FALSE
        OR timeout_ms IS DISTINCT FROM 35000
        OR decision_ref IS DISTINCT FROM 'architecture-policy::concurrency::deterministic-compose-plan-is-interactive'
   );

UPDATE authority_object_registry
   SET source_decision_ref = 'architecture-policy::concurrency::deterministic-compose-plan-is-interactive',
       metadata = jsonb_set(
           jsonb_set(
               jsonb_set(
                   COALESCE(metadata, '{}'::jsonb),
                   '{execution_lane}',
                   '"interactive"'::jsonb,
                   TRUE
               ),
               '{kickoff_required}',
               'false'::jsonb,
               TRUE
           ),
           '{timeout_ms}',
           '35000'::jsonb,
           TRUE
       ),
       updated_at = now()
 WHERE object_ref = 'operation.compose_plan';

UPDATE data_dictionary_objects
   SET metadata = jsonb_set(
           jsonb_set(
               jsonb_set(
                   COALESCE(metadata, '{}'::jsonb),
                   '{execution_lane}',
                   '"interactive"'::jsonb,
                   TRUE
               ),
               '{kickoff_required}',
               'false'::jsonb,
               TRUE
           ),
           '{timeout_ms}',
           '35000'::jsonb,
           TRUE
       ),
       updated_at = now()
 WHERE object_kind = 'operation.compose_plan';

COMMIT;
