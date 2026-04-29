-- Migration 328: Reasoning-effort policy coverage.
--
-- Migration 327 created the base effort matrix and the common task policies.
-- This migration adds long-tail workflow/compile policy coverage plus one
-- explicit default row so new task types do not depend on hidden code fallback.

BEGIN;

INSERT INTO task_type_effort_policy (
    task_type,
    sub_task_type,
    default_effort_slug,
    min_effort_slug,
    max_effort_slug,
    escalation_rules,
    decision_ref,
    metadata
) VALUES
    ('research', '*', 'high', 'medium', 'max', '{"on_low_evidence":"max"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.328"}'::jsonb),
    ('compile', '*', 'medium', 'low', 'high', '{"on_retrieval_gap":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.328"}'::jsonb),
    ('compile_synthesize', '*', 'medium', 'low', 'high', '{"on_conflicting_sections":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.328"}'::jsonb),
    ('plan_section_author', '*', 'medium', 'low', 'high', '{"on_contract_ambiguity":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.328"}'::jsonb),
    ('*', '*', 'medium', 'instant', 'high', '{"fallback_policy":true}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.328","purpose":"default effort policy"}'::jsonb)
ON CONFLICT (task_type, sub_task_type) DO UPDATE SET
    default_effort_slug = EXCLUDED.default_effort_slug,
    min_effort_slug = EXCLUDED.min_effort_slug,
    max_effort_slug = EXCLUDED.max_effort_slug,
    escalation_rules = EXCLUDED.escalation_rules,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
