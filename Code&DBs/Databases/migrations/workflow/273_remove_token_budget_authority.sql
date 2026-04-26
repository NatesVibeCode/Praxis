-- Migration 273: Remove token-budget authority from workflow plumbing.
--
-- Operator decision:
--   operator_decision.architecture_policy.workflow_execution.no_token_budget_authority
--
-- Token usage remains execution telemetry on receipts. It is not an
-- authoring gate, workflow gate, or pre-launch approval authority.

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
    decision_scope_kind,
    decision_scope_ref,
    scope_clamp,
    effective_from,
    effective_to,
    decided_at,
    created_at,
    updated_at
) VALUES (
    'operator_decision.architecture_policy.workflow_execution.no_token_budget_authority',
    'architecture-policy::workflow-execution::no-token-budget-authority',
    'architecture_policy',
    'decided',
    'Token budgets are not workflow execution authority',
    'Token usage belongs in observability receipts after execution, not as pre-launch budget projections, CQRS approval gates, or tool surfaces that can refuse platform work. Token-budget gating is anti-platform because it substitutes a guessed resource estimate for operator intent and provider/runtime authority. Remove token-budget projection and launch refusal paths; keep actual token telemetry only as observed execution data.',
    'nate',
    'conversation',
    'authority_domain',
    'workflow_execution',
    jsonb_build_object(
        'applies_to',
        jsonb_build_array(
            'Workflow execution authority',
            'MCP and CLI launch surfaces',
            'Plan authoring and compose-and-launch gates'
        ),
        'does_not_apply_to',
        jsonb_build_array(
            'Observed token usage telemetry on completed execution receipts',
            'Context-window packing required to fit provider model limits'
        )
    ),
    '2026-04-26T20:21:16.085728+00:00'::timestamptz,
    NULL,
    '2026-04-26T20:21:16.085728+00:00'::timestamptz,
    '2026-04-26T20:21:16.085728+00:00'::timestamptz,
    now()
)
ON CONFLICT (operator_decision_id) DO UPDATE SET
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    decision_status = EXCLUDED.decision_status,
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref = EXCLUDED.decision_scope_ref,
    scope_clamp = EXCLUDED.scope_clamp,
    effective_to = EXCLUDED.effective_to,
    updated_at = now();

DELETE FROM data_dictionary_objects
 WHERE object_kind IN ('budget_gate', 'plan_field:budget');

COMMIT;
