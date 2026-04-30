-- Migration 348: register authority.impact_contract_audit.scan query operation.
--
-- Audit closes the gap left by the candidate-path enforcement chain
-- (Pass 1-8): when authority-bearing files get edited OUTSIDE the
-- code_change_candidate flow (direct git commits, scripted edits,
-- emergency hot-fixes), the impact contract is not anchored. This query
-- takes a list of paths and surfaces drift — paths that are authority-
-- bearing but lack a backing candidate row.
--
-- Pure read. No writes, no events. Caller decides the response (file a
-- bug, gate CI, surface in Moon, etc.).
--
-- Wizard (praxis_operation_forge) was unreachable when this migration
-- landed (broker offline mid-session). Shape mirrors prior wizard output
-- for query operations: posture=observe, idempotency=read_only,
-- registered via register_operation_atomic.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'authority-impact-contract-audit-scan',
    p_operation_name        := 'authority.impact_contract_audit.scan',
    p_handler_ref           := 'runtime.operations.queries.authority_impact_contract_audit.handle_scan_authority_impact_contract_audit',
    p_input_model_ref       := 'runtime.operations.queries.authority_impact_contract_audit.ScanAuthorityImpactContractAudit',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority/impact_contract_audit/scan',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.authority_impact_contract_audit_closes_direct_edit_gap',
    p_summary               := 'Audit a list of paths for impact-contract coverage. For each path the resolver classifies it as authority-bearing or not, and for authority-bearing paths it joins to candidate_authority_impacts to find a backing candidate. Surfaces drift where authority-bearing edits exist without a candidate impact contract — closes the gap for direct commits or scripted edits that bypass the candidate flow.'
);

COMMIT;
