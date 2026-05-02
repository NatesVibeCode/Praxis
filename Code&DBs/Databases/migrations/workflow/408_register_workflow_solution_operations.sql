-- Migration 408: Register Solution operations over durable workflow-chain authority.
--
-- "Solution" is the operator-facing object for a coordinated answer under
-- proof. The backing storage authority remains workflow_chain for this pass;
-- the operation surface gives operators Solution language without adding a
-- second state owner.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-solution-submit',
    p_operation_name        := 'workflow_solution.submit',
    p_handler_ref           := 'runtime.operations.commands.workflow_chain_submit.handle_workflow_chain_submit',
    p_input_model_ref       := 'runtime.operations.commands.workflow_chain_submit.WorkflowChainSubmitCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workflow_solution_submit',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workflow_solution.submitted',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 30000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'conversation.roadmap_burndown.solution_name.20260501',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_solution_submit.20260501',
    p_label                 := 'Workflow Solution Submit',
    p_summary               := 'Submit one durable Solution: a coordinated multi-workflow answer under proof, backed by workflow_chain authority.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-solution-status',
    p_operation_name        := 'workflow_solution.status',
    p_handler_ref           := 'runtime.operations.queries.workflow_solution.handle_query_workflow_solution_status',
    p_input_model_ref       := 'runtime.operations.queries.workflow_solution.WorkflowSolutionStatusQuery',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workflow_solution_status',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'conversation.roadmap_burndown.solution_name.20260501',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_solution_status.20260501',
    p_label                 := 'Workflow Solution Status',
    p_summary               := 'Read status for durable Solutions backed by workflow_chain authority.'
);

COMMIT;
