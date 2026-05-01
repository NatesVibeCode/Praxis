-- Migration 411: Register durable workflow repair queue operations.
--
-- Forge receipts:
--   b63da0b1-ef7b-4c7b-ae5c-4c233a2e6c38 workflow_repair_queue.status
--   f4254c6f-6c5a-4750-9729-14517c175df2 workflow_repair_queue.command
--
-- Migration 410 owns the table/triggers. This slice makes queue inspection
-- and mutation catalog-backed so CLI and MCP surfaces stay thin.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-repair-queue-status',
    p_operation_name        := 'workflow_repair_queue.status',
    p_handler_ref           := 'runtime.operations.queries.workflow_repair_queue.handle_query_workflow_repair_queue_status',
    p_input_model_ref       := 'runtime.operations.queries.workflow_repair_queue.WorkflowRepairQueueStatusQuery',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/workflow_repair_queue_status',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'conversation.workflow_repair_queue.20260501',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_repair_queue_status.20260501',
    p_label                 := 'Workflow Repair Queue Status',
    p_summary               := 'Inspect durable Solution, Workflow, and Job repair queue items.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'workflow-repair-queue-command',
    p_operation_name        := 'workflow_repair_queue.command',
    p_handler_ref           := 'runtime.operations.commands.workflow_repair_queue.handle_workflow_repair_queue_command',
    p_input_model_ref       := 'runtime.operations.commands.workflow_repair_queue.WorkflowRepairQueueCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_authority_ref         := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/workflow_repair_queue_command',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'workflow_repair_queue.commanded',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 15000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'conversation.workflow_repair_queue.20260501',
    p_binding_revision      := 'binding.operation_catalog_registry.workflow_repair_queue_command.20260501',
    p_label                 := 'Workflow Repair Queue Command',
    p_summary               := 'Claim, release, or close durable repair queue work.'
);

COMMIT;
