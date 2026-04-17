BEGIN;

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
)
VALUES
    (
        'operator-decision-record',
        'operator.decision_record',
        'operation_command',
        'command',
        'POST',
        '/api/operator/decision',
        'runtime.operations.commands.operator_control.OperatorDecisionRecordCommand',
        'runtime.operations.commands.operator_control.handle_operator_decision_record',
        'authority.operator_decisions',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.operator_decision_bindings.20260417',
        'decision.operation_catalog_registry.operator_decision_bindings.20260417'
    ),
    (
        'operator-decision-list',
        'operator.decision_list',
        'operation_query',
        'query',
        'GET',
        '/api/operator/decisions',
        'runtime.operations.queries.operator_decisions.QueryOperatorDecisions',
        'runtime.operations.queries.operator_decisions.handle_query_operator_decisions',
        'authority.operator_decisions',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.operator_decision_bindings.20260417',
        'decision.operation_catalog_registry.operator_decision_bindings.20260417'
    )
ON CONFLICT (operation_ref) DO UPDATE
SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
