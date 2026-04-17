BEGIN;

UPDATE operation_catalog_registry
   SET source_kind = CASE source_kind
           WHEN 'cqrs_command' THEN 'operation_command'
           WHEN 'cqrs_query' THEN 'operation_query'
           ELSE source_kind
       END,
       input_model_ref = replace(input_model_ref, 'runtime.cqrs.', 'runtime.operations.'),
       handler_ref = replace(handler_ref, 'runtime.cqrs.', 'runtime.operations.'),
       updated_at = now()
 WHERE source_kind IN ('cqrs_command', 'cqrs_query')
    OR input_model_ref LIKE 'runtime.cqrs.%'
    OR handler_ref LIKE 'runtime.cqrs.%';

UPDATE operation_catalog_source_policy_registry
   SET policy_ref = CASE policy_ref
           WHEN 'cqrs-command' THEN 'operation-command'
           WHEN 'cqrs-query' THEN 'operation-query'
           ELSE policy_ref
       END,
       source_kind = CASE source_kind
           WHEN 'cqrs_command' THEN 'operation_command'
           WHEN 'cqrs_query' THEN 'operation_query'
           ELSE source_kind
       END,
       updated_at = now()
 WHERE policy_ref IN ('cqrs-command', 'cqrs-query')
    OR source_kind IN ('cqrs_command', 'cqrs_query');

ALTER TABLE operation_catalog_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_source_kind_check;

ALTER TABLE operation_catalog_registry
    ADD CONSTRAINT operation_catalog_registry_source_kind_check
    CHECK (source_kind IN ('operation_command', 'operation_query'));

ALTER TABLE operation_catalog_source_policy_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_source_policy_registry_source_kind_check;

ALTER TABLE operation_catalog_source_policy_registry
    ADD CONSTRAINT operation_catalog_source_policy_registry_source_kind_check
    CHECK (source_kind IN ('operation_command', 'operation_query'));

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
) VALUES
    (
        'operator-task-route-eligibility',
        'operator.task_route_eligibility',
        'operation_command',
        'command',
        'POST',
        '/api/operator/task-route-eligibility',
        'runtime.operations.commands.operator_control.TaskRouteEligibilityCommand',
        'runtime.operations.commands.operator_control.handle_task_route_eligibility',
        'authority.task_route_eligibility',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.surface_cleanup.20260416',
        'decision.operation_catalog_registry.surface_cleanup.20260416'
    ),
    (
        'operator-native-primary-cutover-gate',
        'operator.native_primary_cutover_gate',
        'operation_command',
        'command',
        'POST',
        '/api/operator/native-primary-cutover-gate',
        'runtime.operations.commands.operator_control.NativePrimaryCutoverGateCommand',
        'runtime.operations.commands.operator_control.handle_native_primary_cutover_gate',
        'authority.native_primary_cutover_gate',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.surface_cleanup.20260416',
        'decision.operation_catalog_registry.surface_cleanup.20260416'
    ),
    (
        'operator-transport-support',
        'operator.transport_support',
        'operation_query',
        'query',
        'POST',
        '/api/operator/transport-support',
        'runtime.operations.queries.operator_support.QueryTransportSupport',
        'runtime.operations.queries.operator_support.handle_query_transport_support',
        'authority.transport_eligibility',
        'projection.transport_eligibility',
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.surface_cleanup.20260416',
        'decision.operation_catalog_registry.surface_cleanup.20260416'
    )
ON CONFLICT (operation_ref) DO UPDATE SET
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
