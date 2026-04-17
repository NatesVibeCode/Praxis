BEGIN;

ALTER TABLE operation_catalog_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_source_kind_check;

ALTER TABLE operation_catalog_registry
    ADD CONSTRAINT operation_catalog_registry_source_kind_check
    CHECK (
        source_kind IN (
            'cqrs_command',
            'cqrs_query',
            'operation_command',
            'operation_query'
        )
    );

ALTER TABLE operation_catalog_source_policy_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_source_policy_registry_source_kind_check;

ALTER TABLE operation_catalog_source_policy_registry
    ADD CONSTRAINT operation_catalog_source_policy_registry_source_kind_check
    CHECK (
        source_kind IN (
            'cqrs_command',
            'cqrs_query',
            'operation_command',
            'operation_query'
        )
    );

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
        'operator-roadmap-write',
        'operator.roadmap_write',
        'operation_command',
        'command',
        'POST',
        '/api/operator/roadmap-write',
        'runtime.operations.commands.operator_control.RoadmapWriteCommand',
        'runtime.operations.commands.operator_control.handle_operator_roadmap_write',
        'authority.roadmap_items',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.operator_control_bindings.20260416',
        'decision.operation_catalog_registry.operator_control_bindings.20260416'
    ),
    (
        'operator-work-item-closeout',
        'operator.work_item_closeout',
        'operation_command',
        'command',
        'POST',
        '/api/operator/work-item-closeout',
        'runtime.operations.commands.operator_control.WorkItemCloseoutCommand',
        'runtime.operations.commands.operator_control.handle_work_item_closeout',
        'authority.work_item_closeout',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.operator_control_bindings.20260416',
        'decision.operation_catalog_registry.operator_control_bindings.20260416'
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
