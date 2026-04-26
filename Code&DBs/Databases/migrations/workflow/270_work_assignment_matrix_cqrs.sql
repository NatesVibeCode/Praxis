-- Migration 270: Work assignment matrix CQRS read model.
--
-- The provider catalog answers "what model access method can run?"
-- This read model answers "what tier of model should take each work item?"
-- It promotes the audit grouping/tier fields out of bug resume_context into
-- a legible, queryable matrix for future agents.

BEGIN;

CREATE OR REPLACE VIEW work_item_assignment_matrix AS
SELECT
    'bug'::text AS item_kind,
    bug_id AS item_id,
    bug_key AS item_key,
    title,
    status,
    severity,
    priority,
    category,
    COALESCE(NULLIF(resume_context ->> 'audit_group', ''), 'unassigned') AS audit_group,
    CASE
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'A_%' THEN 10
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'B_%' THEN 20
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'C_%' THEN 30
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'D_%' THEN 40
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'E_%' THEN 50
        WHEN COALESCE(resume_context ->> 'audit_group', '') LIKE 'F_%' THEN 60
        ELSE 999
    END AS group_sort_order,
    COALESCE(
        NULLIF(resume_context ->> 'recommended_model_tier', ''),
        'unclassified'
    ) AS recommended_model_tier,
    CASE
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%frontier%'
        THEN 'frontier'
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%strong%'
        THEN 'strong_coder'
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%mid%'
        THEN 'mid_coder'
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%junior%'
          OR lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%cheap%'
        THEN 'cheap_or_junior'
        ELSE 'unclassified'
    END AS recommended_model_tier_group,
    CASE
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%frontier%'
        THEN 10
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%strong%'
        THEN 20
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%mid%'
        THEN 30
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%junior%'
          OR lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%cheap%'
        THEN 40
        ELSE 999
    END AS recommended_model_tier_rank,
    CASE
        WHEN jsonb_typeof(resume_context -> 'suggested_sequence') = 'number'
        THEN (resume_context ->> 'suggested_sequence')::integer
        WHEN (resume_context ->> 'suggested_sequence') ~ '^[0-9]+$'
        THEN (resume_context ->> 'suggested_sequence')::integer
        ELSE NULL
    END AS suggested_sequence,
    COALESCE(NULLIF(resume_context ->> 'assignment_reason', ''), '') AS assignment_reason,
    COALESCE(
        NULLIF(resume_context ->> 'recommended_task_type', ''),
        NULLIF(resume_context ->> 'task_type', ''),
        lower(category)
    ) AS task_type,
    CASE
        WHEN lower(COALESCE(resume_context ->> 'recommended_model_tier', '')) LIKE '%frontier%'
        THEN false
        WHEN NULLIF(resume_context ->> 'recommended_model_tier', '') IS NULL
        THEN false
        ELSE true
    END AS can_delegate_to_less_than_frontier,
    COALESCE(NULLIF(resume_context ->> 'grouping_source', ''), '') AS grouping_source,
    COALESCE(NULLIF(resume_context ->> 'implementation_status', ''), '') AS implementation_status,
    CASE
        WHEN status IN ('FIXED', 'WONT_FIX', 'DEFERRED') THEN 'closed'
        ELSE 'active'
    END AS visibility_state,
    updated_at,
    'table.bugs.resume_context'::text AS source_ref
FROM bugs
WHERE resume_context ? 'audit_group'
   OR resume_context ? 'recommended_model_tier'
   OR resume_context ? 'suggested_sequence'
   OR resume_context ? 'assignment_reason';

COMMENT ON VIEW work_item_assignment_matrix IS
    'CQRS read model for assigning bugs/work items to model tiers and audit groups. Source truth remains bugs.resume_context; this view makes the matrix legible and queryable.';

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'table:work_item_assignment_matrix',
    'Work assignment matrix',
    'projection',
    'Queryable matrix of work items by audit group, recommended model tier, suggested sequence, assignment reason, task type, and delegation suitability.',
    jsonb_build_object(
        'source', 'migration.270_work_assignment_matrix_cqrs',
        'view', 'work_item_assignment_matrix'
    ),
    jsonb_build_object(
        'projection_ref', 'projection.work_item_assignment_matrix',
        'source_ref', 'table.bugs.resume_context',
        'operation_name', 'operator.work_assignment_matrix'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES (
    'view.public.work_item_assignment_matrix',
    'projection',
    'work_item_assignment_matrix',
    'public',
    'authority.bugs',
    'table:work_item_assignment_matrix',
    'active',
    'read_model',
    'praxis.engine',
    'decision.work_assignment_matrix.20260426',
    jsonb_build_object(
        'projection_ref', 'projection.work_item_assignment_matrix',
        'source_ref', 'table.bugs.resume_context'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    default_value,
    valid_values,
    examples,
    deprecation_notes,
    display_order,
    origin_ref,
    metadata
) VALUES
('table:work_item_assignment_matrix', 'audit_group', 'operator', 'text', 'Audit group', 'Workstream grouping used to batch related bugs and decide execution order.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 80, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'recommended_model_tier', 'operator', 'text', 'Recommended model tier', 'Human-readable assignment tier captured during audit, for example frontier, strong_coder, mid_coder, or cheap_model_or_junior.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 100, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'recommended_model_tier_group', 'operator', 'enum', 'Tier group', 'Normalized model tier bucket for filtering and delegation.', true, NULL, '["frontier","strong_coder","mid_coder","cheap_or_junior","unclassified"]'::jsonb, '[]'::jsonb, '', 110, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'suggested_sequence', 'operator', 'number', 'Suggested sequence', 'Relative order for working through items inside the audit group.', false, NULL, '[]'::jsonb, '[]'::jsonb, '', 120, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'assignment_reason', 'operator', 'text', 'Assignment reason', 'Why this work belongs with the recommended model tier.', false, NULL, '[]'::jsonb, '[]'::jsonb, '', 130, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'task_type', 'operator', 'text', 'Task type', 'Task/work category used for filtering the assignment matrix.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 140, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb),
('table:work_item_assignment_matrix', 'can_delegate_to_less_than_frontier', 'operator', 'boolean', 'Delegable below frontier', 'True when the row is explicitly assigned to strong, mid, junior, or cheap model work instead of frontier-only work.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 150, '{"source":"migration.270_work_assignment_matrix_cqrs"}'::jsonb, '{}'::jsonb)
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    default_value = EXCLUDED.default_value,
    valid_values = EXCLUDED.valid_values,
    examples = EXCLUDED.examples,
    deprecation_notes = EXCLUDED.deprecation_notes,
    display_order = EXCLUDED.display_order,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'operation.operator.work_assignment_matrix',
    'Work assignment matrix query',
    'command',
    'CQRS query that exposes work items grouped by audit group, recommended model tier, task type, suggested sequence, and assignment reason.',
    jsonb_build_object(
        'source', 'migration.270_work_assignment_matrix_cqrs',
        'operation_name', 'operator.work_assignment_matrix',
        'operation_kind', 'query'
    ),
    jsonb_build_object(
        'operation_kind', 'query',
        'authority_domain_ref', 'authority.bugs',
        'projection_ref', 'projection.work_item_assignment_matrix',
        'handler_ref', 'runtime.operations.queries.operator_support.handle_query_work_assignment_matrix'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES (
    'operation.operator.work_assignment_matrix',
    'command',
    'operator.work_assignment_matrix',
    NULL,
    'authority.bugs',
    'operation.operator.work_assignment_matrix',
    'active',
    'read_model',
    'praxis.engine',
    'decision.work_assignment_matrix.20260426',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.queries.operator_support.handle_query_work_assignment_matrix',
        'source_kind', 'operation_query'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

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
    authority_domain_ref,
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref,
    input_schema_ref,
    output_schema_ref,
    storage_target_ref,
    receipt_required,
    event_required,
    projection_freshness_policy_ref
) VALUES (
    'operator-work-assignment-matrix',
    'operator.work_assignment_matrix',
    'operation_query',
    'query',
    'GET',
    '/api/operator/work-assignment-matrix',
    'runtime.operations.queries.operator_support.QueryWorkAssignmentMatrix',
    'runtime.operations.queries.operator_support.handle_query_work_assignment_matrix',
    'authority.bugs',
    'authority.bugs',
    'projection.work_item_assignment_matrix',
    'observe',
    'read_only',
    TRUE,
    'binding.operation_catalog_registry.work_assignment_matrix.20260426',
    'decision.work_assignment_matrix.20260426',
    'runtime.operations.queries.operator_support.QueryWorkAssignmentMatrix',
    'operation.output.default',
    'praxis.primary_postgres',
    TRUE,
    FALSE,
    'projection_freshness.default'
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
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    updated_at = now();

COMMIT;
