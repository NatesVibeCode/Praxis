-- Migration 397: Rename Compile→Materialize across operation_catalog_registry
-- + authority_object_registry + data_dictionary_objects rows for the 3
-- operations whose handler files were renamed in this same session.
--
-- Renames:
--   compile_preview          → materialize_preview
--   compile_materialize      → materialize_commit       (avoid materialize_materialize)
--   workflow_context_compile → workflow_context_materialize
--
-- Canonical CQRS path per architecture-policy::cqrs_gateway_robust_determinism:
-- use ``register_operation_atomic`` (defined in migration 350) to insert the
-- new operation rows across all three authority tables, then DELETE the old
-- rows. This matches the forge wizard's recommended write_order
-- (praxis_operation_forge → "Register through praxis_register_operation or a
-- numbered workflow migration using register_operation_atomic").
--
-- Historical receipts in authority_operation_receipts retain their original
-- operation_ref strings — receipts are immutable history; the rename only
-- affects future receipts.

BEGIN;

-- compile_preview → materialize_preview ------------------------------------

SELECT register_operation_atomic(
    p_operation_ref            => 'materialize.preview',
    p_operation_name           => 'materialize_preview',
    p_handler_ref              => 'runtime.operations.queries.materialize_preview.handle_materialize_preview',
    p_input_model_ref          => 'runtime.operations.queries.materialize_preview.MaterializePreviewQuery',
    p_authority_domain_ref     => 'authority.workflow_build',
    p_operation_kind           => 'query',
    p_http_method              => 'POST',
    p_http_path                => '/api/materialize/preview',
    p_posture                  => 'observe',
    p_idempotency_policy       => 'read_only',
    p_label                    => 'Materialize preview',
    p_summary                  => 'Preview the materialized graph that the auto lane would produce, without committing.'
);

DELETE FROM operation_catalog_registry  WHERE operation_ref = 'compile.preview';
DELETE FROM authority_object_registry   WHERE object_ref    = 'operation.compile_preview';
DELETE FROM data_dictionary_objects     WHERE object_kind   = 'operation.compile_preview';

-- compile_materialize → materialize_commit ---------------------------------

-- event_type kept as 'compile.materialized' so existing authority_event_contracts
-- + historical authority_events rows with that label stay coherent. Renaming the
-- event_type itself is a separate slice (requires new event_contract row + the
-- event-type rename across publishers/subscribers).
SELECT register_operation_atomic(
    p_operation_ref            => 'materialize.commit',
    p_operation_name           => 'materialize_commit',
    p_handler_ref              => 'runtime.operations.commands.materialize_commit.handle_materialize_commit',
    p_input_model_ref          => 'runtime.operations.commands.materialize_commit.MaterializeCommand',
    p_authority_domain_ref     => 'authority.workflow_build',
    p_operation_kind           => 'command',
    p_http_method              => 'POST',
    p_http_path                => '/api/materialize/commit',
    p_posture                  => 'operate',
    p_idempotency_policy       => 'non_idempotent',
    p_event_type               => 'compile.materialized',
    p_event_required           => TRUE,
    p_execution_lane           => 'interactive',
    p_label                    => 'Materialize commit',
    p_summary                  => 'Commit a materialized graph from prose intent (auto lane). Renamed from compile_materialize.'
);

DELETE FROM operation_catalog_registry  WHERE operation_ref = 'compile.materialize';
DELETE FROM authority_object_registry   WHERE object_ref    = 'operation.compile_materialize';
DELETE FROM data_dictionary_objects     WHERE object_kind   = 'operation.compile_materialize';

-- workflow_context_compile → workflow_context_materialize ------------------

-- event_type kept as 'workflow_context.compiled' for the same reason — operations
-- rename without touching the event-stream layer.
SELECT register_operation_atomic(
    p_operation_ref            => 'workflow-context-materialize',
    p_operation_name           => 'workflow_context_materialize',
    p_handler_ref              => 'runtime.operations.commands.workflow_context.handle_workflow_context_materialize',
    p_input_model_ref          => 'runtime.operations.commands.workflow_context.MaterializeWorkflowContextCommand',
    p_authority_domain_ref     => 'authority.workflow_context',
    p_operation_kind           => 'command',
    p_http_method              => 'POST',
    p_http_path                => '/api/workflow-context/materialize',
    p_posture                  => 'operate',
    p_idempotency_policy       => 'non_idempotent',
    p_event_type               => 'workflow_context.compiled',
    p_event_required           => TRUE,
    p_execution_lane           => 'background',
    p_label                    => 'Materialize workflow context',
    p_summary                  => 'Materialize a workflow_context binding from prose intent. Renamed from workflow_context_compile.'
);

DELETE FROM operation_catalog_registry  WHERE operation_ref = 'workflow-context-compile';
DELETE FROM authority_object_registry   WHERE object_ref    = 'operation.workflow_context_compile';
DELETE FROM data_dictionary_objects     WHERE object_kind   = 'operation.workflow_context_compile';

COMMIT;
