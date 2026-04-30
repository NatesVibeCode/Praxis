-- Migration 340: Register compile preview as a CQRS query operation.
--
-- The compile authority standing order requires preview and materialize to
-- share the same CQRS front door across MCP, CLI, API, and UI. Materialize
-- already has a command operation; preview was still a direct runtime call.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'compile.preview',
    p_operation_name        := 'compile_preview',
    p_handler_ref           := 'runtime.operations.queries.compile_preview.handle_compile_preview',
    p_input_model_ref       := 'runtime.operations.queries.compile_preview.CompilePreviewQuery',
    p_authority_domain_ref  := 'authority.workflow_build',
    p_authority_ref         := 'authority.workflow_build',
    p_operation_kind        := 'query',
    p_http_method           := 'POST',
    p_http_path             := '/api/compile/preview',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::compile-authority::compile-cqrs-shared-frontdoor',
    p_binding_revision      := 'binding.operation_catalog_registry.compile_preview.20260429',
    p_label                 := 'Operation: compile_preview',
    p_summary               := 'Preview compile/materialize intent without mutation.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, operation_kind, http_path
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'compile.preview';
