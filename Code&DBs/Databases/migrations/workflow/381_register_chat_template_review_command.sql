-- Migration 381: Register chat.template.review as a CQRS command operation.
--
-- Why:
--   Implements the operator-chosen "Review" template shape: 2-shot, critique
--   visible. Dispatches chat.turn.execute for primary, then chat.turn.execute
--   for critic with a synthetic critique-trigger prompt. Both responses are
--   visible — no auto-revision — per the operator decision.
--
-- Authority:
--   authority.chat_conversations — same boundary as chat.turn.execute.
--
-- DEPENDS ON:
--   PREVIEWS.md items #2, #3 (auth_domain decision + register).
--   Migration 379 (chat.turn.execute), which both legs dispatch.
--
-- Pairs with:
--   runtime/operations/commands/chat_template_review.py — handler + Pydantic
--   tests/unit/test_chat_template_review_command.py — focused handler test
--
-- Idempotent: register_operation_atomic upserts on operation_ref.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM authority_domains
         WHERE authority_domain_ref = 'authority.chat_conversations'
    ) THEN
        RAISE NOTICE
            '381_register_chat_template_review_command: authority.chat_conversations '
            'not yet registered. Deferring chat.template.review registration. '
            'Apply PREVIEWS.md items #2 + #3 then re-bootstrap.';
        RETURN;
    END IF;

    PERFORM register_operation_atomic(
        p_operation_ref         := 'chat.command.template_review',
        p_operation_name        := 'chat.template.review',
        p_handler_ref           := 'runtime.operations.commands.chat_template_review.handle_execute_review_template',
        p_input_model_ref       := 'runtime.operations.commands.chat_template_review.ExecuteReviewTemplateCommand',
        p_authority_domain_ref  := 'authority.chat_conversations',
        p_authority_ref         := 'authority.chat_conversations',
        p_operation_kind        := 'command',
        p_http_method           := 'POST',
        p_http_path             := '/api/chat/template/review',
        p_posture               := 'operate',
        p_idempotency_policy    := 'non_idempotent',
        p_event_required        := TRUE,
        p_event_type            := 'chat.template.completed',
        p_timeout_ms            := 120000,
        p_execution_lane        := 'interactive',
        p_kickoff_required      := FALSE,
        p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
        p_binding_revision      := 'binding.operation_catalog_registry.chat_template_review.20260430',
        p_label                 := 'Chat Review Template',
        p_summary               := '2-shot chat turn template: dispatch chat.turn.execute for primary, then dispatch chat.turn.execute for critic to critique primary. Both responses are visible per the operator decision (no auto-revision).'
    );
END $$;

COMMIT;
