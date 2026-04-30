-- Migration 380: Register chat.template.ask_all as a CQRS command operation.
--
-- Why:
--   Implements the operator-chosen "Ask All" template shape: side-by-side N
--   answers, no synthesis. Dispatches chat.turn.execute in parallel for N
--   route pins. Each leg gets its own nested chat.turn.execute receipt; the
--   parent ask_all turn produces a chat.template.completed event linking them.
--
-- Authority:
--   authority.chat_conversations — same boundary as chat.turn.execute.
--
-- DEPENDS ON:
--   PREVIEWS.md items #2, #3 (auth_domain decision + register).
--   Migration 379 (chat.turn.execute), which the legs dispatch.
--
-- Pairs with:
--   runtime/operations/commands/chat_template_ask_all.py — handler + Pydantic
--   tests/unit/test_chat_template_ask_all_command.py — focused handler test
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
            '380_register_chat_template_ask_all_command: authority.chat_conversations '
            'not yet registered. Deferring chat.template.ask_all registration. '
            'Apply PREVIEWS.md items #2 + #3 then re-bootstrap.';
        RETURN;
    END IF;

    PERFORM register_operation_atomic(
        p_operation_ref         := 'chat.command.template_ask_all',
        p_operation_name        := 'chat.template.ask_all',
        p_handler_ref           := 'runtime.operations.commands.chat_template_ask_all.handle_execute_ask_all_template',
        p_input_model_ref       := 'runtime.operations.commands.chat_template_ask_all.ExecuteAskAllTemplateCommand',
        p_authority_domain_ref  := 'authority.chat_conversations',
        p_authority_ref         := 'authority.chat_conversations',
        p_operation_kind        := 'command',
        p_http_method           := 'POST',
        p_http_path             := '/api/chat/template/ask_all',
        p_posture               := 'operate',
        p_idempotency_policy    := 'non_idempotent',
        p_event_required        := TRUE,
        p_event_type            := 'chat.template.completed',
        p_timeout_ms            := 90000,
        p_execution_lane        := 'interactive',
        p_kickoff_required      := FALSE,
        p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
        p_binding_revision      := 'binding.operation_catalog_registry.chat_template_ask_all.20260430',
        p_label                 := 'Chat Ask-All Template',
        p_summary               := 'Multi-participant chat turn template: dispatch chat.turn.execute in parallel for N route pins, return the N answers side-by-side without synthesis. Preserves divergence per the operator decision.'
    );
END $$;

COMMIT;
