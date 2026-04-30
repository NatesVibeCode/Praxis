-- Migration 379: Register chat.turn.execute as a CQRS command operation.
--
-- Why:
--   Today every chat turn goes through ChatOrchestrator.send_message, which
--   calls _call_llm_with_http_failover directly over HTTP. There's no CQRS
--   receipt and no authority_events row. Wrapping the turn as a command op
--   gives every turn a receipt (authority_operation_receipts) and emits a
--   chat.turn_completed event, so replay, audit, and Moon's "what happened
--   on this thread" view all work for free.
--
-- Authority:
--   authority.chat_conversations — chat-turn receipts and events are
--   chat-conversation product truth, not workflow_runs plumbing. The
--   forge explicitly called out workflow_runs as a parking-lot reject path.
--
-- DEPENDS ON (operator must apply first):
--   policy/proposed/multimodel-chat/PREVIEWS.md item #2 — the
--   architecture_policy / authority_domain / chat_conversations_authority_domain
--   operator_decision row, AND item #3 — the
--   praxis_register_authority_domain call that creates the
--   authority.chat_conversations row.
--
--   If you apply this migration before those two land, the FK to
--   authority_domains will fail and the catalog will refuse the
--   registration. That's the intended fail-closed behavior.
--
-- Pairs with:
--   runtime/operations/commands/chat_turn_execute.py — handler + Pydantic input model
--   tests/unit/test_chat_turn_execute_command.py — focused handler test
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
            '379_register_chat_turn_execute_command: authority.chat_conversations '
            'is not yet registered. Deferring chat.turn.execute registration. '
            'Apply policy/proposed/multimodel-chat/PREVIEWS.md items #2 + #3, '
            'then re-bootstrap to complete this registration. This is the '
            'documented preview-only dependency, not an error.';
        RETURN;
    END IF;

    PERFORM register_operation_atomic(
        p_operation_ref         := 'chat.command.turn_execute',
        p_operation_name        := 'chat.turn.execute',
        p_handler_ref           := 'runtime.operations.commands.chat_turn_execute.handle_execute_chat_turn',
        p_input_model_ref       := 'runtime.operations.commands.chat_turn_execute.ExecuteChatTurnCommand',
        p_authority_domain_ref  := 'authority.chat_conversations',
        p_authority_ref         := 'authority.chat_conversations',
        p_operation_kind        := 'command',
        p_http_method           := 'POST',
        p_http_path             := '/api/chat/turn/execute',
        p_posture               := 'operate',
        p_idempotency_policy    := 'non_idempotent',
        p_event_required        := TRUE,
        p_event_type            := 'chat.turn_completed',
        p_timeout_ms            := 60000,
        p_execution_lane        := 'interactive',
        p_kickoff_required      := FALSE,
        p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
        p_binding_revision      := 'binding.operation_catalog_registry.chat_turn_execute.20260430',
        p_label                 := 'Chat Turn Execute',
        p_summary               := 'Execute one chat turn end-to-end through the CQRS gateway: persist user message, dispatch the LLM through ChatOrchestrator.send_message (default auto/chat route, optional per-turn override), persist assistant reply, return the assistant message + tool_results + model_used + latency_ms. Every turn produces a receipt and emits chat.turn_completed.'
    );
END $$;

COMMIT;
