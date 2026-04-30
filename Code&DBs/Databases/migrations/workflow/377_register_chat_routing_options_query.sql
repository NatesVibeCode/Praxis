-- Migration 377: Register chat.routing_options.list as a CQRS query operation.
--
-- Why:
--   The operator console picker drawer (StrategyConsole) currently hardcodes
--   OPERATOR_CHAT_ENGINE / OPERATOR_CHAT_PROVIDER constants in React. The
--   routing authority (task_type_routing) is the source of truth for which
--   provider/model/transport candidates should be surfaced for chat. This
--   operation exposes a clean read of those rows, filtered to permitted=true
--   by default, sorted by rank then route_health_score, and surfacing
--   transport_type per candidate (per the future CLI-in-chat direction).
--
-- Authority:
--   authority.provider_routing — task_type_routing rows are routing authority,
--   not workflow-run product truth. Using authority.workflow_runs as a parking
--   lot was explicitly called out as a reject path by the CQRS Wizard.
--
-- Pairs with:
--   runtime/operations/queries/chat_routing_options.py — handler + Pydantic input model
--   surfaces/mcp/tools/operator.py — tool_praxis_chat_routing_options_list MCP wrapper
--   tests/unit/test_chat_routing_options_query.py — focused handler test
--
-- Idempotent: register_operation_atomic upserts on operation_ref.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'chat.query.routing_options_list',
    p_operation_name        := 'chat.routing_options.list',
    p_handler_ref           := 'runtime.operations.queries.chat_routing_options.handle_query_chat_routing_options',
    p_input_model_ref       := 'runtime.operations.queries.chat_routing_options.QueryChatRoutingOptions',
    p_authority_domain_ref  := 'authority.provider_routing',
    p_authority_ref         := 'authority.provider_routing',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/chat/routing_options',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::cqrs-gateway-robust-determinism::cqrs-gateway-is-engine-bus',
    p_binding_revision      := 'binding.operation_catalog_registry.chat_routing_options_list.20260430',
    p_label                 := 'Chat Routing Options',
    p_summary               := 'List task_type_routing candidates for a chat task slug, surfacing transport_type, rank, route_health_score, and benchmark_score for the operator console picker drawer. Filters to permitted=true rows by default; pass include_disabled=true to surface disabled candidates with their disable signals.'
);

COMMIT;
