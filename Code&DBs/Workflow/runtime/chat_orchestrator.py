"""Chat orchestrator — manages conversation state and LLM interaction.

Each user message triggers:
1. Persist to DB
2. Load conversation history (token-budgeted)
3. Resolve model via task type routing
4. Call LLM with tools
5. Execute any tool calls
6. Loop until LLM produces a final text response
7. Persist assistant message + tool results
"""
from __future__ import annotations

import json
import importlib
import logging
import os
import shlex
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterator

from adapters.llm_client import (
    LLMClientError,
    LLMRequest,
    LLMResponse,
    ToolCall,
    call_llm,
    call_llm_streaming,
)
from runtime.chat_store import ChatStore

_log = logging.getLogger(__name__)

_MAX_TOOL_ITERATIONS = 5
_MAX_HISTORY_TOKENS = 100_000
_CHARS_PER_TOKEN = 4.0
_MAX_CONVERSATION_COST_USD = 10.0
_MAX_VALUABLE_CONTEXT_TOKENS = 1_200
_MAX_VALUABLE_CONTEXT_MESSAGES = 80
_LAST_GOOD_CLI_ROUTE: tuple[str, str] | None = None
_RECENTLY_FAILED_ROUTES: dict[tuple[str, str], float] = {}
_ROUTE_FAILURE_TTL_SECONDS = 300  # skip routes that failed in the last 5 minutes

# Rough cost estimates per 1M tokens (input/output)
_COST_PER_1M = {
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-opus-4-7": (15.0, 75.0),
    "gpt-5.4": (2.5, 10.0),
    "gpt-5.4-mini": (0.4, 1.6),
}
_DEFAULT_COST_PER_1M = (3.0, 15.0)

SYSTEM_PROMPT = """You are an AI operating assistant in a workspace environment. You help users manage their work by pulling data from systems, analyzing it, and routing it into automated workflows.

You have access to tools that can:
- Search the knowledge graph for entities and decisions
- Query recent workflow runs and their status (including job outputs, costs, token usage)
- Run workflows for automated execution
- List available workflow templates
- Check detailed status of workflows (with per-job output previews and costs)
- Get full output of specific completed jobs
- Cancel running workflows
- Run read-only database queries

When the user asks you to do something, use the appropriate tool. Present data clearly. When showing tables, include all relevant columns.
Workflow mutations are command-bus backed. If a tool returns approval_required or queued metadata, report that state instead of pretending the mutation already wrote through.

If the user has selected items (shown in the context), reference them when relevant. If they ask to "route these" or "send these to a workflow", use the run_workflow tool with the selected items.

When showing workflow status, always include cost and token information when available.

Be concise. Take action directly instead of explaining what you could do."""

_CONTEXT_STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "build",
    "chat",
    "could",
    "from",
    "have",
    "help",
    "into",
    "just",
    "make",
    "need",
    "next",
    "please",
    "that",
    "the",
    "then",
    "there",
    "this",
    "what",
    "when",
    "with",
    "work",
    "would",
    "your",
}


def _content_terms(content: str) -> set[str]:
    terms: set[str] = set()
    for raw in content.lower().replace("_", " ").replace("-", " ").split():
        term = "".join(ch for ch in raw if ch.isalnum())
        if len(term) < 4 or term in _CONTEXT_STOPWORDS:
            continue
        terms.add(term)
    return terms


def _context_relevance_score(query_terms: set[str], content: str, title: str) -> float:
    haystack = _content_terms(f"{title} {content}")
    if not query_terms or not haystack:
        return 0.0
    overlap = len(query_terms & haystack)
    if overlap == 0:
        return 0.0
    title_terms = _content_terms(title)
    title_boost = 0.2 if query_terms & title_terms else 0.0
    coverage = overlap / max(len(query_terms), 1)
    density = overlap / max(len(haystack), 1)
    return min(1.0, 0.35 + coverage + density + title_boost)


def _load_chat_tools() -> tuple[list[dict[str, Any]], Any]:
    from runtime.chat_tools import CHAT_TOOLS, execute_tool

    return CHAT_TOOLS, execute_tool


@dataclass(frozen=True, slots=True)
class ResolvedChatRoute:
    provider_slug: str
    model_slug: str
    adapter_type: str
    endpoint_uri: str | None = None
    api_key: str | None = None
    supports_tool_loop: bool = False


def _parse_model_override(model_override: Any) -> tuple[str, str] | None:
    value = str(model_override or "").strip()
    if not value:
        return None
    provider, sep, model = value.partition("/")
    if not sep or not provider.strip() or not model.strip():
        raise RuntimeError("selected chat model must be a provider/model route slug")
    return provider.strip(), model.strip()


class ChatOrchestrator:
    def __init__(self, pg_conn: Any, repo_root: str, chat_store: ChatStore | None = None):
        self._pg = pg_conn
        self._repo_root = repo_root
        self._chat_store = chat_store or ChatStore(pg_conn)

    # ------------------------------------------------------------------
    # Conversation CRUD
    # ------------------------------------------------------------------

    def create_conversation(self, title: str | None = None) -> str:
        return self._chat_store.create_conversation(title)

    def get_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        conv = self._chat_store.get_conversation_summary(conversation_id)
        if not conv:
            return None

        conv = _serialize_row(conv)
        msg_rows = self._chat_store.list_conversation_messages(conversation_id)
        conv["messages"] = [_serialize_message(r) for r in msg_rows]
        return conv

    def list_conversations(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self._chat_store.list_conversations(limit=limit)
        return [_serialize_row(r) for r in rows]

    # ------------------------------------------------------------------
    # Send message (non-streaming)
    # ------------------------------------------------------------------

    def send_message(
        self,
        conversation_id: str,
        user_content: str,
        selection_context: list[dict[str, Any]] | None = None,
        *,
        model_override: Any = None,
    ) -> dict[str, Any]:
        """Send a user message and get a complete response.

        Returns {message_id, content, tool_results, model_used, latency_ms}.
        """
        # Check cost ceiling
        try:
            self._check_cost_ceiling(conversation_id)
        except RuntimeError as exc:
            return {"message_id": None, "content": str(exc), "tool_results": [], "model_used": None, "latency_ms": 0, "error": str(exc)}

        # Persist user message
        user_msg_id = self._persist_message(conversation_id, "user", user_content)

        # Load history
        messages = self._load_history(conversation_id, selection_context)

        routes = self._resolve_route_chain(model_override=model_override)

        if _should_use_cli_fast_path(routes):
            assistant_content, model_used, latency_ms = self._send_via_cli(routes, messages)
            msg_id = self._persist_message(
                conversation_id,
                "assistant",
                assistant_content,
                model_used=model_used,
                latency_ms=latency_ms,
                cost_usd=0.0,
            )
            self._auto_title(conversation_id, user_content)
            self._chat_store.touch_updated_at(conversation_id)
            return {
                "message_id": msg_id,
                "content": assistant_content,
                "tool_results": [],
                "model_used": model_used,
                "latency_ms": latency_ms,
            }

        # Tool loop
        all_tool_results: list[dict] = []
        iteration = 0
        active_http_route: ResolvedChatRoute | None = None

        while iteration < _MAX_TOOL_ITERATIONS:
            iteration += 1
            chat_tools, execute_tool = _load_chat_tools()

            candidate_routes = [active_http_route] if active_http_route is not None else routes
            response, active_http_route = self._call_llm_with_http_failover(
                candidate_routes,
                messages=tuple(messages),
                tools=tuple(chat_tools),
            )
            model_used = f"{response.provider_slug}/{response.model}"

            if response.tool_calls:
                # Execute tool calls
                for tc in response.tool_calls:
                    _log.info("Tool call: %s(%s)", tc.name, json.dumps(tc.input)[:100])
                    result = execute_tool(tc.name, tc.input, self._pg, self._repo_root)
                    all_tool_results.append({"tool_call_id": tc.id, "tool_name": tc.name, "result": result})

                    # Persist tool result
                    self._persist_message(
                        conversation_id, "tool_result", result.get("summary", ""),
                        tool_results=json.dumps(result),
                    )

                    # Add to messages for next LLM call
                    from runtime.http_transport import format_tool_messages
                    from registry.provider_execution_registry import get_profile
                    _prof = get_profile(active_http_route.provider_slug)
                    _proto = _prof.api_protocol_family if _prof else "openai_chat_completions"
                    messages.extend(format_tool_messages(
                        _proto,
                        tool_call_id=tc.id,
                        tool_name=tc.name,
                        tool_input=tc.input,
                        tool_result_content=json.dumps(result.get("summary", "")),
                    ))

                continue  # Loop back to call LLM with tool results

            # No tool calls — final text response
            assistant_content = response.content
            cost = self._estimate_cost(response.usage, response.model or active_http_route.model_slug)
            msg_id = self._persist_message(
                conversation_id, "assistant", assistant_content,
                model_used=model_used, latency_ms=response.latency_ms,
                cost_usd=cost,
            )

            # Auto-title on first response
            self._auto_title(conversation_id, user_content)

            # Update conversation timestamp
            self._chat_store.touch_updated_at(conversation_id)

            return {
                "message_id": msg_id,
                "content": assistant_content,
                "tool_results": all_tool_results,
                "model_used": model_used,
                "latency_ms": response.latency_ms,
            }

        # Max iterations reached
        return {
            "message_id": str(uuid.uuid4()),
            "content": "I reached the maximum number of tool call iterations. Here's what I found so far.",
            "tool_results": all_tool_results,
            "model_used": (
                f"{active_http_route.provider_slug}/{active_http_route.model_slug}"
                if active_http_route is not None
                else None
            ),
            "latency_ms": 0,
        }

    # ------------------------------------------------------------------
    # Send message (streaming via SSE)
    # ------------------------------------------------------------------

    def send_message_streaming(
        self,
        conversation_id: str,
        user_content: str,
        selection_context: list[dict[str, Any]] | None = None,
        *,
        model_override: Any = None,
    ) -> Iterator[dict[str, Any]]:
        """Send a user message and yield streaming events.

        Yields:
            {"event": "text_delta", "data": {"text": "..."}}
            {"event": "tool_call", "data": {"id": "...", "name": "...", "input": {...}}}
            {"event": "tool_result", "data": {structured tool result}}
            {"event": "done", "data": {"message_id": "...", "model_used": "..."}}
            {"event": "error", "data": {"message": "..."}}
        """
        # Persist user message
        self._persist_message(conversation_id, "user", user_content)

        # Load history
        messages = self._load_history(conversation_id, selection_context)

        routes = self._resolve_route_chain(model_override=model_override)

        if _should_use_cli_fast_path(routes):
            assistant_content, model_used, latency_ms = self._send_via_cli(routes, messages)
            msg_id = self._persist_message(
                conversation_id,
                "assistant",
                assistant_content,
                model_used=model_used,
                latency_ms=latency_ms,
                cost_usd=0.0,
            )
            self._chat_store.touch_updated_at(conversation_id)
            yield {"event": "text_delta", "data": {"text": assistant_content}}
            yield {"event": "done", "data": {"message_id": msg_id, "model_used": model_used, "tool_results_count": 0}}
            return

        # Resolve HTTP model lane
        provider, model, endpoint, api_key = self._resolve_model(routes)

        all_tool_results: list[dict] = []
        full_text = ""
        iteration = 0

        while iteration < _MAX_TOOL_ITERATIONS:
            iteration += 1
            chat_tools, execute_tool = _load_chat_tools()

            request = LLMRequest(
                endpoint_uri=endpoint,
                api_key=api_key,
                provider_slug=provider,
                model_slug=model,
                messages=tuple(messages),
                tools=tuple(chat_tools),
                system_prompt=SYSTEM_PROMPT,
                max_tokens=4096,
                temperature=0.2,
            )

            collected_text = ""
            collected_tool_calls: list[dict] = []
            stop_reason = None

            for event in call_llm_streaming(request):
                if event["type"] == "text_delta":
                    collected_text += event["text"]
                    yield {"event": "text_delta", "data": {"text": event["text"]}}

                elif event["type"] == "tool_call_start":
                    yield {"event": "tool_call_start", "data": {"id": event["id"], "name": event["name"]}}

                elif event["type"] == "tool_call_end":
                    collected_tool_calls.append({"id": event["id"], "name": event["name"], "input": event["input"]})

                    # Execute tool immediately
                    result = execute_tool(event["name"], event["input"], self._pg, self._repo_root)
                    all_tool_results.append({"tool_call_id": event["id"], "tool_name": event["name"], "result": result})

                    # Persist and yield
                    self._persist_message(
                        conversation_id, "tool_result", result.get("summary", ""),
                        tool_results=json.dumps(result),
                    )
                    yield {"event": "tool_result", "data": result}

                elif event["type"] == "message_stop":
                    stop_reason = event.get("stop_reason")

                elif event["type"] == "error":
                    yield {"event": "error", "data": {"message": event["message"]}}
                    return

            # If tool calls were made, add to messages and loop
            if collected_tool_calls:
                for tc in collected_tool_calls:
                    tr = next((r for r in all_tool_results if r["tool_call_id"] == tc["id"]), None)
                    summary = tr["result"].get("summary", "") if tr else ""

                    from runtime.http_transport import format_tool_messages
                    from registry.provider_execution_registry import get_profile
                    _prof = get_profile(provider)
                    _proto = _prof.api_protocol_family if _prof else "openai_chat_completions"
                    messages.extend(format_tool_messages(
                        _proto,
                        tool_call_id=tc["id"],
                        tool_name=tc["name"],
                        tool_input=tc["input"],
                        tool_result_content=json.dumps(summary),
                    ))

                continue  # Next iteration with tool results

            # No tool calls — done
            full_text = collected_text
            break

        # Persist final assistant message
        msg_id = self._persist_message(
            conversation_id, "assistant", full_text,
            model_used=f"{provider}/{model}",
        )

        self._chat_store.touch_updated_at(conversation_id)

        yield {"event": "done", "data": {
            "message_id": msg_id,
            "model_used": f"{provider}/{model}",
            "tool_results_count": len(all_tool_results),
        }}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _persist_message(
        self,
        conversation_id: str,
        role: str,
        content: str,
        *,
        tool_calls: str | None = None,
        tool_results: str | None = None,
        model_used: str | None = None,
        latency_ms: int | None = None,
        cost_usd: float | None = None,
    ) -> str:
        return self._chat_store.append_message(
            conversation_id=conversation_id,
            role=role,
            content=content,
            tool_calls=tool_calls,
            tool_results=tool_results,
            model_used=model_used,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
        )

    def _estimate_cost(self, usage: dict[str, int], model: str) -> float:
        """Estimate cost in USD from token usage."""
        rates = _COST_PER_1M.get(model, _DEFAULT_COST_PER_1M)
        input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
        output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))
        return (input_tokens * rates[0] + output_tokens * rates[1]) / 1_000_000

    def _check_cost_ceiling(self, conversation_id: str) -> float:
        """Return cumulative cost. Raises if over ceiling."""
        total = self._chat_store.get_conversation_cost(conversation_id)
        if total >= _MAX_CONVERSATION_COST_USD:
            raise RuntimeError(f"Conversation cost ceiling reached (${total:.2f} / ${_MAX_CONVERSATION_COST_USD:.2f}). Start a new conversation.")
        return total

    def _auto_title(self, conversation_id: str, user_content: str) -> None:
        """Auto-title conversation from first user message if still 'New conversation'."""
        title = self._chat_store.get_title(conversation_id)
        if title not in ("New conversation", None, ""):
            return

        # Generate title: first meaningful words, max 40 chars
        words = user_content.strip().split()
        filler = {"i", "the", "a", "an", "to", "me", "my", "show", "please", "can", "you", "want"}
        meaningful = [w for w in words if w.lower() not in filler][:7]
        title = " ".join(meaningful or words[:5])
        if len(title) > 40:
            title = title[:37] + "..."
        title = title[0].upper() + title[1:] if title else "Conversation"

        self._chat_store.update_title(conversation_id, title)

    def _load_history(
        self,
        conversation_id: str,
        selection_context: list[dict] | None = None,
    ) -> list[dict[str, Any]]:
        """Load conversation messages, token-budgeted."""
        rows = self._chat_store.list_conversation_messages(conversation_id)

        messages: list[dict[str, Any]] = []
        total_chars = 0

        for r in rows:
            role = r["role"]
            content = r["content"] or ""

            if role == "tool_result":
                # For history, just include the summary (not full data)
                content = content[:200]

            total_chars += len(content)
            if total_chars > _MAX_HISTORY_TOKENS * _CHARS_PER_TOKEN:
                break

            if role in ("user", "assistant"):
                messages.append({"role": role, "content": content})

        # Append selection context to the last user message
        if selection_context and messages:
            ctx_text = f"\n\n[User has {len(selection_context)} items selected:\n"
            for item in selection_context[:10]:
                ctx_text += f"  - {json.dumps(item)[:200]}\n"
            if len(selection_context) > 10:
                ctx_text += f"  ... and {len(selection_context) - 10} more]\n"
            messages[-1]["content"] += ctx_text

        if messages:
            valuable_context = self._load_valuable_context(
                conversation_id=conversation_id,
                user_content=str(messages[-1].get("content") or ""),
            )
            if valuable_context:
                messages[-1]["content"] += valuable_context

        return messages

    def _load_valuable_context(self, *, conversation_id: str, user_content: str) -> str:
        """Pack relevant prior chat messages into a bounded prompt appendix.

        This is intentionally deterministic and DB-backed. It does not create a
        second memory authority; it narrows existing persisted chat history into
        a small context packet that can later be replaced by the memory graph.
        """
        query_terms = _content_terms(user_content)
        if not query_terms:
            return ""

        try:
            from memory.packer import ContextPacker, ContextSection, estimate_tokens
        except Exception as exc:
            _log.debug("Skipping valuable chat context: memory packer unavailable: %s", exc)
            return ""

        try:
            rows = self._chat_store.list_recent_context_messages(
                exclude_conversation_id=conversation_id,
                limit=_MAX_VALUABLE_CONTEXT_MESSAGES,
            )
        except Exception as exc:
            _log.debug("Skipping valuable chat context: context query failed: %s", exc)
            return ""

        sections: list[ContextSection] = []
        for row in rows:
            content = str(row.get("content") or "").strip()
            if not content:
                continue
            score = _context_relevance_score(query_terms, content, str(row.get("title") or ""))
            if score <= 0:
                continue
            title = str(row.get("title") or "Previous conversation")
            role = str(row.get("role") or "message")
            clipped = content[:1_000]
            sections.append(
                ContextSection(
                    name=f"Prior chat: {title}",
                    content=f"{title} ({role}): {clipped}",
                    priority=score,
                    token_estimate=estimate_tokens(clipped) + 12,
                    source=f"conversation:{row.get('conversation_id')}:{row.get('id')}",
                )
            )

        if not sections:
            return ""

        packed = ContextPacker(token_budget=_MAX_VALUABLE_CONTEXT_TOKENS).pack(sections)
        if not packed.sections:
            return ""

        lines = [
            "",
            "",
            "[Relevant prior Praxis chat context selected from persisted conversations:",
        ]
        for section in packed.sections[:8]:
            lines.append(f"- {section.content}")
        if packed.dropped_sections:
            lines.append(f"- Omitted {len(packed.dropped_sections)} lower-priority prior context sections.")
        lines.append("Use this only when it is directly relevant; newer instructions in this conversation override older chat context.]")
        return "\n".join(lines)

    def _resolve_route_chain(self, *, model_override: Any = None) -> list[ResolvedChatRoute]:
        """Resolve chat failover routes and filter to currently usable lanes."""
        from registry.provider_execution_registry import resolve_binary
        from runtime.lane_policy import admit_adapter_type, load_provider_lane_policies
        router_mod = importlib.import_module(f"{__package__}.task_type_router")
        TaskTypeRouter = router_mod.TaskTypeRouter

        router = TaskTypeRouter(self._pg)
        raw_decisions = router.resolve_failover_chain("auto/chat")
        if not isinstance(raw_decisions, list):
            raise RuntimeError(
                "task type routing must return a failover chain list for auto/chat"
            )
        if not raw_decisions:
            raise RuntimeError("task type routing returned no decisions for auto/chat")

        lane_policies = load_provider_lane_policies(self._pg)
        if not lane_policies:
            raise RuntimeError(
                "provider lane policy authority returned no active rows for auto/chat"
            )
        now = time.monotonic()
        routes: list[ResolvedChatRoute] = []
        rejections: list[str] = []
        for decision in raw_decisions:
            provider = str(decision.provider_slug)
            model = str(decision.model_slug)
            route_key = (provider, model)

            # Skip routes that failed recently
            failed_at = _RECENTLY_FAILED_ROUTES.get(route_key)
            if failed_at is not None and (now - failed_at) < _ROUTE_FAILURE_TTL_SECONDS:
                continue

            # Fail-closed on missing adapter_type: the router is expected to
            # populate this. A None/empty value means the route is malformed,
            # not a license to silently pick the paid API.
            raw_adapter_type = getattr(decision, "adapter_type", None)
            adapter_type = str(raw_adapter_type or "").strip().lower()
            if not adapter_type:
                _log.warning(
                    "Skipping route %s/%s: decision has no adapter_type (fail-closed)",
                    provider, model,
                )
                rejections.append(f"{provider}/{model}:no_adapter_type")
                continue

            decision_pressure = str(getattr(decision, "spend_pressure", "") or "") or None
            decision_budget_unreachable = bool(
                getattr(decision, "budget_authority_unreachable", False)
            )
            decision_budget_window_data_quality_error = bool(
                getattr(decision, "budget_window_data_quality_error", False)
            )
            # BUG-2A950857: RouteEconomics.allow_payg_fallback is now consulted
            # at admission. ``None`` for decisions that don't carry the attr
            # (older paths / tests) keeps admission permissive.
            raw_allow_payg_fallback = getattr(decision, "allow_payg_fallback", None)
            decision_allow_payg_fallback = (
                bool(raw_allow_payg_fallback)
                if raw_allow_payg_fallback is not None
                else None
            )
            admitted, reason = admit_adapter_type(
                lane_policies,
                provider,
                adapter_type,
                spend_pressure=decision_pressure,
                budget_authority_unreachable=decision_budget_unreachable,
                budget_window_data_quality_error=decision_budget_window_data_quality_error,
                allow_payg_fallback=decision_allow_payg_fallback,
            )
            if not admitted:
                _log.info(
                    "Lane policy rejected %s/%s adapter=%s reason=%s",
                    provider, model, adapter_type, reason,
                )
                rejections.append(f"{provider}/{model}:{reason}")
                continue

            if adapter_type == "cli_llm":
                # Skip CLI routes whose binary isn't installed
                if resolve_binary(provider) is None:
                    _log.debug("Skipping CLI route %s/%s: binary not on PATH", provider, model)
                    rejections.append(f"{provider}/{model}:cli_binary_missing")
                    continue
                routes.append(
                    ResolvedChatRoute(
                        provider_slug=provider,
                        model_slug=model,
                        adapter_type=adapter_type,
                        supports_tool_loop=False,
                    )
                )
                continue

            endpoint = _resolve_http_endpoint(provider, model)
            if endpoint is None:
                rejections.append(f"{provider}/{model}:no_registered_endpoint")
                continue
            api_key = _resolve_api_key(provider, required=False)
            if not api_key:
                rejections.append(f"{provider}/{model}:no_api_key")
                continue
            routes.append(
                ResolvedChatRoute(
                    provider_slug=provider,
                    model_slug=model,
                    adapter_type=adapter_type,
                    endpoint_uri=endpoint,
                    api_key=api_key,
                    supports_tool_loop=True,
                )
            )

        selected_route = _parse_model_override(model_override)
        if selected_route is not None:
            routes = [
                route
                for route in routes
                if (route.provider_slug, route.model_slug) == selected_route
            ]
            if not routes:
                provider, model = selected_route
                raise RuntimeError(
                    f"selected chat model {provider}/{model} is not currently usable for auto/chat"
                )
            return routes

        routes = _prioritize_last_good_cli_route(routes)
        if not routes:
            detail = "; ".join(rejections) if rejections else "no candidates"
            raise RuntimeError(
                f"no usable chat routes available for auto/chat (lane-gated): {detail}"
            )
        return routes

    def _resolve_model(
        self,
        routes: list[ResolvedChatRoute] | None = None,
    ) -> tuple[str, str, str, str]:
        """Resolve the first HTTP-capable chat model route."""
        route = self._resolve_http_routes(routes)[0]
        return (
            route.provider_slug,
            route.model_slug,
            route.endpoint_uri or "",
            route.api_key or "",
        )

    def _resolve_http_routes(
        self,
        routes: list[ResolvedChatRoute] | None = None,
    ) -> list[ResolvedChatRoute]:
        """Return HTTP-capable routes from the resolved failover chain."""
        route_chain = routes if routes is not None else self._resolve_route_chain()
        http_routes = [
            route
            for route in route_chain
            if route.supports_tool_loop and route.endpoint_uri and route.api_key
        ]
        if not http_routes:
            raise RuntimeError("no HTTP-backed chat route available for auto/chat")
        return http_routes

    def _call_llm_with_http_failover(
        self,
        routes: list[ResolvedChatRoute],
        *,
        messages: tuple[dict[str, Any], ...],
        tools: tuple[dict[str, Any], ...],
    ) -> tuple[LLMResponse, ResolvedChatRoute]:
        """Call chat over HTTP, trying each routed candidate before failing."""
        http_routes = self._resolve_http_routes(routes)
        failures: list[str] = []
        last_error: BaseException | None = None
        for route in http_routes:
            request = LLMRequest(
                endpoint_uri=route.endpoint_uri or "",
                api_key=route.api_key or "",
                provider_slug=route.provider_slug,
                model_slug=route.model_slug,
                messages=messages,
                tools=tools,
                system_prompt=SYSTEM_PROMPT,
                max_tokens=4096,
                temperature=0.2,
            )
            try:
                response = call_llm(request)
            except (LLMClientError, RuntimeError) as exc:
                last_error = exc
                _RECENTLY_FAILED_ROUTES[(route.provider_slug, route.model_slug)] = time.monotonic()
                reason_code = str(getattr(exc, "reason_code", type(exc).__name__))
                failures.append(f"{route.provider_slug}/{route.model_slug}:{reason_code}")
                _log.warning(
                    "chat http route failed for %s/%s: %s",
                    route.provider_slug,
                    route.model_slug,
                    exc,
                )
                continue
            _RECENTLY_FAILED_ROUTES.pop((route.provider_slug, route.model_slug), None)
            return response, route

        detail = "; ".join(failures) if failures else "no HTTP candidates"
        raise RuntimeError(
            f"all HTTP-backed chat routes failed for auto/chat: {detail}"
        ) from last_error

    def _send_via_cli(
        self,
        routes: list[ResolvedChatRoute],
        messages: list[dict[str, Any]],
    ) -> tuple[str, str, int]:
        last_error: RuntimeError | None = None
        for route in routes:
            if route.adapter_type != "cli_llm":
                continue
            try:
                content, latency_ms = _run_cli_chat_route(route, messages, repo_root=self._repo_root)
                _record_last_good_cli_route(route)
                _RECENTLY_FAILED_ROUTES.pop((route.provider_slug, route.model_slug), None)
                return content, f"{route.provider_slug}/{route.model_slug}", latency_ms
            except RuntimeError as exc:
                last_error = exc
                _RECENTLY_FAILED_ROUTES[(route.provider_slug, route.model_slug)] = time.monotonic()
                _log.warning(
                    "chat cli route failed for %s/%s: %s",
                    route.provider_slug,
                    route.model_slug,
                    exc,
                )
        if last_error is not None:
            raise last_error
        raise RuntimeError("no CLI-backed chat route available for auto/chat")


def _resolve_api_key(provider: str, *, required: bool = True) -> str | None:
    """Resolve a provider API key via the Keychain-first resolver."""
    from adapters.keychain import resolve_secret
    from registry.provider_execution_registry import resolve_api_key_env_vars

    env = dict(os.environ)
    for env_var in resolve_api_key_env_vars(provider):
        value = resolve_secret(env_var, env=env)
        if value:
            return value.strip() or None
    if required:
        raise RuntimeError(f"no configured API key env var is set for provider {provider!r}")
    return None


def _resolve_http_endpoint(provider: str, model: str | None = None) -> str | None:
    """Resolve a provider HTTP endpoint from the provider_cli_profiles registry.

    Returns None when the registry has no endpoint for the provider; callers
    surface that as a route rejection rather than silently falling back to a
    hardcoded URL.
    """

    from registry.provider_execution_registry import resolve_api_endpoint

    try:
        endpoint = resolve_api_endpoint(provider, model)
    except Exception as exc:  # registry load failures must not crash chat routing
        _log.warning(
            "registry endpoint lookup failed for %s/%s: %s",
            provider,
            model,
            exc,
        )
        return None
    value = str(endpoint or "").strip()
    return value or None


def _prioritize_last_good_cli_route(routes: list[ResolvedChatRoute]) -> list[ResolvedChatRoute]:
    if _LAST_GOOD_CLI_ROUTE is None:
        return routes
    preferred_key = _LAST_GOOD_CLI_ROUTE
    preferred = [route for route in routes if (route.provider_slug, route.model_slug) == preferred_key]
    if not preferred:
        return routes
    remainder = [route for route in routes if (route.provider_slug, route.model_slug) != preferred_key]
    return preferred + remainder


def _should_use_cli_fast_path(routes: list[ResolvedChatRoute]) -> bool:
    if not routes or routes[0].adapter_type != "cli_llm":
        return False
    return not any(route.supports_tool_loop for route in routes)


def _record_last_good_cli_route(route: ResolvedChatRoute) -> None:
    global _LAST_GOOD_CLI_ROUTE
    _LAST_GOOD_CLI_ROUTE = (route.provider_slug, route.model_slug)


def _render_cli_chat_prompt(messages: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for message in messages:
        role = str(message.get("role") or "user").upper()
        content = message.get("content", "")
        if isinstance(content, list):
            content = json.dumps(content, default=str)
        lines.append(f"{role}:\n{str(content).strip()}")
    lines.append("ASSISTANT:")
    return "\n\n".join(lines).strip()


def _cli_json_records(stdout: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    raw = (stdout or "").strip()
    if not raw:
        return records
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        return [parsed]
    for line in raw.splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            parsed_line = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed_line, dict):
            records.append(parsed_line)
    return records


def _extract_cli_chat_text(stdout: str) -> str:
    records = _cli_json_records(stdout)
    last_text = ""
    for record in records:
        item = record.get("item")
        if isinstance(item, dict) and record.get("type") == "item.completed":
            if item.get("type") == "agent_message" and isinstance(item.get("text"), str):
                last_text = item["text"].strip()
                continue
        for key in ("result", "message", "content", "text", "output", "response"):
            value = record.get(key)
            if isinstance(value, str) and value.strip() and not bool(record.get("is_error")):
                last_text = value.strip()
    if last_text:
        return last_text
    return (stdout or "").strip()


def _extract_cli_error(stdout: str, stderr: str) -> str:
    records = _cli_json_records(stdout)
    for record in records:
        if not bool(record.get("is_error")) and record.get("type") != "error":
            continue
        for key in ("result", "message", "content", "text", "error"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if stderr.strip():
        return stderr.strip()
    return (stdout or "").strip() or "CLI chat invocation failed"


def _run_cli_chat_route(
    route: ResolvedChatRoute,
    messages: list[dict[str, Any]],
    *,
    repo_root: str,
) -> tuple[str, int]:
    from adapters.docker_runner import run_on_host
    from registry.provider_execution_registry import build_command, get_profile

    profile = get_profile(route.provider_slug)
    if profile is None:
        raise RuntimeError(f"provider execution registry has no profile for {route.provider_slug!r}")

    prompt = _render_cli_chat_prompt(messages)
    cmd_parts = build_command(
        route.provider_slug,
        route.model_slug,
        system_prompt=SYSTEM_PROMPT,
    )
    stdin_text = prompt
    prompt_mode = str(profile.prompt_mode or "stdin").strip().lower() or "stdin"
    if prompt_mode == "argv":
        cmd_parts.append(prompt)
        stdin_text = ""

    result = run_on_host(
        command=shlex.join(cmd_parts),
        stdin_text=stdin_text,
        timeout=int(profile.default_timeout or 300),
        workdir=repo_root,
    )
    if result.exit_code != 0:
        raise RuntimeError(_extract_cli_error(result.stdout, result.stderr))

    content = _extract_cli_chat_text(result.stdout)
    if not content:
        raise RuntimeError("CLI chat invocation returned no assistant content")
    return content, result.latency_ms


def _serialize_message(row: Any) -> dict[str, Any]:
    d = dict(row)
    for k in ("tool_calls", "tool_results"):
        if d.get(k) and isinstance(d[k], str):
            try:
                d[k] = json.loads(d[k])
            except json.JSONDecodeError:
                pass
    for k in ("created_at",):
        if d.get(k):
            d[k] = str(d[k])
    return d


def _serialize_row(row: Any) -> dict[str, Any]:
    d = dict(row)
    for k in ("created_at", "updated_at"):
        if d.get(k):
            d[k] = str(d[k])
    return d
