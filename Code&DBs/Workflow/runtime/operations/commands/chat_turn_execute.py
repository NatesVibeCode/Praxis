"""CQRS command: execute one chat turn end-to-end.

Wraps :class:`runtime.chat_orchestrator.ChatOrchestrator` so every chat turn
dispatched through the gateway produces an ``authority_operation_receipts``
row plus a ``chat.turn_completed`` event. The brainstorm called this the
biggest architectural fix; today ``chat_orchestrator._call_llm_with_http_failover``
goes direct-to-HTTP with no receipt.

Scope (first pass):

  - Wraps the non-streaming ``send_message`` path. Streaming
    (``send_message_streaming``) keeps its REST-streaming UX for now and
    can migrate to a separate stream-friendly receipt mechanism later.
  - Authority domain is ``authority.chat_conversations`` (must be filed
    via ``policy/proposed/multimodel-chat/PREVIEWS.md`` before this
    migration applies cleanly).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ExecuteChatTurnCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: str = Field(
        description="Conversation row id the turn belongs to.",
    )
    user_content: str = Field(
        description="The user's message content for this turn.",
    )
    selection_context: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Optional list of selection-context entries (Moon canvas refs, "
            "attached files) to splice into the LLM prompt."
        ),
    )
    model_override: str | None = Field(
        default=None,
        description=(
            "Optional per-turn route pin formatted as '<provider_slug>/<model_slug>'."
            " When omitted, the routing authority resolves auto/chat normally."
        ),
    )
    max_tokens: int | None = Field(
        default=None,
        gt=0,
        description="Optional ceiling on assistant response tokens.",
    )

    @field_validator("conversation_id", "user_content", mode="before")
    @classmethod
    def _require_non_empty(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip() if isinstance(value, str) and value.strip() == value else value  # type: ignore[return-value]

    @field_validator("model_override", mode="before")
    @classmethod
    def _normalize_optional_string(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("model_override must be a string")
        cleaned = value.strip()
        return cleaned or None


def _resolve_repo_root(subsystems: Any) -> str:
    repo_root = getattr(subsystems, "_repo_root", None)
    if repo_root is not None:
        return str(repo_root)
    from runtime.workspace_paths import repo_root as workspace_repo_root
    return str(workspace_repo_root())


def handle_execute_chat_turn(
    command: ExecuteChatTurnCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.chat_orchestrator import ChatOrchestrator

    pg = subsystems.get_pg_conn()
    repo_root = _resolve_repo_root(subsystems)
    chat = ChatOrchestrator(pg, repo_root)
    result = chat.send_message(
        conversation_id=command.conversation_id,
        user_content=command.user_content,
        selection_context=command.selection_context,
        model_override=command.model_override,
        max_tokens=command.max_tokens,
    )

    payload: dict[str, Any] = {
        "ok": "error" not in result,
        "conversation_id": command.conversation_id,
        "message_id": result.get("message_id"),
        "content": result.get("content"),
        "tool_results": result.get("tool_results", []),
        "model_used": result.get("model_used"),
        "latency_ms": result.get("latency_ms"),
    }
    if "error" in result:
        payload["error"] = result["error"]
    return payload


__all__ = [
    "ExecuteChatTurnCommand",
    "handle_execute_chat_turn",
]
