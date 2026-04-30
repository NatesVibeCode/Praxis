"""CQRS command: 2-shot review template — primary then critic, both visible.

Implements the operator-chosen "Review" template shape: 2-shot, critique
visible. The critique IS the visible second message. No auto-revision —
the operator decides whether to ask primary to revise next turn.

Dispatches chat.turn.execute twice through the gateway:
  1. Primary leg: user_content goes through pinned primary route.
  2. Critic leg: a synthetic critique-trigger prompt goes through the pinned
     critic route. Because the critic shares the conversation thread, it
     naturally sees the primary's response in chat history.

KNOWN LIMITATION (first pass): the synthetic critique-trigger prompt is
persisted as a user-role message via ``ChatOrchestrator.send_message``. The
chat surface should mark these as template-injected so they don't look like
genuine user input. Follow-up: extract a no-persist LLM-only dispatch helper
or add a ``message_kind`` flag.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from runtime.operations.commands.chat_template_ask_all import ChatRoutePin


_DEFAULT_CRITIC_PROMPT = (
    "[review-template] Critique the previous assistant response. "
    "Identify factual errors, gaps, or stronger alternatives. "
    "Be specific. Do not propose to revise — just surface the critique."
)


class ExecuteReviewTemplateCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: str = Field(description="Conversation row id the turn belongs to.")
    user_content: str = Field(description="The user's message that primary will answer.")
    primary_route: ChatRoutePin = Field(description="Route pin for the primary leg.")
    critic_route: ChatRoutePin = Field(description="Route pin for the critic leg.")
    critic_prompt: str | None = Field(
        default=None,
        description=(
            "Optional override for the synthetic critique-trigger user_content. "
            "Defaults to a generic critique prompt that asks the critic to be "
            "specific about errors, gaps, and stronger alternatives."
        ),
    )
    selection_context: list[dict[str, Any]] | None = None
    max_tokens: int | None = Field(default=None, gt=0)

    @field_validator("conversation_id", "user_content", mode="before")
    @classmethod
    def _require_non_empty(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip() if isinstance(value, str) and value.strip() == value else value  # type: ignore[return-value]

    @field_validator("critic_prompt", mode="before")
    @classmethod
    def _normalize_optional_prompt(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("critic_prompt must be a string")
        cleaned = value.strip()
        return cleaned or None


def _dispatch_leg(
    *,
    subsystems: Any,
    conversation_id: str,
    user_content: str,
    selection_context: list[dict[str, Any]] | None,
    max_tokens: int | None,
    pin: ChatRoutePin,
    role_label: str,
) -> dict[str, Any]:
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    started = time.monotonic()
    try:
        result = execute_operation_from_subsystems(
            subsystems,
            operation_name="chat.turn.execute",
            payload={
                "conversation_id": conversation_id,
                "user_content": user_content,
                "selection_context": selection_context,
                "model_override": pin.to_model_override(),
                "max_tokens": max_tokens,
            },
        )
    except Exception as exc:
        return {
            "ok": False,
            "role": role_label,
            "provider_slug": pin.provider_slug,
            "model_slug": pin.model_slug,
            "error": str(exc),
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    elapsed_ms = int((time.monotonic() - started) * 1000)
    leg: dict[str, Any] = {
        "ok": bool(result.get("ok", True)) if isinstance(result, dict) else True,
        "role": role_label,
        "provider_slug": pin.provider_slug,
        "model_slug": pin.model_slug,
        "latency_ms": elapsed_ms,
    }
    if isinstance(result, dict):
        leg["content"] = result.get("content")
        leg["message_id"] = result.get("message_id")
        leg["model_used"] = result.get("model_used")
        if "error" in result:
            leg["error"] = result["error"]
            leg["ok"] = False
    return leg


def handle_execute_review_template(
    command: ExecuteReviewTemplateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    started = time.monotonic()
    primary = _dispatch_leg(
        subsystems=subsystems,
        conversation_id=command.conversation_id,
        user_content=command.user_content,
        selection_context=command.selection_context,
        max_tokens=command.max_tokens,
        pin=command.primary_route,
        role_label="primary",
    )

    critic_prompt = command.critic_prompt or _DEFAULT_CRITIC_PROMPT
    critic = _dispatch_leg(
        subsystems=subsystems,
        conversation_id=command.conversation_id,
        user_content=critic_prompt,
        selection_context=command.selection_context,
        max_tokens=command.max_tokens,
        pin=command.critic_route,
        role_label="critic",
    )

    total_ms = int((time.monotonic() - started) * 1000)
    return {
        "ok": bool(primary.get("ok") and critic.get("ok")),
        "template": "review",
        "conversation_id": command.conversation_id,
        "primary": primary,
        "critic": critic,
        "total_latency_ms": total_ms,
    }


__all__ = [
    "ExecuteReviewTemplateCommand",
    "handle_execute_review_template",
]
