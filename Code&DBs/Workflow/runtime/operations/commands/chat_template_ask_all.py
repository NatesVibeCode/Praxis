"""CQRS command: dispatch chat.turn.execute in parallel across N route pins.

Implements the operator-chosen "Ask All" template shape: side-by-side N answers,
no synthesis. Each leg dispatches ``chat.turn.execute`` through the gateway, so
the parent ask_all receipt links to N child chat.turn.execute receipts.

KNOWN LIMITATION (first pass): all legs share the same ``conversation_id`` and
each leg currently re-persists the user message via the underlying
``ChatOrchestrator.send_message``. This produces N copies of the user message
in the conversation thread. The architectural goal (per-leg receipts + events)
is met regardless. Follow-up: split each leg into its own forked conversation
or extract a no-persist LLM-only dispatch helper.
"""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ChatRoutePin(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_slug: str = Field(description="Provider slug, e.g. 'openrouter'.")
    model_slug: str = Field(description="Model slug, e.g. 'moonshotai/kimi-k2.6'.")

    @field_validator("provider_slug", "model_slug", mode="before")
    @classmethod
    def _require_non_empty(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip()

    def to_model_override(self) -> str:
        return f"{self.provider_slug}/{self.model_slug}"


class ExecuteAskAllTemplateCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_id: str = Field(description="Conversation row id the turn belongs to.")
    user_content: str = Field(description="The user's message content.")
    route_pins: list[ChatRoutePin] = Field(
        description="One ChatRoutePin per leg. Min 2 legs (template only makes sense with >1).",
        min_length=2,
        max_length=8,
    )
    selection_context: list[dict[str, Any]] | None = None
    max_tokens: int | None = Field(default=None, gt=0)
    max_workers: int = Field(
        default=4,
        ge=1,
        le=8,
        description="Maximum parallel legs in flight at once.",
    )

    @field_validator("conversation_id", "user_content", mode="before")
    @classmethod
    def _require_non_empty(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip() if isinstance(value, str) and value.strip() == value else value  # type: ignore[return-value]


def _dispatch_one_leg(
    *,
    subsystems: Any,
    conversation_id: str,
    user_content: str,
    selection_context: list[dict[str, Any]] | None,
    max_tokens: int | None,
    pin: ChatRoutePin,
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
    except Exception as exc:  # pragma: no cover - defensive, errors return as leg dicts
        return {
            "ok": False,
            "provider_slug": pin.provider_slug,
            "model_slug": pin.model_slug,
            "error": str(exc),
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    elapsed_ms = int((time.monotonic() - started) * 1000)
    leg: dict[str, Any] = {
        "ok": bool(result.get("ok", True)) if isinstance(result, dict) else True,
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


def handle_execute_ask_all_template(
    command: ExecuteAskAllTemplateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    workers = min(command.max_workers, len(command.route_pins))
    legs: list[dict[str, Any]] = [None] * len(command.route_pins)  # type: ignore[list-item]

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _dispatch_one_leg,
                subsystems=subsystems,
                conversation_id=command.conversation_id,
                user_content=command.user_content,
                selection_context=command.selection_context,
                max_tokens=command.max_tokens,
                pin=pin,
            ): index
            for index, pin in enumerate(command.route_pins)
        }
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            try:
                legs[index] = future.result()
            except Exception as exc:
                pin = command.route_pins[index]
                legs[index] = {
                    "ok": False,
                    "provider_slug": pin.provider_slug,
                    "model_slug": pin.model_slug,
                    "error": str(exc),
                }

    total_ms = int((time.monotonic() - started) * 1000)
    successes = sum(1 for leg in legs if leg.get("ok"))
    return {
        "ok": successes == len(legs),
        "template": "ask_all",
        "conversation_id": command.conversation_id,
        "leg_count": len(legs),
        "successful_leg_count": successes,
        "total_latency_ms": total_ms,
        "legs": legs,
    }


__all__ = [
    "ChatRoutePin",
    "ExecuteAskAllTemplateCommand",
    "handle_execute_ask_all_template",
]
