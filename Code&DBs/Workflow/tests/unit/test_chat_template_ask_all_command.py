"""Unit tests for chat.template.ask_all command handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from runtime.operations.commands.chat_template_ask_all import (
    ChatRoutePin,
    ExecuteAskAllTemplateCommand,
    handle_execute_ask_all_template,
)


class _StubSubsystems:
    pass


def _stub_gateway(responses_by_model: dict[str, dict[str, Any]]) -> Any:
    def _inner(_subs: Any, *, operation_name: str, payload: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        assert operation_name == "chat.turn.execute"
        model_override = payload.get("model_override")
        return responses_by_model.get(model_override, {"ok": True, "content": "?", "model_used": model_override, "latency_ms": 1, "message_id": "x"})
    return _inner


def test_command_validates_min_two_legs() -> None:
    with pytest.raises(ValueError):
        ExecuteAskAllTemplateCommand(
            conversation_id="conv-1",
            user_content="hi",
            route_pins=[ChatRoutePin(provider_slug="p", model_slug="m")],
        )


def test_command_rejects_more_than_eight_legs() -> None:
    pins = [ChatRoutePin(provider_slug=f"p{i}", model_slug=f"m{i}") for i in range(9)]
    with pytest.raises(ValueError):
        ExecuteAskAllTemplateCommand(
            conversation_id="conv-1",
            user_content="hi",
            route_pins=pins,
        )


def test_handler_dispatches_one_chat_turn_execute_per_leg() -> None:
    pins = [
        ChatRoutePin(provider_slug="openrouter", model_slug="canvasshotai/kimi-k2.6"),
        ChatRoutePin(provider_slug="together", model_slug="deepseek-ai/DeepSeek-V4-Pro"),
    ]
    cmd = ExecuteAskAllTemplateCommand(
        conversation_id="conv-1",
        user_content="What is 2+2?",
        route_pins=pins,
    )
    responses = {
        "openrouter/canvasshotai/kimi-k2.6": {"ok": True, "content": "Four.", "model_used": "openrouter/canvasshotai/kimi-k2.6", "latency_ms": 80, "message_id": "msg-1"},
        "together/deepseek-ai/DeepSeek-V4-Pro": {"ok": True, "content": "It is 4.", "model_used": "together/deepseek-ai/DeepSeek-V4-Pro", "latency_ms": 110, "message_id": "msg-2"},
    }
    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_stub_gateway(responses)):
        result = handle_execute_ask_all_template(cmd, _StubSubsystems())

    assert result["ok"] is True
    assert result["template"] == "ask_all"
    assert result["leg_count"] == 2
    assert result["successful_leg_count"] == 2
    contents = sorted(leg["content"] for leg in result["legs"])
    assert contents == ["Four.", "It is 4."]


def test_handler_marks_failed_legs_without_aborting_others() -> None:
    pins = [
        ChatRoutePin(provider_slug="good", model_slug="m"),
        ChatRoutePin(provider_slug="bad", model_slug="m"),
    ]
    cmd = ExecuteAskAllTemplateCommand(
        conversation_id="conv-1",
        user_content="hi",
        route_pins=pins,
    )
    def _stub_with_one_failure(_subs: Any, *, operation_name: str, payload: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        if payload["model_override"] == "bad/m":
            return {"ok": False, "error": "provider unavailable", "content": None, "model_used": None, "latency_ms": 0, "message_id": None}
        return {"ok": True, "content": "ok", "model_used": "good/m", "latency_ms": 5, "message_id": "x"}

    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_stub_with_one_failure):
        result = handle_execute_ask_all_template(cmd, _StubSubsystems())

    assert result["ok"] is False
    assert result["leg_count"] == 2
    assert result["successful_leg_count"] == 1
    bad_legs = [leg for leg in result["legs"] if leg["provider_slug"] == "bad"]
    assert len(bad_legs) == 1
    assert bad_legs[0]["ok"] is False
    assert bad_legs[0]["error"] == "provider unavailable"
