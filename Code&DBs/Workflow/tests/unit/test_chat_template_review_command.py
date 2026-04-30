"""Unit tests for chat.template.review command handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from runtime.operations.commands.chat_template_ask_all import ChatRoutePin
from runtime.operations.commands.chat_template_review import (
    ExecuteReviewTemplateCommand,
    handle_execute_review_template,
)


class _StubSubsystems:
    pass


def _recording_gateway(call_log: list[dict[str, Any]], responses: dict[str, dict[str, Any]]) -> Any:
    def _inner(_subs: Any, *, operation_name: str, payload: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        call_log.append({"operation_name": operation_name, "payload": payload})
        return responses.get(payload.get("model_override"), {"ok": True, "content": "?", "model_used": payload.get("model_override"), "latency_ms": 1, "message_id": "x"})
    return _inner


def test_command_requires_both_routes() -> None:
    with pytest.raises(ValueError):
        ExecuteReviewTemplateCommand(
            conversation_id="conv-1",
            user_content="hi",
            primary_route=ChatRoutePin(provider_slug="p", model_slug="m"),
            critic_route=ChatRoutePin(provider_slug="", model_slug="m"),
        )


def test_handler_dispatches_primary_then_critic_in_order() -> None:
    cmd = ExecuteReviewTemplateCommand(
        conversation_id="conv-7",
        user_content="What is gravity?",
        primary_route=ChatRoutePin(provider_slug="together", model_slug="deepseek-ai/DeepSeek-V4-Pro"),
        critic_route=ChatRoutePin(provider_slug="openrouter", model_slug="moonshotai/kimi-k2.6"),
    )
    responses = {
        "together/deepseek-ai/DeepSeek-V4-Pro": {"ok": True, "content": "Gravity is a force.", "model_used": "together/deepseek-ai/DeepSeek-V4-Pro", "latency_ms": 50, "message_id": "p1"},
        "openrouter/moonshotai/kimi-k2.6": {"ok": True, "content": "Critique: too brief; gravity is a curvature of spacetime.", "model_used": "openrouter/moonshotai/kimi-k2.6", "latency_ms": 70, "message_id": "c1"},
    }
    call_log: list[dict[str, Any]] = []
    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_recording_gateway(call_log, responses)):
        result = handle_execute_review_template(cmd, _StubSubsystems())

    assert result["ok"] is True
    assert result["template"] == "review"
    assert result["primary"]["role"] == "primary"
    assert result["primary"]["content"] == "Gravity is a force."
    assert result["critic"]["role"] == "critic"
    assert "curvature of spacetime" in result["critic"]["content"]
    # Primary must dispatch before critic, and critic must use the synthetic prompt.
    assert call_log[0]["payload"]["model_override"] == "together/deepseek-ai/DeepSeek-V4-Pro"
    assert call_log[0]["payload"]["user_content"] == "What is gravity?"
    assert call_log[1]["payload"]["model_override"] == "openrouter/moonshotai/kimi-k2.6"
    assert "Critique" in call_log[1]["payload"]["user_content"] or "critique" in call_log[1]["payload"]["user_content"]


def test_handler_uses_default_critic_prompt_when_unset() -> None:
    cmd = ExecuteReviewTemplateCommand(
        conversation_id="conv-8",
        user_content="hi",
        primary_route=ChatRoutePin(provider_slug="a", model_slug="b"),
        critic_route=ChatRoutePin(provider_slug="c", model_slug="d"),
    )
    call_log: list[dict[str, Any]] = []
    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_recording_gateway(call_log, {})):
        handle_execute_review_template(cmd, _StubSubsystems())
    critic_user_content = call_log[1]["payload"]["user_content"]
    assert "[review-template]" in critic_user_content
    assert "Critique" in critic_user_content


def test_handler_overrides_critic_prompt_when_provided() -> None:
    cmd = ExecuteReviewTemplateCommand(
        conversation_id="conv-9",
        user_content="hi",
        primary_route=ChatRoutePin(provider_slug="a", model_slug="b"),
        critic_route=ChatRoutePin(provider_slug="c", model_slug="d"),
        critic_prompt="Tear it apart, no mercy.",
    )
    call_log: list[dict[str, Any]] = []
    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_recording_gateway(call_log, {})):
        handle_execute_review_template(cmd, _StubSubsystems())
    assert call_log[1]["payload"]["user_content"] == "Tear it apart, no mercy."


def test_handler_marks_overall_failure_when_either_leg_fails() -> None:
    cmd = ExecuteReviewTemplateCommand(
        conversation_id="conv-10",
        user_content="hi",
        primary_route=ChatRoutePin(provider_slug="a", model_slug="b"),
        critic_route=ChatRoutePin(provider_slug="c", model_slug="d"),
    )
    responses = {
        "a/b": {"ok": True, "content": "primary ok", "model_used": "a/b", "latency_ms": 1, "message_id": "p"},
        "c/d": {"ok": False, "error": "critic timed out", "content": None, "model_used": None, "latency_ms": 0, "message_id": None},
    }
    call_log: list[dict[str, Any]] = []
    with patch("runtime.operation_catalog_gateway.execute_operation_from_subsystems", new=_recording_gateway(call_log, responses)):
        result = handle_execute_review_template(cmd, _StubSubsystems())
    assert result["ok"] is False
    assert result["primary"]["ok"] is True
    assert result["critic"]["ok"] is False
    assert result["critic"]["error"] == "critic timed out"
