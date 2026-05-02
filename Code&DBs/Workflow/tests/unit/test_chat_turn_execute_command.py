"""Unit tests for chat.turn.execute command handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from runtime.operations.commands.chat_turn_execute import (
    ExecuteChatTurnCommand,
    handle_execute_chat_turn,
)


class _StubSubsystems:
    def __init__(self, pg: Any = object(), repo_root: str = "/tmp/repo") -> None:
        self._pg = pg
        self._repo_root = repo_root

    def get_pg_conn(self) -> Any:
        return self._pg


class _StubOrchestrator:
    def __init__(self, response: dict[str, Any]):
        self.last_call: dict[str, Any] | None = None
        self._response = response

    def send_message(
        self,
        conversation_id: str,
        user_content: str,
        selection_context: list[dict[str, Any]] | None = None,
        *,
        model_override: Any = None,
        max_tokens: Any = None,
    ) -> dict[str, Any]:
        self.last_call = {
            "conversation_id": conversation_id,
            "user_content": user_content,
            "selection_context": selection_context,
            "model_override": model_override,
            "max_tokens": max_tokens,
        }
        return self._response


def _orchestrator_factory(stub: _StubOrchestrator) -> Any:
    def _ctor(_pg: Any, _repo: str) -> Any:
        return stub
    return _ctor


def test_command_validates_required_fields() -> None:
    with pytest.raises(ValueError):
        ExecuteChatTurnCommand(conversation_id="", user_content="hi")
    with pytest.raises(ValueError):
        ExecuteChatTurnCommand(conversation_id="conv-1", user_content="")


def test_command_normalizes_model_override() -> None:
    cmd = ExecuteChatTurnCommand(
        conversation_id="conv-1",
        user_content="hi",
        model_override="  openrouter/canvasshotai/kimi-k2.6  ",
    )
    assert cmd.model_override == "openrouter/canvasshotai/kimi-k2.6"


def test_command_treats_blank_model_override_as_none() -> None:
    cmd = ExecuteChatTurnCommand(
        conversation_id="conv-1",
        user_content="hi",
        model_override="   ",
    )
    assert cmd.model_override is None


def test_command_rejects_zero_or_negative_max_tokens() -> None:
    with pytest.raises(ValueError):
        ExecuteChatTurnCommand(conversation_id="conv-1", user_content="hi", max_tokens=0)
    with pytest.raises(ValueError):
        ExecuteChatTurnCommand(conversation_id="conv-1", user_content="hi", max_tokens=-1)


def test_handler_returns_assistant_payload() -> None:
    stub = _StubOrchestrator({
        "message_id": "msg-1",
        "content": "Hello!",
        "tool_results": [],
        "model_used": "openrouter/canvasshotai/kimi-k2.6",
        "latency_ms": 124,
    })
    cmd = ExecuteChatTurnCommand(conversation_id="conv-1", user_content="hi")
    with patch("runtime.chat_orchestrator.ChatOrchestrator", side_effect=_orchestrator_factory(stub)):
        result = handle_execute_chat_turn(cmd, _StubSubsystems())
    assert result["ok"] is True
    assert result["message_id"] == "msg-1"
    assert result["content"] == "Hello!"
    assert result["model_used"] == "openrouter/canvasshotai/kimi-k2.6"
    assert result["latency_ms"] == 124


def test_handler_passes_overrides_through_to_orchestrator() -> None:
    stub = _StubOrchestrator({"message_id": "msg-2", "content": "ok", "model_used": "x/y", "latency_ms": 1})
    cmd = ExecuteChatTurnCommand(
        conversation_id="conv-2",
        user_content="hello",
        model_override="together/deepseek-ai/DeepSeek-V4-Pro",
        max_tokens=2048,
        selection_context=[{"type": "canvas_workflow_ref", "ref": "wf-1"}],
    )
    with patch("runtime.chat_orchestrator.ChatOrchestrator", side_effect=_orchestrator_factory(stub)):
        handle_execute_chat_turn(cmd, _StubSubsystems())
    assert stub.last_call is not None
    assert stub.last_call["conversation_id"] == "conv-2"
    assert stub.last_call["model_override"] == "together/deepseek-ai/DeepSeek-V4-Pro"
    assert stub.last_call["max_tokens"] == 2048
    assert stub.last_call["selection_context"] == [{"type": "canvas_workflow_ref", "ref": "wf-1"}]


def test_handler_surfaces_orchestrator_errors() -> None:
    stub = _StubOrchestrator({
        "message_id": None,
        "content": "cost ceiling exceeded",
        "tool_results": [],
        "model_used": None,
        "latency_ms": 0,
        "error": "cost ceiling exceeded",
    })
    cmd = ExecuteChatTurnCommand(conversation_id="conv-3", user_content="hi")
    with patch("runtime.chat_orchestrator.ChatOrchestrator", side_effect=_orchestrator_factory(stub)):
        result = handle_execute_chat_turn(cmd, _StubSubsystems())
    assert result["ok"] is False
    assert result["error"] == "cost ceiling exceeded"
