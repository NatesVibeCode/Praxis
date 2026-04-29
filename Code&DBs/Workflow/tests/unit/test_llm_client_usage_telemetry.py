from __future__ import annotations

from adapters.llm_client import (
    _parse_anthropic_response,
    _parse_google_response,
    _parse_openai_response,
)


def test_openai_usage_parser_normalizes_reasoning_cache_and_audio_tokens() -> None:
    _, usage, _, stop_reason = _parse_openai_response(
        {
            "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 40,
                "total_tokens": 140,
                "prompt_tokens_details": {"cached_tokens": 80, "audio_tokens": 3},
                "completion_tokens_details": {
                    "reasoning_tokens": 25,
                    "audio_tokens": 4,
                    "accepted_prediction_tokens": 2,
                    "rejected_prediction_tokens": 1,
                },
            },
        }
    )

    assert stop_reason == "end_turn"
    assert usage["input_tokens"] == 100
    assert usage["output_tokens"] == 40
    assert usage["reasoning_tokens"] == 25
    assert usage["cache_read_tokens"] == 80
    assert usage["billed_tokens"] == 140
    assert usage["input_audio_tokens"] == 3
    assert usage["output_audio_tokens"] == 4


def test_anthropic_usage_parser_normalizes_cache_and_thinking_tokens() -> None:
    _, usage, _, stop_reason = _parse_anthropic_response(
        {
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 120,
                "output_tokens": 45,
                "cache_creation_input_tokens": 90,
                "cache_read_input_tokens": 30,
                "thinking_tokens": 20,
            },
        }
    )

    assert stop_reason == "end_turn"
    assert usage["prompt_tokens"] == 120
    assert usage["completion_tokens"] == 45
    assert usage["cache_write_tokens"] == 90
    assert usage["cache_read_tokens"] == 30
    assert usage["reasoning_tokens"] == 20


def test_google_usage_parser_normalizes_cache_tool_and_thought_tokens() -> None:
    _, usage, _, stop_reason = _parse_google_response(
        {
            "candidates": [
                {
                    "content": {"parts": [{"text": "ok"}]},
                    "finishReason": "STOP",
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 70,
                "candidatesTokenCount": 20,
                "totalTokenCount": 90,
                "cachedContentTokenCount": 50,
                "toolUsePromptTokenCount": 12,
                "thoughtsTokenCount": 15,
            },
        }
    )

    assert stop_reason == "STOP"
    assert usage["input_tokens"] == 70
    assert usage["output_tokens"] == 20
    assert usage["cache_read_tokens"] == 50
    assert usage["tool_prompt_tokens"] == 12
    assert usage["reasoning_tokens"] == 15
