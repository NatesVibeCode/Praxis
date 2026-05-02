"""OpenRouter API proof rail tests for control_plane_api dispatch.

Live tests are intentionally opt-in:

    PRAXIS_LIVE_OPENROUTER_PROOF=1 pytest Code&DBs/Workflow/tests/integration/test_openrouter_api_proof.py -q
"""

from __future__ import annotations

import json
import os

import pytest

from adapters import llm_client
from adapters.http_transport import HTTPResponse
from adapters.llm_client import LLMClientError, LLMRequest, call_llm, call_llm_streaming


OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"


def _proof_request(*, api_key: str = "test-key", model: str = "openai/gpt-5.4-nano") -> LLMRequest:
    return LLMRequest(
        endpoint_uri=OPENROUTER_ENDPOINT,
        api_key=api_key,
        provider_slug="openrouter",
        model_slug=model,
        messages=({"role": "user", "content": "Reply with exactly: ok"},),
        max_tokens=16,
        temperature=0.0,
        protocol_family="openai_chat_completions",
        timeout_seconds=30,
        retry_attempts=0,
    )


def test_fixture_openrouter_request_body_and_normalized_response(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_request(*, request: LLMRequest, body_bytes: bytes, headers: dict[str, str], timeout_seconds: int) -> HTTPResponse:
        captured["url"] = request.endpoint_uri
        captured["body"] = json.loads(body_bytes.decode("utf-8"))
        captured["headers"] = headers
        return HTTPResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=json.dumps(
                {
                    "id": "chatcmpl-test",
                    "model": request.model_slug,
                    "choices": [
                        {"message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
                    ],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
                }
            ).encode("utf-8"),
        )

    monkeypatch.setattr(llm_client, "_perform_http_request", fake_request)

    response = call_llm(_proof_request())

    assert captured["url"] == OPENROUTER_ENDPOINT
    assert captured["body"] == {
        "model": "openai/gpt-5.4-nano",
        "messages": [{"role": "user", "content": "Reply with exactly: ok"}],
        "temperature": 0.0,
        "max_tokens": 16,
    }
    assert str((captured["headers"] or {}).get("Authorization")).startswith("Bearer ")
    assert response.provider_slug == "openrouter"
    assert response.model == "openai/gpt-5.4-nano"
    assert response.content == "ok"
    assert response.usage["total_tokens"] == 4


def test_fixture_openrouter_failure_is_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_request(*, request: LLMRequest, body_bytes: bytes, headers: dict[str, str], timeout_seconds: int) -> HTTPResponse:
        return HTTPResponse(
            status_code=401,
            headers={"content-type": "application/json"},
            body=b'{"error":{"message":"invalid key"}}',
        )

    monkeypatch.setattr(llm_client, "_perform_http_request", fake_request)

    with pytest.raises(LLMClientError) as exc_info:
        call_llm(_proof_request(api_key="bad-key"))

    assert exc_info.value.reason_code == "llm_client.http_error"
    assert exc_info.value.status_code == 401
    assert "invalid key" in str(exc_info.value)


def test_fixture_openrouter_streaming_accumulates_sse(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeStream:
        status_code = 200
        headers = {"content-type": "text/event-stream"}

        def iter_lines(self):
            yield b'data: {"choices":[{"delta":{"content":"o"},"finish_reason":null}]}\n'
            yield b'data: {"choices":[{"delta":{"content":"k"},"finish_reason":null}]}\n'
            yield b'data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}\n'

        def close(self) -> None:
            return None

    def fake_stream(*, request: LLMRequest, body_bytes: bytes, headers: dict[str, str], timeout_seconds: int) -> FakeStream:
        body = json.loads(body_bytes.decode("utf-8"))
        assert body["stream"] is True
        assert body["model"] == "openai/gpt-5.4-nano"
        return FakeStream()

    monkeypatch.setattr(llm_client, "_open_streaming_http_request", fake_stream)

    events = list(call_llm_streaming(_proof_request()))
    text = "".join(event["text"] for event in events if event["type"] == "text_delta")
    stops = [event for event in events if event["type"] == "message_stop"]

    assert text == "ok"
    assert stops[-1]["usage"]["total_tokens"] == 4


def _live_key() -> str | None:
    try:
        from adapters.keychain import resolve_secret

        return str(resolve_secret("OPENROUTER_API_KEY", env=dict(os.environ)) or "").strip() or None
    except Exception:
        return str(os.environ.get("OPENROUTER_API_KEY") or "").strip() or None


live_openrouter = pytest.mark.skipif(
    os.environ.get("PRAXIS_LIVE_OPENROUTER_PROOF") != "1",
    reason="set PRAXIS_LIVE_OPENROUTER_PROOF=1 to spend a tiny live OpenRouter proof call",
)


@live_openrouter
def test_live_openrouter_non_streaming_chat_completion() -> None:
    api_key = _live_key()
    if not api_key:
        pytest.skip("OPENROUTER_API_KEY is not configured")
    model = os.environ.get("PRAXIS_OPENROUTER_PROOF_MODEL", "openai/gpt-5.4-nano")

    response = call_llm(_proof_request(api_key=api_key, model=model))

    assert response.content.strip()
    assert response.model
    assert isinstance(response.usage, dict)
    assert response.status_code == 200


@live_openrouter
def test_live_openrouter_streaming_chat_completion() -> None:
    api_key = _live_key()
    if not api_key:
        pytest.skip("OPENROUTER_API_KEY is not configured")
    model = os.environ.get("PRAXIS_OPENROUTER_PROOF_MODEL", "openai/gpt-5.4-nano")

    events = list(call_llm_streaming(_proof_request(api_key=api_key, model=model)))
    text = "".join(event.get("text", "") for event in events if event.get("type") == "text_delta")

    assert text.strip()
    assert any(event.get("type") == "message_stop" for event in events)
