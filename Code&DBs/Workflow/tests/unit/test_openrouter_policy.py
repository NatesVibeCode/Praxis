from __future__ import annotations

import json

import pytest

from adapters import llm_client
from adapters.http_transport import HTTPResponse
from adapters.llm_client import LLMClientError, LLMRequest, call_llm
from runtime.http_transport import call_transport


OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"


def _request(
    *,
    model_slug: str = "moonshotai/kimi-k2.6",
    extra_body: dict | None = None,
) -> LLMRequest:
    return LLMRequest(
        endpoint_uri=OPENROUTER_ENDPOINT,
        api_key="test-key",
        provider_slug="openrouter",
        model_slug=model_slug,
        messages=({"role": "user", "content": "Reply with exactly: ok"},),
        max_tokens=16,
        temperature=0.0,
        extra_body=extra_body,
        protocol_family="openai_chat_completions",
        timeout_seconds=30,
        retry_attempts=0,
    )


def test_openrouter_runtime_requests_are_pinned_to_approved_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_request(
        *,
        request: LLMRequest,
        body_bytes: bytes,
        headers: dict[str, str],
        timeout_seconds: int,
    ) -> HTTPResponse:
        del headers, timeout_seconds
        captured["body"] = json.loads(body_bytes.decode("utf-8"))
        return HTTPResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=json.dumps(
                {
                    "id": "chatcmpl-test",
                    "model": request.model_slug,
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 1,
                        "total_tokens": 4,
                    },
                }
            ).encode("utf-8"),
        )

    monkeypatch.setattr(llm_client, "_perform_http_request", fake_request)

    call_llm(_request())

    provider = captured["body"]["provider"]
    assert provider["order"] == ["parasail/int4"]
    assert provider["allow_fallbacks"] is False
    assert provider["require_parameters"] is True
    assert provider["data_collection"] == "deny"
    assert provider["zdr"] is True
    assert "moonshotai" in provider["ignore"]
    assert "siliconflow" in provider["ignore"]


def test_openrouter_runtime_rejects_blocked_endpoint_override() -> None:
    with pytest.raises(LLMClientError) as exc_info:
        call_llm(_request(extra_body={"provider": {"order": ["moonshotai"]}}))

    assert exc_info.value.reason_code == "openrouter_policy.blocked_provider_endpoint"


def test_openrouter_runtime_fails_closed_for_unapproved_models() -> None:
    with pytest.raises(LLMClientError) as exc_info:
        call_llm(_request(model_slug="unknown/model"))

    assert exc_info.value.reason_code == "openrouter_policy.no_approved_endpoint"


def test_http_transport_preserves_openrouter_provider_slug_for_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_request(
        *,
        request: LLMRequest,
        body_bytes: bytes,
        headers: dict[str, str],
        timeout_seconds: int,
    ) -> HTTPResponse:
        del headers, timeout_seconds
        captured["request_provider_slug"] = request.provider_slug
        captured["body"] = json.loads(body_bytes.decode("utf-8"))
        return HTTPResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=json.dumps(
                {
                    "id": "chatcmpl-test",
                    "model": request.model_slug,
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "ok"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 1,
                        "total_tokens": 4,
                    },
                }
            ).encode("utf-8"),
        )

    monkeypatch.setattr(llm_client, "_perform_http_request", fake_request)

    call_transport(
        "openai_chat_completions",
        "Reply with exactly: ok",
        model="moonshotai/kimi-k2.6",
        max_tokens=16,
        timeout=30,
        api_endpoint=OPENROUTER_ENDPOINT,
        api_key="test-key",
        api_key_env="OPENROUTER_API_KEY",
        provider_slug="openrouter",
    )

    assert captured["request_provider_slug"] == "openrouter"
    assert captured["body"]["provider"]["order"] == ["parasail/int4"]
