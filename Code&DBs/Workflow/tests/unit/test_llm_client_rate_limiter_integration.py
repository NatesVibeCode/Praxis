from __future__ import annotations

import pytest

import adapters.llm_client as llm_client_mod
from adapters.llm_client import LLMClientError, LLMRequest, call_llm, call_llm_streaming
from adapters.http_transport import HTTPResponse
from runtime.integrations.rate_limiter import (
    ProviderThrottleRegistry,
    set_global_provider_throttle_registry,
)


_SAMPLE_OPENAI_RESPONSE_BODY = (
    b'{"id":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},'
    b'"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1},'
    b'"model":"gpt-5.4-mini"}'
)


def _fake_http_success(**_: object) -> HTTPResponse:
    return HTTPResponse(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body=_SAMPLE_OPENAI_RESPONSE_BODY,
    )


def _build_request() -> LLMRequest:
    return LLMRequest(
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="sk-test",
        provider_slug="openai",
        model_slug="gpt-5.4-mini",
        messages=({"role": "user", "content": "ping"},),
        max_tokens=8,
        temperature=0.0,
        protocol_family="openai_chat_completions",
        timeout_seconds=5,
        retry_attempts=0,
        retry_backoff_seconds=(),
        retryable_status_codes=(),
    )


def _install_registry(tokens_per_second: float, burst_size: int) -> None:
    registry = ProviderThrottleRegistry.from_rows(
        [
            {
                "provider_slug": "openai",
                "tokens_per_second": tokens_per_second,
                "burst_size": burst_size,
            }
        ]
    )
    set_global_provider_throttle_registry(registry)


@pytest.fixture(autouse=True)
def _reset_global_registry():
    # Tests may install a registry; make sure nothing leaks across cases.
    yield
    set_global_provider_throttle_registry(None)


def test_call_llm_passes_through_when_no_global_registry_installed_without_db_bootstrap(
    monkeypatch,
) -> None:
    """With no registry installed, call_llm must not throttle — backward-compatible."""
    set_global_provider_throttle_registry(None)
    monkeypatch.setattr(llm_client_mod, "perform_http_request", _fake_http_success)

    response = call_llm(_build_request())

    assert response.content == "ok"
    assert response.status_code == 200


def test_call_llm_rejects_second_call_when_bucket_is_exhausted_without_db_bootstrap(
    monkeypatch,
) -> None:
    """With 1 tps / burst=1 and a 5ms wait budget, the second call must surface
    llm_client.rate_limited instead of silently waiting a full second."""
    _install_registry(tokens_per_second=1.0, burst_size=1)
    monkeypatch.setattr(llm_client_mod, "perform_http_request", _fake_http_success)
    # Keep the acquire wait budget tiny so the second call fails quickly.
    monkeypatch.setattr(
        llm_client_mod,
        "_throttle_for_provider",
        # Shim the min(timeout, 30) to a fixed 5ms without patching the
        # public API: re-invoke acquire_for_provider directly with a
        # shorter budget so the test is deterministic.
        lambda request, timeout_seconds: _throttle_with_tight_budget(request),
    )

    first = call_llm(_build_request())
    assert first.content == "ok"

    with pytest.raises(LLMClientError) as excinfo:
        call_llm(_build_request())
    assert excinfo.value.reason_code == "llm_client.rate_limited"


def test_call_llm_streaming_respects_rate_limiter_without_db_bootstrap(monkeypatch) -> None:
    """Streaming path must throttle through the same global registry."""
    _install_registry(tokens_per_second=1.0, burst_size=1)
    monkeypatch.setattr(
        llm_client_mod,
        "_throttle_for_provider",
        lambda request, timeout_seconds: _throttle_with_tight_budget(request),
    )

    class _FakeStream:
        status_code = 200
        headers = {"Content-Type": "text/event-stream"}

        def iter_lines(self):  # pragma: no cover - only reached on success
            return iter([b"data: [DONE]\n"])

        def close(self):
            return None

    monkeypatch.setattr(
        llm_client_mod,
        "open_http_stream",
        lambda **_: _FakeStream(),
    )

    # First streaming call drains the bucket (consumes the iterator so the
    # request actually dispatches).
    list(call_llm_streaming(_build_request()))

    with pytest.raises(LLMClientError) as excinfo:
        list(call_llm_streaming(_build_request()))
    assert excinfo.value.reason_code == "llm_client.rate_limited"


def _throttle_with_tight_budget(request: LLMRequest) -> None:
    """Helper used by tests to force a very short acquire budget."""
    from runtime.integrations.rate_limiter import (
        RateLimitAcquireTimeout,
        acquire_for_provider,
    )

    try:
        acquire_for_provider(request.provider_slug, max_wait_seconds=0.005)
    except RateLimitAcquireTimeout as exc:
        raise LLMClientError("llm_client.rate_limited", str(exc)) from exc
