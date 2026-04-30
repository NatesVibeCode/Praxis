"""Unit tests for the provider rate-limit gateway."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime.provider_rate_limiter import (
    ProviderRateLimit,
    ProviderRateLimitTimeout,
    ProviderRateLimiter,
    UnknownProviderRateLimit,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _limiter_with(policy: ProviderRateLimit) -> ProviderRateLimiter:
    return ProviderRateLimiter(limits={policy.provider_slug: policy})


# ---------------------------------------------------------------------------
# Acquire / release happy path
# ---------------------------------------------------------------------------

def test_acquire_sync_returns_slot_when_capacity_available():
    limiter = _limiter_with(
        ProviderRateLimit("anthropic", tokens_per_second=10, bucket_capacity=5, max_in_flight=2)
    )
    slot = limiter.acquire_sync("anthropic", timeout=1.0)
    assert slot.provider_slug == "anthropic"
    snap = limiter.snapshot()["anthropic"]
    assert snap["in_flight"] == 1
    assert snap["tokens"] == pytest.approx(4.0, rel=0.01)


def test_release_returns_concurrency_slot():
    limiter = _limiter_with(
        ProviderRateLimit("openai", tokens_per_second=10, bucket_capacity=5, max_in_flight=1)
    )
    slot = limiter.acquire_sync("openai", timeout=0.5)
    assert limiter.snapshot()["openai"]["in_flight"] == 1
    limiter.release(slot)
    assert limiter.snapshot()["openai"]["in_flight"] == 0


# ---------------------------------------------------------------------------
# Concurrency cap
# ---------------------------------------------------------------------------

def test_concurrency_cap_blocks_when_full():
    """When max_in_flight is hit, the next acquire must time out instead
    of being admitted (the bucket has tokens; the cap is the gate)."""

    limiter = _limiter_with(
        ProviderRateLimit("openrouter", tokens_per_second=100, bucket_capacity=100, max_in_flight=1)
    )
    held = limiter.acquire_sync("openrouter", timeout=0.5)
    with pytest.raises(ProviderRateLimitTimeout) as exc:
        limiter.acquire_sync("openrouter", timeout=0.1)
    assert exc.value.in_flight == 1
    assert exc.value.provider_slug == "openrouter"
    limiter.release(held)


def test_release_wakes_waiter():
    """A waiter blocked on the concurrency cap acquires immediately when
    the holder releases."""

    limiter = _limiter_with(
        ProviderRateLimit("together", tokens_per_second=100, bucket_capacity=100, max_in_flight=1)
    )
    held = limiter.acquire_sync("together", timeout=0.5)

    import threading

    grants: list[float] = []

    def _waiter():
        grants.append(time.monotonic())
        slot = limiter.acquire_sync("together", timeout=2.0)
        grants.append(time.monotonic())
        limiter.release(slot)

    t = threading.Thread(target=_waiter, daemon=True)
    t.start()
    time.sleep(0.05)
    release_at = time.monotonic()
    limiter.release(held)
    t.join(timeout=2.0)
    assert not t.is_alive()
    # Two timestamps: enter-wait and acquire-after-release.
    assert len(grants) == 2
    # The waiter saw the release within ~100ms (the lock notify path is
    # immediate; allow generous slack for CI variance).
    assert grants[1] - release_at < 0.5


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------

def test_token_bucket_drains_then_blocks():
    """When the bucket runs out of tokens, acquire blocks until refill."""

    limiter = _limiter_with(
        ProviderRateLimit("brave", tokens_per_second=10.0, bucket_capacity=2, max_in_flight=10)
    )
    # Drain the bucket immediately.
    a = limiter.acquire_sync("brave", timeout=0.1)
    b = limiter.acquire_sync("brave", timeout=0.1)
    snap = limiter.snapshot()["brave"]
    assert snap["tokens"] < 1.0

    # Without releasing in_flight, the third acquire blocks on tokens.
    # Release first so concurrency isn't the gate; then acquire should
    # block on tokens and time out under our short deadline.
    limiter.release(a)
    limiter.release(b)
    with pytest.raises(ProviderRateLimitTimeout):
        limiter.acquire_sync("brave", timeout=0.05)


def test_token_bucket_refills_over_time():
    """After enough wall time, the bucket refills and acquire succeeds."""

    limiter = _limiter_with(
        ProviderRateLimit("fireworks", tokens_per_second=20.0, bucket_capacity=1, max_in_flight=10)
    )
    a = limiter.acquire_sync("fireworks", timeout=0.1)
    limiter.release(a)
    # Bucket is empty. Wait ~100ms — that's 2 tokens at 20 tokens/sec.
    time.sleep(0.1)
    b = limiter.acquire_sync("fireworks", timeout=0.5)
    assert b.provider_slug == "fireworks"
    limiter.release(b)


# ---------------------------------------------------------------------------
# Cooldown / Retry-After
# ---------------------------------------------------------------------------

def test_release_with_retry_after_blocks_subsequent_acquire():
    """Honoring 429 / Retry-After: once a caller hands back a slot with a
    cooldown, no new acquire is admitted until the cooldown elapses."""

    limiter = _limiter_with(
        ProviderRateLimit("hubspot", tokens_per_second=100, bucket_capacity=100, max_in_flight=10)
    )
    slot = limiter.acquire_sync("hubspot", timeout=0.5)
    limiter.release(slot, retry_after_seconds=0.2)
    # Immediate retry must time out under a short deadline.
    with pytest.raises(ProviderRateLimitTimeout) as exc:
        limiter.acquire_sync("hubspot", timeout=0.05)
    assert exc.value.cooldown_seconds > 0


def test_cooldown_clears_after_window():
    limiter = _limiter_with(
        ProviderRateLimit("cursor", tokens_per_second=100, bucket_capacity=100, max_in_flight=10)
    )
    slot = limiter.acquire_sync("cursor", timeout=0.5)
    limiter.release(slot, retry_after_seconds=0.1)
    # After the cooldown window, acquire admits again.
    time.sleep(0.15)
    next_slot = limiter.acquire_sync("cursor", timeout=0.5)
    assert next_slot.provider_slug == "cursor"
    limiter.release(next_slot)


# ---------------------------------------------------------------------------
# Async API + context manager
# ---------------------------------------------------------------------------

def test_async_acquire_via_to_thread():
    limiter = _limiter_with(
        ProviderRateLimit("google", tokens_per_second=10, bucket_capacity=5, max_in_flight=2)
    )

    async def _go():
        slot = await limiter.acquire("google", timeout=0.5)
        try:
            assert slot.provider_slug == "google"
        finally:
            limiter.release(slot)

    asyncio.run(_go())


def test_slot_context_manager_releases_on_exit():
    limiter = _limiter_with(
        ProviderRateLimit("deepseek", tokens_per_second=10, bucket_capacity=5, max_in_flight=1)
    )

    async def _go():
        async with limiter.slot("deepseek", timeout=0.5):
            assert limiter.snapshot()["deepseek"]["in_flight"] == 1
        assert limiter.snapshot()["deepseek"]["in_flight"] == 0

    asyncio.run(_go())


def test_slot_context_manager_propagates_retry_after():
    limiter = _limiter_with(
        ProviderRateLimit("openai", tokens_per_second=100, bucket_capacity=100, max_in_flight=10)
    )

    async def _go():
        async with limiter.slot("openai", timeout=0.5) as handle:
            handle.set_retry_after(0.2)
        # Cooldown is now active; immediate re-acquire should time out
        # under a short deadline.
        with pytest.raises(ProviderRateLimitTimeout):
            await limiter.acquire("openai", timeout=0.05)

    asyncio.run(_go())


# ---------------------------------------------------------------------------
# Unknown provider
# ---------------------------------------------------------------------------

def test_unknown_provider_raises_explicitly():
    limiter = ProviderRateLimiter(
        limits={"anthropic": ProviderRateLimit("anthropic", 10, 10, 10)}
    )
    with pytest.raises(UnknownProviderRateLimit):
        limiter.acquire_sync("never_registered", timeout=0.05)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

def test_snapshot_includes_all_registered_providers():
    limiter = ProviderRateLimiter(
        limits={
            "anthropic": ProviderRateLimit("anthropic", 5, 10, 4),
            "openai": ProviderRateLimit("openai", 5, 10, 4),
        }
    )
    snap = limiter.snapshot()
    assert set(snap.keys()) == {"anthropic", "openai"}
    for v in snap.values():
        assert v["tokens"] == pytest.approx(10.0, rel=0.05)
        assert v["in_flight"] == 0
        assert v["max_in_flight"] == 4


# ---------------------------------------------------------------------------
# Wave 3 — call_llm integration (universal LLM dispatch chokepoint)
# ---------------------------------------------------------------------------

def test_call_llm_acquires_concurrency_slot_via_provider_slug(monkeypatch):
    """Every ``call_llm`` invocation must acquire a per-provider slot via
    ``request.provider_slug`` and release it in the finally — covering
    every direct caller (chat_orchestrator, plan_synthesis, plan_fork_author,
    compiler_llm, focused_experiments, plan_cluster_author, plan_pill_triage,
    plus indirect callers via http_transport)."""

    from adapters import llm_client
    from runtime import provider_rate_limiter as prl

    acquired: list[tuple[str, float]] = []
    released: list[str] = []

    class _RecordingLimiter:
        def acquire_sync(self, slug, *, timeout):
            acquired.append((slug, timeout))
            return prl.ProviderSlot(provider_slug=slug)

        def release(self, slot, *, retry_after_seconds=0.0):
            released.append(slot.provider_slug)

    monkeypatch.setattr(prl, "default_rate_limiter", lambda: _RecordingLimiter())

    # Stub the inner pipeline so we never touch the network or build a
    # real protocol body. The slot acquire+release happens around this
    # stub regardless.
    def _stub_inner(**_kwargs):
        return llm_client.LLMResponse(
            content="ok",
            model="gpt-test",
            provider_slug="openrouter",
            usage={},
            raw_response={},
            latency_ms=0,
            status_code=200,
        )

    monkeypatch.setattr(llm_client, "_call_llm_with_slot", _stub_inner)

    request = llm_client.LLMRequest(
        endpoint_uri="https://openrouter.ai/api/v1/chat/completions",
        api_key="test-key",
        provider_slug="openrouter",
        model_slug="anthropic/claude-3-haiku",
        messages=({"role": "user", "content": "ping"},),
        max_tokens=8,
        protocol_family="openai_chat_completions",
        timeout_seconds=5,
    )
    response = llm_client.call_llm(request)
    assert response.content == "ok"
    assert acquired == [("openrouter", 5.0)]
    assert released == ["openrouter"]


def test_call_llm_releases_slot_when_inner_raises(monkeypatch):
    """The finally must run on the failure path — a network exception
    inside ``_call_llm_with_slot`` cannot leak a held slot."""

    from adapters import llm_client
    from runtime import provider_rate_limiter as prl

    released: list[str] = []

    class _RecordingLimiter:
        def acquire_sync(self, slug, *, timeout):
            return prl.ProviderSlot(provider_slug=slug)

        def release(self, slot, *, retry_after_seconds=0.0):
            released.append(slot.provider_slug)

    monkeypatch.setattr(prl, "default_rate_limiter", lambda: _RecordingLimiter())

    def _raising_inner(**_kwargs):
        raise llm_client.LLMClientError("llm_client.network_error", "boom")

    monkeypatch.setattr(llm_client, "_call_llm_with_slot", _raising_inner)

    request = llm_client.LLMRequest(
        endpoint_uri="https://api.anthropic.test/v1/messages",
        api_key="test-key",
        provider_slug="anthropic",
        model_slug="claude-test",
        messages=({"role": "user", "content": "ping"},),
        max_tokens=8,
        protocol_family="anthropic_messages",
        timeout_seconds=5,
    )
    with pytest.raises(llm_client.LLMClientError):
        llm_client.call_llm(request)
    assert released == ["anthropic"]


def test_call_llm_rate_limit_timeout_becomes_llm_client_error(monkeypatch):
    """When the per-provider concurrency cap is hit and the wait budget
    is exhausted, the gate translates the timeout into
    ``LLMClientError(reason_code='llm_client.concurrency_cap_timeout')``
    so callers see the same shape as a token-bucket throttle timeout."""

    from adapters import llm_client
    from runtime import provider_rate_limiter as prl

    class _AlwaysTimeout:
        def acquire_sync(self, slug, *, timeout):
            raise prl.ProviderRateLimitTimeout(
                provider_slug=slug,
                timeout_seconds=timeout,
                in_flight=10,
                bucket_tokens=0.0,
                cooldown_seconds=0.0,
            )

        def release(self, *a, **kw):
            return None

    monkeypatch.setattr(prl, "default_rate_limiter", lambda: _AlwaysTimeout())

    request = llm_client.LLMRequest(
        endpoint_uri="https://api.openai.test/v1/chat/completions",
        api_key="test-key",
        provider_slug="openai",
        model_slug="gpt-test",
        messages=({"role": "user", "content": "ping"},),
        max_tokens=8,
        protocol_family="openai_chat_completions",
        timeout_seconds=2,
    )
    with pytest.raises(llm_client.LLMClientError) as exc:
        llm_client.call_llm(request)
    assert exc.value.reason_code == "llm_client.concurrency_cap_timeout"


def test_call_llm_passes_through_when_provider_unmapped(monkeypatch):
    """Unknown providers fall through with no concurrency gate (safe v1
    default — never block on missing policy). Real production rates are
    still gated by the existing token-bucket throttle in
    ``runtime.integrations.rate_limiter``."""

    from adapters import llm_client
    from runtime import provider_rate_limiter as prl

    def _stub_inner(**_kwargs):
        return llm_client.LLMResponse(
            content="ok",
            model="x",
            provider_slug="never_registered",
            usage={},
            raw_response={},
            latency_ms=0,
            status_code=200,
        )

    monkeypatch.setattr(llm_client, "_call_llm_with_slot", _stub_inner)

    # Use the real default limiter — "never_registered" has no policy, so
    # acquire raises UnknownProviderRateLimit which the gate treats as a
    # pass-through.
    prl.reset_default_rate_limiter()

    request = llm_client.LLMRequest(
        endpoint_uri="https://example.test",
        api_key="test-key",
        provider_slug="never_registered",
        model_slug="test",
        messages=({"role": "user", "content": "ping"},),
        max_tokens=8,
        protocol_family="openai_chat_completions",
        timeout_seconds=2,
    )
    response = llm_client.call_llm(request)
    assert response.content == "ok"


def test_http_transport_chat_protocol_translates_concurrency_timeout(monkeypatch):
    """``_call_chat_completion_protocol`` translates the
    ``llm_client.concurrency_cap_timeout`` reason_code from the gate into
    a ``TransportExecutionError`` so transport-layer callers see a
    consistent error shape."""

    from runtime import http_transport
    from adapters import llm_client

    def _raising_call_llm(_request):
        raise llm_client.LLMClientError(
            "llm_client.concurrency_cap_timeout",
            "concurrency cap exceeded",
        )

    monkeypatch.setattr(http_transport, "call_llm", _raising_call_llm)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    with pytest.raises(http_transport.TransportExecutionError) as exc:
        http_transport._call_chat_completion_protocol(
            "openai_chat_completions",
            "ping",
            model="gpt-test",
            max_tokens=8,
            timeout=2,
            api_endpoint="https://api.openai.test/v1/chat/completions",
            api_key_env="OPENAI_API_KEY",
        )
    assert exc.value.reason_code == "http_transport.rate_limit_timeout"
