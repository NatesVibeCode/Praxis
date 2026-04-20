from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from runtime.integrations.rate_limiter import (
    ProviderThrottleRegistry,
    RateLimitAcquireTimeout,
    RateLimitConfig,
    TokenBucketLimiter,
    acquire_for_provider,
    get_global_provider_throttle_registry,
    set_global_provider_throttle_registry,
)


# ---------------------------------------------------------------------------
# TokenBucketLimiter
# ---------------------------------------------------------------------------


def test_token_bucket_rejects_non_positive_tokens_per_second_without_db_bootstrap() -> None:
    with pytest.raises(ValueError):
        TokenBucketLimiter(tokens_per_second=0.0, burst_size=10)
    with pytest.raises(ValueError):
        TokenBucketLimiter(tokens_per_second=-1.0, burst_size=10)


def test_token_bucket_rejects_zero_burst_without_db_bootstrap() -> None:
    with pytest.raises(ValueError):
        TokenBucketLimiter(tokens_per_second=10.0, burst_size=0)


def test_token_bucket_initial_tokens_equal_burst_size_without_db_bootstrap() -> None:
    limiter = TokenBucketLimiter(tokens_per_second=10.0, burst_size=5)
    assert limiter.available_tokens() == pytest.approx(5.0, abs=1e-3)


def test_try_acquire_deducts_tokens_without_blocking_without_db_bootstrap() -> None:
    limiter = TokenBucketLimiter(tokens_per_second=10.0, burst_size=3)

    # Burst of 3 — three immediate acquires succeed, fourth fails.
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is True
    assert limiter.try_acquire() is False


def test_try_acquire_rejects_request_larger_than_burst_without_db_bootstrap() -> None:
    limiter = TokenBucketLimiter(tokens_per_second=100.0, burst_size=2)

    # 3 tokens > burst of 2 — would never succeed, so rejected immediately.
    assert limiter.try_acquire(tokens=3) is False
    # Bucket should NOT be drained by the rejected request.
    assert limiter.try_acquire(tokens=2) is True


def test_bucket_refills_over_time_without_db_bootstrap() -> None:
    # 20 tokens/sec, small burst — after draining, a short sleep should refill.
    limiter = TokenBucketLimiter(tokens_per_second=20.0, burst_size=2)
    assert limiter.try_acquire(tokens=2) is True
    assert limiter.try_acquire() is False

    time.sleep(0.15)  # 0.15s * 20/s = 3 tokens-worth, capped at burst=2
    assert limiter.try_acquire() is True


def test_acquire_waits_until_tokens_available_without_db_bootstrap() -> None:
    # 50 tokens/sec => ~20ms per token. Drain, then acquire should wait ~20ms.
    limiter = TokenBucketLimiter(tokens_per_second=50.0, burst_size=1)
    assert limiter.try_acquire() is True

    start = time.monotonic()
    waited = limiter.acquire(max_wait_seconds=1.0)
    elapsed = time.monotonic() - start

    # Must have actually waited something, and the returned wait must not
    # wildly exceed wall-clock elapsed.
    assert waited > 0.0
    assert elapsed >= waited - 0.01
    assert waited < 0.5  # generous ceiling for CI slowness


def test_acquire_raises_timeout_when_wait_budget_is_exceeded_without_db_bootstrap() -> None:
    # 1 token/sec, burst=1 — second token takes ~1s, but budget is 50ms.
    limiter = TokenBucketLimiter(tokens_per_second=1.0, burst_size=1)
    assert limiter.try_acquire() is True

    with pytest.raises(RateLimitAcquireTimeout) as excinfo:
        limiter.acquire(max_wait_seconds=0.05)
    assert excinfo.value.requested_tokens == 1.0
    assert excinfo.value.waited_seconds >= 0.0


def test_acquire_of_zero_tokens_is_always_a_no_op_without_db_bootstrap() -> None:
    limiter = TokenBucketLimiter(tokens_per_second=1.0, burst_size=1)
    assert limiter.acquire(tokens=0) == 0.0
    assert limiter.acquire(tokens=-5) == 0.0


def test_acquire_request_larger_than_burst_raises_without_db_bootstrap() -> None:
    # Requested tokens exceed burst capacity — can never succeed, so we
    # surface the timeout immediately instead of spinning until max_wait.
    limiter = TokenBucketLimiter(tokens_per_second=10.0, burst_size=2)
    with pytest.raises(RateLimitAcquireTimeout):
        limiter.acquire(tokens=3, max_wait_seconds=0.0)


def test_token_bucket_is_thread_safe_under_contention_without_db_bootstrap() -> None:
    # 10 threads race for a burst of 5. With a low tps, refill during the
    # contention window is negligible, so at most `burst_size` acquires win.
    limiter = TokenBucketLimiter(tokens_per_second=1.0, burst_size=5)
    successes: list[bool] = []
    lock = threading.Lock()
    start_gate = threading.Event()

    def _worker() -> None:
        start_gate.wait()
        ok = limiter.try_acquire()
        with lock:
            successes.append(ok)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    # Release every thread simultaneously to maximize contention.
    start_gate.set()
    for t in threads:
        t.join()

    # Initial bucket = 5. Up to `burst_size` acquires succeed, and at least
    # one must succeed (we haven't drained anything before the race).
    assert sum(successes) <= 5
    assert sum(successes) >= 1


# ---------------------------------------------------------------------------
# ProviderThrottleRegistry
# ---------------------------------------------------------------------------


def _mk_rows(*rows: dict[str, Any]) -> list[dict[str, Any]]:
    return list(rows)


def test_registry_builds_limiters_from_rate_limit_configs_rows_without_db_bootstrap() -> None:
    registry = ProviderThrottleRegistry.from_rows(
        _mk_rows(
            {"provider_slug": "openai", "tokens_per_second": 10.0, "burst_size": 20},
            {"provider_slug": "anthropic", "tokens_per_second": 5.0, "burst_size": 10},
        )
    )

    assert set(registry.provider_slugs()) == {"openai", "anthropic"}

    openai_cfg = registry.config_for("openai")
    assert openai_cfg == RateLimitConfig(
        provider_slug="openai", tokens_per_second=10.0, burst_size=20
    )

    # Limiters are instantiated lazily and returned consistently.
    limiter = registry.limiter_for("openai")
    assert isinstance(limiter, TokenBucketLimiter)
    assert limiter.tokens_per_second == 10.0
    assert limiter.burst_size == 20
    assert registry.limiter_for("openai") is limiter


def test_registry_skips_invalid_rows_with_warning_without_db_bootstrap(caplog) -> None:
    rows = _mk_rows(
        {"provider_slug": "openai", "tokens_per_second": 10.0, "burst_size": 20},
        {"provider_slug": "  ", "tokens_per_second": 1.0, "burst_size": 1},       # empty slug
        {"provider_slug": "bad_tps", "tokens_per_second": 0.0, "burst_size": 1},
        {"provider_slug": "bad_burst", "tokens_per_second": 1.0, "burst_size": 0},
        {"provider_slug": "bad_numeric", "tokens_per_second": "nope", "burst_size": 1},
    )

    with caplog.at_level("WARNING"):
        registry = ProviderThrottleRegistry.from_rows(rows)

    assert set(registry.provider_slugs()) == {"openai"}
    assert registry.config_for("bad_tps") is None
    assert registry.config_for("bad_burst") is None
    assert registry.config_for("bad_numeric") is None
    assert any("invalid config" in m or "non-numeric" in m for m in caplog.messages), caplog.messages


def test_registry_normalizes_provider_slugs_without_db_bootstrap() -> None:
    registry = ProviderThrottleRegistry.from_rows(
        _mk_rows({"provider_slug": "  OpenAI ", "tokens_per_second": 5.0, "burst_size": 2})
    )

    assert registry.config_for("openai") is not None
    assert registry.config_for("OPENAI") is not None
    assert registry.config_for("  openai  ") is not None


def test_registry_acquire_is_noop_for_unknown_providers_without_db_bootstrap() -> None:
    registry = ProviderThrottleRegistry()
    # Unknown provider — no limit configured, so acquire returns 0.0 instantly.
    assert registry.acquire("unknown-provider") == 0.0
    assert registry.try_acquire("unknown-provider") is True


def test_registry_acquire_timeout_carries_real_provider_slug_without_db_bootstrap() -> None:
    registry = ProviderThrottleRegistry.from_rows(
        _mk_rows({"provider_slug": "openai", "tokens_per_second": 1.0, "burst_size": 1})
    )

    # Drain the bucket, then force a timeout.
    assert registry.try_acquire("openai") is True
    with pytest.raises(RateLimitAcquireTimeout) as excinfo:
        registry.acquire("OpenAI", max_wait_seconds=0.01)
    assert excinfo.value.provider_slug == "openai"


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------


def test_acquire_for_provider_returns_zero_when_no_global_registry_without_db_bootstrap() -> None:
    # Ensure no registry is installed.
    set_global_provider_throttle_registry(None)
    try:
        assert acquire_for_provider("openai") == 0.0
        assert get_global_provider_throttle_registry() is None
    finally:
        set_global_provider_throttle_registry(None)


def test_global_registry_gates_acquire_for_provider_without_db_bootstrap() -> None:
    registry = ProviderThrottleRegistry.from_rows(
        _mk_rows({"provider_slug": "openai", "tokens_per_second": 1.0, "burst_size": 1})
    )
    set_global_provider_throttle_registry(registry)
    try:
        # First acquire fits in burst — returns near-instantly (wall-clock,
        # not literally zero).
        first_wait = acquire_for_provider("openai", max_wait_seconds=0.01)
        assert first_wait < 0.005
        # Second acquire would need to wait ~1s, so times out under 10ms budget.
        with pytest.raises(RateLimitAcquireTimeout):
            acquire_for_provider("openai", max_wait_seconds=0.01)
    finally:
        set_global_provider_throttle_registry(None)
