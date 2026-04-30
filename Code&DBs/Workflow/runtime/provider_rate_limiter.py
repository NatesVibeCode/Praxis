"""Provider rate-limit gateway — token-bucket + concurrency cap per provider.

Phase E of the public-beta concurrency push. Centralizes rate-limit
enforcement for outbound provider calls (Anthropic, OpenAI, OpenRouter,
DeepSeek, Together, Brave, etc.) so a swarm of agents cannot burn through
org-level rate limits or burst 40 calls/half-second to a single upstream.

Two pressure mechanisms layered per provider:

  1. **Token bucket** — refills at the provider's documented rate.
     ``acquire`` blocks when the bucket is empty until the next refill.
  2. **Concurrency cap** — at most ``max_in_flight`` active calls.
     ``acquire`` blocks when the cap is hit until a slot is released.

Honors provider 429 / Retry-After signals: callers release the slot with
``retry_after_seconds`` to put the bucket into a cool-down window before
the next acquire is admitted. That spares the bucket from thrashing into a
series of rejected calls.

Scope notes:

* **Process-local v1** — one broker container = one rate-limit
  coordinator. v2 (multi-broker / distributed) moves the buckets behind
  Redis with the same public API.
* **Async-first API** — most LLM callers are inside the gateway's
  ``aexecute_operation_binding`` async dispatch path, so the natural shape
  is ``async with provider_slot(...): ...``. ``acquire_sync`` is a thin
  wrapper for the few sync callers that exist (sandbox-side probes).
* **Decoupled from observability** — this module enforces; it does not
  decide what to do on rejection. The caller catches ``ProviderRateLimitTimeout``
  and either backs off, queues, or fails the operation — typically writing
  a typed_gap or recording feedback through the existing helpers.

Standing-order references:
  feedback_check_rate_limit_first
  feedback_openrouter_spike_deprioritization

Integration pattern (for future packets that wire the gateway in):

    from runtime.provider_rate_limiter import default_rate_limiter

    async def call_provider(...):
        limiter = default_rate_limiter()
        async with limiter.slot("openrouter", timeout=10.0) as handle:
            response = await http_post("https://openrouter.ai/...", ...)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "30"))
                handle.set_retry_after(retry_after)
                raise ProviderRateLimited(...)
            return response

    # ProviderRateLimitTimeout from acquire bubbles to the caller; the
    # gateway catches it (reason_code='provider_rate_limit.acquire_timeout')
    # and writes a failed receipt. Caller chooses retry/backoff/queue.

Wired call sites (Phase E across waves 2 + 3):
  * ``adapters/llm_client.py:call_llm`` — **the universal LLM dispatch
    chokepoint.** Acquires a per-provider concurrency slot via
    ``request.provider_slug`` before the retry loop, releases in a finally
    so the slot is freed regardless of success / 429 / timeout / network
    error. Layered on top of the existing token-bucket throttle in
    ``runtime/integrations/rate_limiter.py`` (``_throttle_for_provider``):
    token-bucket gates RPS, this gate gates simultaneous in-flight count.
    Concurrency-cap timeout surfaces as
    ``LLMClientError(reason_code='llm_client.concurrency_cap_timeout')``.
  * ``runtime/http_transport.py:_call_chat_completion_protocol`` —
    transport-tier wrapper for the chat-completion families. Does not
    re-acquire (would double-count); just translates the
    ``llm_client.concurrency_cap_timeout`` reason_code into a
    ``TransportExecutionError(reason_code='http_transport.rate_limit_timeout')``
    so transport-tier callers see a consistent error shape.
  * ``runtime/http_transport.py:_call_cursor_background_agent`` — cursor
    background-agent launch+poll path. Cursor doesn't go through
    ``call_llm`` (it has its own client), so this site retains a direct
    ``acquire_sync``/``release`` pair around the inner agent run. One
    slot covers launch + status polls + conversation fetch as a unit.

Direct callers of ``call_llm`` that are now gated automatically:
  * ``runtime/chat_orchestrator.py``
  * ``runtime/plan_synthesis.py``
  * ``runtime/plan_fork_author.py``
  * ``runtime/plan_pill_triage.py``
  * ``runtime/plan_cluster_author.py``
  * ``runtime/compiler_llm.py``
  * ``runtime/focused_experiments.py``
  * any future module that builds an ``LLMRequest`` and calls ``call_llm``

Decision modules NOT wired (intentionally — they don't make HTTP calls):
  * ``runtime/task_type_router.py`` — picks provider/model, but dispatch
    happens downstream through ``call_llm``.
  * ``runtime/auto_router.py`` — same shape.

First-party direct API clients NOT wired (no first-party callers):
  * Brave Search — ``BRAVE_SEARCH_API_KEY`` is mounted in compose for the
    connector framework, but no runtime module makes direct HTTP requests
    to ``search.brave.com`` today.
  * HubSpot — same shape; ``HUBSPOT_ACCESS_TOKEN`` is connector-only.
  * Together / Fireworks — speak the OpenAI chat-completions protocol, so
    they go through ``call_llm`` and are already gated as long as the
    caller passes ``provider_slug='together'`` / ``'fireworks'``.

Future wave-up call sites (when concrete callers appear):
  * ``runtime/http_transport._json_request`` and cursor status+conversation
    polls (currently ride inside the parent cursor slot — fine for now;
    if individual polls become a thrash vector, add a sub-poll gate).
  * If/when a first-party Brave/HubSpot client is written, wrap its HTTP
    call site with ``async with default_rate_limiter().slot(slug, ...): ...``.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Mapping


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ProviderRateLimit:
    """Per-provider rate-limit policy.

    Attributes:
        provider_slug: canonical provider name (matches
            ``provider_authority`` slugs: ``anthropic``, ``openai``,
            ``openrouter``, ``deepseek``, ``together``, ``fireworks``,
            ``brave``, etc.)
        tokens_per_second: bucket refill rate. ``0`` means "unlimited"
            (the bucket never empties); use this only for providers with
            no documented limit, e.g. internal endpoints.
        bucket_capacity: maximum bucket size (burst allowance).
        max_in_flight: per-provider concurrency cap.
    """

    provider_slug: str
    tokens_per_second: float
    bucket_capacity: float
    max_in_flight: int


# Conservative starting defaults. Operators tune via
# ``PRAXIS_PROVIDER_RATE_LIMITS`` env (JSON-encoded) once a provider's
# real limits are known. The point of v1 is "agents queue instead of
# stampede" — exact numbers can be calibrated against provider docs and
# observed 429 response headers (rate_limit_prober already records those).
_DEFAULT_LIMITS: tuple[ProviderRateLimit, ...] = (
    # Anthropic CLI / OAuth subscription — generous in-flight, modest RPM.
    ProviderRateLimit("anthropic", tokens_per_second=5.0, bucket_capacity=20.0, max_in_flight=8),
    # OpenAI / Codex CLI.
    ProviderRateLimit("openai", tokens_per_second=5.0, bucket_capacity=20.0, max_in_flight=8),
    # OpenRouter — varies by routed model; conservative shared bucket.
    ProviderRateLimit("openrouter", tokens_per_second=3.0, bucket_capacity=15.0, max_in_flight=6),
    # DeepSeek direct API — research-only per standing order, low budget.
    ProviderRateLimit("deepseek", tokens_per_second=1.0, bucket_capacity=5.0, max_in_flight=2),
    # Together V4-Pro / V4-Flash — primary inference rail.
    ProviderRateLimit("together", tokens_per_second=8.0, bucket_capacity=30.0, max_in_flight=12),
    # Fireworks — secondary inference.
    ProviderRateLimit("fireworks", tokens_per_second=4.0, bucket_capacity=15.0, max_in_flight=6),
    # Google Gemini.
    ProviderRateLimit("google", tokens_per_second=4.0, bucket_capacity=15.0, max_in_flight=6),
    # Brave Search — free tier, respect-free-API standing order.
    ProviderRateLimit("brave", tokens_per_second=1.0, bucket_capacity=2.0, max_in_flight=1),
    # HubSpot — CRM API limits per app.
    ProviderRateLimit("hubspot", tokens_per_second=2.0, bucket_capacity=10.0, max_in_flight=4),
    # Cursor agent CLI.
    ProviderRateLimit("cursor", tokens_per_second=2.0, bucket_capacity=10.0, max_in_flight=4),
)


def _load_env_overrides() -> dict[str, ProviderRateLimit]:
    """Load per-provider overrides from the ``PRAXIS_PROVIDER_RATE_LIMITS``
    environment variable.

    Format: a JSON object whose keys are provider slugs and values are
    ``{"tokens_per_second": float, "bucket_capacity": float, "max_in_flight": int}``.
    Missing keys fall through to the default policy. Invalid JSON or shape
    is silently ignored — no point taking down rate-limit enforcement on a
    bad env var.
    """

    raw = os.environ.get("PRAXIS_PROVIDER_RATE_LIMITS")
    if not raw:
        return {}
    try:
        import json

        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    overrides: dict[str, ProviderRateLimit] = {}
    for slug, body in parsed.items():
        if not isinstance(slug, str) or not isinstance(body, dict):
            continue
        try:
            overrides[slug] = ProviderRateLimit(
                provider_slug=slug,
                tokens_per_second=float(body.get("tokens_per_second", 0)),
                bucket_capacity=float(body.get("bucket_capacity", 0)),
                max_in_flight=int(body.get("max_in_flight", 0)),
            )
        except (TypeError, ValueError):
            continue
    return overrides


def _build_initial_limits() -> dict[str, ProviderRateLimit]:
    base = {limit.provider_slug: limit for limit in _DEFAULT_LIMITS}
    base.update(_load_env_overrides())
    return base


# ---------------------------------------------------------------------------
# Bucket state
# ---------------------------------------------------------------------------

@dataclass
class _BucketState:
    """Mutable per-provider bucket — protected by ``_lock``."""

    policy: ProviderRateLimit
    tokens: float = 0.0
    last_refill_at: float = 0.0
    in_flight: int = 0
    cooldown_until: float = 0.0
    waiters: int = 0


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ProviderRateLimitTimeout(TimeoutError):
    """Raised by ``acquire`` when the wait exceeds the caller's timeout."""

    reason_code = "provider_rate_limit.acquire_timeout"

    def __init__(
        self,
        *,
        provider_slug: str,
        timeout_seconds: float,
        in_flight: int,
        bucket_tokens: float,
        cooldown_seconds: float,
    ) -> None:
        super().__init__(
            f"provider rate-limit acquire timed out after {timeout_seconds:.2f}s "
            f"(provider={provider_slug!r}, in_flight={in_flight}, "
            f"tokens={bucket_tokens:.2f}, cooldown={cooldown_seconds:.2f}s)"
        )
        self.provider_slug = provider_slug
        self.timeout_seconds = timeout_seconds
        self.in_flight = in_flight
        self.bucket_tokens = bucket_tokens
        self.cooldown_seconds = cooldown_seconds
        self.details = {
            "provider_slug": provider_slug,
            "timeout_seconds": timeout_seconds,
            "in_flight": in_flight,
            "bucket_tokens": bucket_tokens,
            "cooldown_seconds": cooldown_seconds,
        }


class UnknownProviderRateLimit(KeyError):
    """Raised when an acquire targets a provider that has no policy.

    Operators should add the provider to ``_DEFAULT_LIMITS`` (a code change)
    or set ``PRAXIS_PROVIDER_RATE_LIMITS`` (an env override) — silently
    admitting an unknown provider would defeat the purpose of the gateway.
    """

    reason_code = "provider_rate_limit.unknown_provider"


# ---------------------------------------------------------------------------
# Slot handle
# ---------------------------------------------------------------------------

@dataclass
class ProviderSlot:
    """Handle returned by ``acquire``. Pass to ``release`` (or use the
    context-manager helper) to free the slot when the provider call is
    done — including on errors and 429s.
    """

    provider_slug: str
    acquired_at: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

class ProviderRateLimiter:
    """Per-provider token-bucket + concurrency-cap gateway.

    Public surface:

    * ``acquire(provider_slug, *, timeout)`` — async, returns
      ``ProviderSlot`` or raises ``ProviderRateLimitTimeout``.
    * ``acquire_sync(provider_slug, *, timeout)`` — same, blocking.
    * ``release(slot, *, retry_after_seconds=0.0)`` — release a slot;
      ``retry_after_seconds`` puts the bucket in cool-down so the next
      acquire is delayed by at least that long.
    * ``slot(provider_slug, *, timeout)`` — async context-manager helper.

    The class is thread-safe and asyncio-safe; one process-wide instance
    is exposed via ``default_rate_limiter()``.
    """

    def __init__(self, *, limits: Mapping[str, ProviderRateLimit] | None = None) -> None:
        if limits is None:
            limits = _build_initial_limits()
        self._buckets: dict[str, _BucketState] = {}
        for slug, policy in limits.items():
            self._buckets[slug] = _BucketState(
                policy=policy,
                tokens=policy.bucket_capacity,
                last_refill_at=time.monotonic(),
            )
        self._lock = threading.Condition()

    # -- bucket math -------------------------------------------------------

    def _refill_locked(self, bucket: _BucketState, now: float) -> None:
        if bucket.policy.tokens_per_second <= 0:
            # Unlimited — keep the bucket at capacity.
            bucket.tokens = bucket.policy.bucket_capacity
            bucket.last_refill_at = now
            return
        elapsed = max(0.0, now - bucket.last_refill_at)
        bucket.tokens = min(
            bucket.policy.bucket_capacity,
            bucket.tokens + elapsed * bucket.policy.tokens_per_second,
        )
        bucket.last_refill_at = now

    def _try_take_locked(self, bucket: _BucketState, now: float) -> bool:
        """Try to consume one token + one concurrency slot.

        Returns True if the slot was granted; False if the caller must wait.
        """

        if bucket.cooldown_until > now:
            return False
        if bucket.in_flight >= bucket.policy.max_in_flight:
            return False
        self._refill_locked(bucket, now)
        if bucket.tokens < 1.0:
            return False
        bucket.tokens -= 1.0
        bucket.in_flight += 1
        return True

    def _wait_seconds_locked(self, bucket: _BucketState, now: float) -> float:
        """Estimate how long the caller must wait before the next slot
        could plausibly become available. Used to size short polling
        intervals — not load-bearing for correctness."""

        cooldown_left = max(0.0, bucket.cooldown_until - now)
        if cooldown_left > 0:
            return cooldown_left
        if bucket.in_flight >= bucket.policy.max_in_flight:
            return 0.05  # waiting for a release; short poll
        if bucket.policy.tokens_per_second <= 0:
            return 0.0
        deficit = max(0.0, 1.0 - bucket.tokens)
        return deficit / bucket.policy.tokens_per_second

    # -- public API --------------------------------------------------------

    def acquire_sync(
        self,
        provider_slug: str,
        *,
        timeout: float = 30.0,
    ) -> ProviderSlot:
        """Blocking acquire. Raises ``UnknownProviderRateLimit`` for
        unknown providers and ``ProviderRateLimitTimeout`` after ``timeout``
        seconds without a slot."""

        bucket = self._buckets.get(provider_slug)
        if bucket is None:
            raise UnknownProviderRateLimit(
                f"no rate-limit policy registered for provider {provider_slug!r}"
            )
        deadline = time.monotonic() + max(0.0, timeout)
        with self._lock:
            bucket.waiters += 1
            try:
                while True:
                    now = time.monotonic()
                    if self._try_take_locked(bucket, now):
                        return ProviderSlot(provider_slug=provider_slug, acquired_at=now)
                    remaining = deadline - now
                    if remaining <= 0:
                        raise ProviderRateLimitTimeout(
                            provider_slug=provider_slug,
                            timeout_seconds=timeout,
                            in_flight=bucket.in_flight,
                            bucket_tokens=bucket.tokens,
                            cooldown_seconds=max(0.0, bucket.cooldown_until - now),
                        )
                    wait_hint = self._wait_seconds_locked(bucket, now)
                    self._lock.wait(timeout=min(remaining, max(wait_hint, 0.01)))
            finally:
                bucket.waiters -= 1

    async def acquire(
        self,
        provider_slug: str,
        *,
        timeout: float = 30.0,
    ) -> ProviderSlot:
        """Async acquire — wraps ``acquire_sync`` in ``asyncio.to_thread``
        so it does not pin the event loop while waiting."""

        return await asyncio.to_thread(
            self.acquire_sync, provider_slug, timeout=timeout
        )

    def release(
        self,
        slot: ProviderSlot,
        *,
        retry_after_seconds: float = 0.0,
    ) -> None:
        """Release a previously-acquired slot.

        ``retry_after_seconds`` reflects a provider 429 / Retry-After
        signal — when set, the bucket enters cool-down for that long
        before any new acquire is admitted. Subsequent waiters are
        woken so they can re-check the deadline and back off cleanly."""

        bucket = self._buckets.get(slot.provider_slug)
        if bucket is None:
            return
        with self._lock:
            if bucket.in_flight > 0:
                bucket.in_flight -= 1
            if retry_after_seconds > 0:
                cooldown_until = time.monotonic() + retry_after_seconds
                if cooldown_until > bucket.cooldown_until:
                    bucket.cooldown_until = cooldown_until
            self._lock.notify_all()

    def slot(self, provider_slug: str, *, timeout: float = 30.0) -> "_ProviderSlotContext":
        """Async context-manager helper.

        Usage::

            async with limiter.slot("openai") as handle:
                response = await call_openai(...)
                if response.status_code == 429:
                    handle.set_retry_after(30)
        """
        return _ProviderSlotContext(self, provider_slug, timeout)

    # -- introspection -----------------------------------------------------

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        """Read-only snapshot of every bucket — useful for tests and
        operator observability dashboards."""

        out: dict[str, dict[str, float | int]] = {}
        with self._lock:
            now = time.monotonic()
            for slug, bucket in self._buckets.items():
                self._refill_locked(bucket, now)
                out[slug] = {
                    "tokens": round(bucket.tokens, 4),
                    "capacity": bucket.policy.bucket_capacity,
                    "in_flight": bucket.in_flight,
                    "max_in_flight": bucket.policy.max_in_flight,
                    "cooldown_seconds": max(0.0, bucket.cooldown_until - now),
                    "waiters": bucket.waiters,
                }
        return out


# ---------------------------------------------------------------------------
# Async context manager for the slot() helper
# ---------------------------------------------------------------------------

class _ProviderSlotContext:
    def __init__(
        self,
        limiter: ProviderRateLimiter,
        provider_slug: str,
        timeout: float,
    ) -> None:
        self._limiter = limiter
        self._provider_slug = provider_slug
        self._timeout = timeout
        self._slot: ProviderSlot | None = None
        self._retry_after_seconds: float = 0.0

    async def __aenter__(self) -> "_ProviderSlotContext":
        self._slot = await self._limiter.acquire(
            self._provider_slug, timeout=self._timeout
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._slot is not None:
            self._limiter.release(
                self._slot, retry_after_seconds=self._retry_after_seconds
            )

    def set_retry_after(self, seconds: float) -> None:
        """Tell the limiter the provider returned a 429; cool down the
        bucket for ``seconds`` before the next acquire is admitted."""
        self._retry_after_seconds = max(0.0, float(seconds))


# ---------------------------------------------------------------------------
# Process-wide singleton
# ---------------------------------------------------------------------------

_default: ProviderRateLimiter | None = None
_default_lock = threading.Lock()


def default_rate_limiter() -> ProviderRateLimiter:
    """Return the lazily-initialized process-wide rate limiter.

    Production callers grab this and use it directly; tests construct
    their own ``ProviderRateLimiter`` instance so they don't share state
    across cases.
    """
    global _default
    if _default is None:
        with _default_lock:
            if _default is None:
                _default = ProviderRateLimiter()
    return _default


def reset_default_rate_limiter() -> None:
    """Test helper: drop the process-wide singleton so the next call to
    ``default_rate_limiter`` produces a fresh instance with default policy.
    Not for production use — there is no way to migrate in-flight callers
    cleanly across a reset."""
    global _default
    with _default_lock:
        _default = None


__all__ = [
    "ProviderRateLimit",
    "ProviderRateLimiter",
    "ProviderRateLimitTimeout",
    "ProviderSlot",
    "UnknownProviderRateLimit",
    "default_rate_limiter",
    "reset_default_rate_limiter",
]
