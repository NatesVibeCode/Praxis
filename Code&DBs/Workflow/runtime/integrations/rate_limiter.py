"""Token-bucket rate limiter backed by rate_limit_configs.

Each provider_slug gets its own `TokenBucketLimiter` (classic leaky-token
bucket) with `tokens_per_second` refill and `burst_size` capacity, seeded from
the `rate_limit_configs` table (migration 095_ipaas_rate_limit_config.sql).

Usage:
    >>> registry = load_provider_throttle_registry(conn)
    >>> registry.acquire("openai")          # may block
    >>> registry.acquire("openai", tokens=5, max_wait_seconds=10.0)

If a provider has no configured limit, `acquire()` returns 0.0 immediately —
so callers can unconditionally wrap every outbound request without branching.

The registry is thread-safe; a single instance can front an LLM client shared
across worker threads. A process-wide singleton is exposed via
`get_global_provider_throttle_registry()` / `set_global_provider_throttle_registry()`
so the HTTP transport layer can wrap every outbound provider request without
threading the registry through every call site.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """One row of rate_limit_configs, normalized."""

    provider_slug: str
    tokens_per_second: float
    burst_size: int


class RateLimitAcquireTimeout(RuntimeError):
    """Raised when acquire() cannot obtain tokens within max_wait_seconds."""

    def __init__(
        self,
        provider_slug: str,
        waited_seconds: float,
        *,
        requested_tokens: float,
    ) -> None:
        super().__init__(
            f"rate_limiter: provider {provider_slug!r} exceeded wait budget "
            f"({waited_seconds:.2f}s) for {requested_tokens} token(s)"
        )
        self.provider_slug = provider_slug
        self.waited_seconds = waited_seconds
        self.requested_tokens = requested_tokens


class TokenBucketLimiter:
    """Thread-safe leaky-token-bucket rate limiter.

    Tokens refill continuously at `tokens_per_second`, capped at `burst_size`.
    `acquire(n)` deducts n tokens, blocking up to `max_wait_seconds` if the
    bucket is empty. `try_acquire(n)` never blocks — it returns False instead.
    """

    __slots__ = ("_tps", "_burst", "_tokens", "_last_refill", "_lock")

    def __init__(self, tokens_per_second: float, burst_size: int) -> None:
        if tokens_per_second <= 0:
            raise ValueError(
                f"tokens_per_second must be > 0, got {tokens_per_second!r}"
            )
        if burst_size < 1:
            raise ValueError(f"burst_size must be >= 1, got {burst_size!r}")
        self._tps = float(tokens_per_second)
        self._burst = int(burst_size)
        self._tokens = float(burst_size)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    @property
    def tokens_per_second(self) -> float:
        return self._tps

    @property
    def burst_size(self) -> int:
        return self._burst

    def available_tokens(self) -> float:
        """Return the current (refilled) token count. Useful for introspection."""
        with self._lock:
            self._refill_locked()
            return self._tokens

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._burst, self._tokens + elapsed * self._tps)
            self._last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking: deduct tokens if available, return success."""
        if tokens <= 0:
            return True
        if tokens > self._burst:
            # A single request larger than our burst capacity could never succeed.
            return False
        with self._lock:
            self._refill_locked()
            if self._tokens + 1e-9 >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(
        self,
        tokens: float = 1.0,
        *,
        max_wait_seconds: float = 30.0,
    ) -> float:
        """Block until `tokens` can be deducted. Returns waited seconds.

        Raises `RateLimitAcquireTimeout` when the bucket would not have enough
        tokens within `max_wait_seconds`.
        """
        if tokens <= 0:
            return 0.0
        if tokens > self._burst:
            raise RateLimitAcquireTimeout(
                provider_slug="<anonymous>",
                waited_seconds=0.0,
                requested_tokens=tokens,
            )
        start = time.monotonic()
        deadline = start + max(0.0, max_wait_seconds)
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens + 1e-9 >= tokens:
                    self._tokens -= tokens
                    return time.monotonic() - start
                deficit = tokens - self._tokens
                wait_for = deficit / self._tps
            now = time.monotonic()
            if now + wait_for > deadline + 1e-6:
                raise RateLimitAcquireTimeout(
                    provider_slug="<anonymous>",
                    waited_seconds=now - start,
                    requested_tokens=tokens,
                )
            # Sleep in short pulses so cancellation / shutdown remains responsive.
            time.sleep(min(wait_for, 0.1))


class ProviderThrottleRegistry:
    """Registry of per-provider TokenBucketLimiters, lazily instantiated."""

    def __init__(
        self,
        configs: Mapping[str, RateLimitConfig] | None = None,
    ) -> None:
        self._configs: dict[str, RateLimitConfig] = {
            self._normalize_slug(slug): cfg
            for slug, cfg in (configs or {}).items()
            if self._normalize_slug(slug)
        }
        self._limiters: dict[str, TokenBucketLimiter] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _normalize_slug(value: Any) -> str:
        return str(value or "").strip().lower()

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, Any]]) -> "ProviderThrottleRegistry":
        """Build a registry from `rate_limit_configs` rows (dict or asyncpg Record)."""
        configs: dict[str, RateLimitConfig] = {}
        for row in rows:
            slug = cls._normalize_slug(row.get("provider_slug"))
            if not slug:
                continue
            try:
                tps = float(row.get("tokens_per_second") or 0.0)
                burst = int(row.get("burst_size") or 0)
            except (TypeError, ValueError):
                _log.warning(
                    "provider_throttle_registry: skipping non-numeric row for %r",
                    slug,
                )
                continue
            if tps <= 0 or burst < 1:
                _log.warning(
                    "provider_throttle_registry: skipping invalid config for %r "
                    "(tokens_per_second=%r, burst_size=%r)",
                    slug, tps, burst,
                )
                continue
            configs[slug] = RateLimitConfig(
                provider_slug=slug,
                tokens_per_second=tps,
                burst_size=burst,
            )
        return cls(configs=configs)

    def config_for(self, provider_slug: str) -> Optional[RateLimitConfig]:
        return self._configs.get(self._normalize_slug(provider_slug))

    def limiter_for(self, provider_slug: str) -> Optional[TokenBucketLimiter]:
        """Return a limiter (lazy-constructed) for this provider, or None."""
        slug = self._normalize_slug(provider_slug)
        if not slug:
            return None
        with self._lock:
            limiter = self._limiters.get(slug)
            if limiter is not None:
                return limiter
            cfg = self._configs.get(slug)
            if cfg is None:
                return None
            limiter = TokenBucketLimiter(
                tokens_per_second=cfg.tokens_per_second,
                burst_size=cfg.burst_size,
            )
            self._limiters[slug] = limiter
            return limiter

    def acquire(
        self,
        provider_slug: str,
        *,
        tokens: float = 1.0,
        max_wait_seconds: float = 30.0,
    ) -> float:
        """Acquire tokens from this provider's bucket. No-op if unconfigured.

        Returns waited seconds (0.0 if provider has no configured limit).
        Raises `RateLimitAcquireTimeout` with `provider_slug` filled in.
        """
        limiter = self.limiter_for(provider_slug)
        if limiter is None:
            return 0.0
        try:
            return limiter.acquire(tokens=tokens, max_wait_seconds=max_wait_seconds)
        except RateLimitAcquireTimeout as exc:
            # Re-raise with the real provider slug (limiter can't know it).
            raise RateLimitAcquireTimeout(
                provider_slug=self._normalize_slug(provider_slug),
                waited_seconds=exc.waited_seconds,
                requested_tokens=exc.requested_tokens,
            ) from None

    def try_acquire(
        self,
        provider_slug: str,
        *,
        tokens: float = 1.0,
    ) -> bool:
        """Non-blocking. True if unconfigured OR tokens deducted successfully."""
        limiter = self.limiter_for(provider_slug)
        if limiter is None:
            return True
        return limiter.try_acquire(tokens=tokens)

    def provider_slugs(self) -> tuple[str, ...]:
        return tuple(sorted(self._configs.keys()))


def load_provider_throttle_registry(
    conn: "SyncPostgresConnection",
) -> ProviderThrottleRegistry:
    """Load rate_limit_configs from Postgres and build a registry."""
    rows = conn.fetch(
        """
        SELECT provider_slug, tokens_per_second, burst_size
        FROM rate_limit_configs
        """
    )
    return ProviderThrottleRegistry.from_rows(rows)


# ---------------------------------------------------------------------------
# Process-wide singleton (optional). The HTTP transport layer installs one on
# boot so that every LLM request throttles through the same buckets regardless
# of entry point. Tests can install a controlled registry or `None` to disable.
# ---------------------------------------------------------------------------

_GLOBAL_REGISTRY_LOCK = threading.Lock()
_GLOBAL_REGISTRY: ProviderThrottleRegistry | None = None


def set_global_provider_throttle_registry(
    registry: ProviderThrottleRegistry | None,
) -> None:
    """Install (or clear) the process-wide throttle registry."""
    global _GLOBAL_REGISTRY
    with _GLOBAL_REGISTRY_LOCK:
        _GLOBAL_REGISTRY = registry


def get_global_provider_throttle_registry() -> ProviderThrottleRegistry | None:
    """Return the installed registry, or None if no registry is active."""
    with _GLOBAL_REGISTRY_LOCK:
        return _GLOBAL_REGISTRY


def acquire_for_provider(
    provider_slug: str,
    *,
    tokens: float = 1.0,
    max_wait_seconds: float = 30.0,
) -> float:
    """Module-level convenience: acquire through the installed global registry.

    Returns 0.0 if no global registry is installed (i.e. rate limiting is off).
    Never raises when rate limiting is disabled — callers can wrap every
    outbound request unconditionally.
    """
    registry = get_global_provider_throttle_registry()
    if registry is None:
        return 0.0
    return registry.acquire(
        provider_slug,
        tokens=tokens,
        max_wait_seconds=max_wait_seconds,
    )


__all__ = [
    "RateLimitConfig",
    "RateLimitAcquireTimeout",
    "TokenBucketLimiter",
    "ProviderThrottleRegistry",
    "load_provider_throttle_registry",
    "set_global_provider_throttle_registry",
    "get_global_provider_throttle_registry",
    "acquire_for_provider",
]
