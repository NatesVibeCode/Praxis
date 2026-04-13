"""Circuit breaker state machine for provider routing.

Implements a three-state circuit breaker (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
per provider_slug. Replaces the simple consecutive-failure counter in
route_outcomes with proper backoff, half-open probing, and recovery.

State transitions:

    CLOSED  -- failure_count >= threshold --> OPEN
    OPEN    -- recovery_timeout elapsed   --> HALF_OPEN
    HALF_OPEN -- probe succeeds           --> CLOSED
    HALF_OPEN -- probe fails              --> OPEN (reset recovery timer)
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .failure_classifier import classify_failure

_log = logging.getLogger(__name__)

def _require_cb_config() -> tuple[int, float]:
    """Read circuit breaker values from authoritative config."""
    from registry.config_registry import get_config

    cfg = get_config()
    threshold = cfg.get_int("breaker.failure_threshold")
    recovery = cfg.get_float("breaker.recovery_timeout_s")
    return threshold, recovery


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# State enum
# ---------------------------------------------------------------------------

class CircuitState(str, Enum):
    """Three-state circuit breaker."""

    CLOSED = "CLOSED"       # healthy, allowing all traffic
    OPEN = "OPEN"           # broken, rejecting traffic
    HALF_OPEN = "HALF_OPEN" # testing with limited probe requests


# ---------------------------------------------------------------------------
# Per-provider circuit breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """Circuit breaker for a single provider_slug.

    Parameters
    ----------
    provider_slug:
        Identifier for the provider this breaker guards.
    failure_threshold:
        Number of consecutive failures before opening the circuit.
    recovery_timeout_s:
        Seconds to wait in OPEN state before transitioning to HALF_OPEN.
    half_open_max_calls:
        Maximum probe requests allowed in HALF_OPEN state before
        blocking until a probe result arrives.
    """

    def __init__(
        self,
        provider_slug: str,
        *,
        failure_threshold: int | None = None,
        recovery_timeout_s: float | None = None,
        half_open_max_calls: int = 1,
    ) -> None:
        # Require explicit Postgres-backed circuit-breaker values when the caller
        # does not override them.
        if failure_threshold is None or recovery_timeout_s is None:
            _def_thresh, _def_rec = _require_cb_config()
            if failure_threshold is None:
                failure_threshold = _def_thresh
            if recovery_timeout_s is None:
                recovery_timeout_s = _def_rec
        self.provider_slug = provider_slug
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self.half_open_max_calls = half_open_max_calls

        self._lock = threading.Lock()
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_at: datetime | None = None
        self._opened_at: datetime | None = None
        self._half_open_after: datetime | None = None
        self._half_open_calls: int = 0

    # -- state properties (read without lock for observability) ----------------

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def last_failure_at(self) -> datetime | None:
        return self._last_failure_at

    @property
    def opened_at(self) -> datetime | None:
        return self._opened_at

    @property
    def half_open_after(self) -> datetime | None:
        return self._half_open_after

    # -- transitions -----------------------------------------------------------

    def record_success(self) -> None:
        """Record a successful request outcome."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # Probe succeeded -- recover
                self._state = CircuitState.CLOSED
                recovery_time_ms = None
                if self._opened_at is not None:
                    recovery_time_ms = int(
                        (_utc_now() - self._opened_at).total_seconds() * 1000
                    )
                self._failure_count = 0
                self._half_open_calls = 0
                self._opened_at = None
                self._half_open_after = None
                _log.info(
                    "circuit_breaker[%s]: HALF_OPEN -> CLOSED (probe succeeded)",
                    self.provider_slug,
                )
                # Circuit state change — caller with conn emits to event log.
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0
            self._success_count += 1

    def record_failure(self, *, failure_code: str | None = None) -> None:
        """Record a failed request outcome.

        Parameters
        ----------
        failure_code : str | None
            The failure code from the dispatch. If provided and the failure is
            not retryable (e.g., credential error, input error), the failure
            is logged but does not count toward the circuit breaker threshold.
            This prevents permanent failures from tripping the breaker.
        """
        # Check if this is a retryable failure
        if failure_code is not None:
            classification = classify_failure(failure_code)
            if not classification.is_retryable:
                _log.debug(
                    "circuit_breaker[%s]: permanent failure (%s) not counted",
                    self.provider_slug,
                    failure_code,
                )
                return

        now = _utc_now()
        with self._lock:
            self._failure_count += 1
            self._last_failure_at = now

            if self._state == CircuitState.HALF_OPEN:
                # Probe failed -- reopen with fresh timeout
                self._state = CircuitState.OPEN
                self._opened_at = now
                self._half_open_after = datetime.fromtimestamp(
                    now.timestamp() + self.recovery_timeout_s,
                    tz=timezone.utc,
                )
                self._half_open_calls = 0
                _log.info(
                    "circuit_breaker[%s]: HALF_OPEN -> OPEN (probe failed)",
                    self.provider_slug,
                )
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._opened_at = now
                self._half_open_after = datetime.fromtimestamp(
                    now.timestamp() + self.recovery_timeout_s,
                    tz=timezone.utc,
                )
                _log.info(
                    "circuit_breaker[%s]: CLOSED -> OPEN "
                    "(failures=%d >= threshold=%d, recovery in %.0fs)",
                    self.provider_slug,
                    self._failure_count,
                    self.failure_threshold,
                    self.recovery_timeout_s,
                )
                # Circuit state change — caller with conn emits to event log.

    def allow_request(self) -> bool:
        """Check whether a request should be allowed through.

        Returns True if the request may proceed, False if the circuit
        is open and the provider should be skipped.
        """
        now = _utc_now()
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has elapsed
                if (
                    self._half_open_after is not None
                    and now >= self._half_open_after
                ):
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 1
                    _log.info(
                        "circuit_breaker[%s]: OPEN -> HALF_OPEN "
                        "(recovery timeout elapsed, allowing probe)",
                        self.provider_slug,
                    )
                    return True
                return False

            # HALF_OPEN: allow up to half_open_max_calls
            if self._half_open_calls < self.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

    # -- observability ---------------------------------------------------------

    def state_summary(self) -> dict[str, Any]:
        """Return a JSON-serializable summary of the breaker state."""
        with self._lock:
            return {
                "provider_slug": self.provider_slug,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout_s": self.recovery_timeout_s,
                "half_open_max_calls": self.half_open_max_calls,
                "last_failure_at": (
                    self._last_failure_at.isoformat()
                    if self._last_failure_at
                    else None
                ),
                "opened_at": (
                    self._opened_at.isoformat() if self._opened_at else None
                ),
                "half_open_after": (
                    self._half_open_after.isoformat()
                    if self._half_open_after
                    else None
                ),
                "half_open_calls": self._half_open_calls,
            }

# ---------------------------------------------------------------------------
# Registry -- one breaker per provider
# ---------------------------------------------------------------------------

class CircuitBreakerRegistry:
    """Manages one CircuitBreaker per provider_slug.

    Thread-safe. Breakers are created on first access with default config.
    """

    def __init__(
        self,
        *,
        failure_threshold: int | None = None,
        recovery_timeout_s: float | None = None,
        half_open_max_calls: int = 1,
    ) -> None:
        # Require explicit Postgres-backed circuit-breaker values when the caller
        # does not override them.
        if failure_threshold is None or recovery_timeout_s is None:
            _def_thresh, _def_rec = _require_cb_config()
            if failure_threshold is None:
                failure_threshold = _def_thresh
            if recovery_timeout_s is None:
                recovery_timeout_s = _def_rec

        self._failure_threshold = failure_threshold
        self._recovery_timeout_s = recovery_timeout_s
        self._half_open_max_calls = half_open_max_calls
        self._lock = threading.Lock()
        self._breakers: dict[str, CircuitBreaker] = {}

    def _make_breaker(self, provider_slug: str) -> CircuitBreaker:
        return CircuitBreaker(
            provider_slug,
            failure_threshold=self._failure_threshold,
            recovery_timeout_s=self._recovery_timeout_s,
            half_open_max_calls=self._half_open_max_calls,
        )

    def get(self, provider_slug: str) -> CircuitBreaker:
        """Return the breaker for a provider, creating one if needed."""
        with self._lock:
            breaker = self._breakers.get(provider_slug)
            if breaker is None:
                breaker = self._make_breaker(provider_slug)
                self._breakers[provider_slug] = breaker
            return breaker

    def record_outcome(
        self,
        provider_slug: str,
        *,
        succeeded: bool,
        failure_code: str | None = None,
    ) -> None:
        """Record an outcome.

        Parameters
        ----------
        provider_slug : str
            Provider identifier.
        succeeded : bool
            Whether the request succeeded.
        failure_code : str | None
            The failure code, if succeeded=False. Used to filter out permanent
            failures that shouldn't count toward the circuit breaker threshold.
        """
        breaker = self.get(provider_slug)
        if succeeded:
            breaker.record_success()
        else:
            breaker.record_failure(failure_code=failure_code)

    def allow_request(self, provider_slug: str) -> bool:
        """Check whether the provider's circuit allows a request."""
        return self.get(provider_slug).allow_request()

    def all_states(self) -> dict[str, dict[str, Any]]:
        """Return a summary dict for every known breaker."""
        with self._lock:
            slugs = list(self._breakers.keys())
        return {slug: self.get(slug).state_summary() for slug in slugs}

# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_CIRCUIT_BREAKERS: CircuitBreakerRegistry | None = None
_CIRCUIT_BREAKERS_LOCK = threading.Lock()


def get_circuit_breakers() -> CircuitBreakerRegistry:
    """Return the module-level CircuitBreakerRegistry singleton."""
    global _CIRCUIT_BREAKERS

    registry = _CIRCUIT_BREAKERS
    if registry is not None:
        return registry

    with _CIRCUIT_BREAKERS_LOCK:
        registry = _CIRCUIT_BREAKERS
        if registry is None:
            registry = CircuitBreakerRegistry()
            _CIRCUIT_BREAKERS = registry
        return registry
