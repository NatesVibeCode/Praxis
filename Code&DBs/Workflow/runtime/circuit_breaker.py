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
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from runtime._workflow_database import resolve_runtime_database_url
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

from .failure_classifier import classify_failure

_log = logging.getLogger(__name__)
_OVERRIDE_DECISION_PREFIX = "circuit-breaker::"
_OVERRIDE_CACHE_TTL_S = 2.0

def _require_cb_config() -> tuple[int, float]:
    """Read circuit breaker values from authoritative config."""
    from registry.config_registry import get_config

    cfg = get_config()
    threshold = cfg.get_int("breaker.failure_threshold")
    recovery = cfg.get_float("breaker.recovery_timeout_s")
    return threshold, recovery


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class ManualCircuitOverride:
    provider_slug: str
    override_state: "CircuitState"
    operator_decision_id: str
    decision_key: str
    decision_kind: str
    decision_status: str
    rationale: str
    decided_by: str
    decision_source: str
    effective_from: datetime
    effective_to: datetime | None
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "provider_slug": self.provider_slug,
            "override_state": self.override_state.value,
            "operator_decision_id": self.operator_decision_id,
            "decision_key": self.decision_key,
            "decision_kind": self.decision_kind,
            "decision_status": self.decision_status,
            "rationale": self.rationale,
            "decided_by": self.decided_by,
            "decision_source": self.decision_source,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "updated_at": self.updated_at.isoformat(),
        }


def _override_state_from_kind(decision_kind: str) -> CircuitState | None:
    if decision_kind == "circuit_breaker_force_open":
        return CircuitState.OPEN
    if decision_kind == "circuit_breaker_force_closed":
        return CircuitState.CLOSED
    return None


def _parse_manual_override(row: Any) -> ManualCircuitOverride | None:
    decision_key = str(row.get("decision_key") or "").strip()
    decision_kind = str(row.get("decision_kind") or "").strip()
    if not decision_key.startswith(_OVERRIDE_DECISION_PREFIX):
        return None
    override_state = _override_state_from_kind(decision_kind)
    if override_state is None:
        return None
    suffix = decision_key[len(_OVERRIDE_DECISION_PREFIX):]
    provider_slug = suffix.split("::", 1)[0].strip().lower()
    if not provider_slug:
        return None
    effective_from = row.get("effective_from")
    updated_at = row.get("updated_at")
    if not isinstance(effective_from, datetime) or not isinstance(updated_at, datetime):
        return None
    effective_to = row.get("effective_to")
    return ManualCircuitOverride(
        provider_slug=provider_slug,
        override_state=override_state,
        operator_decision_id=str(row.get("operator_decision_id") or ""),
        decision_key=decision_key,
        decision_kind=decision_kind,
        decision_status=str(row.get("decision_status") or ""),
        rationale=str(row.get("rationale") or ""),
        decided_by=str(row.get("decided_by") or ""),
        decision_source=str(row.get("decision_source") or ""),
        effective_from=effective_from,
        effective_to=effective_to if isinstance(effective_to, datetime) else None,
        updated_at=updated_at,
    )


def _provider_slug_from_decision_key(decision_key: str) -> str | None:
    if not decision_key.startswith(_OVERRIDE_DECISION_PREFIX):
        return None
    suffix = decision_key[len(_OVERRIDE_DECISION_PREFIX):]
    provider_slug = suffix.split("::", 1)[0].strip().lower()
    return provider_slug or None


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

    def state_summary(
        self,
        *,
        effective_state: CircuitState | None = None,
        manual_override: ManualCircuitOverride | None = None,
    ) -> dict[str, Any]:
        """Return a JSON-serializable summary of the breaker state."""
        with self._lock:
            return {
                "provider_slug": self.provider_slug,
                "state": (effective_state or self._state).value,
                "runtime_state": self._state.value,
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
                "manual_override": (
                    None if manual_override is None else manual_override.to_json()
                ),
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
        self._override_lock = threading.Lock()
        self._manual_overrides: dict[str, ManualCircuitOverride] = {}
        self._manual_override_cache_until = 0.0

    def _make_breaker(self, provider_slug: str) -> CircuitBreaker:
        return CircuitBreaker(
            provider_slug,
            failure_threshold=self._failure_threshold,
            recovery_timeout_s=self._recovery_timeout_s,
            half_open_max_calls=self._half_open_max_calls,
        )

    def get(self, provider_slug: str) -> CircuitBreaker:
        """Return the breaker for a provider, creating one if needed."""
        normalized_provider_slug = provider_slug.strip().lower()
        with self._lock:
            breaker = self._breakers.get(normalized_provider_slug)
            if breaker is None:
                breaker = self._make_breaker(normalized_provider_slug)
                self._breakers[normalized_provider_slug] = breaker
            return breaker

    def invalidate_manual_override_cache(self) -> None:
        with self._override_lock:
            self._manual_overrides = {}
            self._manual_override_cache_until = 0.0

    def _query_manual_overrides(self) -> dict[str, ManualCircuitOverride]:
        database_url = resolve_runtime_database_url(required=False)
        if database_url is None:
            return {}
        conn = SyncPostgresConnection(
            get_workflow_pool(env={"WORKFLOW_DATABASE_URL": database_url})
        )
        rows = conn.execute(
            """
            SELECT
                operator_decision_id,
                decision_key,
                decision_kind,
                decision_status,
                rationale,
                decided_by,
                decision_source,
                effective_from,
                effective_to,
                updated_at
            FROM operator_decisions
            WHERE decision_kind IN (
                    'circuit_breaker_reset',
                    'circuit_breaker_force_open',
                    'circuit_breaker_force_closed'
              )
              AND effective_from <= now()
              AND (effective_to IS NULL OR effective_to > now())
            ORDER BY updated_at DESC, operator_decision_id DESC
            """
        )
        overrides: dict[str, ManualCircuitOverride] = {}
        seen_providers: set[str] = set()
        for row in rows:
            provider_slug = _provider_slug_from_decision_key(
                str(row.get("decision_key") or "").strip()
            )
            if not provider_slug or provider_slug in seen_providers:
                continue
            seen_providers.add(provider_slug)
            if str(row.get("decision_kind") or "").strip() == "circuit_breaker_reset":
                continue
            override = _parse_manual_override(row)
            if override is None:
                continue
            overrides[override.provider_slug] = override
        return overrides

    def _manual_override_map(self) -> dict[str, ManualCircuitOverride]:
        now = time.monotonic()
        with self._override_lock:
            if now < self._manual_override_cache_until:
                return dict(self._manual_overrides)
        try:
            overrides = self._query_manual_overrides()
        except Exception as exc:
            _log.warning("circuit_breaker overrides unavailable: %s", exc)
            overrides = {}
        with self._override_lock:
            self._manual_overrides = dict(overrides)
            self._manual_override_cache_until = now + _OVERRIDE_CACHE_TTL_S
            return dict(self._manual_overrides)

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
        normalized_provider_slug = provider_slug.strip().lower()
        override = self._manual_override_map().get(normalized_provider_slug)
        if override is not None:
            return override.override_state != CircuitState.OPEN
        return self.get(normalized_provider_slug).allow_request()

    def all_states(self) -> dict[str, dict[str, Any]]:
        """Return a summary dict for every known breaker."""
        overrides = self._manual_override_map()
        with self._lock:
            slugs = list(self._breakers.keys())
        all_slugs = sorted(set(slugs) | set(overrides.keys()))
        payload: dict[str, dict[str, Any]] = {}
        for slug in all_slugs:
            override = overrides.get(slug)
            breaker = self.get(slug)
            payload[slug] = breaker.state_summary(
                effective_state=override.override_state if override is not None else None,
                manual_override=override,
            )
        return payload

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


def invalidate_circuit_breaker_override_cache() -> None:
    """Drop the process-local manual override cache so operator writes apply immediately."""

    registry = _CIRCUIT_BREAKERS
    if registry is not None:
        registry.invalidate_manual_override_cache()
