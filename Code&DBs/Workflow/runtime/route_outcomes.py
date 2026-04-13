"""Route outcome recording and health checks.

Tracks the result of each dispatch route with explicit provider/model/adapter
identity so the dispatcher can skip routes that are consistently failing
without collapsing all health into a provider-only bucket.
"""

from __future__ import annotations

import os
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Route outcome record
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RouteOutcome:
    """Immutable record of a single dispatch through a route."""

    provider_slug: str
    model_slug: str | None
    adapter_type: str
    status: Literal["succeeded", "failed"]
    failure_code: str | None
    latency_ms: int
    recorded_at: datetime
    route_key: str = ""
    failure_category: str = ""
    is_retryable: bool | None = None
    is_transient: bool | None = None


# ---------------------------------------------------------------------------
# Ring-buffer store
# ---------------------------------------------------------------------------

_DEFAULT_BUFFER_SIZE = int(os.environ.get("PRAXIS_ROUTE_OUTCOME_BUFFER", "50"))
_ROUTE_HEALTH_EXTERNAL_CATEGORIES = frozenset(
    {
        "rate_limit",
        "timeout",
        "provider_error",
        "network_error",
        "credential_error",
        "input_error",
        "infrastructure",
    }
)


def _route_key(
    provider_slug: str,
    model_slug: str | None = None,
    adapter_type: str | None = None,
) -> str:
    provider = str(provider_slug or "").strip()
    if not provider:
        raise ValueError("provider_slug must be a non-empty string")
    key = provider
    model = str(model_slug or "").strip()
    if model:
        key = f"{key}/{model}"
    adapter = str(adapter_type or "").strip()
    if adapter:
        key = f"{key}@{adapter}"
    return key


class RouteOutcomeStore:
    """In-memory ring buffer of route outcomes, keyed by provider_slug.

    Thread-safe — run_workflow_parallel calls record concurrently.
    """

    def __init__(self, *, buffer_size: int = _DEFAULT_BUFFER_SIZE) -> None:
        self._buffer_size = buffer_size
        self._lock = threading.Lock()
        # route key aliases -> deque of RouteOutcome (newest at right)
        self._buffers: dict[str, deque[RouteOutcome]] = defaultdict(
            lambda: deque(maxlen=self._buffer_size),
        )

    # -- writes -------------------------------------------------------------

    def record_outcome(self, outcome: RouteOutcome) -> None:
        """Append an outcome to the route's ring buffer."""
        with self._lock:
            aliases = [
                outcome.provider_slug,
                _route_key(outcome.provider_slug, outcome.model_slug),
                _route_key(outcome.provider_slug, outcome.model_slug, outcome.adapter_type),
            ]
            if outcome.route_key:
                aliases.append(outcome.route_key)
            route_outcome = outcome if outcome.route_key else RouteOutcome(
                provider_slug=outcome.provider_slug,
                model_slug=outcome.model_slug,
                adapter_type=outcome.adapter_type,
                status=outcome.status,
                failure_code=outcome.failure_code,
                latency_ms=outcome.latency_ms,
                recorded_at=outcome.recorded_at,
                route_key=aliases[-1],
                failure_category=outcome.failure_category,
                is_retryable=outcome.is_retryable,
                is_transient=outcome.is_transient,
            )
            for alias in dict.fromkeys(aliases):
                self._buffers[alias].append(route_outcome)

    # -- reads --------------------------------------------------------------

    def recent_outcomes(
        self,
        provider_slug: str,
        *,
        model_slug: str | None = None,
        adapter_type: str | None = None,
        limit: int = 5,
    ) -> tuple[RouteOutcome, ...]:
        """Return the last *limit* outcomes for a route, newest first."""
        with self._lock:
            buf = self._buffers.get(_route_key(provider_slug, model_slug, adapter_type))
            if buf is None:
                return ()
            # deque is oldest-left, newest-right — reverse slice
            items = list(buf)[-limit:]
            items.reverse()
            return tuple(items)

    def consecutive_failures(
        self,
        provider_slug: str,
        *,
        model_slug: str | None = None,
        adapter_type: str | None = None,
    ) -> int:
        """Count consecutive route-relevant failures from newest to oldest."""
        with self._lock:
            buf = self._buffers.get(_route_key(provider_slug, model_slug, adapter_type))
            if buf is None:
                return 0
            count = 0
            for outcome in reversed(buf):
                if outcome.status == "succeeded":
                    if count > 0:
                        break
                    continue
                if not _counts_against_route_health(outcome):
                    continue
                count += 1
            return count

    def is_route_healthy(
        self,
        provider_slug: str,
        *,
        model_slug: str | None = None,
        adapter_type: str | None = None,
        max_consecutive_failures: int | None = None,
    ) -> bool:
        """A route is healthy unless it has hit the consecutive-failure cap.

        When *max_consecutive_failures* is not provided, the value is
        read from the Postgres-backed config registry.
        """
        if max_consecutive_failures is None:
            from registry.config_registry import get_config

            max_consecutive_failures = get_config().get_int(
                "health.max_consecutive_failures"
            )
        return (
            self.consecutive_failures(
                provider_slug,
                model_slug=model_slug,
                adapter_type=adapter_type,
            )
            < max_consecutive_failures
        )


def _counts_against_route_health(outcome: RouteOutcome) -> bool:
    """Return whether a failed outcome should penalize route health."""
    if outcome.status != "failed":
        return False
    category = str(outcome.failure_category or "").strip().lower()
    if not category:
        return True
    return category not in _ROUTE_HEALTH_EXTERNAL_CATEGORIES
