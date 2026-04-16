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
from typing import Any, Callable, Literal


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
    run_id: str | None = None


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
    """Route outcome history backed by workflow_metrics with a local overlay.

    Thread-safe — run_workflow_parallel calls record concurrently.
    The in-memory ring buffer is a hot cache for current-process writes; the
    Postgres workflow_metrics table is the durable source of truth.
    """

    def __init__(
        self,
        *,
        buffer_size: int = _DEFAULT_BUFFER_SIZE,
        metrics_view_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._buffer_size = buffer_size
        self._lock = threading.Lock()
        # route key aliases -> deque of RouteOutcome (newest at right)
        self._buffers: dict[str, deque[RouteOutcome]] = defaultdict(
            lambda: deque(maxlen=self._buffer_size),
        )
        self._metrics_view_factory = metrics_view_factory

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
                run_id=outcome.run_id,
            )
            for alias in dict.fromkeys(aliases):
                self._buffers[alias].append(route_outcome)

    def provider_slugs(self) -> tuple[str, ...]:
        """Return known provider slugs from both local and durable history."""
        providers: set[str] = set()
        with self._lock:
            for buf in self._buffers.values():
                for outcome in buf:
                    if outcome.provider_slug:
                        providers.add(outcome.provider_slug)

        for provider_slug in self._db_provider_slugs():
            providers.add(provider_slug)
        return tuple(sorted(providers))

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
        local = self._recent_local_outcomes(
            provider_slug,
            model_slug=model_slug,
            adapter_type=adapter_type,
            limit=limit,
        )
        db_outcomes = self._db_recent_outcomes(
            provider_slug,
            model_slug=model_slug,
            adapter_type=adapter_type,
            limit=max(limit, self._buffer_size * 4),
        )
        merged = self._merge_outcomes(db_outcomes, local)
        return merged[:limit]

    def consecutive_failures(
        self,
        provider_slug: str,
        *,
        model_slug: str | None = None,
        adapter_type: str | None = None,
    ) -> int:
        """Count consecutive route-relevant failures from newest to oldest."""
        local = self._recent_local_outcomes(
            provider_slug,
            model_slug=model_slug,
            adapter_type=adapter_type,
            limit=self._buffer_size,
        )
        local_count = self._count_consecutive_failures(local)
        if any(outcome.status == "succeeded" for outcome in local):
            return local_count

        db_outcomes = self._db_recent_outcomes(
            provider_slug,
            model_slug=model_slug,
            adapter_type=adapter_type,
            limit=max(self._buffer_size * 4, 50),
        )
        merged = self._merge_outcomes(db_outcomes, local)
        return self._count_consecutive_failures(merged)

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

    def _recent_local_outcomes(
        self,
        provider_slug: str,
        *,
        model_slug: str | None,
        adapter_type: str | None,
        limit: int,
    ) -> tuple[RouteOutcome, ...]:
        if limit <= 0:
            return ()
        with self._lock:
            buf = self._buffers.get(_route_key(provider_slug, model_slug, adapter_type))
            if buf is None:
                return ()
            items = list(buf)[-max(0, int(limit)) :]
            items.reverse()
            return tuple(items)

    def _count_consecutive_failures(self, outcomes: tuple[RouteOutcome, ...]) -> int:
        count = 0
        for outcome in outcomes:
            if outcome.status == "succeeded":
                if count > 0:
                    break
                continue
            if not _counts_against_route_health(outcome):
                continue
            count += 1
        return count

    def _dedupe_key(self, outcome: RouteOutcome) -> tuple[str, ...]:
        run_id = str(outcome.run_id or "").strip()
        if run_id:
            return ("run", run_id)
        return (
            "route",
            outcome.route_key or _route_key(outcome.provider_slug, outcome.model_slug, outcome.adapter_type),
            outcome.status,
            str(outcome.failure_code or ""),
            str(outcome.failure_category or ""),
            str(outcome.latency_ms),
            outcome.recorded_at.isoformat(),
        )

    def _merge_outcomes(
        self,
        *groups: tuple[RouteOutcome, ...],
    ) -> tuple[RouteOutcome, ...]:
        merged: dict[tuple[str, ...], RouteOutcome] = {}
        for group in groups:
            for outcome in group:
                key = self._dedupe_key(outcome)
                current = merged.get(key)
                if current is None or outcome.recorded_at >= current.recorded_at:
                    merged[key] = outcome
        ordered = sorted(
            merged.values(),
            key=lambda outcome: (
                outcome.recorded_at,
                outcome.run_id or "",
                outcome.route_key or "",
                outcome.provider_slug,
                outcome.model_slug or "",
                outcome.adapter_type,
            ),
            reverse=True,
        )
        return tuple(ordered)

    def _metrics_view(self) -> Any | None:
        if self._metrics_view_factory is None:
            try:
                from .observability import get_workflow_metrics_view
            except Exception:
                return None
            factory = get_workflow_metrics_view
        else:
            factory = self._metrics_view_factory
        try:
            return factory()
        except Exception:
            return None

    def _db_recent_outcomes(
        self,
        provider_slug: str,
        *,
        model_slug: str | None,
        adapter_type: str | None,
        limit: int,
    ) -> tuple[RouteOutcome, ...]:
        metrics_view = self._metrics_view()
        if metrics_view is None:
            return ()
        try:
            rows = metrics_view.recent_route_outcomes(
                provider_slug=provider_slug,
                model_slug=model_slug,
                adapter_type=adapter_type,
                limit=max(0, int(limit)),
            )
        except Exception:
            return ()
        outcomes: list[RouteOutcome] = []
        for row in rows:
            provider = str(row.get("provider_slug") or "").strip()
            if not provider:
                continue
            model = row.get("model_slug")
            adapter = str(row.get("adapter_type") or "").strip()
            created_at = row.get("created_at")
            if not isinstance(created_at, datetime):
                continue
            outcomes.append(
                RouteOutcome(
                    provider_slug=provider,
                    model_slug=str(model).strip() if isinstance(model, str) and model.strip() else None,
                    adapter_type=adapter,
                    status=str(row.get("status") or "failed"),
                    failure_code=(
                        str(row.get("failure_code")).strip()
                        if row.get("failure_code") not in (None, "")
                        else None
                    ),
                    latency_ms=int(row.get("latency_ms") or 0),
                    recorded_at=created_at,
                    route_key=_route_key(provider, row.get("model_slug"), row.get("adapter_type")),
                    failure_category=str(row.get("failure_category") or ""),
                    is_retryable=(
                        bool(row.get("is_retryable"))
                        if row.get("is_retryable") is not None
                        else None
                    ),
                    is_transient=(
                        bool(row.get("is_transient"))
                        if row.get("is_transient") is not None
                        else None
                    ),
                    run_id=str(row.get("run_id") or "").strip() or None,
                )
            )
        return tuple(outcomes)

    def _db_provider_slugs(self) -> tuple[str, ...]:
        metrics_view = self._metrics_view()
        if metrics_view is None:
            return ()
        try:
            slugs = metrics_view.provider_slugs()
        except Exception:
            return ()
        return tuple(
            slug
            for slug in (str(item).strip() for item in slugs)
            if slug
        )


def _counts_against_route_health(outcome: RouteOutcome) -> bool:
    """Return whether a failed outcome should penalize route health."""
    if outcome.status != "failed":
        return False
    category = str(outcome.failure_category or "").strip().lower()
    if not category:
        return True
    return category not in _ROUTE_HEALTH_EXTERNAL_CATEGORIES
