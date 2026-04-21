"""Projection freshness samples.

Every derived read model trails its authority source. CQRS gave us the
split; this module gives the read side a way to say how far behind it
is. One explicit measurement per known projection:

- Event-log cursors expose (channel head id, subscriber cursor id,
  lag, head wall-clock age, cursor advance age).
- Process caches expose (epoch, last refresh wall-clock age).
- The workflow outbox exposes (head evidence_seq, consumer
  evidence_seq, lag, head captured-at age) per run.

Samples are pure values. The hooks that populate them live next to the
projection they describe; this module only shapes the measurement and
composes the well-known collector.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol


EVENT_LOG_CURSOR = "event_log_cursor"
PROCESS_CACHE = "process_cache"
OUTBOX_CURSOR = "outbox_cursor"

SLA_HEALTHY = "healthy"
SLA_WARNING = "warning"
SLA_CRITICAL = "critical"
SLA_UNKNOWN = "unknown"

READ_SIDE_CIRCUIT_CLOSED = "closed"
READ_SIDE_CIRCUIT_OPEN = "open"


class _AsyncConnection(Protocol):
    async def fetchrow(self, query: str, *args: object) -> Any: ...


class _SyncConnection(Protocol):
    def fetchrow(self, query: str, *args: object) -> Any: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _seconds_between(reference: datetime | None, now: datetime) -> float | None:
    if reference is None:
        return None
    aware = _as_utc(reference)
    if aware is None:
        return None
    return max(0.0, (now - aware).total_seconds())


@dataclass(frozen=True, slots=True)
class ProjectionFreshness:
    """One measurement of how far a derived read model trails authority.

    The ``source_kind`` discriminator determines which optional fields
    carry signal. ``staleness_seconds`` is always the single best read:
    event-log cursors report how long since the cursor last advanced;
    process caches report how long since the cache was last populated;
    outbox cursors report how long since the newest row was captured.
    """

    projection_id: str
    source_kind: str
    observed_at: datetime
    staleness_seconds: float | None = None
    channel: str | None = None
    subscriber_id: str | None = None
    cursor_event_id: int | None = None
    head_event_id: int | None = None
    lag_events: int | None = None
    head_emitted_at: datetime | None = None
    cursor_advanced_at: datetime | None = None
    cache_key: str | None = None
    epoch: int | None = None
    last_refreshed_at: datetime | None = None
    run_id: str | None = None
    head_evidence_seq: int | None = None
    consumer_evidence_seq: int | None = None
    head_captured_at: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "projection_id": self.projection_id,
            "source_kind": self.source_kind,
            "observed_at": self.observed_at.isoformat(),
            "staleness_seconds": self.staleness_seconds,
        }
        if self.source_kind == EVENT_LOG_CURSOR:
            payload.update(
                channel=self.channel,
                subscriber_id=self.subscriber_id,
                cursor_event_id=self.cursor_event_id,
                head_event_id=self.head_event_id,
                lag_events=self.lag_events,
                head_emitted_at=(
                    None if self.head_emitted_at is None else self.head_emitted_at.isoformat()
                ),
                cursor_advanced_at=(
                    None if self.cursor_advanced_at is None else self.cursor_advanced_at.isoformat()
                ),
            )
        elif self.source_kind == PROCESS_CACHE:
            payload.update(
                cache_key=self.cache_key,
                epoch=self.epoch,
                last_refreshed_at=(
                    None if self.last_refreshed_at is None else self.last_refreshed_at.isoformat()
                ),
            )
        elif self.source_kind == OUTBOX_CURSOR:
            payload.update(
                run_id=self.run_id,
                head_evidence_seq=self.head_evidence_seq,
                consumer_evidence_seq=self.consumer_evidence_seq,
                lag_events=self.lag_events,
                head_captured_at=(
                    None if self.head_captured_at is None else self.head_captured_at.isoformat()
                ),
            )
        return payload


@dataclass(frozen=True, slots=True)
class ProjectionFreshnessSlaPolicy:
    """Operator policy for deciding whether read-model freshness is acceptable."""

    warning_staleness_seconds: float
    critical_staleness_seconds: float
    warning_lag_events: int
    critical_lag_events: int
    policy_source: str = "platform_config"

    def to_json(self) -> dict[str, Any]:
        return {
            "warning_staleness_seconds": self.warning_staleness_seconds,
            "critical_staleness_seconds": self.critical_staleness_seconds,
            "warning_lag_events": self.warning_lag_events,
            "critical_lag_events": self.critical_lag_events,
            "policy_source": self.policy_source,
        }


@dataclass(frozen=True, slots=True)
class ProjectionFreshnessAlert:
    """One projection freshness breach with the exact SLA clause that fired."""

    projection_id: str
    status: str
    reason_code: str
    source_kind: str
    staleness_seconds: float | None
    lag_events: int | None
    read_side_circuit_breaker: str

    def to_json(self) -> dict[str, Any]:
        return {
            "projection_id": self.projection_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "source_kind": self.source_kind,
            "staleness_seconds": self.staleness_seconds,
            "lag_events": self.lag_events,
            "read_side_circuit_breaker": self.read_side_circuit_breaker,
        }


@dataclass(frozen=True, slots=True)
class ProjectionFreshnessSlaReport:
    """Aggregated SLA verdict for read-model freshness samples."""

    status: str
    read_side_circuit_breaker: str
    policy: ProjectionFreshnessSlaPolicy
    sample_count: int
    alert_count: int
    alerts: tuple[ProjectionFreshnessAlert, ...]
    unknown_projection_ids: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "projection_freshness_sla",
            "status": self.status,
            "read_side_circuit_breaker": self.read_side_circuit_breaker,
            "sample_count": self.sample_count,
            "alert_count": self.alert_count,
            "unknown_projection_ids": list(self.unknown_projection_ids),
            "policy": self.policy.to_json(),
            "alerts": [alert.to_json() for alert in self.alerts],
        }


def _sample_lag_events(sample: ProjectionFreshness) -> int | None:
    if sample.source_kind in {EVENT_LOG_CURSOR, OUTBOX_CURSOR}:
        return sample.lag_events
    return None


def _sample_alert(
    sample: ProjectionFreshness,
    *,
    policy: ProjectionFreshnessSlaPolicy,
) -> ProjectionFreshnessAlert | None:
    lag_events = _sample_lag_events(sample)
    staleness_seconds = sample.staleness_seconds

    if lag_events is not None and lag_events >= policy.critical_lag_events:
        return ProjectionFreshnessAlert(
            projection_id=sample.projection_id,
            status=SLA_CRITICAL,
            reason_code="projection_lag_events_critical",
            source_kind=sample.source_kind,
            staleness_seconds=staleness_seconds,
            lag_events=lag_events,
            read_side_circuit_breaker=READ_SIDE_CIRCUIT_OPEN,
        )
    if (
        staleness_seconds is not None
        and staleness_seconds >= policy.critical_staleness_seconds
    ):
        return ProjectionFreshnessAlert(
            projection_id=sample.projection_id,
            status=SLA_CRITICAL,
            reason_code="projection_staleness_seconds_critical",
            source_kind=sample.source_kind,
            staleness_seconds=staleness_seconds,
            lag_events=lag_events,
            read_side_circuit_breaker=READ_SIDE_CIRCUIT_OPEN,
        )
    if lag_events is not None and lag_events > policy.warning_lag_events:
        return ProjectionFreshnessAlert(
            projection_id=sample.projection_id,
            status=SLA_WARNING,
            reason_code="projection_lag_events_warning",
            source_kind=sample.source_kind,
            staleness_seconds=staleness_seconds,
            lag_events=lag_events,
            read_side_circuit_breaker=READ_SIDE_CIRCUIT_CLOSED,
        )
    if (
        staleness_seconds is not None
        and staleness_seconds >= policy.warning_staleness_seconds
    ):
        return ProjectionFreshnessAlert(
            projection_id=sample.projection_id,
            status=SLA_WARNING,
            reason_code="projection_staleness_seconds_warning",
            source_kind=sample.source_kind,
            staleness_seconds=staleness_seconds,
            lag_events=lag_events,
            read_side_circuit_breaker=READ_SIDE_CIRCUIT_CLOSED,
        )
    return None


def evaluate_projection_freshness_sla(
    samples: tuple[ProjectionFreshness, ...],
    *,
    policy: ProjectionFreshnessSlaPolicy,
) -> ProjectionFreshnessSlaReport:
    """Evaluate projection samples into alerts and a read-side gate verdict."""

    alerts = tuple(
        alert
        for sample in samples
        for alert in (_sample_alert(sample, policy=policy),)
        if alert is not None
    )
    unknown_projection_ids = tuple(
        sample.projection_id
        for sample in samples
        if sample.staleness_seconds is None and _sample_lag_events(sample) in (None, 0)
    )
    if any(alert.status == SLA_CRITICAL for alert in alerts):
        status = SLA_CRITICAL
        read_side_circuit_breaker = READ_SIDE_CIRCUIT_OPEN
    elif alerts:
        status = SLA_WARNING
        read_side_circuit_breaker = READ_SIDE_CIRCUIT_CLOSED
    elif unknown_projection_ids:
        status = SLA_UNKNOWN
        read_side_circuit_breaker = READ_SIDE_CIRCUIT_CLOSED
    else:
        status = SLA_HEALTHY
        read_side_circuit_breaker = READ_SIDE_CIRCUIT_CLOSED
    return ProjectionFreshnessSlaReport(
        status=status,
        read_side_circuit_breaker=read_side_circuit_breaker,
        policy=policy,
        sample_count=len(samples),
        alert_count=len(alerts),
        alerts=alerts,
        unknown_projection_ids=unknown_projection_ids,
    )


async def sample_event_log_cursor_freshness(
    conn: _AsyncConnection,
    *,
    channel: str,
    subscriber_id: str,
    projection_id: str,
    observed_at: datetime | None = None,
) -> ProjectionFreshness:
    """Measure subscriber lag for one event-log channel."""

    now = _as_utc(observed_at) or _utc_now()
    head_row = await conn.fetchrow(
        "SELECT MAX(id) AS head_id, MAX(emitted_at) AS head_at "
        "FROM event_log WHERE channel = $1",
        channel,
    )
    cursor_row = await conn.fetchrow(
        "SELECT last_event_id, updated_at "
        "FROM event_log_cursors WHERE subscriber_id = $1 AND channel = $2",
        subscriber_id,
        channel,
    )
    head_event_id = int(head_row["head_id"]) if head_row and head_row["head_id"] is not None else 0
    head_emitted_at = _as_utc(head_row["head_at"]) if head_row else None
    cursor_event_id = (
        int(cursor_row["last_event_id"]) if cursor_row and cursor_row["last_event_id"] is not None else 0
    )
    cursor_advanced_at = _as_utc(cursor_row["updated_at"]) if cursor_row else None
    lag_events = max(0, head_event_id - cursor_event_id)
    return ProjectionFreshness(
        projection_id=projection_id,
        source_kind=EVENT_LOG_CURSOR,
        observed_at=now,
        staleness_seconds=_seconds_between(cursor_advanced_at, now),
        channel=channel,
        subscriber_id=subscriber_id,
        cursor_event_id=cursor_event_id,
        head_event_id=head_event_id,
        lag_events=lag_events,
        head_emitted_at=head_emitted_at,
        cursor_advanced_at=cursor_advanced_at,
    )


def sample_process_cache_freshness(
    *,
    projection_id: str,
    cache_key: str,
    epoch: int,
    last_refreshed_at: datetime | None,
    observed_at: datetime | None = None,
) -> ProjectionFreshness:
    """Measure how long since a process-local cache was last populated."""

    now = _as_utc(observed_at) or _utc_now()
    refreshed = _as_utc(last_refreshed_at)
    return ProjectionFreshness(
        projection_id=projection_id,
        source_kind=PROCESS_CACHE,
        observed_at=now,
        staleness_seconds=_seconds_between(refreshed, now),
        cache_key=cache_key,
        epoch=int(epoch),
        last_refreshed_at=refreshed,
    )


async def sample_outbox_cursor_freshness(
    conn: _AsyncConnection,
    *,
    run_id: str,
    consumer_evidence_seq: int | None,
    projection_id: str,
    observed_at: datetime | None = None,
) -> ProjectionFreshness:
    """Measure subscriber lag against workflow_outbox head for one run."""

    now = _as_utc(observed_at) or _utc_now()
    normalized_consumer = int(consumer_evidence_seq) if consumer_evidence_seq is not None else 0
    head_row = await conn.fetchrow(
        "SELECT MAX(evidence_seq) AS head_seq, MAX(captured_at) AS head_captured_at "
        "FROM workflow_outbox WHERE run_id = $1",
        run_id,
    )
    head_evidence_seq = (
        int(head_row["head_seq"]) if head_row and head_row["head_seq"] is not None else 0
    )
    head_captured_at = _as_utc(head_row["head_captured_at"]) if head_row else None
    lag_events = max(0, head_evidence_seq - normalized_consumer)
    return ProjectionFreshness(
        projection_id=projection_id,
        source_kind=OUTBOX_CURSOR,
        observed_at=now,
        staleness_seconds=_seconds_between(head_captured_at, now) if lag_events else 0.0,
        run_id=run_id,
        head_evidence_seq=head_evidence_seq,
        consumer_evidence_seq=normalized_consumer,
        lag_events=lag_events,
        head_captured_at=head_captured_at,
    )


def _compose_event_log_freshness(
    *,
    head_row: Any,
    cursor_row: Any,
    channel: str,
    subscriber_id: str,
    projection_id: str,
    now: datetime,
) -> ProjectionFreshness:
    head_event_id = int(head_row["head_id"]) if head_row and head_row["head_id"] is not None else 0
    head_emitted_at = _as_utc(head_row["head_at"]) if head_row else None
    cursor_event_id = (
        int(cursor_row["last_event_id"]) if cursor_row and cursor_row["last_event_id"] is not None else 0
    )
    cursor_advanced_at = _as_utc(cursor_row["updated_at"]) if cursor_row else None
    lag_events = max(0, head_event_id - cursor_event_id)
    return ProjectionFreshness(
        projection_id=projection_id,
        source_kind=EVENT_LOG_CURSOR,
        observed_at=now,
        staleness_seconds=_seconds_between(cursor_advanced_at, now),
        channel=channel,
        subscriber_id=subscriber_id,
        cursor_event_id=cursor_event_id,
        head_event_id=head_event_id,
        lag_events=lag_events,
        head_emitted_at=head_emitted_at,
        cursor_advanced_at=cursor_advanced_at,
    )


def _compose_outbox_freshness(
    *,
    head_row: Any,
    run_id: str,
    consumer_evidence_seq: int,
    projection_id: str,
    now: datetime,
) -> ProjectionFreshness:
    head_evidence_seq = (
        int(head_row["head_seq"]) if head_row and head_row["head_seq"] is not None else 0
    )
    head_captured_at = _as_utc(head_row["head_captured_at"]) if head_row else None
    lag_events = max(0, head_evidence_seq - consumer_evidence_seq)
    return ProjectionFreshness(
        projection_id=projection_id,
        source_kind=OUTBOX_CURSOR,
        observed_at=now,
        staleness_seconds=_seconds_between(head_captured_at, now) if lag_events else 0.0,
        run_id=run_id,
        head_evidence_seq=head_evidence_seq,
        consumer_evidence_seq=consumer_evidence_seq,
        lag_events=lag_events,
        head_captured_at=head_captured_at,
    )


def sample_event_log_cursor_freshness_sync(
    conn: _SyncConnection,
    *,
    channel: str,
    subscriber_id: str,
    projection_id: str,
    observed_at: datetime | None = None,
) -> ProjectionFreshness:
    """Sync variant of :func:`sample_event_log_cursor_freshness`."""

    now = _as_utc(observed_at) or _utc_now()
    head_row = conn.fetchrow(
        "SELECT MAX(id) AS head_id, MAX(emitted_at) AS head_at "
        "FROM event_log WHERE channel = $1",
        channel,
    )
    cursor_row = conn.fetchrow(
        "SELECT last_event_id, updated_at "
        "FROM event_log_cursors WHERE subscriber_id = $1 AND channel = $2",
        subscriber_id,
        channel,
    )
    return _compose_event_log_freshness(
        head_row=head_row,
        cursor_row=cursor_row,
        channel=channel,
        subscriber_id=subscriber_id,
        projection_id=projection_id,
        now=now,
    )


def sample_outbox_cursor_freshness_sync(
    conn: _SyncConnection,
    *,
    run_id: str,
    consumer_evidence_seq: int | None,
    projection_id: str,
    observed_at: datetime | None = None,
) -> ProjectionFreshness:
    """Sync variant of :func:`sample_outbox_cursor_freshness`."""

    now = _as_utc(observed_at) or _utc_now()
    normalized_consumer = int(consumer_evidence_seq) if consumer_evidence_seq is not None else 0
    head_row = conn.fetchrow(
        "SELECT MAX(evidence_seq) AS head_seq, MAX(captured_at) AS head_captured_at "
        "FROM workflow_outbox WHERE run_id = $1",
        run_id,
    )
    return _compose_outbox_freshness(
        head_row=head_row,
        run_id=run_id,
        consumer_evidence_seq=normalized_consumer,
        projection_id=projection_id,
        now=now,
    )


def collect_projection_freshness_sync(
    conn: _SyncConnection,
    *,
    observed_at: datetime | None = None,
) -> tuple[ProjectionFreshness, ...]:
    """Sync variant of :func:`collect_projection_freshness`.

    Uses a :class:`SyncPostgresConnection`-shaped object (``conn.fetchrow``
    returns synchronously) so callers on the sync side — health probes,
    CLI surfaces — can collect freshness without running an event loop.
    """

    from runtime.bug_candidates_projection_subscriber import (
        BUG_CANDIDATES_PROJECTION_ID as _BUG_CANDIDATES_PROJECTION_ID,
        DEFAULT_SUBSCRIBER_ID as _BUG_CANDIDATES_SUBSCRIBER_ID,
    )
    from runtime.circuit_breaker import manual_override_cache_refresh_state
    from runtime.operator_decision_projection_subscriber import (
        DEFAULT_SUBSCRIBER_ID as _DECISION_SUBSCRIBER_ID,
        OPERATOR_DECISION_PROJECTION_ID as _DECISION_PROJECTION_ID,
    )
    from runtime.route_authority_snapshot import iter_route_authority_cache_states
    from runtime.semantic_projection_subscriber import (
        DEFAULT_SUBSCRIBER_ID as _SEMANTIC_SUBSCRIBER_ID,
        SEMANTIC_PROJECTION_ID as _SEMANTIC_PROJECTION_ID,
    )
    from runtime.event_log import CHANNEL_RECEIPT, CHANNEL_SEMANTIC_ASSERTION

    now = _as_utc(observed_at) or _utc_now()
    samples: list[ProjectionFreshness] = [
        sample_event_log_cursor_freshness_sync(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            subscriber_id=_SEMANTIC_SUBSCRIBER_ID,
            projection_id=_SEMANTIC_PROJECTION_ID,
            observed_at=now,
        ),
        sample_event_log_cursor_freshness_sync(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            subscriber_id=_DECISION_SUBSCRIBER_ID,
            projection_id=_DECISION_PROJECTION_ID,
            observed_at=now,
        ),
        sample_event_log_cursor_freshness_sync(
            conn,
            channel=CHANNEL_RECEIPT,
            subscriber_id=_BUG_CANDIDATES_SUBSCRIBER_ID,
            projection_id=_BUG_CANDIDATES_PROJECTION_ID,
            observed_at=now,
        ),
    ]
    breaker_epoch, breaker_refreshed_at = manual_override_cache_refresh_state()
    samples.append(
        sample_process_cache_freshness(
            projection_id="circuit_breaker_manual_override_cache",
            cache_key="process_local",
            epoch=breaker_epoch,
            last_refreshed_at=breaker_refreshed_at,
            observed_at=now,
        )
    )
    for cache_key, epoch, last_refreshed_at in iter_route_authority_cache_states():
        samples.append(
            sample_process_cache_freshness(
                projection_id="route_authority_snapshot",
                cache_key=cache_key,
                epoch=epoch,
                last_refreshed_at=last_refreshed_at,
                observed_at=now,
            )
        )
    return tuple(samples)


async def collect_projection_freshness(
    conn: _AsyncConnection,
    *,
    observed_at: datetime | None = None,
) -> tuple[ProjectionFreshness, ...]:
    """Return freshness samples for well-known process-wide projections.

    Includes:
      - the semantic_current_assertions projection (event-log cursor),
      - the circuit-breaker manual-override cache (process cache),
      - every live route-authority cache key (process cache).

    Per-run outbox lag is not collected here — outbox subscribers carry
    their own ``consumer_evidence_seq`` and should call
    :func:`sample_outbox_cursor_freshness` explicitly.
    """

    from runtime.bug_candidates_projection_subscriber import (
        BUG_CANDIDATES_PROJECTION_ID as _BUG_CANDIDATES_PROJECTION_ID,
        DEFAULT_SUBSCRIBER_ID as _BUG_CANDIDATES_SUBSCRIBER_ID,
    )
    from runtime.circuit_breaker import manual_override_cache_refresh_state
    from runtime.operator_decision_projection_subscriber import (
        DEFAULT_SUBSCRIBER_ID as _DECISION_SUBSCRIBER_ID,
        OPERATOR_DECISION_PROJECTION_ID as _DECISION_PROJECTION_ID,
    )
    from runtime.route_authority_snapshot import iter_route_authority_cache_states
    from runtime.semantic_projection_subscriber import (
        DEFAULT_SUBSCRIBER_ID as _SEMANTIC_SUBSCRIBER_ID,
        SEMANTIC_PROJECTION_ID as _SEMANTIC_PROJECTION_ID,
    )
    from runtime.event_log import CHANNEL_RECEIPT, CHANNEL_SEMANTIC_ASSERTION

    now = _as_utc(observed_at) or _utc_now()
    samples: list[ProjectionFreshness] = []
    samples.append(
        await sample_event_log_cursor_freshness(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            subscriber_id=_SEMANTIC_SUBSCRIBER_ID,
            projection_id=_SEMANTIC_PROJECTION_ID,
            observed_at=now,
        )
    )
    samples.append(
        await sample_event_log_cursor_freshness(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            subscriber_id=_DECISION_SUBSCRIBER_ID,
            projection_id=_DECISION_PROJECTION_ID,
            observed_at=now,
        )
    )
    samples.append(
        await sample_event_log_cursor_freshness(
            conn,
            channel=CHANNEL_RECEIPT,
            subscriber_id=_BUG_CANDIDATES_SUBSCRIBER_ID,
            projection_id=_BUG_CANDIDATES_PROJECTION_ID,
            observed_at=now,
        )
    )
    breaker_epoch, breaker_refreshed_at = manual_override_cache_refresh_state()
    samples.append(
        sample_process_cache_freshness(
            projection_id="circuit_breaker_manual_override_cache",
            cache_key="process_local",
            epoch=breaker_epoch,
            last_refreshed_at=breaker_refreshed_at,
            observed_at=now,
        )
    )
    for cache_key, epoch, last_refreshed_at in iter_route_authority_cache_states():
        samples.append(
            sample_process_cache_freshness(
                projection_id="route_authority_snapshot",
                cache_key=cache_key,
                epoch=epoch,
                last_refreshed_at=last_refreshed_at,
                observed_at=now,
            )
        )
    return tuple(samples)


__all__ = [
    "EVENT_LOG_CURSOR",
    "OUTBOX_CURSOR",
    "PROCESS_CACHE",
    "ProjectionFreshness",
    "ProjectionFreshnessAlert",
    "ProjectionFreshnessSlaPolicy",
    "ProjectionFreshnessSlaReport",
    "READ_SIDE_CIRCUIT_CLOSED",
    "READ_SIDE_CIRCUIT_OPEN",
    "SLA_CRITICAL",
    "SLA_HEALTHY",
    "SLA_UNKNOWN",
    "SLA_WARNING",
    "collect_projection_freshness",
    "collect_projection_freshness_sync",
    "evaluate_projection_freshness_sla",
    "sample_event_log_cursor_freshness",
    "sample_event_log_cursor_freshness_sync",
    "sample_outbox_cursor_freshness",
    "sample_outbox_cursor_freshness_sync",
    "sample_process_cache_freshness",
]
