from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from runtime.projection_freshness import (
    EVENT_LOG_CURSOR,
    OUTBOX_CURSOR,
    PROCESS_CACHE,
    ProjectionFreshness,
    ProjectionFreshnessSlaPolicy,
    READ_SIDE_CIRCUIT_CLOSED,
    READ_SIDE_CIRCUIT_OPEN,
    SLA_CRITICAL,
    SLA_HEALTHY,
    SLA_UNKNOWN,
    SLA_WARNING,
    collect_projection_freshness,
    collect_projection_freshness_sync,
    evaluate_projection_freshness_sla,
    sample_event_log_cursor_freshness,
    sample_event_log_cursor_freshness_sync,
    sample_outbox_cursor_freshness,
    sample_outbox_cursor_freshness_sync,
    sample_process_cache_freshness,
)


_BASE_NOW = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)


class _FakeAsyncConn:
    """Minimal async connection double that answers a scripted queue of fetchrow calls."""

    def __init__(self, rows: list[dict[str, Any] | None]) -> None:
        self._rows = list(rows)
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: object) -> Any:
        self.queries.append((query, args))
        if not self._rows:
            return None
        return self._rows.pop(0)


class _FakeSyncConn:
    """Sync twin of :class:`_FakeAsyncConn` for sync-path samplers."""

    def __init__(self, rows: list[dict[str, Any] | None]) -> None:
        self._rows = list(rows)
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def fetchrow(self, query: str, *args: object) -> Any:
        self.queries.append((query, args))
        if not self._rows:
            return None
        return self._rows.pop(0)


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_process_cache_sample_reports_elapsed_seconds_and_serializes() -> None:
    last_refreshed = _BASE_NOW - timedelta(seconds=42)

    sample = sample_process_cache_freshness(
        projection_id="circuit_breaker_manual_override_cache",
        cache_key="process_local",
        epoch=3,
        last_refreshed_at=last_refreshed,
        observed_at=_BASE_NOW,
    )

    assert sample.source_kind == PROCESS_CACHE
    assert sample.epoch == 3
    assert sample.cache_key == "process_local"
    assert sample.staleness_seconds == pytest.approx(42.0)
    assert sample.last_refreshed_at == last_refreshed

    payload = sample.to_json()
    assert payload["source_kind"] == PROCESS_CACHE
    assert payload["epoch"] == 3
    assert payload["last_refreshed_at"] == last_refreshed.isoformat()
    assert "run_id" not in payload
    assert "channel" not in payload


def test_process_cache_sample_with_unpopulated_cache_has_null_staleness() -> None:
    sample = sample_process_cache_freshness(
        projection_id="route_authority_snapshot",
        cache_key="workflow_pool:test",
        epoch=0,
        last_refreshed_at=None,
        observed_at=_BASE_NOW,
    )

    assert sample.staleness_seconds is None
    assert sample.last_refreshed_at is None


def test_event_log_cursor_sample_computes_lag_and_age() -> None:
    advanced_at = _BASE_NOW - timedelta(seconds=7)
    head_at = _BASE_NOW - timedelta(seconds=2)
    conn = _FakeAsyncConn(
        rows=[
            {"head_id": 120, "head_at": head_at},
            {"last_event_id": 95, "updated_at": advanced_at},
        ]
    )

    sample = _run(
        sample_event_log_cursor_freshness(
            conn,
            channel="semantic_assertion",
            subscriber_id="semantic_projection_refresher",
            projection_id="semantic_current_assertions",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.source_kind == EVENT_LOG_CURSOR
    assert sample.head_event_id == 120
    assert sample.cursor_event_id == 95
    assert sample.lag_events == 25
    assert sample.head_emitted_at == head_at
    assert sample.cursor_advanced_at == advanced_at
    assert sample.staleness_seconds == pytest.approx(7.0)


def test_event_log_cursor_sample_handles_empty_channel_and_missing_cursor() -> None:
    conn = _FakeAsyncConn(
        rows=[
            {"head_id": None, "head_at": None},
            None,
        ]
    )

    sample = _run(
        sample_event_log_cursor_freshness(
            conn,
            channel="semantic_assertion",
            subscriber_id="semantic_projection_refresher",
            projection_id="semantic_current_assertions",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.head_event_id == 0
    assert sample.cursor_event_id == 0
    assert sample.lag_events == 0
    assert sample.cursor_advanced_at is None
    assert sample.staleness_seconds is None


def test_event_log_cursor_sample_clamps_lag_when_cursor_is_ahead() -> None:
    conn = _FakeAsyncConn(
        rows=[
            {"head_id": 10, "head_at": _BASE_NOW},
            {"last_event_id": 42, "updated_at": _BASE_NOW},
        ]
    )

    sample = _run(
        sample_event_log_cursor_freshness(
            conn,
            channel="semantic_assertion",
            subscriber_id="test",
            projection_id="semantic_current_assertions",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.head_event_id == 10
    assert sample.cursor_event_id == 42
    assert sample.lag_events == 0


def test_outbox_cursor_sample_computes_lag_and_captured_age() -> None:
    captured_at = _BASE_NOW - timedelta(seconds=3)
    conn = _FakeAsyncConn(
        rows=[
            {"head_seq": 17, "head_captured_at": captured_at},
        ]
    )

    sample = _run(
        sample_outbox_cursor_freshness(
            conn,
            run_id="run-123",
            consumer_evidence_seq=12,
            projection_id="workflow_outbox_run",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.source_kind == OUTBOX_CURSOR
    assert sample.run_id == "run-123"
    assert sample.head_evidence_seq == 17
    assert sample.consumer_evidence_seq == 12
    assert sample.lag_events == 5
    assert sample.head_captured_at == captured_at
    assert sample.staleness_seconds == pytest.approx(3.0)


def test_outbox_cursor_sample_caught_up_reports_zero_staleness() -> None:
    captured_at = _BASE_NOW - timedelta(seconds=60)
    conn = _FakeAsyncConn(
        rows=[
            {"head_seq": 17, "head_captured_at": captured_at},
        ]
    )

    sample = _run(
        sample_outbox_cursor_freshness(
            conn,
            run_id="run-123",
            consumer_evidence_seq=17,
            projection_id="workflow_outbox_run",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.lag_events == 0
    assert sample.staleness_seconds == 0.0


def test_outbox_cursor_sample_handles_empty_run() -> None:
    conn = _FakeAsyncConn(
        rows=[
            {"head_seq": None, "head_captured_at": None},
        ]
    )

    sample = _run(
        sample_outbox_cursor_freshness(
            conn,
            run_id="run-123",
            consumer_evidence_seq=None,
            projection_id="workflow_outbox_run",
            observed_at=_BASE_NOW,
        )
    )

    assert sample.head_evidence_seq == 0
    assert sample.consumer_evidence_seq == 0
    assert sample.lag_events == 0


def test_projection_freshness_to_json_only_includes_variant_fields() -> None:
    event_sample = ProjectionFreshness(
        projection_id="p",
        source_kind=EVENT_LOG_CURSOR,
        observed_at=_BASE_NOW,
        staleness_seconds=1.0,
        channel="semantic_assertion",
        subscriber_id="sub",
        cursor_event_id=1,
        head_event_id=2,
        lag_events=1,
    )
    outbox_sample = ProjectionFreshness(
        projection_id="p",
        source_kind=OUTBOX_CURSOR,
        observed_at=_BASE_NOW,
        staleness_seconds=0.0,
        run_id="r",
        head_evidence_seq=5,
        consumer_evidence_seq=5,
        lag_events=0,
    )

    event_payload = event_sample.to_json()
    outbox_payload = outbox_sample.to_json()

    assert event_payload["channel"] == "semantic_assertion"
    assert "cache_key" not in event_payload
    assert "run_id" not in event_payload

    assert outbox_payload["run_id"] == "r"
    assert "channel" not in outbox_payload
    assert "cache_key" not in outbox_payload


def test_projection_freshness_sla_reports_healthy_when_samples_are_current() -> None:
    policy = ProjectionFreshnessSlaPolicy(
        warning_staleness_seconds=300.0,
        critical_staleness_seconds=900.0,
        warning_lag_events=0,
        critical_lag_events=100,
    )
    report = evaluate_projection_freshness_sla(
        (
            ProjectionFreshness(
                projection_id="semantic_current_assertions",
                source_kind=EVENT_LOG_CURSOR,
                observed_at=_BASE_NOW,
                staleness_seconds=1.0,
                lag_events=0,
            ),
        ),
        policy=policy,
    )

    assert report.status == SLA_HEALTHY
    assert report.read_side_circuit_breaker == READ_SIDE_CIRCUIT_CLOSED
    assert report.alert_count == 0
    assert report.to_json()["policy"]["policy_source"] == "platform_config"


def test_projection_freshness_sla_warns_on_any_event_lag() -> None:
    policy = ProjectionFreshnessSlaPolicy(
        warning_staleness_seconds=300.0,
        critical_staleness_seconds=900.0,
        warning_lag_events=0,
        critical_lag_events=100,
    )
    report = evaluate_projection_freshness_sla(
        (
            ProjectionFreshness(
                projection_id="bug_candidates_current",
                source_kind=EVENT_LOG_CURSOR,
                observed_at=_BASE_NOW,
                staleness_seconds=4.0,
                lag_events=1,
            ),
        ),
        policy=policy,
    )

    assert report.status == SLA_WARNING
    assert report.read_side_circuit_breaker == READ_SIDE_CIRCUIT_CLOSED
    assert report.alerts[0].reason_code == "projection_lag_events_warning"


def test_projection_freshness_sla_opens_read_side_circuit_on_critical_breach() -> None:
    policy = ProjectionFreshnessSlaPolicy(
        warning_staleness_seconds=300.0,
        critical_staleness_seconds=900.0,
        warning_lag_events=0,
        critical_lag_events=100,
    )
    report = evaluate_projection_freshness_sla(
        (
            ProjectionFreshness(
                projection_id="operator_decisions_current",
                source_kind=EVENT_LOG_CURSOR,
                observed_at=_BASE_NOW,
                staleness_seconds=901.0,
                lag_events=2,
            ),
        ),
        policy=policy,
    )

    assert report.status == SLA_CRITICAL
    assert report.read_side_circuit_breaker == READ_SIDE_CIRCUIT_OPEN
    assert report.alerts[0].reason_code == "projection_staleness_seconds_critical"
    assert report.alerts[0].read_side_circuit_breaker == READ_SIDE_CIRCUIT_OPEN


def test_projection_freshness_sla_marks_unmeasured_samples_unknown() -> None:
    policy = ProjectionFreshnessSlaPolicy(
        warning_staleness_seconds=300.0,
        critical_staleness_seconds=900.0,
        warning_lag_events=0,
        critical_lag_events=100,
    )
    report = evaluate_projection_freshness_sla(
        (
            ProjectionFreshness(
                projection_id="route_authority_snapshot",
                source_kind=PROCESS_CACHE,
                observed_at=_BASE_NOW,
                staleness_seconds=None,
                cache_key="workflow_pool:test",
            ),
        ),
        policy=policy,
    )

    assert report.status == SLA_UNKNOWN
    assert report.unknown_projection_ids == ("route_authority_snapshot",)


def test_route_authority_iter_states_tracks_refresh_and_invalidate() -> None:
    from runtime.route_authority_snapshot import (
        RouteAuthoritySnapshot,
        RouteAuthoritySnapshotStore,
    )

    class _ExplicitKeyConn:
        def __init__(self, cache_key: str) -> None:
            self._authority_cache_key = cache_key
            self._authority_scope = object()

    store = RouteAuthoritySnapshotStore()

    def _load_snapshot(_conn: object) -> RouteAuthoritySnapshot:
        return RouteAuthoritySnapshot(
            route_policy={"default": "ok"},
            failure_zones={},
            task_profiles={},
            benchmark_metrics={},
        )

    assert store.iter_strong_cache_states() == []

    conn = _ExplicitKeyConn("workflow_pool:observability-test")
    store.get_snapshot(conn, load_snapshot=_load_snapshot)

    states = store.iter_strong_cache_states()
    assert len(states) == 1
    cache_key, epoch, refreshed_at = states[0]
    assert cache_key == "workflow_pool:observability-test"
    assert epoch == 0
    assert isinstance(refreshed_at, datetime)

    store.invalidate_cache_key("workflow_pool:observability-test")
    states_after = store.iter_strong_cache_states()
    assert len(states_after) == 1
    cache_key_after, epoch_after, _ = states_after[0]
    assert cache_key_after == "workflow_pool:observability-test"
    assert epoch_after == epoch + 1


def test_circuit_breaker_manual_override_refresh_state_tracks_epoch_and_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from runtime import circuit_breaker as circuit_breaker_module

    module = importlib.reload(circuit_breaker_module)

    monkeypatch.setattr(module, "_require_cb_config", lambda: (3, 45.0))
    monkeypatch.setattr(
        module.CircuitBreakerRegistry,
        "_query_manual_overrides",
        lambda self: {},
    )

    registry = module.get_circuit_breakers()

    epoch_before, refreshed_before = registry.manual_override_cache_refresh_state()
    assert epoch_before == 0
    assert refreshed_before is None

    registry._manual_override_map()

    epoch_after_load, refreshed_after_load = registry.manual_override_cache_refresh_state()
    assert epoch_after_load == 0
    assert isinstance(refreshed_after_load, datetime)

    registry.invalidate_manual_override_cache()

    epoch_after_invalidate, refreshed_after_invalidate = (
        registry.manual_override_cache_refresh_state()
    )
    assert epoch_after_invalidate == epoch_after_load + 1
    assert refreshed_after_invalidate == refreshed_after_load


def test_collect_projection_freshness_aggregates_known_projections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    head_at = _BASE_NOW - timedelta(seconds=1)
    semantic_cursor_at = _BASE_NOW - timedelta(seconds=5)
    decision_cursor_at = _BASE_NOW - timedelta(seconds=7)
    bug_candidates_cursor_at = _BASE_NOW - timedelta(seconds=3)
    conn = _FakeAsyncConn(
        rows=[
            {"head_id": 10, "head_at": head_at},
            {"last_event_id": 9, "updated_at": semantic_cursor_at},
            {"head_id": 10, "head_at": head_at},
            {"last_event_id": 8, "updated_at": decision_cursor_at},
            {"head_id": 15, "head_at": head_at},
            {"last_event_id": 12, "updated_at": bug_candidates_cursor_at},
        ]
    )

    from runtime import circuit_breaker as circuit_breaker_module
    from runtime import route_authority_snapshot as route_authority_module

    monkeypatch.setattr(
        circuit_breaker_module,
        "manual_override_cache_refresh_state",
        lambda: (2, _BASE_NOW - timedelta(seconds=11)),
    )
    monkeypatch.setattr(
        route_authority_module,
        "iter_route_authority_cache_states",
        lambda: [
            ("workflow_pool:test-a", 4, _BASE_NOW - timedelta(seconds=2)),
            ("workflow_pool:test-b", 1, None),
        ],
    )

    samples = _run(collect_projection_freshness(conn, observed_at=_BASE_NOW))

    kinds = [s.source_kind for s in samples]
    assert kinds == [
        EVENT_LOG_CURSOR,
        EVENT_LOG_CURSOR,
        EVENT_LOG_CURSOR,
        PROCESS_CACHE,
        PROCESS_CACHE,
        PROCESS_CACHE,
    ]

    semantic_sample = samples[0]
    assert semantic_sample.projection_id == "semantic_current_assertions"
    assert semantic_sample.lag_events == 1
    assert semantic_sample.staleness_seconds == pytest.approx(5.0)

    decision_sample = samples[1]
    assert decision_sample.projection_id == "operator_decisions_current"
    assert decision_sample.subscriber_id == "operator_decision_projection_refresher"
    assert decision_sample.lag_events == 2
    assert decision_sample.staleness_seconds == pytest.approx(7.0)

    bug_candidates_sample = samples[2]
    assert bug_candidates_sample.projection_id == "bug_candidates_current"
    assert bug_candidates_sample.subscriber_id == "bug_candidates_refresher"
    assert bug_candidates_sample.lag_events == 3
    assert bug_candidates_sample.staleness_seconds == pytest.approx(3.0)

    breaker_sample = samples[3]
    assert breaker_sample.projection_id == "circuit_breaker_manual_override_cache"
    assert breaker_sample.epoch == 2
    assert breaker_sample.staleness_seconds == pytest.approx(11.0)

    route_sample_a = samples[4]
    route_sample_b = samples[5]
    assert route_sample_a.projection_id == "route_authority_snapshot"
    assert route_sample_a.cache_key == "workflow_pool:test-a"
    assert route_sample_a.epoch == 4
    assert route_sample_a.staleness_seconds == pytest.approx(2.0)
    assert route_sample_b.cache_key == "workflow_pool:test-b"
    assert route_sample_b.staleness_seconds is None


def test_event_log_cursor_sync_matches_async_shape() -> None:
    advanced_at = _BASE_NOW - timedelta(seconds=4)
    head_at = _BASE_NOW - timedelta(seconds=1)
    conn = _FakeSyncConn(
        rows=[
            {"head_id": 50, "head_at": head_at},
            {"last_event_id": 48, "updated_at": advanced_at},
        ]
    )

    sample = sample_event_log_cursor_freshness_sync(
        conn,
        channel="semantic_assertion",
        subscriber_id="semantic_projection_refresher",
        projection_id="semantic_current_assertions",
        observed_at=_BASE_NOW,
    )

    assert sample.source_kind == EVENT_LOG_CURSOR
    assert sample.head_event_id == 50
    assert sample.cursor_event_id == 48
    assert sample.lag_events == 2
    assert sample.cursor_advanced_at == advanced_at
    assert sample.staleness_seconds == pytest.approx(4.0)


def test_event_log_cursor_sync_handles_empty_channel() -> None:
    conn = _FakeSyncConn(
        rows=[
            {"head_id": None, "head_at": None},
            None,
        ]
    )

    sample = sample_event_log_cursor_freshness_sync(
        conn,
        channel="semantic_assertion",
        subscriber_id="semantic_projection_refresher",
        projection_id="semantic_current_assertions",
        observed_at=_BASE_NOW,
    )

    assert sample.head_event_id == 0
    assert sample.cursor_event_id == 0
    assert sample.lag_events == 0
    assert sample.staleness_seconds is None


def test_outbox_cursor_sync_computes_lag() -> None:
    captured_at = _BASE_NOW - timedelta(seconds=2)
    conn = _FakeSyncConn(
        rows=[
            {"head_seq": 9, "head_captured_at": captured_at},
        ]
    )

    sample = sample_outbox_cursor_freshness_sync(
        conn,
        run_id="run-xyz",
        consumer_evidence_seq=4,
        projection_id="workflow_outbox_run",
        observed_at=_BASE_NOW,
    )

    assert sample.source_kind == OUTBOX_CURSOR
    assert sample.head_evidence_seq == 9
    assert sample.consumer_evidence_seq == 4
    assert sample.lag_events == 5
    assert sample.staleness_seconds == pytest.approx(2.0)


def test_outbox_cursor_sync_caught_up_is_zero_staleness() -> None:
    captured_at = _BASE_NOW - timedelta(seconds=90)
    conn = _FakeSyncConn(
        rows=[
            {"head_seq": 9, "head_captured_at": captured_at},
        ]
    )

    sample = sample_outbox_cursor_freshness_sync(
        conn,
        run_id="run-xyz",
        consumer_evidence_seq=9,
        projection_id="workflow_outbox_run",
        observed_at=_BASE_NOW,
    )

    assert sample.lag_events == 0
    assert sample.staleness_seconds == 0.0


def test_collect_projection_freshness_sync_aggregates_known_projections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    head_at = _BASE_NOW - timedelta(seconds=1)
    semantic_cursor_at = _BASE_NOW - timedelta(seconds=6)
    decision_cursor_at = _BASE_NOW - timedelta(seconds=4)
    bug_candidates_cursor_at = _BASE_NOW - timedelta(seconds=2)
    conn = _FakeSyncConn(
        rows=[
            {"head_id": 8, "head_at": head_at},
            {"last_event_id": 6, "updated_at": semantic_cursor_at},
            {"head_id": 8, "head_at": head_at},
            {"last_event_id": 7, "updated_at": decision_cursor_at},
            {"head_id": 20, "head_at": head_at},
            {"last_event_id": 15, "updated_at": bug_candidates_cursor_at},
        ]
    )

    from runtime import circuit_breaker as circuit_breaker_module
    from runtime import route_authority_snapshot as route_authority_module

    monkeypatch.setattr(
        circuit_breaker_module,
        "manual_override_cache_refresh_state",
        lambda: (1, _BASE_NOW - timedelta(seconds=9)),
    )
    monkeypatch.setattr(
        route_authority_module,
        "iter_route_authority_cache_states",
        lambda: [("workflow_pool:sync", 2, _BASE_NOW - timedelta(seconds=3))],
    )

    samples = collect_projection_freshness_sync(conn, observed_at=_BASE_NOW)

    kinds = [s.source_kind for s in samples]
    assert kinds == [
        EVENT_LOG_CURSOR,
        EVENT_LOG_CURSOR,
        EVENT_LOG_CURSOR,
        PROCESS_CACHE,
        PROCESS_CACHE,
    ]

    semantic_sample = samples[0]
    assert semantic_sample.projection_id == "semantic_current_assertions"
    assert semantic_sample.lag_events == 2
    assert semantic_sample.staleness_seconds == pytest.approx(6.0)

    decision_sample = samples[1]
    assert decision_sample.projection_id == "operator_decisions_current"
    assert decision_sample.subscriber_id == "operator_decision_projection_refresher"
    assert decision_sample.lag_events == 1
    assert decision_sample.staleness_seconds == pytest.approx(4.0)

    bug_candidates_sample = samples[2]
    assert bug_candidates_sample.projection_id == "bug_candidates_current"
    assert bug_candidates_sample.subscriber_id == "bug_candidates_refresher"
    assert bug_candidates_sample.lag_events == 5
    assert bug_candidates_sample.staleness_seconds == pytest.approx(2.0)

    breaker_sample = samples[3]
    assert breaker_sample.projection_id == "circuit_breaker_manual_override_cache"
    assert breaker_sample.epoch == 1
    assert breaker_sample.staleness_seconds == pytest.approx(9.0)

    route_sample = samples[4]
    assert route_sample.projection_id == "route_authority_snapshot"
    assert route_sample.cache_key == "workflow_pool:sync"
    assert route_sample.epoch == 2
    assert route_sample.staleness_seconds == pytest.approx(3.0)
