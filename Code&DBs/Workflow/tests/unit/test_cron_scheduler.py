"""Tests for runtime.cron_scheduler and heartbeat wiring."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sys
from pathlib import Path
import types


def _import_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
cron_scheduler = _import_module(
    "runtime.cron_scheduler",
    _WORKFLOW_ROOT / "runtime" / "cron_scheduler.py",
)
heartbeat_runner = _import_module(
    "runtime.heartbeat_runner",
    _WORKFLOW_ROOT / "runtime" / "heartbeat_runner.py",
)


class _Conn:
    def __init__(self, triggers: list[dict[str, object]]) -> None:
        self._triggers = triggers
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT id, workflow_id, cron_expression, last_fired_at FROM workflow_triggers"):
            return self._triggers
        return []


class _CleanupMissingConn(_Conn):
    def execute(self, query: str, *args):
        if "to_regprocedure('cleanup_system_events(integer)')" in query:
            self.calls.append((query, args))
            return [{"procedure_name": None}]
        if "cleanup_system_events(30)" in query:
            raise RuntimeError("function cleanup_system_events(integer) does not exist")
        return super().execute(query, *args)


class _Engine:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def _connect(self):
        return None


def test_tick_fires_due_schedule_trigger():
    now = datetime.now(timezone.utc)
    conn = _Conn(
        [
            {
                "id": "trig-1",
                "workflow_id": "wf-1",
                "cron_expression": "@hourly",
                "last_fired_at": now - timedelta(hours=2),
            }
        ]
    )

    fired = cron_scheduler.CronScheduler(conn).tick()

    assert fired == 1
    insert_calls = [
        args
        for query, args in conn.calls
        if "INSERT INTO system_events" in query and args and args[0] == "schedule.fired"
    ]
    assert len(insert_calls) == 1
    payload = json.loads(insert_calls[0][3])
    assert payload == {
        "trigger_id": "trig-1",
        "workflow_id": "wf-1",
        "cron_expression": "@hourly",
    }
    assert any("UPDATE workflow_triggers SET last_fired_at = NOW(), fire_count = fire_count + 1" in query for query, _ in conn.calls)


def test_tick_skips_not_due_schedule_trigger():
    now = datetime(2026, 4, 11, 12, 10, tzinfo=timezone.utc)
    conn = _Conn(
        [
            {
                "id": "trig-1",
                "workflow_id": "wf-1",
                "cron_expression": "*/15 * * * *",
                "last_fired_at": now - timedelta(minutes=4),
            }
        ]
    )

    original_utc_now = cron_scheduler._utc_now
    cron_scheduler._utc_now = lambda: now
    try:
        fired = cron_scheduler.CronScheduler(conn).tick()
    finally:
        cron_scheduler._utc_now = original_utc_now

    assert fired == 0
    assert not any("INSERT INTO system_events" in query for query, _ in conn.calls)


def test_tick_fires_when_never_fired():
    conn = _Conn(
        [
            {
                "id": "trig-2",
                "workflow_id": "wf-2",
                "cron_expression": "@daily",
                "last_fired_at": None,
            }
        ]
    )

    fired = cron_scheduler.CronScheduler(conn).tick()

    assert fired == 1


def test_build_modules_includes_cron_module_when_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat_runner, "MemoryEngine", _Engine)
    runner = heartbeat_runner.HeartbeatRunner(
        engine_db_path=str(tmp_path / "test.db"),
        results_dir=str(tmp_path / "results"),
        conn=_Conn([]),
        embedder=object(),
    )

    modules = runner.build_modules()

    assert any(getattr(module, "name", "") == "cron_scheduler" for module in modules)


def test_build_modules_include_orchestrator_when_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat_runner, "MemoryEngine", _Engine)
    runner = heartbeat_runner.HeartbeatRunner(
        engine_db_path=str(tmp_path / "test.db"),
        results_dir=str(tmp_path / "results"),
        conn=_Conn([]),
        embedder=object(),
    )

    module_names = {getattr(module, "name", "") for module in runner.build_modules()}

    assert "trigger_evaluator" in module_names
    assert "auto_review_flush" in module_names
    assert "relationship_integrity_scanner" in module_names
    assert "schema_consistency_scanner" in module_names
    assert "content_quality_scanner" in module_names


def test_build_modules_include_rate_limit_prober_when_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat_runner, "MemoryEngine", _Engine)
    runner = heartbeat_runner.HeartbeatRunner(
        engine_db_path=str(tmp_path / "test.db"),
        results_dir=str(tmp_path / "results"),
        conn=_Conn([]),
    )

    module_names = {getattr(module, "name", "") for module in runner.build_modules()}

    assert "rate_limit_prober" in module_names


def test_build_modules_include_semantic_projection_refresh_when_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat_runner, "MemoryEngine", _Engine)
    runner = heartbeat_runner.HeartbeatRunner(
        engine_db_path=str(tmp_path / "test.db"),
        results_dir=str(tmp_path / "results"),
        conn=_Conn([]),
        workflow_env={"WORKFLOW_DATABASE_URL": "postgresql://example"},
    )

    module_names = {getattr(module, "name", "") for module in runner.build_modules()}

    assert "semantic_projection_refresh" in module_names
    assert "authority_memory_refresh" in module_names
    assert "operator_decision_projection_refresh" in module_names
    assert "bug_candidates_refresh" not in module_names


def test_authority_memory_refresh_module_refreshes_projection(monkeypatch):
    fake_projection = types.ModuleType("runtime.authority_memory_projection")
    calls: list[dict[str, object]] = []

    async def _refresh_authority_memory_projection(*, env=None, as_of=None):
        calls.append({"env": dict(env or {}), "as_of": as_of})
        return {"refreshed": True}

    fake_projection.refresh_authority_memory_projection = _refresh_authority_memory_projection
    monkeypatch.setitem(sys.modules, "runtime.authority_memory_projection", fake_projection)

    result = heartbeat_runner._AuthorityMemoryProjectionRefreshModule(
        workflow_env={"WORKFLOW_DATABASE_URL": "postgresql://example"},
    ).run()

    assert result.module_name == "authority_memory_refresh"
    assert result.ok is True
    assert calls == [{"env": {"WORKFLOW_DATABASE_URL": "postgresql://example"}, "as_of": None}]


def test_authority_memory_refresh_module_skips_without_workflow_authority(monkeypatch):
    fake_projection = types.ModuleType("runtime.authority_memory_projection")

    async def _refresh_authority_memory_projection(*, env=None, as_of=None):
        raise AssertionError("authority memory refresh should not run without workflow authority")

    fake_projection.refresh_authority_memory_projection = _refresh_authority_memory_projection
    monkeypatch.setitem(sys.modules, "runtime.authority_memory_projection", fake_projection)
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    result = heartbeat_runner._AuthorityMemoryProjectionRefreshModule(workflow_env={}).run()

    assert result.module_name == "authority_memory_refresh"
    assert result.ok is True


def test_operator_decision_projection_refresh_module_refreshes_projection(monkeypatch):
    fake_projection = types.ModuleType("runtime.operator_decision_projection_subscriber")
    calls: list[dict[str, object]] = []

    class _Subscriber:
        def consume_available(self, *, limit=100, subscriber_id="operator_decision_projection_refresher", as_of=None, env=None):
            calls.append(
                {
                    "limit": limit,
                    "subscriber_id": subscriber_id,
                    "as_of": as_of,
                    "env": dict(env or {}),
                }
            )
            return {"refreshed": True}

    fake_projection.OperatorDecisionProjectionSubscriber = _Subscriber
    monkeypatch.setitem(sys.modules, "runtime.operator_decision_projection_subscriber", fake_projection)

    result = heartbeat_runner._OperatorDecisionProjectionRefreshModule(
        workflow_env={"WORKFLOW_DATABASE_URL": "postgresql://example"},
        limit=7,
    ).run()

    assert result.module_name == "operator_decision_projection_refresh"
    assert result.ok is True
    assert calls == [
        {
            "limit": 7,
            "subscriber_id": "operator_decision_projection_refresher",
            "as_of": None,
            "env": {"WORKFLOW_DATABASE_URL": "postgresql://example"},
        }
    ]


def test_bug_candidates_refresh_module_refreshes_projection(monkeypatch):
    fake_projection = types.ModuleType("runtime.bug_candidates_projection_subscriber")
    calls: list[dict[str, object]] = []

    class _Subscriber:
        def consume_available(self, *, limit=100, subscriber_id="bug_candidates_refresher", as_of=None, env=None):
            calls.append(
                {
                    "limit": limit,
                    "subscriber_id": subscriber_id,
                    "as_of": as_of,
                    "env": dict(env or {}),
                }
            )
            return {"refreshed": True}

    fake_projection.BugCandidatesProjectionSubscriber = _Subscriber
    monkeypatch.setitem(sys.modules, "runtime.bug_candidates_projection_subscriber", fake_projection)

    result = heartbeat_runner._BugCandidatesRefreshModule(
        workflow_env={"WORKFLOW_DATABASE_URL": "postgresql://example"},
        limit=9,
    ).run()

    assert result.module_name == "bug_candidates_refresh"
    assert result.ok is True
    assert calls == [
        {
            "limit": 9,
            "subscriber_id": "bug_candidates_refresher",
            "as_of": None,
            "env": {"WORKFLOW_DATABASE_URL": "postgresql://example"},
        }
    ]


def test_trigger_evaluator_module_runs_runtime_trigger_loop(monkeypatch):
    fake_triggers = types.ModuleType("runtime.triggers")
    calls: list[object] = []

    def _evaluate_triggers(conn):
        calls.append(conn)
        return 3

    fake_triggers.evaluate_triggers = _evaluate_triggers
    monkeypatch.setitem(sys.modules, "runtime.triggers", fake_triggers)

    conn = _Conn([])
    result = heartbeat_runner._TriggerEvaluatorModule(conn).run()

    assert calls == [conn]
    assert result.module_name == "trigger_evaluator"
    assert result.ok is True


def test_system_events_cleanup_skips_missing_function():
    conn = _CleanupMissingConn([])
    result = heartbeat_runner.SystemEventsCleanupModule(conn).run()

    assert result.module_name == "system_events_cleanup"
    assert result.ok is True
    assert not any("cleanup_system_events(30)" in query for query, _ in conn.calls)


def test_database_maintenance_module_drains_multiple_batches(monkeypatch):
    fake_maintenance = types.ModuleType("runtime.database_maintenance")
    run_limits: list[int] = []
    results = iter([
        types.SimpleNamespace(
            claimed=50,
            completed=50,
            skipped=0,
            failed=0,
            enqueued=0,
            findings=("embed_entity:entity-1",),
            errors=(),
        ),
        types.SimpleNamespace(
            claimed=50,
            completed=50,
            skipped=0,
            failed=0,
            enqueued=0,
            findings=("refresh_vector_neighbors:entity-1",),
            errors=(),
        ),
        types.SimpleNamespace(
            claimed=22,
            completed=22,
            skipped=0,
            failed=0,
            enqueued=0,
            findings=("refresh_vector_neighbors:entity-2",),
            errors=(),
        ),
    ])

    class _Processor:
        def __init__(self, conn, embedder=None) -> None:
            assert conn == "conn"
            assert embedder == "embedder"

        def run_once(self, limit: int = 25):
            run_limits.append(limit)
            return next(results)

    fake_maintenance.DatabaseMaintenanceProcessor = _Processor
    monkeypatch.setitem(sys.modules, "runtime.database_maintenance", fake_maintenance)

    result = heartbeat_runner._DatabaseMaintenanceModule("conn", embedder="embedder").run()

    assert run_limits == [50, 50, 50]
    assert result.module_name == "database_maintenance"
    assert result.ok is True


def test_auto_review_flush_module_runs_due_flush(monkeypatch):
    fake_auto_review = types.ModuleType("runtime.auto_review")
    calls: list[object] = []

    class _Accumulator:
        def flush_due(self):
            calls.append("flush_due")
            return "review-run-1"

    def _get_review_accumulator(conn=None):
        calls.append(conn)
        return _Accumulator()

    fake_auto_review.get_review_accumulator = _get_review_accumulator
    monkeypatch.setitem(sys.modules, "runtime.auto_review", fake_auto_review)

    result = heartbeat_runner._AutoReviewFlushModule("conn").run()

    assert calls == ["conn", "flush_due"]
    assert result.module_name == "auto_review_flush"
    assert result.ok is True


def test_semantic_projection_refresh_module_consumes_events(monkeypatch):
    fake_subscriber = types.ModuleType("runtime.semantic_projection_subscriber")
    calls: list[dict[str, object]] = []

    def _consume_semantic_projection_events(*, limit: int, env=None):
        calls.append({"limit": limit, "env": dict(env or {})})
        return {"refreshed": True}

    fake_subscriber.consume_semantic_projection_events = _consume_semantic_projection_events
    monkeypatch.setitem(sys.modules, "runtime.semantic_projection_subscriber", fake_subscriber)

    result = heartbeat_runner._SemanticProjectionRefreshModule(
        workflow_env={"WORKFLOW_DATABASE_URL": "postgresql://example"},
        limit=25,
    ).run()

    assert result.module_name == "semantic_projection_refresh"
    assert result.ok is True
    assert calls == [
        {
            "limit": 25,
            "env": {"WORKFLOW_DATABASE_URL": "postgresql://example"},
        }
    ]


def test_semantic_projection_refresh_module_skips_without_workflow_authority(monkeypatch):
    fake_subscriber = types.ModuleType("runtime.semantic_projection_subscriber")

    def _consume_semantic_projection_events(*, limit: int, env=None):
        raise AssertionError("semantic projection refresh should not run without workflow authority")

    fake_subscriber.consume_semantic_projection_events = _consume_semantic_projection_events
    monkeypatch.setitem(sys.modules, "runtime.semantic_projection_subscriber", fake_subscriber)
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    result = heartbeat_runner._SemanticProjectionRefreshModule(workflow_env={}).run()

    assert result.module_name == "semantic_projection_refresh"
    assert result.ok is True


def test_review_batch_accumulator_flush_due_waits_until_age_threshold(monkeypatch):
    import runtime.auto_review as auto_review

    accumulator = auto_review.ReviewBatchAccumulator(conn=None)
    accumulator._queue = [types.SimpleNamespace(run_id="run-1")]
    accumulator._first_added_at = 100.0
    accumulator._max_wait_seconds = 60.0

    flush_calls: list[str] = []

    def _flush():
        flush_calls.append("flush")
        return "review-run-1"

    monkeypatch.setattr(accumulator, "flush", _flush)
    monkeypatch.setattr(auto_review.time, "monotonic", lambda: 150.0)

    assert accumulator.flush_due() is None
    assert flush_calls == []

    monkeypatch.setattr(auto_review.time, "monotonic", lambda: 161.0)
    assert accumulator.flush_due() == "review-run-1"
    assert flush_calls == ["flush"]


def test_heartbeat_modules_return_ok_error_protocol():
    """Verify all wrapper modules return the simplified ok/error protocol."""
    from runtime.heartbeat import HeartbeatModuleResult
    conn = _Conn([])
    module = heartbeat_runner.SystemEventsCleanupModule(conn)
    result = module.run()
    assert isinstance(result, HeartbeatModuleResult)
    assert hasattr(result, "ok")
    assert hasattr(result, "error")
    assert not hasattr(result, "findings")
