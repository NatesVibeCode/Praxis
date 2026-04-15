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


def test_build_modules_include_rate_limit_prober_when_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat_runner, "MemoryEngine", _Engine)
    runner = heartbeat_runner.HeartbeatRunner(
        engine_db_path=str(tmp_path / "test.db"),
        results_dir=str(tmp_path / "results"),
        conn=_Conn([]),
    )

    module_names = {getattr(module, "name", "") for module in runner.build_modules()}

    assert "rate_limit_prober" in module_names


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
