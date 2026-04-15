"""Tests for runtime.scheduler schedule emission and dispatch wiring."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib
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
scheduler = _import_module(
    "runtime.scheduler",
    _WORKFLOW_ROOT / "runtime" / "scheduler.py",
)


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        return []


def test_run_scheduler_tick_emits_schedule_fired_event(tmp_path, monkeypatch):
    spec_path = tmp_path / "daily-report.json"
    spec_path.write_text("{\"prompt\": \"do work\"}\n", encoding="utf-8")
    state = scheduler.SchedulerState(state_path=str(tmp_path / "scheduler_state.json"))
    config = scheduler.SchedulerConfig(
        jobs=(
            scheduler.ScheduledJob(
                name="daily-report",
                spec_path=str(spec_path),
                cron_expression="0 12 * * *",
            ),
        ),
        config_path=str(tmp_path / "scheduler.json"),
    )
    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)
    event_conn = _Conn()

    monkeypatch.setattr(
        scheduler,
        "submit_workflow_command",
        lambda _conn, **params: {
            "run_id": "run-1",
            "status": "queued",
            "command_id": "control.command.request.1",
            "command_status": "succeeded",
            "result_ref": "workflow_run:run-1",
            **params,
        },
    )
    monkeypatch.setattr(scheduler, "_workflow_pg_conn", lambda: object())

    results = scheduler.run_scheduler_tick(
        config,
        state=state,
        now=now,
        event_conn=event_conn,
    )

    assert len(results) == 1
    assert results[0]["job_name"] == "daily-report"
    assert results[0]["status"] == "queued"
    assert results[0]["run_id"] == "run-1"
    assert results[0]["command_id"] == "control.command.request.1"
    assert results[0]["command_status"] == "succeeded"

    insert_calls = [
        args
        for query, args in event_conn.calls
        if "INSERT INTO system_events" in query and args and args[0] == "schedule.fired"
    ]
    assert len(insert_calls) == 1
    payload = json.loads(insert_calls[0][3])
    assert payload == {
        "job_name": "daily-report",
        "spec_path": str(spec_path),
        "cron_expression": "0 12 * * *",
        "last_run_at": None,
        "fired_at": now.isoformat(),
    }
    assert state.get_last_run("daily-report") is not None
    assert state.get_last_run("daily-report").isoformat() == now.isoformat()
