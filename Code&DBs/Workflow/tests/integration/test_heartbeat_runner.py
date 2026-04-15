"""Integration tests for HeartbeatRunner."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import importlib.util as _ilu

_rt_dir = Path(__file__).resolve().parent.parent.parent / "runtime"

_hbr = sys.modules.get("runtime.heartbeat_runner")
if _hbr is None:
    _hbr_spec = _ilu.spec_from_file_location("runtime.heartbeat_runner", str(_rt_dir / "heartbeat_runner.py"))
    _hbr = _ilu.module_from_spec(_hbr_spec)
    sys.modules["runtime.heartbeat_runner"] = _hbr
    _hbr_spec.loader.exec_module(_hbr)
HeartbeatRunner = _hbr.HeartbeatRunner

_hb = sys.modules["runtime.heartbeat"]
DuplicateScanner = _hb.DuplicateScanner
GapScanner = _hb.GapScanner
HeartbeatModule = _hb.HeartbeatModule
OrphanEdgeCleanup = _hb.OrphanEdgeCleanup
StaleEntityDetector = _hb.StaleEntityDetector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_results(tmp_path):
    d = tmp_path / "results"
    d.mkdir()
    return str(d)


class _FakeConn:
    def __init__(self) -> None:
        self.heartbeat_status_row: dict[str, Any] | None = None

    def execute(self, query: str, *args: Any):
        del query, args
        return []

    def execute_script(self, sql: str) -> None:
        self.last_schema_sql = sql

    def fetchrow(self, query: str, *args: Any):
        normalized = " ".join(query.split())
        if "INSERT INTO heartbeat_status_current" in normalized:
            payload = json.loads(str(args[4]))
            self.heartbeat_status_row = {
                "cycle_id": str(args[0]),
                "started_at": args[1],
                "completed_at": args[2],
                "total_findings": 0,
                "total_actions": 0,
                "total_errors": int(args[3]),
                "status_payload": payload,
                "updated_at": datetime(2026, 4, 11, 20, 0, tzinfo=timezone.utc),
            }
            return self.heartbeat_status_row
        if "FROM heartbeat_status_current" in normalized:
            return self.heartbeat_status_row
        del args
        return None

    def fetchval(self, query: str, *args: Any):
        del query, args
        return None


class _FakeMemoryEngine:
    def __init__(self, conn=None, *, db_path: str | None = None, embedder=None) -> None:
        del db_path, embedder
        self._conn = conn or _FakeConn()

    def _connect(self):
        return self._conn

    def list(self, *_args: Any, **_kwargs: Any):
        return []


@pytest.fixture(autouse=True)
def patch_memory_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_hbr, "MemoryEngine", _FakeMemoryEngine)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBuildModules:
    def test_codebase_index_uses_workspace_root(self):
        assert _hbr._resolve_repo_root_for_codebase_index() == _WORKFLOW_ROOT.parent.parent

    def test_creates_four_base_modules(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        modules = runner.build_modules()
        assert len(modules) == 4
        assert all(isinstance(m, HeartbeatModule) for m in modules)

    def test_module_types(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        modules = runner.build_modules()
        types = {type(m) for m in modules}
        assert types == {
            StaleEntityDetector,
            DuplicateScanner,
            OrphanEdgeCleanup,
            GapScanner,
        }


class TestRunOnce:
    def test_produces_cycle_result(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        result = runner.run_once()
        assert result.cycle_id
        assert result.started_at <= result.completed_at
        assert result.errors == 0

    def test_stops_writing_json_to_results_dir(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        runner.run_once()
        assert os.listdir(tmp_results) == []

    def test_persists_latest_summary_to_db_when_conn_available(self, tmp_results):
        conn = _FakeConn()
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False, conn=conn)
        result = runner.run_once()
        snapshot = _hbr.latest_heartbeat_status(conn=conn)

        assert snapshot is not None
        assert snapshot.cycle_id == result.cycle_id
        assert snapshot.errors == result.errors
        assert snapshot.summary["cycle_id"] == result.cycle_id
        assert snapshot.summary["module_count"] >= 4


class TestHeartbeatSummary:
    def test_summarize_cycle_payload_passthrough(self):
        """summarize_cycle_payload now passes through the dict as-is."""
        payload = {"cycle_id": "cycle-1", "errors": 0}
        summary = _hbr.summarize_cycle_payload(payload)
        assert summary["cycle_id"] == "cycle-1"


class TestRunLoop:
    def test_max_cycles_runs_exactly_n(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        calls: list[str] = []
        real_run_once = runner.run_once

        def _wrapped_run_once():
            result = real_run_once()
            calls.append(result.cycle_id)
            return result

        runner.run_once = _wrapped_run_once
        runner.run_loop(interval_seconds=0, max_cycles=2)
        assert len(calls) == 2


class TestEmptyDatabase:
    def test_no_crash_on_empty_db(self, tmp_results):
        runner = HeartbeatRunner(results_dir=tmp_results, include_probers=False)
        result = runner.run_once()
        assert result.errors == 0
