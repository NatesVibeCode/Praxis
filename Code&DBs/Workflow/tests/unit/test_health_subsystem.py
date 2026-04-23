"""Tests for runtime.health."""

import importlib
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

# The runtime package __init__.py imports modules incompatible with Python 3.9.
# Import the module directly to bypass the package init.
_spec = importlib.util.spec_from_file_location(
    "runtime.health",
    os.path.join(os.path.dirname(__file__), "..", "..", "runtime", "health.py"),
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["runtime.health"] = _mod
_spec.loader.exec_module(_mod)

import runtime.queue_admission as queue_admission

DiskSpaceProbe = _mod.DiskSpaceProbe
FileExistsProbe = _mod.FileExistsProbe
HealthProbe = _mod.HealthProbe
HealthStatus = _mod.HealthStatus
PostgresProbe = _mod.PostgresProbe
PreflightCheck = _mod.PreflightCheck
PreflightResult = _mod.PreflightResult
PreflightRunner = _mod.PreflightRunner
QueueAdmissionGate = _mod.QueueAdmissionGate
QueueDepthProbe = _mod.QueueDepthProbe
WaveHealth = _mod.WaveHealth
WaveHealthMonitor = _mod.WaveHealthMonitor
queue_admission_check = _mod.queue_admission_check


# ---- PostgresProbe --------------------------------------------------------

class TestPostgresProbe:
    def test_pass_valid_url(self):
        probe = PostgresProbe("postgresql://user:pw@localhost:5432/mydb")
        result = probe.check()
        assert result.passed is True

    def test_pass_postgres_scheme(self):
        probe = PostgresProbe("postgres://host/db")
        result = probe.check()
        assert result.passed is True

    def test_fail_bad_scheme(self):
        probe = PostgresProbe("mysql://host/db")
        result = probe.check()
        assert result.passed is False

    def test_fail_no_host(self):
        probe = PostgresProbe("postgresql:///db")
        result = probe.check()
        assert result.passed is False

    def test_internal_database_resolution_fails_closed_without_authority(self, monkeypatch):
        monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
        monkeypatch.setattr(
            _mod,
            "resolve_workflow_database_url",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                _mod.PostgresConfigurationError(
                    "postgres.config_missing",
                    "WORKFLOW_DATABASE_URL must be set",
                )
            ),
        )
        assert _mod._resolve_database_url(None) is None


def test_run_async_uses_worker_thread_inside_running_loop():
    caller_thread = threading.get_ident()

    async def _sample():
        return threading.get_ident()

    async def _invoke():
        return _mod._run_async(_sample())

    worker_thread = _mod._run_async(_invoke())

    assert worker_thread != caller_thread


def test_health_check_sync_works_inside_running_loop(monkeypatch):
    async def _fake_health_check(**_kwargs):
        return PreflightResult(
            overall=HealthStatus.HEALTHY,
            checks=(),
            timestamp=datetime.now(timezone.utc),
            duration_ms=1.0,
            details={"mode": "test"},
        )

    monkeypatch.setattr(_mod, "health_check", _fake_health_check)

    async def _invoke():
        return _mod.health_check_sync()

    result = _mod._run_async(_invoke())

    assert result.overall is HealthStatus.HEALTHY
    assert result.details == {"mode": "test"}


# ---- DiskSpaceProbe -------------------------------------------------------

class TestDiskSpaceProbe:
    def test_pass_enough_space(self, tmp_path):
        probe = DiskSpaceProbe(str(tmp_path), min_mb=1)
        result = probe.check()
        assert result.passed is True

    def test_fail_not_enough_space(self, tmp_path):
        probe = DiskSpaceProbe(str(tmp_path), min_mb=999_999_999)
        result = probe.check()
        assert result.passed is False


# ---- FileExistsProbe ------------------------------------------------------

class TestFileExistsProbe:
    def test_pass_file_exists(self, tmp_path):
        f = tmp_path / "config.yaml"
        f.write_text("key: value")
        probe = FileExistsProbe(str(f))
        result = probe.check()
        assert result.passed is True

    def test_fail_file_missing(self, tmp_path):
        probe = FileExistsProbe(str(tmp_path / "nope.yaml"))
        result = probe.check()
        assert result.passed is False


# ---- QueueDepthProbe ------------------------------------------------------

class _FakeAsyncConnection:
    def __init__(self, row):
        self._row = row

    async def fetchrow(self, query):
        return self._row

    async def close(self):
        return None


class _FakeAsyncpg:
    def __init__(self, row):
        self._row = row

    async def connect(self, _url):
        return _FakeAsyncConnection(self._row)


class TestQueueDepthProbe:
    def test_reports_ok_below_warning_threshold(self):
        probe = QueueDepthProbe(
            database_url="postgresql://example/db",
            warning_threshold=5,
            critical_threshold=10,
        )
        fake_asyncpg = _FakeAsyncpg({"pending": 2, "ready": 1, "claimed": 3, "running": 4})
        with patch.object(_mod, "_asyncpg_module", return_value=fake_asyncpg):
            result = probe.check()

        assert result.passed is True
        assert result.status == "ok"
        assert result.details == {
            "pending": 2,
            "ready": 1,
            "claimed": 3,
            "running": 4,
            "total_queued": 3,
            "warning_threshold": 5,
            "critical_threshold": 10,
            "utilization_pct": 30.0,
        }

    def test_reports_warning_at_warning_threshold(self):
        probe = QueueDepthProbe(
            database_url="postgresql://example/db",
            warning_threshold=5,
            critical_threshold=10,
        )
        fake_asyncpg = _FakeAsyncpg({"pending": 3, "ready": 2, "claimed": 0, "running": 1})
        with patch.object(_mod, "_asyncpg_module", return_value=fake_asyncpg):
            result = probe.check()

        assert result.passed is True
        assert result.status == "warning"
        assert result.details["utilization_pct"] == 50.0

    def test_reports_critical_and_fails_at_critical_threshold(self):
        probe = QueueDepthProbe(
            database_url="postgresql://example/db",
            warning_threshold=5,
            critical_threshold=10,
        )
        fake_asyncpg = _FakeAsyncpg({"pending": 6, "ready": 4, "claimed": 2, "running": 1})
        with patch.object(_mod, "_asyncpg_module", return_value=fake_asyncpg):
            result = probe.check()

        assert result.passed is False
        assert result.status == "critical"
        assert result.details["total_queued"] == 10
        assert result.details["utilization_pct"] == 100.0


# ---- QueueAdmissionGate ---------------------------------------------------

class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query):
        self.query = query

    def fetchone(self):
        return self._row


class _FakePsycopgConnection:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self._row)


class _FakePsycopg2:
    def __init__(self, row):
        self._row = row

    def connect(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return _FakePsycopgConnection(self._row)


class _FakeSyncQueueConn:
    def __init__(self, row):
        self._row = row

    def execute(self, query):
        self.query = query
        return [self._row]


class TestQueueAdmissionGate:
    def test_admits_when_projected_depth_stays_within_threshold(self):
        gate = QueueAdmissionGate(database_url="postgresql://example/db", critical_threshold=10)
        fake_psycopg2 = _FakePsycopg2((8,))
        with patch.object(queue_admission, "_psycopg2_module", return_value=fake_psycopg2):
            decision = gate.check(job_count=2)

        assert decision.admitted is True
        assert decision.queue_depth == 8
        assert decision.utilization_pct == 80.0

    def test_rejects_when_projected_depth_exceeds_threshold(self):
        gate = QueueAdmissionGate(database_url="postgresql://example/db", critical_threshold=10)
        fake_psycopg2 = _FakePsycopg2((10,))
        with patch.object(queue_admission, "_psycopg2_module", return_value=fake_psycopg2):
            decision = gate.check(job_count=1)

        assert decision.admitted is False
        assert decision.queue_depth == 10
        assert decision.utilization_pct == 100.0
        assert "at or above critical threshold 10" in decision.reason

    def test_helper_uses_gate_defaults(self):
        fake_psycopg2 = _FakePsycopg2((3,))
        with patch.dict(os.environ, {"WORKFLOW_DATABASE_URL": "postgresql://example/db"}, clear=False):
            with patch.object(queue_admission, "_psycopg2_module", return_value=fake_psycopg2):
                decision = queue_admission_check(job_count=1, critical_threshold=10)

        assert decision.admitted is True
        assert decision.queue_depth == 3

    def test_shared_gate_checks_existing_sync_connection(self):
        gate = QueueAdmissionGate(critical_threshold=10)

        decision = gate.check_connection(_FakeSyncQueueConn({"count": 8}), job_count=2)

        assert decision.admitted is True
        assert decision.queue_depth == 8
        assert decision.utilization_pct == 80.0


# ---- PreflightRunner aggregation ------------------------------------------

class _StubProbe(HealthProbe):
    def __init__(self, probe_name: str, passes: bool):
        self._name = probe_name
        self._passes = passes

    @property
    def name(self) -> str:
        return self._name

    def check(self) -> PreflightCheck:
        return PreflightCheck(
            name=self._name,
            passed=self._passes,
            message="ok" if self._passes else "fail",
            duration_ms=1.0,
        )


class TestPreflightRunner:
    def test_all_pass_is_healthy(self):
        runner = PreflightRunner([_StubProbe("a", True), _StubProbe("b", True)])
        result = runner.run()
        assert result.overall == HealthStatus.HEALTHY
        assert len(result.checks) == 2
        assert isinstance(result.timestamp, datetime)

    def test_some_fail_is_degraded(self):
        runner = PreflightRunner([
            _StubProbe("a", True),
            _StubProbe("b", False),
            _StubProbe("c", True),
        ])
        result = runner.run()
        assert result.overall == HealthStatus.DEGRADED

    def test_majority_fail_is_unhealthy(self):
        runner = PreflightRunner([
            _StubProbe("a", False),
            _StubProbe("b", False),
            _StubProbe("c", True),
        ])
        result = runner.run()
        assert result.overall == HealthStatus.UNHEALTHY

    def test_all_fail_is_unhealthy(self):
        runner = PreflightRunner([_StubProbe("a", False), _StubProbe("b", False)])
        result = runner.run()
        assert result.overall == HealthStatus.UNHEALTHY

    def test_empty_probes_is_unknown(self):
        runner = PreflightRunner([])
        result = runner.run()
        assert result.overall == HealthStatus.UNKNOWN

    def test_run_with_timeout(self):
        runner = PreflightRunner([_StubProbe("a", True)])
        result = runner.run_with_timeout(timeout_seconds=5.0)
        assert result.overall == HealthStatus.HEALTHY


# ---- WaveHealthMonitor ----------------------------------------------------

class TestWaveHealthMonitor:
    def test_record_and_query(self):
        mon = WaveHealthMonitor()
        mon.record_workflow("w1", "job-a", True, 2.0)
        mon.record_workflow("w1", "job-b", True, 3.0)
        health = mon.wave_health("w1")
        assert health.wave_id == "w1"
        assert health.total_jobs == 2
        assert health.succeeded == 2
        assert health.failed == 0

    def test_pass_rate(self):
        mon = WaveHealthMonitor()
        mon.record_workflow("w1", "a", True, 1.0)
        mon.record_workflow("w1", "b", False, 1.0)
        mon.record_workflow("w1", "c", True, 1.0)
        mon.record_workflow("w1", "d", False, 1.0)
        health = mon.wave_health("w1")
        assert health.pass_rate == pytest.approx(0.5)
        assert health.total_jobs == 4
        assert health.succeeded == 2
        assert health.failed == 2

    def test_avg_duration(self):
        mon = WaveHealthMonitor()
        mon.record_workflow("w1", "a", True, 2.0)
        mon.record_workflow("w1", "b", True, 4.0)
        health = mon.wave_health("w1")
        assert health.avg_duration == pytest.approx(3.0)

    def test_stall_detection_recent_not_stalled(self):
        mon = WaveHealthMonitor()
        mon.record_workflow("w1", "a", True, 1.0)
        # Just recorded, so should not be stalled with generous threshold.
        assert mon.stall_detection("w1", max_idle_seconds=300) is False

    def test_stall_detection_old_activity(self):
        mon = WaveHealthMonitor()
        mon.record_workflow("w1", "a", True, 1.0)
        # Patch the record timestamp to the past.
        records = mon._waves["w1"]
        records[0] = records[0].__class__(
            job_label=records[0].job_label,
            succeeded=records[0].succeeded,
            duration_seconds=records[0].duration_seconds,
            timestamp=datetime.now(timezone.utc) - timedelta(seconds=600),
        )
        assert mon.stall_detection("w1", max_idle_seconds=300) is True

    def test_empty_wave_defaults(self):
        mon = WaveHealthMonitor()
        health = mon.wave_health("nonexistent")
        assert health.total_jobs == 0
        assert health.succeeded == 0
        assert health.failed == 0
        assert health.pass_rate == 0.0
        assert health.avg_duration == 0.0
        assert health.stalled is True

    def test_stall_detection_unknown_wave(self):
        mon = WaveHealthMonitor()
        assert mon.stall_detection("unknown") is True
