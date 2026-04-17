from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from runtime.workflow import _admission as _admission_mod
from surfaces.api.handlers import workflow_query

_ROOT = Path(__file__).resolve().parents[2]
_HEALTH_SPEC = importlib.util.spec_from_file_location(
    "runtime.health",
    _ROOT / "runtime" / "health.py",
)
_HEALTH_MOD = importlib.util.module_from_spec(_HEALTH_SPEC)
sys.modules["runtime.health"] = _HEALTH_MOD
assert _HEALTH_SPEC.loader is not None
_HEALTH_SPEC.loader.exec_module(_HEALTH_MOD)


class _FakeAsyncConnection:
    def __init__(self, row):
        self._row = row

    async def fetchrow(self, query):
        self.query = query
        return self._row

    async def close(self):
        return None


class _FakeAsyncpg:
    def __init__(self, row):
        self._row = row

    async def connect(self, _url):
        return _FakeAsyncConnection(self._row)


class _FakePg:
    def __init__(self, row):
        self._row = row

    def execute(self, query: str, *params):
        self.query = query
        self.params = params
        return [self._row]


def test_health_queue_depth_probe_uses_shared_thresholds() -> None:
    probe = _HEALTH_MOD.QueueDepthProbe(database_url="postgresql://example/db")
    fake_asyncpg = _FakeAsyncpg({"pending": 600, "ready": 400, "claimed": 1, "running": 2})

    with patch.object(_HEALTH_MOD, "_asyncpg_module", return_value=fake_asyncpg):
        result = probe.check()

    assert result.passed is False
    assert result.status == "critical"
    assert result.details["total_queued"] == 1000
    assert result.details["utilization_pct"] == 100.0


def test_workflow_query_queue_snapshot_uses_shared_thresholds() -> None:
    payload = workflow_query._queue_depth_snapshot(
        _FakePg({"pending": 600, "ready": 400, "claimed": 1, "running": 2})
    )

    assert payload["queue_depth"] == 1000
    assert payload["queue_depth_status"] == "critical"
    assert payload["queue_depth_utilization_pct"] == 100.0


def test_runtime_admission_consults_shared_queue_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeGate:
        def __init__(self, *, critical_threshold: int = 1000, **_kwargs) -> None:
            captured["critical_threshold"] = critical_threshold

        def check_connection(self, conn, *, job_count: int = 1):
            captured["conn"] = conn
            captured["job_count"] = job_count
            return SimpleNamespace(
                admitted=False,
                queue_depth=1000,
                reason="queue depth 1000 is at or above critical threshold 1000",
            )

    monkeypatch.setattr(_admission_mod, "QueueAdmissionGate", _FakeGate)

    with pytest.raises(
        RuntimeError,
        match="queue admission rejected: queue depth 1000 is at or above critical threshold 1000",
    ):
        _admission_mod._enforce_queue_admission(object(), job_count=1)

    assert captured["job_count"] == 1
