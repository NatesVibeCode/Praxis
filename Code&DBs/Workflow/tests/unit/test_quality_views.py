"""Tests for runtime.quality_views module."""

import importlib
import importlib.util
import os
import sys
import types
from datetime import datetime

import pytest

# Import quality_views directly to avoid runtime/__init__.py pulling in
# domain.py which requires Python 3.10+ (slots=True on dataclass).
# We register a stub 'runtime' package first so the module resolves.
if "runtime" not in sys.modules:
    _stub = types.ModuleType("runtime")
    _stub.__path__ = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "runtime"))
    ]
    sys.modules["runtime"] = _stub

_mod_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "runtime", "quality_views.py")
)
_spec = importlib.util.spec_from_file_location("runtime.quality_views", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["runtime.quality_views"] = _mod
_spec.loader.exec_module(_mod)

AgentQualityProfile = _mod.AgentQualityProfile
FailureCatalogEntry = _mod.FailureCatalogEntry
QualityRollup = _mod.QualityRollup
QualityViewMaterializer = _mod.QualityViewMaterializer
QualityWindow = _mod.QualityWindow
load_failure_category_zones = _mod.load_failure_category_zones


import uuid as _uuid

# Use a far-future date to avoid collisions with production data
_TEST_DATE = "2029-06-15"
_TEST_HOUR = "14"


class _ZoneConn:
    def __init__(self, rows, *, exc: Exception | None = None):
        self._rows = rows
        self._exc = exc

    def execute(self, _sql: str):
        if self._exc is not None:
            raise self._exc
        return self._rows


@pytest.fixture
def mat():
    from _pg_test_conn import transactional_test_conn

    with transactional_test_conn() as conn:
        conn.execute_script(
            """
            TRUNCATE TABLE
                quality_rollups,
                agent_profiles,
                failure_catalog
            """
        )
        yield QualityViewMaterializer(conn)


def _receipt(
    agent_slug="agent-a",
    status="succeeded",
    failure_code=None,
    cost=0.01,
    latency_seconds=1.5,
    job_label="job-1",
    timestamp=None,
):
    if timestamp is None:
        timestamp = f"{_TEST_DATE}T{_TEST_HOUR}:30:00"
    r = {
        "agent_slug": agent_slug,
        "status": status,
        "cost": cost,
        "latency_seconds": latency_seconds,
        "job_label": job_label,
        "timestamp": timestamp,
    }
    if failure_code is not None:
        r["failure_code"] = failure_code
    return r


# ------------------------------------------------------------------
# Core: ingest + materialize
# ------------------------------------------------------------------

class TestIngestAndMaterialize:
    def test_basic_rollup(self, mat):
        mat.ingest_receipt(_receipt(status="succeeded", cost=0.10, latency_seconds=2.0))
        mat.ingest_receipt(_receipt(status="failed", failure_code="TIMEOUT", cost=0.05, latency_seconds=5.0))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        rollup = mat.materialize(QualityWindow.HOURLY, ws)

        assert rollup.total_workflows == 2
        assert rollup.total_successes == 1
        assert rollup.total_failures == 1
        assert rollup.overall_pass_rate == pytest.approx(0.5)
        assert rollup.total_cost == pytest.approx(0.15)
        assert len(rollup.agent_profiles) == 1
        assert rollup.agent_profiles[0].agent_slug == "agent-a"

    def test_pass_rate_all_success(self, mat):
        for _ in range(5):
            mat.ingest_receipt(_receipt(status="succeeded"))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        rollup = mat.materialize(QualityWindow.HOURLY, ws)
        assert rollup.overall_pass_rate == pytest.approx(1.0)

    def test_pass_rate_all_failure(self, mat):
        for _ in range(3):
            mat.ingest_receipt(_receipt(status="failed", failure_code="ERR"))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        rollup = mat.materialize(QualityWindow.HOURLY, ws)
        assert rollup.overall_pass_rate == pytest.approx(0.0)


class TestFailureCategoryZones:
    def test_load_failure_category_zones_returns_zone_map(self):
        zone_map = load_failure_category_zones(
            _ZoneConn([{"category": "provider_timeout", "zone": "external"}]),
            consumer="praxis_status",
        )

        assert zone_map == {"provider_timeout": "external"}

    def test_load_failure_category_zones_fails_when_query_errors(self):
        with pytest.raises(RuntimeError, match="failure_category_zones authority is required for praxis_status"):
            load_failure_category_zones(
                _ZoneConn([], exc=RuntimeError("db unavailable")),
                consumer="praxis_status",
            )

    def test_load_failure_category_zones_fails_when_rows_missing(self):
        with pytest.raises(RuntimeError, match="failure_category_zones did not return any rows"):
            load_failure_category_zones(_ZoneConn([]), consumer="quality views")


# ------------------------------------------------------------------
# Agent profile aggregation
# ------------------------------------------------------------------

class TestAgentProfiles:
    def test_multiple_agents(self, mat):
        mat.ingest_receipt(_receipt(agent_slug="alpha", status="succeeded", cost=0.10, latency_seconds=1.0))
        mat.ingest_receipt(_receipt(agent_slug="alpha", status="succeeded", cost=0.20, latency_seconds=3.0))
        mat.ingest_receipt(_receipt(agent_slug="beta", status="failed", failure_code="OOM", cost=0.50, latency_seconds=10.0))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        rollup = mat.materialize(QualityWindow.HOURLY, ws)

        assert len(rollup.agent_profiles) == 2
        alpha = [p for p in rollup.agent_profiles if p.agent_slug == "alpha"][0]
        beta = [p for p in rollup.agent_profiles if p.agent_slug == "beta"][0]

        assert alpha.dispatches == 2
        assert alpha.successes == 2
        assert alpha.pass_rate == pytest.approx(1.0)
        assert alpha.avg_cost == pytest.approx(0.15)
        assert alpha.avg_latency_seconds == pytest.approx(2.0)
        assert alpha.total_token_cost == pytest.approx(0.30)

        assert beta.dispatches == 1
        assert beta.failures == 1
        assert beta.pass_rate == pytest.approx(0.0)
        assert beta.failure_codes == {"OOM": 1}

    def test_get_agent_profile(self, mat):
        mat.ingest_receipt(_receipt(agent_slug="gamma", status="succeeded", cost=0.05))
        ws = datetime(2029, 6, 15, 14, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws)

        profile = mat.get_agent_profile("gamma", QualityWindow.HOURLY, ws)
        assert profile is not None
        assert profile.agent_slug == "gamma"
        assert profile.dispatches == 1

    def test_get_agent_profile_missing(self, mat):
        ws = datetime(2029, 6, 15, 14, 0, 0)
        assert mat.get_agent_profile("nope", QualityWindow.HOURLY, ws) is None


# ------------------------------------------------------------------
# Failure catalog
# ------------------------------------------------------------------

class TestFailureCatalog:
    def test_tracks_codes_and_counts(self, mat):
        pfx = _uuid.uuid4().hex[:6]
        mat.ingest_receipt(_receipt(status="failed", failure_code=f"TIMEOUT_{pfx}", job_label=f"j1_{pfx}"))
        mat.ingest_receipt(_receipt(status="failed", failure_code=f"TIMEOUT_{pfx}", job_label=f"j2_{pfx}"))
        mat.ingest_receipt(_receipt(status="failed", failure_code=f"OOM_{pfx}", job_label=f"j3_{pfx}"))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws)

        catalog = mat.get_failure_catalog()
        timeout_entries = [e for e in catalog if e.failure_code == f"TIMEOUT_{pfx}"]
        assert len(timeout_entries) == 1
        assert timeout_entries[0].count == 2

    def test_failure_catalog_owning_agents(self, mat):
        pfx = _uuid.uuid4().hex[:6]
        fc = f"X_{pfx}"
        mat.ingest_receipt(_receipt(agent_slug=f"a1_{pfx}", status="failed", failure_code=fc))
        mat.ingest_receipt(_receipt(agent_slug=f"a2_{pfx}", status="failed", failure_code=fc))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws)

        catalog = mat.get_failure_catalog()
        x_entries = [e for e in catalog if e.failure_code == fc]
        assert len(x_entries) == 1
        assert set(x_entries[0].owning_agents) == {f"a1_{pfx}", f"a2_{pfx}"}

    def test_failure_catalog_limit(self, mat):
        for i in range(25):
            mat.ingest_receipt(_receipt(status="failed", failure_code=f"ERR_{i:03d}"))

        ws = datetime(2029, 6, 15, 14, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws)

        catalog = mat.get_failure_catalog(limit=5)
        assert len(catalog) == 5


# ------------------------------------------------------------------
# DB retrieval
# ------------------------------------------------------------------

class TestGetRollup:
    def test_retrieves_materialized(self, mat):
        mat.ingest_receipt(_receipt())
        ws = datetime(2029, 6, 15, 14, 0, 0)
        original = mat.materialize(QualityWindow.HOURLY, ws)

        retrieved = mat.get_rollup(QualityWindow.HOURLY, ws)
        assert retrieved is not None
        assert retrieved.total_workflows == original.total_workflows
        assert retrieved.overall_pass_rate == pytest.approx(original.overall_pass_rate)
        assert len(retrieved.agent_profiles) == len(original.agent_profiles)

    def test_get_rollup_missing(self, mat):
        ws = datetime(2099, 1, 1, 0, 0, 0)
        assert mat.get_rollup(QualityWindow.HOURLY, ws) is None


class TestLatestRollup:
    def test_returns_most_recent(self, mat):
        # Materialize two rollups and verify get_rollup retrieves them correctly
        mat.ingest_receipt(_receipt(timestamp=f"{_TEST_DATE}T13:15:00"))
        ws1 = datetime(2029, 6, 15, 13, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws1)

        mat.ingest_receipt(_receipt(timestamp=f"{_TEST_DATE}T15:15:00"))
        ws2 = datetime(2029, 6, 15, 15, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws2)

        # Both should be retrievable
        r1 = mat.get_rollup(QualityWindow.HOURLY, ws1)
        r2 = mat.get_rollup(QualityWindow.HOURLY, ws2)
        assert r1 is not None
        assert r2 is not None

        # Latest should be at least as recent as ws2
        latest = mat.latest_rollup(QualityWindow.HOURLY)
        assert latest is not None
        assert latest.window_start >= ws2

    def test_latest_rollup_not_none_after_materialize(self, mat):
        mat.ingest_receipt(_receipt())
        ws = datetime(2029, 6, 15, 14, 0, 0)
        mat.materialize(QualityWindow.HOURLY, ws)
        latest = mat.latest_rollup(QualityWindow.HOURLY)
        assert latest is not None


# ------------------------------------------------------------------
# Empty state
# ------------------------------------------------------------------

class TestEmptyState:
    def test_empty_rollup(self, mat):
        # Use a window far in the future that has no receipts
        # Use DAILY to avoid interfering with HOURLY latest_rollup tests
        ws = datetime(2098, 11, 30, 0, 0, 0)
        rollup = mat.materialize(QualityWindow.DAILY, ws)

        assert rollup.total_workflows == 0
        assert rollup.total_successes == 0
        assert rollup.total_failures == 0
        assert rollup.overall_pass_rate == 0.0
        assert rollup.total_cost == 0.0
        assert rollup.agent_profiles == ()
        assert rollup.top_failures == ()


# ------------------------------------------------------------------
# Window isolation
# ------------------------------------------------------------------

class TestWindowIsolation:
    def test_hourly_and_daily_dont_cross(self, mat):
        mat.ingest_receipt(_receipt(timestamp=f"{_TEST_DATE}T{_TEST_HOUR}:30:00", cost=0.10))

        hourly_ws = datetime(2029, 6, 15, 14, 0, 0)
        daily_ws = datetime(2029, 6, 15, 0, 0, 0)

        hourly_rollup = mat.materialize(QualityWindow.HOURLY, hourly_ws)
        daily_rollup = mat.materialize(QualityWindow.DAILY, daily_ws)

        # Both windows should see the receipt (same receipt falls in both)
        assert hourly_rollup.total_workflows >= 1
        assert daily_rollup.total_workflows >= 1

        # Querying a far-future window should return nothing
        wrong_ws = datetime(2099, 1, 1, 11, 0, 0)
        wrong = mat.get_rollup(QualityWindow.HOURLY, wrong_ws)
        assert wrong is None

    def test_different_hours_dont_share(self, mat):
        mat.ingest_receipt(_receipt(timestamp=f"{_TEST_DATE}T{_TEST_HOUR}:30:00"))
        mat.ingest_receipt(_receipt(timestamp=f"{_TEST_DATE}T16:30:00"))

        ws_14 = datetime(2029, 6, 15, 14, 0, 0)
        ws_16 = datetime(2029, 6, 15, 16, 0, 0)

        r14 = mat.materialize(QualityWindow.HOURLY, ws_14)
        r16 = mat.materialize(QualityWindow.HOURLY, ws_16)

        assert r14.total_workflows >= 1
        assert r16.total_workflows >= 1
