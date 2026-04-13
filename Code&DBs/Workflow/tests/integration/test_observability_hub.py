"""Integration tests for the ObservabilityHub coordinator.

Uses importlib-based direct file imports to avoid triggering the runtime
package __init__.py (which requires Python 3.10+ features).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Direct-file import helper (bypasses runtime/__init__.py)
# ---------------------------------------------------------------------------

_RUNTIME_DIR = str(
    Path(__file__).resolve().parents[2] / "runtime"
)


def _direct_import(module_name, file_name):
    key = f"runtime.{module_name}"
    if key in sys.modules:
        return sys.modules[key]
    path = os.path.join(_RUNTIME_DIR, file_name)
    spec = importlib.util.spec_from_file_location(key, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


_obs_hub = _direct_import("observability_hub", "observability_hub.py")
_health = _direct_import("health", "health.py")

ObservabilityHub = _obs_hub.ObservabilityHub
ReceiptIngester = _obs_hub.ReceiptIngester

HealthProbe = _health.HealthProbe
HealthStatus = _health.HealthStatus
PreflightCheck = _health.PreflightCheck


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture()
def hub():
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    try:
        yield ObservabilityHub(conn)
    finally:
        conn.close()


def _make_receipt(
    agent_slug="agent-a",
    status="succeeded",
    failure_code=None,
    cost=0.01,
    latency=1.5,
    job_label="job-1",
    timestamp=None,
    **extra,
):
    ts = timestamp or datetime.now(timezone.utc).isoformat()
    r = {
        "agent_slug": agent_slug,
        "status": status,
        "cost": cost,
        "latency_seconds": latency,
        "job_label": job_label,
        "timestamp": ts,
    }
    if failure_code:
        r["failure_code"] = failure_code
    r.update(extra)
    return r


# ---------------------------------------------------------------------------
# ObservabilityHub tests
# ---------------------------------------------------------------------------


class TestIngestReceipt:
    def test_feeds_quality_materializer(self, hub):
        """Ingesting a receipt should populate the quality materializer so that
        materializing produces a rollup with the receipt data."""
        receipt = _make_receipt(status="succeeded")
        hub.ingest_receipt(receipt)

        ts = datetime.fromisoformat(receipt["timestamp"])
        rollup = hub.materialize_quality("hourly", ts.replace(minute=0, second=0, microsecond=0))

        assert rollup.total_workflows == 1
        assert rollup.total_successes == 1
        assert rollup.overall_pass_rate == 1.0

    def test_auto_bug_filing_on_repeated_failures(self, hub):
        """After 3+ receipts with the same failure code, a bug should be auto-filed."""
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(3):
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-%d" % i,
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert len(bugs) == 1
        assert code in bugs[0].title
        assert bugs[0].filed_by == "observability_hub"

    def test_auto_bug_dedup(self, hub):
        """Filing the same failure code 6 times should still only produce 1 bug."""
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(6):
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-%d" % i,
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert len(bugs) == 1

    def test_auto_bug_links_failure_evidence_and_signature_tags(self, hub):
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(3):
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-observe",
                    run_id=f"run-{i}",
                    receipt_id=f"receipt-{i}",
                    failure_category="runtime_failed",
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert len(bugs) == 1
        bug = bugs[0]
        assert any(tag.startswith("failure_code:") for tag in bug.tags)
        assert "auto-filed" in bug.tags

        evidence = hub._get_bug_tracker().list_evidence(bug.bug_id)
        roles = {(row["evidence_kind"], row["evidence_role"]) for row in evidence}
        assert ("receipt", "observed_in") in roles
        assert ("run", "observed_in") in roles

    def test_different_failure_codes_file_separate_bugs(self, hub):
        """Different failure codes each get their own bug after threshold."""
        codes = [f"CODE_A_{uuid.uuid4().hex[:8]}", f"CODE_B_{uuid.uuid4().hex[:8]}"]
        for code in codes:
            for i in range(3):
                hub.ingest_receipt(
                    _make_receipt(
                        status="failed",
                        failure_code=code,
                        job_label="job-%s-%d" % (code, i),
                    )
                )

        bugs = [bug for bug in hub.get_bugs(limit=500) if any(code in bug.title for code in codes)]
        assert len(bugs) == 2
        titles = set(b.title for b in bugs)
        assert any(codes[0] in t for t in titles)
        assert any(codes[1] in t for t in titles)


class TestRefreshOperatorPanel:
    def test_returns_valid_snapshot(self, hub):
        snap = hub.refresh_operator_panel(
            circuit_breakers={"openai": True, "anthropic": False},
            loop_warnings=2,
            write_conflicts=1,
            governance_blocks=0,
            pending_jobs=5,
            running_jobs=3,
            active_leases=4,
            posture="operate",
        )

        assert snap.posture == "operate"
        assert snap.pending_jobs == 5
        assert snap.running_jobs == 3
        assert snap.active_leases == 4
        assert snap.loop_warnings == 2
        assert snap.write_conflicts == 1
        assert snap.governance_blocks == 0
        assert "openai" in snap.circuit_breaker_open
        assert "anthropic" not in snap.circuit_breaker_open


class TestFileBugAndGetBugs:
    def test_file_and_retrieve(self, hub):
        title = f"Test bug {uuid.uuid4().hex[:8]}"
        bug = hub.file_bug(
            title=title,
            severity="P1",
            category="RUNTIME",
            description="Something broke",
            filed_by="test",
        )
        assert bug.title == title
        assert bug.severity.value == "P1"

        bugs = hub.get_bugs(limit=500)
        assert any(existing.bug_id == bug.bug_id for existing in bugs)

    def test_filter_by_status(self, hub):
        bug = hub.file_bug(f"a_{uuid.uuid4().hex[:8]}", "P2", "RUNTIME", "desc", "test")
        bugs_open = hub.get_bugs(status="OPEN")
        assert any(existing.bug_id == bug.bug_id for existing in bugs_open)
        bugs_fixed = hub.get_bugs(status="FIXED")
        assert all(existing.bug_id != bug.bug_id for existing in bugs_fixed)

    def test_filter_by_severity(self, hub):
        title = f"a_{uuid.uuid4().hex[:8]}"
        bug = hub.file_bug(title, "P0", "RUNTIME", "critical", "test")
        hub.file_bug(f"b_{uuid.uuid4().hex[:8]}", "P3", "RUNTIME", "minor", "test")
        p0_bugs = hub.get_bugs(severity="P0")
        assert any(existing.bug_id == bug.bug_id and existing.title == title for existing in p0_bugs)


class TestHealthCheck:
    def test_with_passing_probes(self, hub):
        class OkProbe(HealthProbe):
            @property
            def name(self):
                return "ok_probe"

            def check(self):
                return PreflightCheck(name="ok_probe", passed=True, message="ok", duration_ms=0.1)

        result = hub.health_check([OkProbe(), OkProbe()])
        assert result.overall == HealthStatus.HEALTHY
        assert len(result.checks) == 2

    def test_with_failing_probes(self, hub):
        class FailProbe(HealthProbe):
            @property
            def name(self):
                return "fail_probe"

            def check(self):
                return PreflightCheck(name="fail_probe", passed=False, message="bad", duration_ms=0.1)

        result = hub.health_check([FailProbe(), FailProbe()])
        assert result.overall == HealthStatus.UNHEALTHY

    def test_mixed_probes_degraded(self, hub):
        class OkProbe(HealthProbe):
            @property
            def name(self):
                return "ok"

            def check(self):
                return PreflightCheck(name="ok", passed=True, message="ok", duration_ms=0.1)

        class FailProbe(HealthProbe):
            @property
            def name(self):
                return "fail"

            def check(self):
                return PreflightCheck(name="fail", passed=False, message="bad", duration_ms=0.1)

        result = hub.health_check([OkProbe(), OkProbe(), FailProbe()])
        assert result.overall == HealthStatus.DEGRADED


# ---------------------------------------------------------------------------
# ReceiptIngester tests
# ---------------------------------------------------------------------------


class TestReceiptIngester:
    def test_load_recent(self, tmp_dir):
        receipts_dir = os.path.join(tmp_dir, "receipts")
        os.makedirs(receipts_dir)

        # Write two recent receipts
        for i in range(2):
            path = os.path.join(receipts_dir, "receipt_%d.json" % i)
            with open(path, "w") as fh:
                json.dump(
                    _make_receipt(
                        status="succeeded" if i == 0 else "failed",
                        failure_code="ERR" if i == 1 else None,
                    ),
                    fh,
                )

        ingester = ReceiptIngester(receipts_dir)
        recent = ingester.load_recent(since_hours=1)
        assert len(recent) == 2

    def test_load_recent_skips_old_files(self, tmp_dir):
        receipts_dir = os.path.join(tmp_dir, "receipts")
        os.makedirs(receipts_dir)

        path = os.path.join(receipts_dir, "old_receipt.json")
        with open(path, "w") as fh:
            json.dump(_make_receipt(), fh)

        # Set mtime to 2 hours ago
        old_time = time.time() - 7200
        os.utime(path, (old_time, old_time))

        ingester = ReceiptIngester(receipts_dir)
        recent = ingester.load_recent(since_hours=1)
        assert len(recent) == 0

    def test_compute_pass_rate(self):
        receipts = [
            _make_receipt(status="succeeded"),
            _make_receipt(status="succeeded"),
            _make_receipt(status="failed", failure_code="ERR"),
        ]
        rate = ReceiptIngester.compute_pass_rate(receipts)
        assert abs(rate - 2.0 / 3.0) < 1e-9

    def test_compute_pass_rate_empty(self):
        assert ReceiptIngester.compute_pass_rate([]) == 0.0

    def test_top_failure_codes(self):
        receipts = [
            _make_receipt(status="failed", failure_code="TIMEOUT"),
            _make_receipt(status="failed", failure_code="TIMEOUT"),
            _make_receipt(status="failed", failure_code="OOM"),
            _make_receipt(status="succeeded"),
        ]
        codes = ReceiptIngester.top_failure_codes(receipts)
        assert codes == {"TIMEOUT": 2, "OOM": 1}

    def test_top_failure_codes_limit(self):
        receipts = [
            _make_receipt(status="failed", failure_code="CODE_%d" % i)
            for i in range(20)
        ]
        codes = ReceiptIngester.top_failure_codes(receipts, limit=5)
        assert len(codes) == 5

    def test_nonexistent_directory(self):
        ingester = ReceiptIngester("/nonexistent/path")
        assert ingester.load_recent() == []
