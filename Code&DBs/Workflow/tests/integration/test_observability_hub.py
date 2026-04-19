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


def _seed_receipt_authority(
    conn,
    *,
    run_id: str,
    receipt_id: str,
    workflow_id: str,
    request_id: str,
    occurred_at: datetime,
) -> None:
    suffix = receipt_id.replace(":", "_").replace(".", "_")
    workflow_definition_id = f"workflow_definition.{suffix}"
    admission_decision_id = f"admission_decision.{suffix}"
    definition_hash = f"sha256:{suffix}"
    envelope = json.dumps(
        {
            "kind": "observability_test",
            "workflow_id": workflow_id,
            "run_id": run_id,
        }
    )
    conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES (
            $1, $2, 1, 1, $3, 'active', $4::jsonb, '{"nodes":[],"edges":[]}'::jsonb, $5, NULL
        )
        ON CONFLICT (workflow_definition_id) DO NOTHING
        """,
        workflow_definition_id,
        workflow_id,
        definition_hash,
        envelope,
        occurred_at,
    )
    conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES (
            $1, $2, $3, 'admit', 'test.observability_hub.seed', $4, 'test', 'policy:test', 'validation:test', 'authority:test'
        )
        ON CONFLICT (admission_decision_id) DO NOTHING
        """,
        admission_decision_id,
        workflow_id,
        request_id,
        occurred_at,
    )
    conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 1, $9::jsonb, $10, $11,
            'claim_accepted', NULL, $12, $12, $12, NULL, NULL
        )
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id,
        workflow_id,
        request_id,
        f"digest:{suffix}",
        f"authority:{suffix}",
        workflow_definition_id,
        definition_hash,
        request_id,
        envelope,
        f"context_bundle:{suffix}",
        admission_decision_id,
        occurred_at,
    )
    conn.execute(
        """
        INSERT INTO receipts (
            receipt_id,
            receipt_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            attempt_no,
            supersedes_receipt_id,
            started_at,
            finished_at,
            evidence_seq,
            executor_type,
            status,
            inputs,
            outputs,
            artifacts,
            failure_code,
            decision_refs
        ) VALUES (
            $1, 'workflow_completion_receipt', 1, $2, $3, $4, NULL, 'node.observe', 1, NULL,
            $5, $5, 1, 'native_operator', 'failed', $6::jsonb, '{}'::jsonb, '[]'::jsonb, NULL, '[]'::jsonb
        )
        ON CONFLICT (receipt_id) DO NOTHING
        """,
        receipt_id,
        workflow_id,
        run_id,
        request_id,
        occurred_at,
        json.dumps({"transition_seq": 1}),
    )


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

    def test_ingest_receipt_does_not_auto_file_repeated_failures(self, hub):
        """ReceiptStore owns workflow-result auto-bug filing; the hub only tracks panel signals."""
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(3):
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-timeout",
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert bugs == []
        assert hub.operator_snapshot().recent_failure_codes.get(code) == 3

    def test_repeated_failure_tracking_does_not_create_bug_authority(self, hub):
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(6):
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-timeout",
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert bugs == []
        assert hub.operator_snapshot().recent_failure_codes.get(code) == 6

    def test_receipt_identity_is_tracked_without_evidence_side_effects(self, hub):
        code = f"TIMEOUT_EXCEEDED_{uuid.uuid4().hex[:8]}"
        for i in range(3):
            occurred_at = datetime.now(timezone.utc)
            run_id = f"run-{i}"
            receipt_id = f"receipt-{i}"
            _seed_receipt_authority(
                hub._conn,
                run_id=run_id,
                receipt_id=receipt_id,
                workflow_id=f"workflow.observe.{i}",
                request_id=f"request.observe.{i}",
                occurred_at=occurred_at,
            )
            hub.ingest_receipt(
                _make_receipt(
                    status="failed",
                    failure_code=code,
                    job_label="job-observe",
                    run_id=run_id,
                    receipt_id=receipt_id,
                    failure_category="runtime_failed",
                    timestamp=occurred_at.isoformat(),
                )
            )

        bugs = [bug for bug in hub.get_bugs(limit=500) if code in bug.title]
        assert bugs == []
        assert hub.operator_snapshot().last_run_id == "run-2"
        assert hub.operator_snapshot().last_failure_category == "runtime_failed"

    def test_different_failure_codes_track_panel_counts_only(self, hub):
        codes = [f"CODE_A_{uuid.uuid4().hex[:8]}", f"CODE_B_{uuid.uuid4().hex[:8]}"]
        for code in codes:
            for i in range(3):
                hub.ingest_receipt(
                    _make_receipt(
                        status="failed",
                        failure_code=code,
                        job_label="job-%s" % code,
                    )
                )

        bugs = [bug for bug in hub.get_bugs(limit=500) if any(code in bug.title for code in codes)]
        assert bugs == []
        snapshot = hub.operator_snapshot()
        assert snapshot.recent_failure_codes.get(codes[0]) == 3
        assert snapshot.recent_failure_codes.get(codes[1]) == 3


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
