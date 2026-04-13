"""Tests for surfaces.web_dashboard — dashboard data assembly layer."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from surfaces.web_dashboard import (
    DashboardAssembler,
    DashboardPayload,
    DashboardSection,
    LiveDataFeed,
)

# ── Sample data ────────────────────────────────────────────────

SAMPLE_RECEIPTS = [
    {
        "agent": "alpha",
        "status": "success",
        "duration": 12.5,
        "cost": 0.03,
        "timestamp": "2026-04-03T10:00:00Z",
        "label": "build-foo",
        "code": "",
    },
    {
        "agent": "alpha",
        "status": "success",
        "duration": 8.0,
        "cost": 0.02,
        "timestamp": "2026-04-03T11:00:00Z",
        "label": "build-bar",
        "code": "",
    },
    {
        "agent": "beta",
        "status": "failed",
        "duration": 3.0,
        "cost": 0.01,
        "timestamp": "2026-04-03T12:00:00Z",
        "label": "lint-check",
        "code": "E101",
    },
    {
        "agent": "beta",
        "status": "failed",
        "duration": 5.0,
        "cost": 0.015,
        "timestamp": "2026-04-04T09:00:00Z",
        "label": "test-suite",
        "code": "E202",
    },
    {
        "agent": "gamma",
        "status": "success",
        "duration": 20.0,
        "cost": 0.05,
        "timestamp": "2026-04-04T10:00:00Z",
        "label": "deploy",
        "code": "",
    },
]


# ── DashboardSection & DashboardPayload ───────────────────────

class TestDataclasses:
    def test_section_is_frozen(self):
        sec = DashboardSection(name="x", data={}, updated_at=datetime.now(timezone.utc))
        with pytest.raises(AttributeError):
            sec.name = "y"

    def test_payload_is_frozen(self):
        now = datetime.now(timezone.utc)
        p = DashboardPayload(sections=(), generated_at=now, version="0.1.0")
        with pytest.raises(AttributeError):
            p.version = "999"


# ── Section builders ──────────────────────────────────────────

class TestDispatchSummary:
    def test_basic_stats(self):
        result = DashboardAssembler.build_workflow_summary(SAMPLE_RECEIPTS)
        assert result["total"] == 5
        assert result["succeeded"] == 3
        assert result["failed"] == 2
        assert result["pass_rate"] == 60.0
        assert result["avg_duration"] == pytest.approx(9.7, abs=0.1)

    def test_empty_receipts(self):
        result = DashboardAssembler.build_workflow_summary([])
        assert result["total"] == 0
        assert result["pass_rate"] == 0.0
        assert result["avg_duration"] == 0.0


class TestAgentLeaderboard:
    def test_per_agent_breakdown(self):
        lb = DashboardAssembler.build_agent_leaderboard(SAMPLE_RECEIPTS)
        assert set(lb.keys()) == {"alpha", "beta", "gamma"}
        assert lb["alpha"]["dispatches"] == 2
        assert lb["alpha"]["pass_rate"] == 100.0
        assert lb["beta"]["pass_rate"] == 0.0
        assert lb["gamma"]["dispatches"] == 1

    def test_empty_receipts(self):
        lb = DashboardAssembler.build_agent_leaderboard([])
        assert lb == {}


class TestCircuitBreakerStatus:
    def test_passthrough(self):
        breakers = {"openai": True, "anthropic": False}
        result = DashboardAssembler.build_circuit_breaker_status(breakers)
        assert result == {"openai": True, "anthropic": False}

    def test_empty(self):
        assert DashboardAssembler.build_circuit_breaker_status({}) == {}


class TestRecentFailures:
    def test_returns_only_failures(self):
        result = DashboardAssembler.build_recent_failures(SAMPLE_RECEIPTS)
        assert result["count"] == 2
        assert len(result["failures"]) == 2
        # Sorted by timestamp descending
        assert result["failures"][0]["code"] == "E202"

    def test_limit(self):
        result = DashboardAssembler.build_recent_failures(SAMPLE_RECEIPTS, limit=1)
        assert len(result["failures"]) == 1

    def test_no_failures(self):
        ok = [r for r in SAMPLE_RECEIPTS if r["status"] == "success"]
        result = DashboardAssembler.build_recent_failures(ok)
        assert result["count"] == 0
        assert result["failures"] == []


class TestCostSummary:
    def test_totals(self):
        result = DashboardAssembler.build_cost_summary(SAMPLE_RECEIPTS)
        assert result["total_cost"] == pytest.approx(0.125, abs=0.001)
        assert "alpha" in result["cost_by_agent"]
        assert "2026-04-03" in result["cost_by_day"]

    def test_empty(self):
        result = DashboardAssembler.build_cost_summary([])
        assert result["total_cost"] == 0.0
        assert result["cost_by_agent"] == {}
        assert result["cost_by_day"] == {}


# ── Assembler ─────────────────────────────────────────────────

class TestAssembler:
    def test_register_and_assemble(self):
        asm = DashboardAssembler()
        asm.register("ping", lambda: {"ok": True})
        payload = asm.assemble()
        assert len(payload.sections) == 1
        assert payload.sections[0].name == "ping"
        assert payload.sections[0].data == {"ok": True}

    def test_to_json_valid(self):
        asm = DashboardAssembler()
        asm.register("test", lambda: {"val": 42})
        payload = asm.assemble()
        raw = DashboardAssembler.to_json(payload)
        parsed = json.loads(raw)
        assert parsed["version"] == "0.1.0"
        assert parsed["sections"][0]["data"]["val"] == 42

    def test_write_to_file(self):
        asm = DashboardAssembler()
        asm.register("x", lambda: {"a": 1})
        payload = asm.assemble()
        with tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, "sub", "dashboard.json")
            DashboardAssembler.write_to_file(payload, out)
            assert Path(out).exists()
            data = json.loads(Path(out).read_text())
            assert data["sections"][0]["name"] == "x"


# ── LiveDataFeed ──────────────────────────────────────────────

class TestLiveDataFeed:
    def _write_receipts(self, tmpdir: str, receipts: list[dict]) -> None:
        for i, r in enumerate(receipts):
            with open(os.path.join(tmpdir, f"receipt_{i}.json"), "w") as f:
                json.dump(r, f)

    def test_load_receipts_from_dir(self):
        with tempfile.TemporaryDirectory() as td:
            self._write_receipts(td, SAMPLE_RECEIPTS)
            feed = LiveDataFeed(td)
            loaded = feed.load_receipts(since_hours=999)
            assert len(loaded) == 5

    def test_load_receipts_empty_dir(self):
        with tempfile.TemporaryDirectory() as td:
            feed = LiveDataFeed(td)
            assert feed.load_receipts() == []

    def test_load_receipts_missing_dir(self):
        feed = LiveDataFeed("/nonexistent/path/receipts")
        assert feed.load_receipts() == []

    def test_full_dashboard_returns_all_sections(self):
        with tempfile.TemporaryDirectory() as td:
            self._write_receipts(td, SAMPLE_RECEIPTS)
            feed = LiveDataFeed(td)
            payload = feed.full_dashboard()
            names = {s.name for s in payload.sections}
            assert names == {"workflow_summary", "agent_leaderboard", "recent_failures", "cost_summary"}

    def test_write_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            receipts_dir = os.path.join(td, "receipts")
            os.makedirs(receipts_dir)
            self._write_receipts(receipts_dir, SAMPLE_RECEIPTS)
            out = os.path.join(td, "snapshot.json")
            feed = LiveDataFeed(receipts_dir)
            feed.write_snapshot(out)
            assert Path(out).exists()
            data = json.loads(Path(out).read_text())
            assert "sections" in data

    def test_empty_receipts_valid_dashboard(self):
        with tempfile.TemporaryDirectory() as td:
            feed = LiveDataFeed(td)
            payload = feed.full_dashboard()
            assert isinstance(payload, DashboardPayload)
            # All section data should have zero/empty values
            for sec in payload.sections:
                assert isinstance(sec.data, dict)
