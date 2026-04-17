from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import runtime.engineering_observability as observability_mod
from runtime.health_map import HealthMapper
from runtime.trend_detector import TrendDirection


def _bug(*, bug_id: str, title: str, severity: str, status: str, category: str):
    return SimpleNamespace(
        bug_id=bug_id,
        title=title,
        severity=SimpleNamespace(value=severity),
        status=SimpleNamespace(value=status),
        category=SimpleNamespace(value=category),
    )


class _FakeHealthMapper:
    def analyze_directory(self, root: str):
        return [
            SimpleNamespace(
                module_path=str(Path(root) / "engine.py"),
                health_score=24,
                line_count=620,
                function_count=14,
            )
        ]


class _FakeRiskScorer:
    def compute_from_receipts(self):
        return [
            SimpleNamespace(
                file_path="runtime/engine.py",
                risk_score=57.5,
                touch_count=6,
                success_rate=0.5,
                avg_duration_ms=1800,
                failure_codes=("IMPORT_ERROR",),
                last_touched=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
            )
        ]


class _FakeTrendDetector:
    def detect_from_receipts(self):
        return [
            SimpleNamespace(
                metric_name="cost_trend",
                provider_slug="anthropic",
                direction=TrendDirection.DEGRADING,
                baseline_value=0.5,
                current_value=0.9,
                change_pct=80.0,
                sample_count=7,
                severity="warning",
            )
        ]


class _FakeBugTracker:
    def __init__(self) -> None:
        self.failure_packet_calls: list[dict[str, object]] = []
        self._bugs = [
            _bug(
                bug_id="BUG-1",
                title="Import path regression",
                severity="P1",
                status="OPEN",
                category="IMPORT",
            ),
            _bug(
                bug_id="BUG-2",
                title="Missing verification receipt",
                severity="P2",
                status="IN_PROGRESS",
                category="VERIFY",
            ),
        ]
        self._packets = {
            "BUG-1": {
                "latest_receipt": {
                    "write_paths": ("runtime/engine.py",),
                    "verified_paths": ("runtime/engine.py",),
                },
                "lifecycle": {
                    "recurrence_count": 4,
                    "impacted_run_count": 3,
                    "has_regression_after_fix": True,
                },
                "replay_context": {"ready": True},
                "fix_verification": {"fix_verified": True},
                "observability_state": "complete",
                "observability_gaps": (),
            },
            "BUG-2": {
                "latest_receipt": {
                    "write_paths": ("runtime/engine.py",),
                    "verified_paths": (),
                },
                "lifecycle": {
                    "recurrence_count": 2,
                    "impacted_run_count": 1,
                    "has_regression_after_fix": False,
                },
                "replay_context": {"ready": False},
                "fix_verification": {"fix_verified": False},
                "observability_state": "degraded",
                "observability_gaps": ("receipt.missing",),
            },
        }

    def list_bugs(self, *args, **kwargs):
        del args, kwargs
        return list(self._bugs)

    def failure_packet(
        self,
        bug_id: str,
        *,
        receipt_limit: int = 1,
        allow_backfill: bool = True,
    ):
        self.failure_packet_calls.append(
            {
                "bug_id": bug_id,
                "receipt_limit": receipt_limit,
                "allow_backfill": allow_backfill,
            }
        )
        return self._packets[bug_id]

    def stats(self):
        return SimpleNamespace(
            total=2,
            by_status={"OPEN": 1, "IN_PROGRESS": 1},
            by_severity={"P1": 1, "P2": 1},
            by_category={"IMPORT": 1, "VERIFY": 1},
            open_count=2,
            mttr_hours=4.5,
            packet_ready_count=2,
            replay_ready_count=1,
            replay_blocked_count=1,
            fix_verified_count=1,
            underlinked_count=0,
            observability_state="complete",
            errors=(),
        )


def test_build_code_hotspots_merges_static_risk_and_bug_signals(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "engine.py").write_text("def run():\n    return True\n", encoding="utf-8")

    monkeypatch.setattr(observability_mod, "HealthMapper", _FakeHealthMapper)
    monkeypatch.setattr(observability_mod, "RiskScorer", _FakeRiskScorer)

    tracker = _FakeBugTracker()
    payload = observability_mod.build_code_hotspots(
        repo_root=tmp_path,
        bug_tracker=tracker,
        roots=("runtime",),
        limit=5,
    )

    assert payload["summary"]["linked_bug_count"] == 2
    assert payload["files"][0]["file_path"] == "runtime/engine.py"
    assert payload["files"][0]["open_bug_count"] == 2
    assert payload["files"][0]["regression_count"] == 1
    assert payload["files"][0]["risk_score"] == 57.5
    assert "under-observed bug(s)" in " ".join(payload["files"][0]["signals"])
    assert payload["components"][0]["component"] == "runtime"
    assert tracker.failure_packet_calls
    assert all(call["allow_backfill"] is False for call in tracker.failure_packet_calls)


def test_build_bug_scoreboard_surfaces_recurring_and_under_observed_bugs(tmp_path: Path) -> None:
    tracker = _FakeBugTracker()

    payload = observability_mod.build_bug_scoreboard(
        bug_tracker=tracker,
        limit=5,
        repo_root=tmp_path,
    )

    assert payload["summary"]["total_bugs"] == 2
    assert payload["summary"]["replay_ready_bugs"] == 1
    assert payload["top_recurring"][0]["bug_id"] == "BUG-1"
    assert payload["regressions"][0]["bug_id"] == "BUG-1"
    assert payload["under_observed"][0]["bug_id"] == "BUG-2"
    assert tracker.failure_packet_calls
    assert all(call["allow_backfill"] is False for call in tracker.failure_packet_calls)


def test_build_platform_observability_flattens_probe_state() -> None:
    payload = observability_mod.build_platform_observability(
        platform_payload={
            "preflight": {
                "overall": "degraded",
                "checks": [
                    {
                        "name": "queue_depth",
                        "passed": True,
                        "status": "warning",
                        "message": "Queue growing",
                        "details": {"total_queued": 7},
                    },
                    {
                        "name": "api_liveness",
                        "passed": False,
                        "status": "failed",
                        "message": "API not reachable",
                        "details": {},
                    },
                ],
            },
            "operator_snapshot": {
                "posture": "operate",
                "pending_jobs": 3,
                "running_jobs": 2,
                "active_leases": 1,
                "circuit_breaker_open": ["openai"],
                "loop_warnings": 1,
                "write_conflicts": 2,
                "governance_blocks": 0,
            },
            "lane_recommendation": {
                "recommended_posture": "operate",
                "reasons": ["queue backed up"],
            },
        }
    )

    assert payload["summary"]["overall"] == "degraded"
    assert payload["summary"]["queue_depth"] == 7
    assert payload["summary"]["failed_checks"] == 1
    assert payload["summary"]["warning_checks"] == 1
    assert payload["checks"][0]["status"] == "warning"
    assert "queue backed up" in payload["degraded_causes"]


def test_build_trend_observability_summarizes_recent_trends(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(observability_mod, "TrendDetector", _FakeTrendDetector)

    payload = observability_mod.build_trend_observability()

    assert payload["summary"] == {
        "total_trends": 1,
        "critical_trends": 0,
        "warning_trends": 1,
        "info_trends": 0,
        "degrading_trends": 1,
        "accelerating_trends": 0,
        "improving_trends": 0,
    }
    assert payload["trends"][0]["provider_slug"] == "anthropic"
    assert "cost_trend" in payload["trend_digest"]


def test_health_mapper_skips_test_filenames(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "real.py").write_text("def keep():\n    return 1\n", encoding="utf-8")
    (runtime_dir / "test_widget.py").write_text("def skip():\n    return 1\n", encoding="utf-8")
    (runtime_dir / "widget_test.py").write_text("def skip():\n    return 1\n", encoding="utf-8")

    mapper = HealthMapper()
    results = mapper.analyze_directory(str(runtime_dir))

    assert {Path(item.module_path).name for item in results} == {"real.py"}
