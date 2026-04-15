from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://localhost/test")

import runtime.engineering_observability as observability_mod
from runtime.trend_detector import TrendDirection
import surfaces.api.handlers.workflow_admin as workflow_admin
import surfaces.api.rest as rest


def test_code_hotspots_endpoint_passes_filters_to_builder(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeSubsystems:
        def get_bug_tracker(self):
            return object()

    def _fake_build_code_hotspots(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "files": []}

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _FakeSubsystems())
    monkeypatch.setattr(observability_mod, "build_code_hotspots", _fake_build_code_hotspots)

    with TestClient(rest.app) as client:
        response = client.get(
            "/api/observability/code-hotspots?limit=7&roots=runtime,surfaces/api&path_prefix=runtime"
        )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "files": []}
    assert captured["limit"] == 7
    assert captured["roots"] == ["runtime", "surfaces/api"]
    assert captured["path_prefix"] == "runtime"


def test_platform_observability_endpoint_gracefully_degrades_on_health_failure(monkeypatch) -> None:
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: SimpleNamespace())

    def _boom(_subs, _body):
        raise RuntimeError("health unavailable")

    monkeypatch.setattr(workflow_admin, "_handle_health", _boom)

    with TestClient(rest.app) as client:
        response = client.get("/api/observability/platform")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sources"]["platform_health"]["available"] is False
    assert "health unavailable" in payload["sources"]["platform_health"]["detail"]


def test_platform_observability_endpoint_includes_trend_observability(monkeypatch) -> None:
    class _FakeTrendDetector:
        def detect_from_receipts(self):
            return [
                SimpleNamespace(
                    metric_name="latency_p50",
                    provider_slug="openai",
                    direction=TrendDirection.DEGRADING,
                    baseline_value=100.0,
                    current_value=160.0,
                    change_pct=60.0,
                    sample_count=9,
                    severity="warning",
                )
            ]

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: SimpleNamespace())
    monkeypatch.setattr(workflow_admin, "_handle_health", lambda _subs, _body: {
        "preflight": {"overall": "healthy", "checks": []},
        "operator_snapshot": {},
        "lane_recommendation": {"recommended_posture": "green", "reasons": []},
    })
    monkeypatch.setattr(observability_mod, "TrendDetector", _FakeTrendDetector)

    with TestClient(rest.app) as client:
        response = client.get("/api/observability/platform")

    assert response.status_code == 200
    payload = response.json()
    assert payload["trend_observability"]["summary"] == {
        "total_trends": 1,
        "critical_trends": 0,
        "warning_trends": 1,
        "info_trends": 0,
        "degrading_trends": 1,
        "accelerating_trends": 0,
        "improving_trends": 0,
    }
    assert payload["trend_observability"]["trends"][0]["metric_name"] == "latency_p50"
