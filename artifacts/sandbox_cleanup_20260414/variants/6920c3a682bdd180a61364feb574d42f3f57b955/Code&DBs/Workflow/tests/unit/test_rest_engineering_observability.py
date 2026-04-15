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
