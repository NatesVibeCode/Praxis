from __future__ import annotations

from pathlib import Path


WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
APP_SRC = WORKFLOW_ROOT / "surfaces" / "app" / "src"
CANVAS_RUN_PANEL = APP_SRC / "canvas" / "CanvasRunPanel.tsx"
LIVE_RUN_SNAPSHOT = APP_SRC / "dashboard" / "useLiveRunSnapshot.ts"
RUN_DETAIL_VIEW = APP_SRC / "dashboard" / "RunDetailView.tsx"


def test_run_panel_consumers_use_shared_run_api_helpers() -> None:
    canvas_source = CANVAS_RUN_PANEL.read_text(encoding="utf-8")
    snapshot_source = LIVE_RUN_SNAPSHOT.read_text(encoding="utf-8")
    run_detail_source = RUN_DETAIL_VIEW.read_text(encoding="utf-8")

    assert "runJobsPath" in canvas_source
    assert "runsRecentPath" in canvas_source
    assert "workflowRunStreamPath" in canvas_source
    assert "fetch(`/api/runs/recent?limit=10`)" not in canvas_source
    assert "fetch(`/api/runs/${encodeURIComponent(runId)}/jobs/${job.id}`)" not in canvas_source
    assert "new EventSource(`/api/workflow-runs/${encodeURIComponent(runId)}/stream`)" not in canvas_source

    assert "runJobsPath" in run_detail_source
    assert "fetch(`/api/runs/${encodeURIComponent(runId)}/jobs/${jobId}`)" not in run_detail_source

    assert "runDetailPath" in snapshot_source
    assert "runsRecentPath" in snapshot_source
    assert "workflowRunStreamPath" in snapshot_source
    assert "fetch(`/api/runs/${encodeURIComponent(runId)}`)" not in snapshot_source
    assert "fetch('/api/runs/recent?limit=100')" not in snapshot_source
    assert "const streamUrl = `/api/workflow-runs/${encodeURIComponent(runId)}/stream`" not in snapshot_source
