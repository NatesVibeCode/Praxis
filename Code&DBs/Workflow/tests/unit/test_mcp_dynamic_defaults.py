from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))
os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

from runtime.wave_orchestrator import WaveOrchestrator
from surfaces.mcp.tools import artifacts as artifacts_tools
from surfaces.mcp.tools import wave as wave_tools


class _FakeArtifactStore:
    def latest_sandbox_id(self) -> str | None:
        return "sandbox-live"

    def list_by_sandbox(self, sandbox_id: str):
        return [
            SimpleNamespace(
                artifact_id="art-1",
                file_path="out/report.txt",
                byte_count=10,
                line_count=1,
                captured_at=SimpleNamespace(isoformat=lambda: "2026-04-13T12:00:00+00:00"),
            )
        ]


def test_praxis_artifacts_list_defaults_to_latest_sandbox(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_tools,
        "_subs",
        SimpleNamespace(get_artifact_store=lambda: _FakeArtifactStore()),
    )

    payload = artifacts_tools.tool_praxis_artifacts({"action": "list"})

    assert payload["sandbox_id"] == "sandbox-live"
    assert payload["count"] == 1
    assert "using latest sandbox sandbox-live" in payload["note"]


def test_praxis_wave_next_defaults_to_current_wave(monkeypatch) -> None:
    orch = WaveOrchestrator("orch-defaults")
    orch.add_wave("wave-live", [{"label": "build"}, {"label": "test", "depends_on": ["build"]}])
    orch.start_wave("wave-live")

    monkeypatch.setattr(
        wave_tools,
        "_subs",
        SimpleNamespace(get_wave_orchestrator=lambda: orch),
    )

    payload = wave_tools.tool_praxis_wave({"action": "next"})

    assert payload["wave_id"] == "wave-live"
    assert payload["runnable_jobs"] == ["build"]
    assert "wave_id omitted; using wave-live" == payload["note"]
