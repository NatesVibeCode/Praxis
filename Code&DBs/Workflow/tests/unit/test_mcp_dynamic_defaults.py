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
    def list_by_sandbox(self, sandbox_id: str):
        assert sandbox_id == "sandbox-live"
        return [
            SimpleNamespace(
                artifact_id="art-1",
                file_path="out/report.txt",
                byte_count=10,
                line_count=1,
                captured_at=SimpleNamespace(isoformat=lambda: "2026-04-13T12:00:00+00:00"),
            )
        ]


def test_praxis_artifacts_list_requires_explicit_sandbox_id(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_tools,
        "_subs",
        SimpleNamespace(get_artifact_store=lambda: _FakeArtifactStore()),
    )

    payload = artifacts_tools.tool_praxis_artifacts({"action": "list"})

    assert payload["reason_code"] == "sandbox_id.required"
    assert "sandbox_id is required" in payload["error"]


def test_praxis_artifacts_list_rejects_demo_placeholder(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_tools,
        "_subs",
        SimpleNamespace(get_artifact_store=lambda: _FakeArtifactStore()),
    )

    payload = artifacts_tools.tool_praxis_artifacts(
        {"action": "list", "sandbox_id": "sandbox_abc123"}
    )

    assert payload["reason_code"] == "sandbox_id.placeholder_not_allowed"
    assert payload["sandbox_id"] == "sandbox_abc123"


def test_praxis_artifacts_list_accepts_explicit_sandbox_id(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_tools,
        "_subs",
        SimpleNamespace(get_artifact_store=lambda: _FakeArtifactStore()),
    )

    payload = artifacts_tools.tool_praxis_artifacts(
        {"action": "list", "sandbox_id": "sandbox-live"}
    )

    assert payload["sandbox_id"] == "sandbox-live"
    assert payload["count"] == 1
    assert "note" not in payload


def test_praxis_wave_next_requires_explicit_wave_id(monkeypatch) -> None:
    orch = WaveOrchestrator("orch-defaults")
    orch.add_wave("wave-live", [{"label": "build"}, {"label": "test", "depends_on": ["build"]}])
    orch.start_wave("wave-live")

    monkeypatch.setattr(
        wave_tools,
        "_subs",
        SimpleNamespace(get_wave_orchestrator=lambda: orch),
    )

    payload = wave_tools.tool_praxis_wave({"action": "next"})

    assert payload["reason_code"] == "wave_id.required"
    assert "wave_id is required" in payload["error"]


def test_praxis_wave_next_rejects_demo_placeholder(monkeypatch) -> None:
    orch = WaveOrchestrator("orch-defaults")
    orch.add_wave("wave-live", [{"label": "build"}, {"label": "test", "depends_on": ["build"]}])
    orch.start_wave("wave-live")

    monkeypatch.setattr(
        wave_tools,
        "_subs",
        SimpleNamespace(get_wave_orchestrator=lambda: orch),
    )

    payload = wave_tools.tool_praxis_wave({"action": "next", "wave_id": "wave_abc123"})

    assert payload["reason_code"] == "wave_id.placeholder_not_allowed"
    assert payload["wave_id"] == "wave_abc123"


def test_praxis_wave_next_accepts_explicit_wave_id(monkeypatch) -> None:
    orch = WaveOrchestrator("orch-defaults")
    orch.add_wave("wave-live", [{"label": "build"}, {"label": "test", "depends_on": ["build"]}])
    orch.start_wave("wave-live")

    monkeypatch.setattr(
        wave_tools,
        "_subs",
        SimpleNamespace(get_wave_orchestrator=lambda: orch),
    )

    payload = wave_tools.tool_praxis_wave({"action": "next", "wave_id": "wave-live"})

    assert payload["wave_id"] == "wave-live"
    assert payload["runnable_jobs"] == ["build"]
    assert "note" not in payload
