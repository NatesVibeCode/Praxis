from __future__ import annotations

import json
from pathlib import Path

from surfaces.mcp.tools import research_workflow


def test_research_workflow_writes_generated_specs_under_workflow_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    launched_paths: list[str] = []

    monkeypatch.setattr(research_workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        research_workflow,
        "_launch_workflow",
        lambda spec_path: launched_paths.append(spec_path) or {"run_id": "run-research"},
    )

    result = research_workflow.tool_praxis_research_workflow(
        {
            "action": "run",
            "topic": "Agent architecture patterns",
            "workers": 2,
            "agent": "test/research",
        },
    )

    spec_path = Path(launched_paths[0])
    assert spec_path == tmp_path / "artifacts" / "workflow" / "research" / "research_agent_architecture_patterns.queue.json"
    assert spec_path.exists()
    assert not (tmp_path / "config" / "specs").exists()
    assert result["workflow_spec_path"] == str(spec_path)

    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    assert spec["name"] == "Research: Agent architecture patterns"
    assert spec["target_repo"] == str(tmp_path)
    assert spec["jobs"][1]["replicate"] == 2
