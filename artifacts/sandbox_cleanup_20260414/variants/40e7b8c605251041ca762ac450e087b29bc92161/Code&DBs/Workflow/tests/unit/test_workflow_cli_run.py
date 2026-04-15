from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from surfaces.cli import workflow_cli


def _write_spec(tmp_path: Path) -> str:
    payload = {
        "name": "cli run smoke",
        "workflow_id": "cli_run_smoke",
        "phase": "test",
        "jobs": [
            {
                "label": "run_job",
                "agent": "openai/gpt-5.4-mini",
                "prompt": "Run identity check.",
            }
        ],
    }
    path = tmp_path / "spec.queue.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def test_cmd_run_writes_async_result_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    result_path = tmp_path / "workflow-result.json"

    monkeypatch.setattr(
        workflow_cli,
        "run_cli_tool",
        lambda tool_name, params: (
            0,
            {
                "run_id": "workflow_test123",
                "status": "queued",
                "spec_name": "cli run smoke",
                "total_jobs": 1,
                "command_id": "control.command.submit.321",
                "command_status": "succeeded",
                "approval_required": False,
                "result_ref": "workflow_run:workflow_test123",
                "stream_url": "/api/workflow-runs/workflow_test123/stream",
                "status_url": "/api/workflow-runs/workflow_test123/status",
            },
        ),
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            dry_run=False,
            job_id="job-123",
            run_id=None,
            result_file=str(result_path),
        )
    )

    assert result == 0
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["job_id"] == "job-123"
    assert payload["run_id"] == "workflow_test123"
    assert payload["workflow_id"] == "cli_run_smoke"
    assert payload["status"] == "queued"
    assert payload["total_jobs"] == 1
    assert payload["command_id"] == "control.command.submit.321"
    assert payload["command_status"] == "succeeded"
    assert payload["approval_required"] is False
    assert payload["result_ref"] == "workflow_run:workflow_test123"
    assert payload["stream_url"] == "/api/workflow-runs/workflow_test123/stream"
    assert payload["status_url"] == "/api/workflow-runs/workflow_test123/status"
