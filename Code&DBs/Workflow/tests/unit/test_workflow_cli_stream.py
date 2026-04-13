from __future__ import annotations

import argparse
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from surfaces.cli import workflow_cli


class _FakeNotificationConsumer:
    def __init__(self, conn) -> None:
        self.conn = conn

    def iter_run(self, run_id: str, total_jobs: int, timeout_seconds=None, poll_interval: float = 2.0):
        del run_id, total_jobs, timeout_seconds, poll_interval
        yield SimpleNamespace(
            job_label="build_a",
            status="succeeded",
            agent_slug="openai/gpt-5.4-mini",
            duration_seconds=1.2,
            failure_code="",
            created_at=datetime.now(timezone.utc),
        )


def test_cmd_stream_returns_terminal_summary_when_run_already_done(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    statuses = [
        {
            "run_id": "workflow_done",
            "spec_name": "stream smoke",
            "total_jobs": 1,
            "status": "succeeded",
            "jobs": [{"status": "succeeded"}],
        }
    ]

    def _get_status(conn, run_id):
        del conn, run_id
        return statuses.pop(0)

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("runtime.workflow.unified", fromlist=["*"]),
        "get_run_status",
        _get_status,
    )

    result = workflow_cli.cmd_stream(
        argparse.Namespace(run_id="workflow_done", timeout=None, poll_interval=0.01)
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "start  run_id=workflow_done" in output
    assert "done   status=succeeded" in output


def test_cmd_stream_yields_job_progress_and_done(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    statuses = [
        {
            "run_id": "workflow_live",
            "spec_name": "stream smoke",
            "total_jobs": 1,
            "status": "running",
            "jobs": [],
        },
        {
            "run_id": "workflow_live",
            "spec_name": "stream smoke",
            "total_jobs": 1,
            "status": "succeeded",
            "jobs": [{"status": "succeeded"}],
        },
    ]

    def _get_status(conn, run_id):
        del conn, run_id
        return statuses.pop(0)

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        __import__("runtime.workflow.unified", fromlist=["*"]),
        "get_run_status",
        _get_status,
    )
    monkeypatch.setattr(
        __import__("runtime.workflow_notifications", fromlist=["*"]),
        "WorkflowNotificationConsumer",
        _FakeNotificationConsumer,
    )

    result = workflow_cli.cmd_stream(
        argparse.Namespace(run_id="workflow_live", timeout=0.1, poll_interval=0.01)
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "job    label=build_a status=succeeded" in output
    assert "progress completed=1 total=1 passed=1 failed=0" in output
    assert "done   status=succeeded passed=1 failed=0 total=1" in output
