from __future__ import annotations

import argparse
import json
import os
import subprocess
from io import StringIO
from pathlib import Path

import pytest

from surfaces.cli.commands import workflow as workflow_commands
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


def test_cmd_active_uses_status_snapshot_authority(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object]):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "in_flight_status_authority": "runtime.workflow.unified.get_run_status",
            "in_flight_workflows": [
                {
                    "run_id": "workflow_live",
                    "completed_jobs": 1,
                    "status_authority": "runtime.workflow.unified.get_run_status",
                }
            ],
        }

    monkeypatch.setattr(workflow_cli, "run_cli_tool", _fake_run_cli_tool)

    result = workflow_cli.cmd_active(argparse.Namespace())

    assert result == 0
    assert captured == {
        "tool_name": "praxis_status_snapshot",
        "params": {"since_hours": 24},
    }
    payload = json.loads(capsys.readouterr().out)
    assert payload == [
        {
            "run_id": "workflow_live",
            "completed_jobs": 1,
            "status_authority": "runtime.workflow.unified.get_run_status",
        }
    ]


def test_cmd_run_writes_async_result_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    result_path = tmp_path / "workflow-result.json"
    captured: dict[str, object] = {}
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())

    monkeypatch.setattr(
        workflow_cli,
        "submit_workflow_command",
        lambda _conn, **params: captured.update({"params": params}) or {
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
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            dry_run=False,
            fresh=False,
            job_id="job-123",
            run_id="workflow_forced123",
            result_file=str(result_path),
        )
    )

    assert result == 0
    assert captured == {
        "params": {
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.run",
            "spec_path": spec_path,
            "inline_spec": None,
            "repo_root": workflow_cli._repo_root(),
            "run_id": "workflow_forced123",
            "force_fresh_run": False,
            "spec_name": "cli run smoke",
            "total_jobs": 1,
        },
    }
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


def test_cmd_run_passes_force_fresh_run_without_public_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    captured: dict[str, object] = {}
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())

    monkeypatch.setattr(
        workflow_cli,
        "submit_workflow_command",
        lambda _conn, **params: captured.update({"params": params}) or {
            "run_id": "workflow_fresh456",
            "status": "queued",
            "spec_name": "cli run smoke",
            "total_jobs": 1,
            "command_id": "control.command.submit.654",
            "command_status": "succeeded",
            "approval_required": False,
            "result_ref": "workflow_run:workflow_fresh456",
            "stream_url": "/api/workflow-runs/workflow_fresh456/stream",
            "status_url": "/api/workflow-runs/workflow_fresh456/status",
        },
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            dry_run=False,
            fresh=True,
            job_id=None,
            run_id=None,
            result_file=None,
        )
    )

    assert result == 0
    assert captured == {
        "params": {
            "requested_by_kind": "cli",
            "requested_by_ref": "workflow_cli.run",
            "spec_path": spec_path,
            "inline_spec": None,
            "repo_root": workflow_cli._repo_root(),
            "run_id": None,
            "force_fresh_run": True,
            "spec_name": "cli run smoke",
            "total_jobs": 1,
        },
    }


def test_cmd_run_stays_silent_about_result_file_when_not_requested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = _write_spec(tmp_path)
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())

    monkeypatch.setattr(
        workflow_cli,
        "submit_workflow_command",
        lambda _conn, **params: {
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
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            dry_run=False,
            fresh=False,
            job_id=None,
            run_id=None,
            result_file=None,
        )
    )

    assert result == 0
    rendered = capsys.readouterr().out
    assert "Result written to:" not in rendered
    assert "LIVE STREAM" in rendered
    assert "./scripts/praxis workflow stream workflow_test123" in rendered
    assert "./scripts/praxis workflow run-status workflow_test123 --summary" in rendered


def test_cmd_run_preview_execution_prints_structured_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = _write_spec(tmp_path)
    preview_payload = {
        "action": "preview",
        "preview_mode": "execution",
        "spec_name": "cli run smoke",
        "total_jobs": 1,
        "jobs": [{"label": "run_job", "mcp_tool_names": ["praxis_query"]}],
    }
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())
    monkeypatch.setattr(
        workflow_cli,
        "submit_workflow_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("preview should not submit a workflow command")
        ),
    )
    import runtime.workflow.unified as unified

    monkeypatch.setattr(
        unified,
        "preview_workflow_execution",
        lambda _conn, **_kwargs: dict(preview_payload),
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            preview_execution=True,
            dry_run=False,
            fresh=False,
            job_id=None,
            run_id=None,
            result_file=None,
        )
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == preview_payload


def test_cmd_run_renders_live_snapshot_metrics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = _write_spec(tmp_path)
    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: object())

    monkeypatch.setattr(
        workflow_cli,
        "submit_workflow_command",
        lambda _conn, **params: {
            "run_id": "workflow_live123",
            "status": "succeeded",
            "status_source": "live_snapshot",
            "terminal_reason": "runtime.workflow_succeeded",
            "spec_name": "cli run smoke",
            "total_jobs": 1,
            "command_id": "control.command.submit.999",
            "command_status": "succeeded",
            "approval_required": False,
            "result_ref": "workflow_run:workflow_live123",
            "stream_url": "/api/workflow-runs/workflow_live123/stream",
            "status_url": "/api/workflow-runs/workflow_live123/status",
            "run_metrics": {
                "completed_jobs": 1,
                "total_jobs": 1,
                "elapsed_seconds": 0.4,
                "health_state": "healthy",
                "job_status_counts": {"succeeded": 1},
                "total_cost_usd": 0.0123,
                "total_duration_ms": 400,
                "total_tokens_in": 12,
                "total_tokens_out": 34,
                "terminal_reason": "runtime.workflow_succeeded",
            },
        },
    )

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            dry_run=False,
            fresh=False,
            job_id=None,
            run_id=None,
            result_file=None,
        )
    )

    assert result == 0
    rendered = capsys.readouterr().out
    assert "Submission status: succeeded" in rendered
    assert "Status source: live_snapshot" in rendered
    assert "Terminal reason: runtime.workflow_succeeded" in rendered
    assert "Run metrics: 1/1 completed | health=healthy | elapsed=0.4s" in rendered
    assert "Job states: succeeded=1" in rendered
    assert "Usage: cost=$0.0123 | tokens_in=12 | tokens_out=34" in rendered


def test_detached_launch_failure_does_not_claim_result_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "Code&DBs").mkdir()
    (repo_root / "CodeDBs").symlink_to(repo_root / "Code&DBs")
    stdout = StringIO()
    captured: dict[str, object] = {}

    class _DeadProcess:
        pid = 4242

        def poll(self) -> int:
            return 1

    def _fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured["env"] = dict(kwargs["env"])
        return _DeadProcess()

    monkeypatch.setattr(workflow_commands, "cli_repo_root", lambda: repo_root)
    monkeypatch.setattr(
        workflow_commands,
        "workflow_database_authority_for_repo",
        lambda _repo_root, env=None: workflow_commands.SimpleNamespace(
            database_url="postgresql://127.0.0.1:5432/praxis",
            source="docker",
        ),
    )
    monkeypatch.setattr(workflow_commands.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(workflow_commands.time, "sleep", lambda _seconds: None)

    result = workflow_commands._launch_detached_frontdoor(
        command_name="run",
        args=[spec_path],
        stdout=stdout,
        result_file_base="workflow_run_result",
        success_prefix="Workflow submitted",
        emit_parent=False,
    )

    assert result == 1
    assert "Workflow run process exited before durable submission completed." in stdout.getvalue()
    assert "DB authority source: docker" in stdout.getvalue()
    assert "No result file was written." in stdout.getvalue()
    assert "Result file:" not in stdout.getvalue()
    assert captured["command"][:4] == [workflow_commands.sys.executable, "-m", "surfaces.cli.main", "workflow"]
    assert captured["command"][4] == "run"


def test_detached_launch_env_hydrates_workflow_mcp_runtime_keys_from_repo_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".env").write_text(
        "\n".join(
            [
                "WORKFLOW_DATABASE_URL=postgresql://repo-env.example/praxis",
                "PRAXIS_WORKFLOW_MCP_URL=http://mcp.local/mcp",
                "PRAXIS_WORKFLOW_MCP_SIGNING_SECRET=test-signing-secret",
                "OPENAI_API_KEY=must-not-be-forwarded-from-repo-env",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("PRAXIS_WORKFLOW_MCP_URL", raising=False)
    monkeypatch.delenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(
        workflow_commands,
        "workflow_database_authority_for_repo",
        lambda _repo_root, env=None: workflow_commands.SimpleNamespace(
            database_url="postgresql://repo-env.example/praxis",
            source="repo_env",
        ),
    )

    env, _source = workflow_commands._detached_launch_env(repo_root)

    assert env["PRAXIS_WORKFLOW_MCP_URL"] == "http://mcp.local/mcp"
    assert env["PRAXIS_WORKFLOW_MCP_SIGNING_SECRET"] == "test-signing-secret"
    assert "OPENAI_API_KEY" not in env


def test_detached_spawn_launch_reads_result_file_and_reports_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "Code&DBs").mkdir()
    (repo_root / "CodeDBs").symlink_to(repo_root / "Code&DBs")
    stdout = StringIO()
    captured: dict[str, object] = {}

    class _AliveProcess:
        pid = 9898

        def poll(self) -> None:
            return None

    def _fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured["env"] = dict(kwargs["env"])
        result_index = captured["command"].index("--result-file") + 1
        result_path = Path(captured["command"][result_index])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            json.dumps(
                {
                    "run_id": "workflow_spawn_child123",
                    "workflow_id": "cli_run_smoke",
                    "parent_run_id": "workflow_parent_123",
                    "status": "queued",
                }
            ),
            encoding="utf-8",
        )
        return _AliveProcess()

    monkeypatch.setattr(workflow_commands, "cli_repo_root", lambda: repo_root)
    monkeypatch.setattr(
        workflow_commands,
        "workflow_database_authority_for_repo",
        lambda _repo_root, env=None: workflow_commands.SimpleNamespace(
            database_url="postgresql://127.0.0.1:5432/praxis",
            source="process_env",
        ),
    )
    monkeypatch.setattr(workflow_commands.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(workflow_commands.time, "sleep", lambda _seconds: None)

    result = workflow_commands._launch_detached_frontdoor(
        command_name="spawn",
        args=["workflow_parent_123", spec_path, "--reason", "phase.review"],
        stdout=stdout,
        result_file_base="workflow_spawn_result",
        success_prefix="Child workflow spawned",
        emit_parent=True,
    )

    assert result == 0
    rendered = stdout.getvalue()
    assert "Child workflow spawned: workflow_spawn_child123" in rendered
    assert "Parent run: workflow_parent_123" in rendered
    assert "DB authority source: process_env" in rendered
    assert "Result file:" in rendered
    assert "LIVE STREAM" in rendered
    assert "./scripts/praxis workflow stream workflow_spawn_child123" in rendered
    assert "--foreground-submit" in captured["command"]
    assert captured["env"]["WORKFLOW_DATABASE_AUTHORITY_SOURCE"] == "process_env"
    assert "Workflow" in captured["env"]["PYTHONPATH"]


def test_cmd_spawn_writes_async_result_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    result_path = tmp_path / "workflow-spawn-result.json"
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        workflow_cli,
        "run_cli_tool",
        lambda tool_name, params: captured.update({"tool_name": tool_name, "params": params}) or (
            0,
            {
                "run_id": "workflow_spawn_child123",
                "status": "queued",
                "spec_name": "cli run smoke",
                "total_jobs": 1,
                "command_id": "control.command.spawn.321",
                "command_status": "succeeded",
                "approval_required": False,
                "result_ref": "workflow_run:workflow_spawn_child123",
                "stream_url": "/api/workflow-runs/workflow_spawn_child123/stream",
                "status_url": "/api/workflow-runs/workflow_spawn_child123/status",
            },
        ),
    )

    result = workflow_cli.cmd_spawn(
        argparse.Namespace(
            parent_run_id="workflow_parent_123",
            spec=spec_path,
            reason="phase.review",
            parent_job_label="phase_50_review_spawn",
            lineage_depth=1,
            fresh=False,
            job_id="spawn-job-123",
            run_id="workflow_spawn_forced123",
            result_file=str(result_path),
        )
    )

    assert result == 0
    assert captured == {
        "tool_name": "praxis_workflow",
        "params": {
            "action": "spawn",
            "parent_run_id": "workflow_parent_123",
            "spec_path": spec_path,
            "dispatch_reason": "phase.review",
            "parent_job_label": "phase_50_review_spawn",
            "lineage_depth": 1,
            "run_id": "workflow_spawn_forced123",
        },
    }
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["job_id"] == "spawn-job-123"
    assert payload["run_id"] == "workflow_spawn_child123"
    assert payload["parent_run_id"] == "workflow_parent_123"
    assert payload["workflow_id"] == "cli_run_smoke"
    assert payload["status"] == "queued"
