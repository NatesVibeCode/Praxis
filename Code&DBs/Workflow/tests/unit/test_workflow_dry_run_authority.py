from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


_mod_path = Path(__file__).resolve().parents[2] / "surfaces" / "cli" / "workflow_runner.py"
_spec = importlib.util.spec_from_file_location("workflow_runner_dry_run_authority", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workflow_runner_dry_run_authority"] = _mod
_spec.loader.exec_module(_mod)


def test_workflow_runner_dry_run_delegates_to_runtime_authority(monkeypatch) -> None:
    import runtime.workflow.dry_run as dry_run_module

    captured: dict[str, object] = {}

    spec = _mod.WorkflowSpec(
        name="dry run authority",
        workflow_id="workflow.dry_run_authority",
        phase="TEST",
        jobs=[
            {
                "label": "test_job",
                "agent": "anthropic/claude-sonnet-4",
                "prompt": "simulate me",
            }
        ],
        verify_refs=[],
        outcome_goal="",
        anti_requirements=[],
        raw={},
    )

    def _fake_dry_run_workflow(observed_spec, *, pg_conn=None, repo_root=None):
        captured["spec"] = observed_spec
        captured["pg_conn"] = pg_conn
        captured["repo_root"] = repo_root
        return dry_run_module.DryRunResult(
            spec_name=observed_spec.name,
            total_jobs=2,
            succeeded=1,
            failed=0,
            skipped=0,
            blocked=1,
            duration_seconds=0.25,
            receipts_written=("dry_run:test_job", "dry_run:blocked_job"),
            job_results=(
                dry_run_module.DryRunJobResult(
                    job_label="test_job",
                    agent_slug="anthropic/claude-sonnet-4",
                    status="succeeded",
                    exit_code=0,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
                ),
                dry_run_module.DryRunJobResult(
                    job_label="blocked_job",
                    agent_slug="anthropic/claude-sonnet-4",
                    status="blocked",
                    exit_code=None,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
                ),
            ),
        )

    monkeypatch.setattr(dry_run_module, "dry_run_workflow", _fake_dry_run_workflow)

    notifications: list[dict[str, object]] = []
    monkeypatch.setattr(_mod, "dispatch_notification_payload", notifications.append)

    runner = _mod.WorkflowRunner.__new__(_mod.WorkflowRunner)
    runner._pg_conn = object()
    runner._repo_root = "/tmp/praxis"
    result = runner.run_workflow(spec, dry_run=True, run_id="workflow_run:test")

    assert captured["spec"] is spec
    assert captured["pg_conn"] is runner._pg_conn
    assert captured["repo_root"] == "/tmp/praxis"
    assert result.total_jobs == 2
    assert result.succeeded == 1
    assert result.blocked == 1
    assert result.receipts_written == ("dry_run:test_job", "dry_run:blocked_job")
    assert result.job_results[0].stdout == "[dry-run] Would execute workflow job 'test_job'"
    assert result.job_results[1].status == "blocked"
    assert result.job_results[1].stderr == "Blocked by dry-run governance or dependency simulation."
    assert notifications[0]["reason_code"] == "workflow_runner.batch_complete"
    assert notifications[0]["run_id"] == "workflow_run:test"
    assert notifications[0]["blocked"] == 1


def test_workflow_runner_api_execution_does_not_retry_locally(monkeypatch) -> None:
    """Retry authority belongs to durable workflow job transitions."""
    calls = 0

    def _fail_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("rate limit")

    monkeypatch.setattr(_mod, "execute_api_in_sandbox", _fail_once)
    runner = _mod.WorkflowRunner.__new__(_mod.WorkflowRunner)
    runner._repo_root = Path(__file__).resolve().parents[4]

    result = runner._execute_api_job(
        label="api_job",
        agent_slug="openai/gpt-5.4",
        agent_config=SimpleNamespace(),
        prompt="hello",
        timeout=30,
    )

    assert calls == 1
    assert result.status == "failed"
    assert result.retry_count == 0
    assert "RuntimeError: rate limit" in result.stderr
