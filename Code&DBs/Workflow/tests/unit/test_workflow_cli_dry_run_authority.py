from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from surfaces.cli import workflow_cli


def _write_spec(tmp_path: Path) -> str:
    payload = (
        '{'
        '"name":"cli dry run authority",'
        '"workflow_id":"cli_dry_run_authority",'
        '"phase":"test",'
        '"jobs":[{"label":"run_job","agent":"openai/gpt-5.4-mini","prompt":"Run identity check.","workdir":"/tmp"}]'
        '}'
    )
    path = tmp_path / "spec.queue.json"
    path.write_text(payload, encoding="utf-8")
    return str(path)


def test_cmd_run_dry_run_passes_runtime_authority_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    captured: dict[str, object] = {}
    pg_conn = object()

    monkeypatch.setattr(workflow_cli, "_get_pg_conn", lambda: pg_conn)
    monkeypatch.setattr(workflow_cli, "_repo_root", lambda: "/tmp/praxis")

    import runtime.workflow.dry_run as dry_run_module

    def _fake_dry_run(spec, *, pg_conn=None, repo_root=None):
        captured["spec_name"] = spec.name
        captured["pg_conn"] = pg_conn
        captured["repo_root"] = repo_root
        return dry_run_module.DryRunResult(
            spec_name=spec.name,
            total_jobs=1,
            succeeded=1,
            failed=0,
            skipped=0,
            blocked=0,
            duration_seconds=0.01,
            receipts_written=("dry_run:run_job",),
            job_results=(
                dry_run_module.DryRunJobResult(
                    job_label="run_job",
                    agent_slug="openai/gpt-5.4-mini",
                    status="succeeded",
                    exit_code=0,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
                ),
            ),
        )

    monkeypatch.setattr(dry_run_module, "dry_run_workflow", _fake_dry_run)

    result = workflow_cli.cmd_run(
        argparse.Namespace(
            spec=spec_path,
            preview_execution=False,
            dry_run=True,
            fresh=False,
            job_id=None,
            run_id=None,
            result_file=None,
        )
    )

    assert result == 0
    assert captured == {
        "spec_name": "cli dry run authority",
        "pg_conn": pg_conn,
        "repo_root": "/tmp/praxis",
    }
