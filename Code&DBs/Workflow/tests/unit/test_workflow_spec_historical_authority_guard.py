from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from runtime.workflow_spec import (
    WorkflowSpec,
    WorkflowSpecError,
    load_raw,
    normalize_operator_local_repo_paths,
)


def _write_spec(path: Path, prompt: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "name": "Historical packet guard",
                "jobs": [{"label": "repair", "prompt": prompt}],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_retired_database_url_prevents_queue_spec_execution(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path / "Code&DBs/Workflow/artifacts/workflow/bug_fix.queue.json",
        "Database: postgresql://nate@127.0.0.1:5432/dag_workflow",
    )

    with pytest.raises(WorkflowSpecError) as excinfo:
        WorkflowSpec.load(str(spec_path))

    message = str(excinfo.value)
    assert "cannot be executed as a live workflow" in message
    assert "retired localhost dag_workflow database authority" in message


def test_direct_psql_repair_instruction_prevents_queue_spec_execution(
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(
        tmp_path / "config/cascade/specs/old_fix.queue.json",
        'Run psql "$WORKFLOW_DATABASE_URL" -c "UPDATE bugs SET status = \'FIXED\'"',
    )

    with pytest.raises(WorkflowSpecError) as excinfo:
        load_raw(str(spec_path))

    assert "direct psql/SQL repair instruction" in str(excinfo.value)


def test_clean_repo_relative_queue_spec_still_loads(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path / "artifacts/workflow/current.queue.json",
        "Use praxis workflow tools call praxis_bugs and repo-relative paths only.",
    )

    spec = WorkflowSpec.load(str(spec_path))

    assert spec.name == "Historical packet guard"
    assert spec.jobs[0]["label"] == "repair"


def test_operator_local_host_path_is_auto_normalized(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """BUG-C585EFE6: host-absolute repo prefixes are self-fixable; submitter
    must rewrite them to /workspace and emit a soft-warn breadcrumb instead
    of failing the chain submit."""

    spec_path = _write_spec(
        tmp_path / "artifacts/workflow/host_absolute.queue.json",
        (
            "Read the coordination file `/Users/nate/Praxis/Code&DBs/"
            "Workflow/artifacts/workflow/bug_resolution_program/foo.json`."
        ),
    )

    with caplog.at_level(logging.WARNING, logger="runtime.workflow_spec"):
        loaded = load_raw(str(spec_path))

    prompt = loaded["jobs"][0]["prompt"]
    assert "/Users/nate/Praxis" not in prompt
    assert "/workspace/Code&DBs/Workflow/artifacts" in prompt
    assert any(
        "operator-local prefix" in record.getMessage()
        for record in caplog.records
    )


def test_operator_local_volumes_path_is_auto_normalized(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path / "artifacts/workflow/host_absolute_volumes.queue.json",
        (
            "Open /Volumes/Users/natha/Documents/Builds/Praxis/scripts/"
            "praxis_bugs.py for review."
        ),
    )

    loaded = load_raw(str(spec_path))

    prompt = loaded["jobs"][0]["prompt"]
    assert "/Volumes/Users/natha" not in prompt
    assert "/workspace/scripts/praxis_bugs.py" in prompt


def test_normalize_helper_is_pure_and_idempotent() -> None:
    raw = (
        "[a](/Users/nate/Praxis/x.py) and "
        "[b](/Volumes/Users/natha/Documents/Builds/Praxis/y.py)"
    )
    once, breadcrumbs_once = normalize_operator_local_repo_paths(raw)
    twice, breadcrumbs_twice = normalize_operator_local_repo_paths(once)

    assert "/Users/nate/Praxis" not in once
    assert "/Volumes/Users/natha" not in once
    assert once.count("/workspace") == 2
    assert once == twice
    assert len(breadcrumbs_once) == 2
    assert breadcrumbs_twice == []


def test_normalize_leaves_clean_text_alone() -> None:
    raw = "Use repo-relative paths only: Code&DBs/Workflow/runtime/foo.py"
    out, breadcrumbs = normalize_operator_local_repo_paths(raw)
    assert out == raw
    assert breadcrumbs == []


def test_retired_localhost_dsn_still_blocks_after_normalization(
    tmp_path: Path,
) -> None:
    """Path normalization must not soften the genuine retired-authority
    blockers — localhost DSNs are not self-fixable."""

    spec_path = _write_spec(
        tmp_path / "artifacts/workflow/mixed.queue.json",
        (
            "Connect to postgresql://localhost:5432/praxis from "
            "/Users/nate/Praxis/scripts/cleanup.py"
        ),
    )

    with pytest.raises(WorkflowSpecError) as excinfo:
        load_raw(str(spec_path))

    assert "retired localhost Praxis.db authority" in str(excinfo.value)
