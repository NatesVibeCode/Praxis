from __future__ import annotations

import json
from pathlib import Path

import pytest

from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError, load_raw


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
