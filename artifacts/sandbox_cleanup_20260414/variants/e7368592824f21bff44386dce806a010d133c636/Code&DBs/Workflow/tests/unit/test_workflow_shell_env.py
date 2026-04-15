from __future__ import annotations

import subprocess
from pathlib import Path


def test_workflow_env_bootstrap_falls_back_to_repo_contract() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    helper = repo_root / "scripts" / "_workflow_env.sh"

    completed = subprocess.run(
        [
            "bash",
            "-lc",
            f"unset WORKFLOW_DATABASE_URL; source {helper!s}; workflow_load_repo_env; printf '%s' \"$WORKFLOW_DATABASE_URL\"",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == "postgresql://localhost:5432/praxis"
