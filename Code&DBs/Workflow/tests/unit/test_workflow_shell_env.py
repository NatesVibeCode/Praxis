from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_workflow_env_bootstrap_fails_closed_without_authority() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    helper = repo_root / "scripts" / "_workflow_env.sh"

    completed = subprocess.run(
        [
            "bash",
            "-lc",
            f"unset WORKFLOW_DATABASE_URL; source {helper!s}; workflow_resolve_docker_database_url() {{ return 1; }}; workflow_load_repo_env",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "must be set explicitly by Docker or Cloudflare authority" in completed.stderr


def test_workflow_env_bootstrap_preserves_explicit_authority() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    helper = repo_root / "scripts" / "_workflow_env.sh"
    env = os.environ.copy()
    env["WORKFLOW_DATABASE_URL"] = "postgresql://sandbox.example/praxis"

    completed = subprocess.run(
        [
            "bash",
            "-lc",
            f"source {helper!s}; workflow_load_repo_env; printf '%s' \"$WORKFLOW_DATABASE_URL\"",
        ],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == "postgresql://sandbox.example/praxis"


def test_workflow_env_detects_repo_root_when_sourced_from_zsh() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    helper = repo_root / "scripts" / "_workflow_env.sh"

    completed = subprocess.run(
        [
            "zsh",
            "-lc",
            f"source {helper!s}; printf '%s' \"$workflow_env_repo_root\"",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == str(repo_root)
