from __future__ import annotations

import os
import subprocess
from pathlib import Path

from surfaces._workflow_database import workflow_database_authority_for_repo


_REPO_ROOT = Path(__file__).resolve().parents[4]
_HELPER = _REPO_ROOT / "scripts" / "_workflow_env.sh"
_WORKFLOW_ROOT = _REPO_ROOT / "Code&DBs" / "Workflow"


def _sandbox_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    (repo_root / "Code&DBs").mkdir(parents=True)
    (repo_root / "Code&DBs" / "Workflow").symlink_to(_WORKFLOW_ROOT, target_is_directory=True)
    return repo_root


def _helper_authority(repo_root: Path, *, env: dict[str, str]) -> tuple[str, str]:
    shell_env = os.environ.copy()
    if "WORKFLOW_DATABASE_URL" not in env:
        shell_env.pop("WORKFLOW_DATABASE_URL", None)
    shell_env.update(env)
    shell_env["WORKFLOW_ENV_REPO_ROOT"] = str(repo_root)
    completed = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"source {_HELPER!s}; "
                "workflow_load_repo_env; "
                "printf '%s\\n%s' \"$WORKFLOW_DATABASE_URL\" \"$WORKFLOW_DATABASE_AUTHORITY_SOURCE\""
            ),
        ],
        cwd=repo_root,
        env=shell_env,
        check=True,
        capture_output=True,
        text=True,
    )
    url, source = completed.stdout.splitlines()
    return url.strip(), source.strip()


def test_workflow_env_bootstrap_fails_closed_without_authority(tmp_path: Path) -> None:
    repo_root = _sandbox_repo(tmp_path)
    completed = subprocess.run(
        [
            "bash",
            "-c",
            f"source {_HELPER!s}; workflow_load_repo_env",
        ],
        cwd=repo_root,
        env={
            "PATH": os.environ.get("PATH", ""),
            "WORKFLOW_ENV_REPO_ROOT": str(repo_root),
        },
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "WORKFLOW_DATABASE_URL must be set in process env or declared in" in completed.stderr


def test_workflow_env_bootstrap_matches_explicit_python_authority(tmp_path: Path) -> None:
    repo_root = _sandbox_repo(tmp_path)
    env = {
        "PATH": os.environ.get("PATH", ""),
        "WORKFLOW_DATABASE_URL": "postgresql://sandbox.example/praxis",
    }

    resolved_url, resolved_source = _helper_authority(repo_root, env=env)
    authority = workflow_database_authority_for_repo(repo_root, env=env)

    assert (resolved_url, resolved_source) == (
        str(authority.database_url),
        authority.source,
    )


def test_workflow_env_bootstrap_matches_repo_env_python_authority(tmp_path: Path) -> None:
    repo_root = _sandbox_repo(tmp_path)
    repo_env_path = repo_root / ".env"
    repo_env_path.write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo-env.example/praxis\n",
        encoding="utf-8",
    )
    env = {"PATH": os.environ.get("PATH", "")}

    resolved_url, resolved_source = _helper_authority(repo_root, env=env)
    authority = workflow_database_authority_for_repo(repo_root, env=env)

    assert (resolved_url, resolved_source) == (
        str(authority.database_url),
        authority.source,
    )
    assert resolved_source == f"repo_env:{repo_env_path}"


def test_workflow_env_bootstrap_matches_docker_python_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = _sandbox_repo(tmp_path)
    (repo_root / "docker-compose.yml").write_text("services:\n  postgres:\n    image: postgres\n", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -eu",
                "if [ \"$1\" = compose ] && [ \"$4\" = ps ]; then",
                "  printf '%s\\n' 'postgres-container'",
                "elif [ \"$1\" = inspect ]; then",
                "  printf '%s\\n' 'healthy'",
                "elif [ \"$1\" = compose ] && [ \"$4\" = port ]; then",
                "  printf '%s\\n' '0.0.0.0:6543'",
                "else",
                "  exit 1",
                "fi",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {"PATH": f"{fake_bin}:{os.environ.get('PATH', '')}"}
    monkeypatch.setenv("PATH", env["PATH"])

    resolved_url, resolved_source = _helper_authority(repo_root, env=env)
    authority = workflow_database_authority_for_repo(repo_root, env=env)

    assert (resolved_url, resolved_source) == (
        str(authority.database_url),
        authority.source,
    )
    assert resolved_url == "postgresql://postgres@127.0.0.1:6543/praxis"
    assert resolved_source == "docker"


def test_workflow_env_detects_repo_root_when_sourced_from_zsh() -> None:
    completed = subprocess.run(
        [
            "zsh",
            "-lc",
            f"source {_HELPER!s}; printf '%s' \"$workflow_env_repo_root\"",
        ],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == str(_REPO_ROOT)
