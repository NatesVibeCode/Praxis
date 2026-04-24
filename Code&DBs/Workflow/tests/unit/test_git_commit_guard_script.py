from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_PRAXIS_ROOT = Path(__file__).resolve().parents[4]
_GUARD_SRC = _PRAXIS_ROOT / "scripts" / "git-commit-guard"


@pytest.fixture
def mini_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init"], check=True, cwd=repo, capture_output=True)
    scripts = repo / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy(_GUARD_SRC, scripts / "git-commit-guard")
    (scripts / "git-commit-guard").chmod(0o755)
    return repo


def _run_guard(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(repo / "scripts" / "git-commit-guard"), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )


def test_git_commit_guard_status_json_clear(mini_repo: Path) -> None:
    proc = _run_guard(mini_repo, "--json")
    assert proc.returncode == 0
    payload = json.loads(proc.stdout)
    assert payload["ok"] is True
    assert payload["git_index_locked"] is False
    assert payload["claimed"] is False


def test_git_commit_guard_claim_blocks_second_owner(mini_repo: Path) -> None:
    assert _run_guard(mini_repo, "claim", "--owner", "agent-a", "--json").returncode == 0
    proc = _run_guard(mini_repo, "claim", "--owner", "agent-b", "--json")
    assert proc.returncode == 1
    payload = json.loads(proc.stdout)
    assert payload["ok"] is False
    assert payload["owner"] == "agent-a"

    st = _run_guard(mini_repo, "--json")
    assert st.returncode == 1
    body = json.loads(st.stdout)
    assert body["claimed"] is True

    assert _run_guard(mini_repo, "release", "--owner", "agent-b", "--json").returncode == 1
    assert _run_guard(mini_repo, "release", "--owner", "agent-a", "--json").returncode == 0
    assert _run_guard(mini_repo, "--json").returncode == 0
