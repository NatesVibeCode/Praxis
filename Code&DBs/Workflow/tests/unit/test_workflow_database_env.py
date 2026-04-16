from __future__ import annotations

from pathlib import Path

import pytest

from storage.postgres import PostgresConfigurationError
from surfaces import _workflow_database


def test_workflow_database_env_falls_back_to_docker_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "docker-compose.yml").write_text("services:\n  postgres:\n", encoding="utf-8")

    monkeypatch.setattr(_workflow_database, "_read_repo_env_file", lambda _path: {})
    monkeypatch.setattr(
        _workflow_database,
        "_try_resolve_docker_database_url",
        lambda _repo_root: "postgresql://127.0.0.1:5432/praxis",
    )

    resolved = _workflow_database.workflow_database_env_for_repo(repo_root, env={})

    assert resolved == {
        "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:5432/praxis",
        "PATH": "",
    }


def test_workflow_database_env_fails_closed_without_any_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(_workflow_database, "_read_repo_env_file", lambda _path: {})
    monkeypatch.setattr(_workflow_database, "_try_resolve_docker_database_url", lambda _repo_root: None)

    with pytest.raises(PostgresConfigurationError, match="WORKFLOW_DATABASE_URL"):
        _workflow_database.workflow_database_env_for_repo(repo_root, env={})
