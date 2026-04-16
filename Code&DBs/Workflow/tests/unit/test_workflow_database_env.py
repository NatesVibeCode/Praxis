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
    captured: dict[str, object] = {}

    def _fake_resolve_runtime_database_url(*, env, repo_root: Path, required: bool) -> str:
        captured["env"] = env
        captured["repo_root"] = repo_root
        captured["required"] = required
        return "postgresql://127.0.0.1:5432/praxis"

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_url",
        _fake_resolve_runtime_database_url,
    )

    resolved = _workflow_database.workflow_database_env_for_repo(repo_root, env={})

    assert resolved == {
        "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:5432/praxis",
        "PATH": "",
    }
    assert captured == {
        "env": {},
        "repo_root": repo_root,
        "required": True,
    }


def test_workflow_database_url_for_repo_uses_runtime_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    captured: dict[str, object] = {}

    def _fake_resolve_runtime_database_url(*, env, repo_root: Path, required: bool) -> str:
        captured["env"] = env
        captured["repo_root"] = repo_root
        captured["required"] = required
        return "postgresql://127.0.0.1:5432/praxis"

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_url",
        _fake_resolve_runtime_database_url,
    )

    resolved = _workflow_database.workflow_database_url_for_repo(repo_root, env={})

    assert resolved == "postgresql://127.0.0.1:5432/praxis"
    assert captured == {
        "env": {},
        "repo_root": repo_root,
        "required": True,
    }


def test_workflow_database_env_fails_closed_without_any_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _raise_missing_authority(*, env, repo_root: Path, required: bool) -> str:
        raise PostgresConfigurationError(
            "postgres.config_missing",
            "WORKFLOW_DATABASE_URL is required",
        )

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_url",
        _raise_missing_authority,
    )

    with pytest.raises(PostgresConfigurationError, match="WORKFLOW_DATABASE_URL"):
        _workflow_database.workflow_database_env_for_repo(repo_root, env={})
