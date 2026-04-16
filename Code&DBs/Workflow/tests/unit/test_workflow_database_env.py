from __future__ import annotations

from pathlib import Path

import pytest

from runtime._workflow_database import WorkflowDatabaseAuthority
from storage.postgres import PostgresConfigurationError
from surfaces import _workflow_database


def test_workflow_database_env_falls_back_to_docker_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    captured: dict[str, object] = {}

    def _fake_resolve_runtime_database_authority(*, env, repo_root: Path, required: bool) -> WorkflowDatabaseAuthority:
        captured["env"] = env
        captured["repo_root"] = repo_root
        captured["required"] = required
        return WorkflowDatabaseAuthority(
            database_url="postgresql://127.0.0.1:5432/praxis",
            source="docker",
        )

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_authority",
        _fake_resolve_runtime_database_authority,
    )

    resolved = _workflow_database.workflow_database_env_for_repo(repo_root, env={})

    assert resolved == {
        "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:5432/praxis",
        "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "docker",
        "PATH": "",
    }
    assert captured == {
        "env": {},
        "repo_root": repo_root,
        "required": True,
    }


def test_workflow_database_authority_for_repo_uses_runtime_authority(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    captured: dict[str, object] = {}

    def _fake_resolve_runtime_database_authority(*, env, repo_root: Path, required: bool) -> WorkflowDatabaseAuthority:
        captured["env"] = env
        captured["repo_root"] = repo_root
        captured["required"] = required
        return WorkflowDatabaseAuthority(
            database_url="postgresql://repo-authority.example/praxis",
            source="repo_env:/tmp/repo/.env",
        )

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_authority",
        _fake_resolve_runtime_database_authority,
    )

    resolved = _workflow_database.workflow_database_authority_for_repo(repo_root, env={})

    assert resolved == WorkflowDatabaseAuthority(
        database_url="postgresql://repo-authority.example/praxis",
        source="repo_env:/tmp/repo/.env",
    )
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

    def _raise_missing_authority(*, env, repo_root: Path, required: bool) -> WorkflowDatabaseAuthority:
        raise PostgresConfigurationError(
            "postgres.config_missing",
            "WORKFLOW_DATABASE_URL is required",
        )

    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_authority",
        _raise_missing_authority,
    )

    with pytest.raises(PostgresConfigurationError, match="WORKFLOW_DATABASE_URL"):
        _workflow_database.workflow_database_env_for_repo(repo_root, env={})
