from __future__ import annotations

from pathlib import Path

import pytest

from runtime._workflow_database import resolve_runtime_database_url
from storage.postgres.validators import PostgresConfigurationError


def test_resolve_runtime_database_url_preserves_explicit_authority() -> None:
    assert (
        resolve_runtime_database_url("postgresql://repo.test/workflow", repo_root=Path("/tmp"))
        == "postgresql://repo.test/workflow"
    )


def test_resolve_runtime_database_url_uses_repo_env_when_process_authority_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )

    assert resolve_runtime_database_url(repo_root=tmp_path) == "postgresql://repo.test/workflow"


def test_resolve_runtime_database_url_prefers_reachable_launchd_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        "runtime._workflow_database._try_resolve_launchd_database_url",
        lambda _repo_root: ("postgresql://launchd.test/praxis", "com.praxis.engine"),
    )
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )

    assert resolve_runtime_database_url(repo_root=tmp_path) == "postgresql://launchd.test/praxis"


def test_resolve_runtime_database_url_falls_back_to_docker_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    (tmp_path / "docker-compose.yml").write_text("services:\n  postgres:\n", encoding="utf-8")
    monkeypatch.setattr(
        "runtime._workflow_database._try_resolve_docker_database_url",
        lambda _repo_root: "postgresql://127.0.0.1:5432/praxis",
    )

    assert resolve_runtime_database_url(repo_root=tmp_path) == "postgresql://127.0.0.1:5432/praxis"


def test_resolve_runtime_database_url_uses_runtime_repo_root_when_repo_root_omitted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("runtime._workflow_database._runtime_repo_root", lambda: tmp_path)

    assert resolve_runtime_database_url() == "postgresql://repo.test/workflow"


def test_resolve_runtime_database_url_returns_none_when_optional_and_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    assert resolve_runtime_database_url(repo_root=tmp_path, required=False) is None


def test_resolve_runtime_database_url_fails_closed_when_required_and_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    with pytest.raises(PostgresConfigurationError, match="WORKFLOW_DATABASE_URL must be set"):
        resolve_runtime_database_url(repo_root=tmp_path, required=True)
