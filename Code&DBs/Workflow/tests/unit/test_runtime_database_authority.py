from __future__ import annotations

from pathlib import Path

import pytest

from runtime import _workflow_database as workflow_database_module
from runtime._workflow_database import (
    _try_resolve_docker_database_url,
    resolve_runtime_database_url,
)
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


def test_docker_database_url_includes_explicit_postgres_role(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Guard against libpq falling back to the OS user when the DSN omits a role."""

    compose_file = tmp_path / "docker-compose.yml"
    compose_file.write_text("services:\n  postgres:\n", encoding="utf-8")

    class _StubCompletedProcess:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    docker_outputs = iter(
        [
            _StubCompletedProcess("abc123\n"),
            _StubCompletedProcess("healthy\n"),
            _StubCompletedProcess("0.0.0.0:5432\n"),
        ]
    )

    def _fake_subprocess_run(*_args, **_kwargs):
        return next(docker_outputs)

    monkeypatch.setattr(
        workflow_database_module.subprocess,
        "run",
        _fake_subprocess_run,
    )

    resolved = _try_resolve_docker_database_url(tmp_path)

    assert resolved == "postgresql://postgres@127.0.0.1:5432/praxis"
    assert resolved is not None
    assert "postgres@" in resolved, (
        "docker-resolved DSN must pin the canonical postgres role so libpq "
        "never falls back to the OS user (see BUG-5A367F0C)"
    )


def test_authority_scripts_do_not_bake_machine_specific_postgres_dsn() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    for relative in ("scripts/refresh_authority_memory.py", "scripts/praxis_atlas.py"):
        script_text = (repo_root / relative).read_text(encoding="utf-8")
        assert "praxis-postgres-1.orb.local" not in script_text
