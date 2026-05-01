from __future__ import annotations

from pathlib import Path

import pytest

from runtime._workflow_database import (
    resolve_runtime_database_url,
)
import storage.postgres.connection as pg_connection
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


def test_resolve_runtime_database_url_normalizes_container_host_alias_for_host_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pg_connection, "_running_inside_container", lambda env=None: False)
    monkeypatch.setattr(pg_connection, "_hostname_resolves", lambda hostname: False)

    assert (
        resolve_runtime_database_url(
            "postgresql://user:pass@host.docker.internal:5432/praxis?sslmode=disable",
            repo_root=Path("/tmp"),
        )
        == "postgresql://user:pass@localhost:5432/praxis?sslmode=disable"
    )


def test_resolve_runtime_database_url_preserves_container_host_alias_inside_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pg_connection, "_running_inside_container", lambda env=None: True)
    monkeypatch.setattr(pg_connection, "_hostname_resolves", lambda hostname: False)

    assert (
        resolve_runtime_database_url(
            "postgresql://host.docker.internal:5432/praxis",
            repo_root=Path("/tmp"),
        )
        == "postgresql://host.docker.internal:5432/praxis"
    )


def test_resolve_runtime_database_url_does_not_discover_launchd_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    launchd_dir = tmp_path / "LaunchAgents"
    launchd_dir.mkdir()
    monkeypatch.setenv("PRAXIS_LAUNCHD_DIR", str(launchd_dir))
    (launchd_dir / "com.praxis.engine.plist").write_text(
        "<plist><dict><key>EnvironmentVariables</key><dict>"
        "<key>WORKFLOW_DATABASE_URL</key>"
        "<string>postgresql://stale-launchd.test/praxis</string>"
        "</dict></dict></plist>",
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )

    assert resolve_runtime_database_url(repo_root=tmp_path) == "postgresql://repo.test/workflow"


def test_resolve_runtime_database_url_does_not_discover_docker_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    (tmp_path / "docker-compose.yml").write_text("services:\n  postgres:\n", encoding="utf-8")

    with pytest.raises(PostgresConfigurationError, match="registry/runtime environment"):
        resolve_runtime_database_url(repo_root=tmp_path)


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

    with pytest.raises(PostgresConfigurationError, match="registry/runtime environment"):
        resolve_runtime_database_url(repo_root=tmp_path, required=True)


def test_authority_scripts_do_not_bake_machine_specific_postgres_dsn() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    for relative in ("scripts/refresh_authority_memory.py",):
        script_text = (repo_root / relative).read_text(encoding="utf-8")
        assert "praxis-postgres-1.orb.local" not in script_text
