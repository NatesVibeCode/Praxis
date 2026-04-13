from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import storage.dev_postgres as dev_postgres
from storage.dev_postgres import (
    PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV,
    local_postgres_bootstrap,
    local_postgres_health,
    local_postgres_status,
)
from storage.postgres import PostgresConfigurationError, resolve_workflow_database_url


def test_local_postgres_config_fails_closed_on_missing_workflow_database_url() -> None:
    with pytest.raises(PostgresConfigurationError) as exc_info:
        dev_postgres.resolve_local_postgres_config(
            env={PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(_repo_postgres_data_dir())},
        )

    assert exc_info.value.reason_code == "postgres.config_missing"


def test_local_postgres_config_fails_closed_on_invalid_data_dir_override() -> None:
    with pytest.raises(PostgresConfigurationError) as exc_info:
        dev_postgres.resolve_local_postgres_config(
            env={
                "WORKFLOW_DATABASE_URL": "postgresql://localhost:55432/workflow",
                PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: "/tmp/dag-postgres-does-not-exist",
            },
        )

    assert exc_info.value.reason_code == "dev_postgres.config_missing"


@pytest.mark.parametrize(
    "helper",
    [local_postgres_health, local_postgres_bootstrap],
    ids=("health", "bootstrap"),
)
def test_local_postgres_health_and_bootstrap_fail_closed_on_database_url_cluster_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    helper,
) -> None:
    data_dir = _init_fake_postgres_data_dir(tmp_path / "postgres-data", port=55432)
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")

    with pytest.raises(PostgresConfigurationError) as exc_info:
        helper(
            env={
                "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:55433/workflow",
                PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
            },
        )

    assert exc_info.value.reason_code == "dev_postgres.identity_mismatch"


def test_local_postgres_status_reports_partial_schema_as_not_bootstrapped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = _init_fake_postgres_data_dir(tmp_path / "postgres-data", port=55432)
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")
    monkeypatch.setattr(dev_postgres, "_pg_ctl_status", lambda config: (True, "running"))

    class _FakeConnection:
        async def fetchrow(self, query: str) -> dict[str, str]:
            assert "current_setting('data_directory')" in query
            return {"data_directory": str(data_dir), "port": "55432"}

        async def fetch(
            self,
            query: str,
            expected_payload: str,
        ) -> list[dict[str, str]]:
            assert "jsonb_array_elements" in query
            assert "pg_catalog.pg_class" in query
            assert "workflow_events" in expected_payload
            return [{"object_type": "table", "object_name": "workflow_events"}]

        async def close(self) -> None:
            return None

    async def _fake_connect(database_url: str, timeout: float):
        assert database_url == "postgresql://127.0.0.1:55432/workflow"
        assert timeout == dev_postgres._CONNECTION_TIMEOUT_S
        return _FakeConnection()

    monkeypatch.setattr(dev_postgres.asyncpg, "connect", _fake_connect)

    status = local_postgres_health(
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:55432/workflow",
            PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
        },
    )

    assert status.process_running is True
    assert status.database_reachable is True
    assert status.schema_bootstrapped is False
    assert "workflow_events" in status.missing_schema_objects
    assert status.compile_artifact_authority_ready is True
    assert status.execution_packet_authority_ready is True
    assert status.repo_snapshot_authority_ready is True


def test_local_postgres_config_ignores_stale_postmaster_pid_port_when_validating_database_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = _init_fake_postgres_data_dir(tmp_path / "postgres-data", port=55432)
    (data_dir / "postmaster.pid").write_text(
        "\n".join(("99999", str(data_dir), "1712094890", "55433")) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")

    config = dev_postgres.resolve_local_postgres_config(
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:55432/workflow",
            PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
        },
    )

    assert config.cluster_port == 55432


def test_local_postgres_config_falls_back_to_default_port_when_postmaster_opts_omits_p(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "postgres-data"
    data_dir.mkdir(parents=True)
    (data_dir / "postmaster.opts").write_text(
        f'postgres "-D" "{data_dir}"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")

    config = dev_postgres.resolve_local_postgres_config(
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:5432/workflow",
            PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
        },
    )

    assert config.cluster_port == 5432


def test_local_postgres_health_fails_closed_on_live_cluster_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = _init_fake_postgres_data_dir(tmp_path / "postgres-data", port=55432)
    other_data_dir = _init_fake_postgres_data_dir(tmp_path / "other-postgres-data", port=55432)
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")
    monkeypatch.setattr(dev_postgres, "_pg_ctl_status", lambda config: (True, "running"))

    class _FakeConnection:
        async def fetchrow(self, query: str) -> dict[str, str]:
            assert "current_setting('data_directory')" in query
            return {"data_directory": str(other_data_dir), "port": "55432"}

        async def close(self) -> None:
            return None

    async def _fake_connect(database_url: str, timeout: float):
        assert database_url == "postgresql://127.0.0.1:55432/workflow"
        assert timeout == dev_postgres._CONNECTION_TIMEOUT_S
        return _FakeConnection()

    monkeypatch.setattr(dev_postgres.asyncpg, "connect", _fake_connect)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        local_postgres_health(
            env={
                "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:55432/workflow",
                PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
            },
        )

    assert exc_info.value.reason_code == "dev_postgres.identity_mismatch"


def test_local_postgres_bootstrap_fails_closed_while_cluster_stopped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = _init_fake_postgres_data_dir(tmp_path / "postgres-data", port=55432)
    (data_dir / "postmaster.pid").write_text(
        "\n".join(("99999", str(data_dir), "1712094890", "55432")) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dev_postgres, "_resolve_pg_ctl", lambda: "pg_ctl")
    monkeypatch.setattr(dev_postgres, "_pg_ctl_status", lambda config: (False, "no server running"))

    async def _unexpected_connect(*args, **kwargs):
        raise AssertionError("bootstrap must not connect when pg_ctl reports the cluster stopped")

    monkeypatch.setattr(dev_postgres.asyncpg, "connect", _unexpected_connect)

    with pytest.raises(dev_postgres.DevPostgresError) as exc_info:
        local_postgres_bootstrap(
            env={
                "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:55432/workflow",
                PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV: str(data_dir),
            },
        )

    assert exc_info.value.reason_code == "dev_postgres.not_running"


def test_local_postgres_health_and_bootstrap_path_are_explicit_and_idempotent() -> None:
    env = _workflow_env()
    status_before = local_postgres_health(env=env)

    if not status_before.process_running:
        pytest.skip("repo-local Postgres is not running in this workspace")
    if not status_before.database_reachable:
        pytest.skip("repo-local Postgres is running but the workflow database is not reachable")

    status_after_bootstrap = local_postgres_bootstrap(env=env)

    assert status_after_bootstrap.process_running is True
    assert status_after_bootstrap.database_reachable is True
    assert status_after_bootstrap.schema_bootstrapped is True

    status_after = local_postgres_health(env=env)
    assert status_after.process_running is True
    assert status_after.database_reachable is True
    assert status_after.schema_bootstrapped is True
    assert local_postgres_status(env=env) == status_after


def test_run_coroutine_closes_coroutine_before_failing_in_active_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop():
        return None

    coroutine = _noop()
    monkeypatch.setattr(dev_postgres.asyncio, "get_running_loop", lambda: object())

    with pytest.raises(dev_postgres.DevPostgresError) as exc_info:
        dev_postgres._run_coroutine(coroutine)

    assert exc_info.value.reason_code == "dev_postgres.event_loop_active"
    assert asyncio.iscoroutine(coroutine)
    assert coroutine.cr_frame is None


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for the local Postgres helper integration test: "
            f"{exc.reason_code}"
        )

    env = {"WORKFLOW_DATABASE_URL": database_url}
    env[PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV] = str(_repo_postgres_data_dir())
    return env


def _repo_postgres_data_dir() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "Databases"
        / "postgres-dev"
        / "data"
    )


def _init_fake_postgres_data_dir(path: Path, *, port: int) -> Path:
    path.mkdir(parents=True)
    (path / "postmaster.opts").write_text(
        f'postgres "-D" "{path}" "-p" "{port}"\n',
        encoding="utf-8",
    )
    return path
