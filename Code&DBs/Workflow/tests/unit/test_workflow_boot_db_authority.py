from __future__ import annotations

import asyncio

import pytest

import storage.dev_postgres as dev_postgres
import storage.postgres as storage_postgres
from storage.migrations import WorkflowMigrationExpectedObject
from storage.postgres import connection as pg_connection
from storage.postgres import schema as pg_schema
import surfaces._boot as workflow_boot


class _FakeAcquireContext:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakePool:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquireContext:
        return _FakeAcquireContext(self._conn)


def _disabled_dev_postgres(*_args, **_kwargs):
    raise dev_postgres.DevPostgresError("dev_postgres.disabled", "disabled for explicit authority test")


def _run_sync(awaitable):
    return asyncio.run(awaitable)


def test_workflow_database_status_falls_back_to_explicit_authority_when_dev_postgres_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = object()
    calls: list[str] = []
    readiness = pg_schema.WorkflowSchemaReadiness(
        expected_objects=(),
        missing_objects=(WorkflowMigrationExpectedObject("table", "verification_registry"),),
        missing_by_migration={},
    )

    async def _inspect(_conn: object):
        return readiness

    monkeypatch.setattr(dev_postgres, "local_postgres_health", _disabled_dev_postgres)
    monkeypatch.setattr(
        pg_connection,
        "_run_sync",
        lambda awaitable: calls.append("run_sync") or _run_sync(awaitable),
    )
    monkeypatch.setattr(
        pg_connection,
        "get_workflow_pool",
        lambda env=None: calls.append("get_workflow_pool") or _FakePool(fake_conn),
    )
    monkeypatch.setattr(
        pg_connection,
        "resolve_workflow_database_url",
        lambda env=None: "postgresql://localhost:5432/praxis_test",
    )
    monkeypatch.setattr(storage_postgres, "inspect_workflow_schema", _inspect)

    status = workflow_boot.workflow_database_status(
        env={"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}
    )

    assert status.database_url == "postgresql://localhost:5432/praxis_test"
    assert status.database_reachable is True
    assert status.process_running is True
    assert status.schema_bootstrapped is False
    assert status.missing_schema_objects == ("verification_registry",)
    assert status.verification_registry_ready is False
    assert status.compile_artifact_authority_ready is False
    assert calls[:2] == ["get_workflow_pool", "run_sync"]


def test_workflow_database_bootstrap_falls_back_to_explicit_authority_when_dev_postgres_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = object()
    calls: list[str] = []
    readiness = pg_schema.WorkflowSchemaReadiness(
        expected_objects=(),
        missing_objects=(),
        missing_by_migration={},
    )
    bootstrap_calls: list[object] = []
    seed_calls: list[object] = []

    async def _inspect(_conn: object):
        return readiness

    async def _bootstrap(conn: object):
        bootstrap_calls.append(conn)

    async def _seed(conn: object):
        seed_calls.append(conn)

    monkeypatch.setattr(dev_postgres, "local_postgres_bootstrap", _disabled_dev_postgres)
    monkeypatch.setattr(
        pg_connection,
        "_run_sync",
        lambda awaitable: calls.append("run_sync") or _run_sync(awaitable),
    )
    monkeypatch.setattr(
        pg_connection,
        "get_workflow_pool",
        lambda env=None: calls.append("get_workflow_pool") or _FakePool(fake_conn),
    )
    monkeypatch.setattr(
        pg_connection,
        "resolve_workflow_database_url",
        lambda env=None: "postgresql://localhost:5432/praxis_test",
    )
    monkeypatch.setattr(storage_postgres, "inspect_workflow_schema", _inspect)
    monkeypatch.setattr(pg_schema, "bootstrap_workflow_schema", _bootstrap)
    monkeypatch.setattr(
        "storage.postgres.fresh_install_seed.seed_fresh_install_authority_async",
        _seed,
    )

    status = workflow_boot.workflow_database_status(
        env={"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"},
        bootstrap=True,
    )

    assert bootstrap_calls == [fake_conn]
    assert seed_calls == [fake_conn]
    assert status.database_reachable is True
    assert status.schema_bootstrapped is True
    assert status.missing_schema_objects == ()
    assert calls[:2] == ["get_workflow_pool", "run_sync"]
