from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest
import storage.postgres.schema as postgres_schema


class _FakeTransaction:
    def __init__(self, conn: "_FakeAsyncConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeTransaction":
        self._conn.transaction_entries += 1
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._conn.transaction_exits += 1


class _FakeAsyncConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_entries = 0
        self.transaction_exits = 0

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def execute(self, query: str, *params: object) -> str:
        self.executed.append((query, params))
        return "OK"


class _FakeLockConn:
    def __init__(
        self,
        *,
        try_lock_results: tuple[bool, ...],
        holder_row: dict[str, object] | None,
    ) -> None:
        self._try_lock_results = iter(try_lock_results)
        self._holder_row = holder_row
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *params: object) -> bool:
        self.fetchval_calls.append((query, params))
        return next(self._try_lock_results)

    async def fetchrow(self, query: str, *params: object):
        self.fetchrow_calls.append((query, params))
        return self._holder_row


def _readiness(
    *,
    bootstrapped: bool,
    missing_objects: tuple[tuple[str, str], ...] = (),
    missing_by_migration: dict[str, tuple[tuple[str, str], ...]] | None = None,
) -> postgres_schema.WorkflowSchemaReadiness:
    missing_objects = tuple(
        postgres_schema.WorkflowMigrationExpectedObject(
            object_type=object_type,
            object_name=name,
        )
        for object_type, name in missing_objects
    )
    grouped_missing = {
        filename: tuple(
            postgres_schema.WorkflowMigrationExpectedObject(
                object_type=object_type,
                object_name=name,
            )
            for object_type, name in objects
        )
        for filename, objects in (missing_by_migration or {}).items()
    }
    if bootstrapped:
        return postgres_schema.WorkflowSchemaReadiness(
            expected_objects=(),
            missing_objects=(),
            missing_by_migration={},
        )
    return postgres_schema.WorkflowSchemaReadiness(
        expected_objects=missing_objects,
        missing_objects=missing_objects,
        missing_by_migration=grouped_missing,
    )


def test_bootstrap_workflow_schema_skips_advisory_lock_when_schema_is_ready(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    inspect_calls: list[int] = []

    async def _inspect(_conn):
        inspect_calls.append(1)
        return _readiness(bootstrapped=True)

    monkeypatch.setattr(postgres_schema, "inspect_workflow_schema", _inspect)

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert inspect_calls == [1]
    assert conn.executed == []
    assert conn.transaction_entries == 0
    assert conn.transaction_exits == 0


def test_bootstrap_workflow_schema_locks_and_applies_only_missing_migrations(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    readiness_states = iter(
        (
            _readiness(
                bootstrapped=False,
                missing_objects=(("table", "compile_artifacts"),),
                missing_by_migration={
                    "001_compile_artifacts.sql": (("table", "compile_artifacts"),),
                },
            ),
            _readiness(
                bootstrapped=False,
                missing_objects=(("table", "compile_artifacts"),),
                missing_by_migration={
                    "001_compile_artifacts.sql": (("table", "compile_artifacts"),),
                },
            ),
        )
    )
    applied: list[str] = []

    async def _inspect(_conn):
        return next(readiness_states)

    async def _bootstrap_migration(_conn, filename: str) -> None:
        applied.append(filename)

    monkeypatch.setattr(postgres_schema, "inspect_workflow_schema", _inspect)
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    acquire_calls: list[_FakeAsyncConn] = []

    async def _acquire_lock(_conn) -> float:
        acquire_calls.append(_conn)
        return 0.0

    monkeypatch.setattr(postgres_schema, "_acquire_schema_bootstrap_lock", _acquire_lock)
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_manifest",
        lambda: (
            SimpleNamespace(filename="001_compile_artifacts.sql"),
            SimpleNamespace(filename="002_other.sql"),
        ),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_expected_objects",
        lambda filename: (
            (
                postgres_schema.WorkflowMigrationExpectedObject(
                    object_type="table",
                    object_name="compile_artifacts",
                ),
            )
            if filename == "001_compile_artifacts.sql"
            else (
                postgres_schema.WorkflowMigrationExpectedObject(
                    object_type="table",
                    object_name="other_table",
                ),
            )
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert conn.transaction_entries == 1
    assert conn.transaction_exits == 1
    assert acquire_calls == [conn]
    assert conn.executed == []
    assert applied == ["001_compile_artifacts.sql"]


def test_bootstrap_workflow_schema_applies_missing_constraint_migration(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    readiness_states = iter(
        (
            _readiness(
                bootstrapped=False,
                missing_objects=(("constraint", "workflow_chain_waves.workflow_chain_waves_status_v2_check"),),
                missing_by_migration={
                    "090_workflow_chain_cancellation_and_alignment.sql": (
                        ("constraint", "workflow_chain_waves.workflow_chain_waves_status_v2_check"),
                    ),
                },
            ),
            _readiness(
                bootstrapped=False,
                missing_objects=(("constraint", "workflow_chain_waves.workflow_chain_waves_status_v2_check"),),
                missing_by_migration={
                    "090_workflow_chain_cancellation_and_alignment.sql": (
                        ("constraint", "workflow_chain_waves.workflow_chain_waves_status_v2_check"),
                    ),
                },
            ),
        )
    )
    applied: list[str] = []

    async def _inspect(_conn):
        return next(readiness_states)

    async def _bootstrap_migration(_conn, filename: str) -> None:
        applied.append(filename)

    monkeypatch.setattr(postgres_schema, "inspect_workflow_schema", _inspect)
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_manifest",
        lambda: (
            SimpleNamespace(filename="089_control_operator_frames.sql"),
            SimpleNamespace(filename="090_workflow_chain_cancellation_and_alignment.sql"),
        ),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_expected_objects",
        lambda filename: (
            (
                postgres_schema.WorkflowMigrationExpectedObject(
                    object_type="constraint",
                    object_name="workflow_chain_waves.workflow_chain_waves_status_v2_check",
                ),
            )
            if filename == "090_workflow_chain_cancellation_and_alignment.sql"
            else (
                postgres_schema.WorkflowMigrationExpectedObject(
                    object_type="table",
                    object_name="run_operator_frames",
                ),
            )
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert applied == ["090_workflow_chain_cancellation_and_alignment.sql"]


def test_bootstrap_workflow_schema_applies_partially_drifted_migration(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    readiness_states = iter(
        (
            _readiness(
                bootstrapped=False,
                missing_objects=(("column", "provider_policies.preferred_provider_ref"),),
                missing_by_migration={
                    "074_provider_policy_multi_provider_refs.sql": (
                        ("column", "provider_policies.preferred_provider_ref"),
                    ),
                },
            ),
            _readiness(
                bootstrapped=False,
                missing_objects=(("column", "provider_policies.preferred_provider_ref"),),
                missing_by_migration={
                    "074_provider_policy_multi_provider_refs.sql": (
                        ("column", "provider_policies.preferred_provider_ref"),
                    ),
                },
            ),
        )
    )
    applied: list[str] = []

    async def _inspect(_conn):
        return next(readiness_states)

    async def _bootstrap_migration(_conn, filename: str) -> None:
        applied.append(filename)

    monkeypatch.setattr(postgres_schema, "inspect_workflow_schema", _inspect)
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_manifest",
        lambda: (
            SimpleNamespace(filename="073_workflow_run_packet_inspection.sql"),
            SimpleNamespace(filename="074_provider_policy_multi_provider_refs.sql"),
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert applied == ["074_provider_policy_multi_provider_refs.sql"]


def test_acquire_schema_bootstrap_lock_logs_wait_holder_details(
    monkeypatch,
    caplog,
) -> None:
    conn = _FakeLockConn(
        try_lock_results=(False, True),
        holder_row={
            "pid": 4242,
            "application_name": "dag-worker",
            "state": "idle in transaction",
            "wait_event_type": "Client",
            "wait_event": "ClientRead",
            "xact_age_s": 17.5,
            "query_text": "SELECT pg_advisory_xact_lock($1::bigint)",
        },
    )
    monotonic_values = iter((100.0, 102.2, 102.6))

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(postgres_schema, "_schema_bootstrap_monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(postgres_schema.asyncio, "sleep", _sleep)
    monkeypatch.setattr(postgres_schema, "_SCHEMA_BOOTSTRAP_WAIT_WARNING_THRESHOLD_S", 2.0)
    monkeypatch.setattr(postgres_schema, "_SCHEMA_BOOTSTRAP_WAIT_LOG_INTERVAL_S", 10.0)

    with caplog.at_level(logging.WARNING):
        wait_s = asyncio.run(postgres_schema._acquire_schema_bootstrap_lock(conn))

    assert wait_s == pytest.approx(2.6)
    assert len(conn.fetchrow_calls) == 1
    assert "waiting 2.20s for schema bootstrap advisory lock 741001" in caplog.text
    assert "holder_pid=4242" in caplog.text
    assert "application_name=dag-worker" in caplog.text
    assert "idle in transaction" in caplog.text
    assert "acquired after 2.60s wait" in caplog.text
