from __future__ import annotations

import asyncio
import logging

import asyncpg
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


def _control_readiness(*, bootstrapped: bool) -> postgres_schema.ControlPlaneSchemaReadiness:
    expected = (
        postgres_schema.WorkflowMigrationExpectedObject(
            object_type="table",
            object_name="workflow_runs",
        ),
    )
    return postgres_schema.ControlPlaneSchemaReadiness(
        expected_objects=expected,
        missing_objects=() if bootstrapped else expected,
    )


def _migration_audit(
    *,
    missing: tuple[str, ...] = (),
) -> postgres_schema.WorkflowMigrationAudit:
    return postgres_schema.WorkflowMigrationAudit(
        declared=missing,
        applied=(),
        missing=missing,
        drifted=(),
        extra=(),
    )


def test_platform_config_rows_are_schema_readiness_authority() -> None:
    assert postgres_schema._ROW_EXPECTATION_KEY_COLUMNS["platform_config"] == "config_key"


def test_parse_task_type_routing_row_key_supports_dot_syntax_with_dotted_model() -> None:
    assert postgres_schema._parse_task_type_routing_row_key(
        "plan_pill_match.openrouter.openai/gpt-5.4-mini",
    ) == ("plan_pill_match", "openrouter", "openai/gpt-5.4-mini")


def test_parse_task_type_routing_row_key_supports_legacy_pipe_syntax() -> None:
    assert postgres_schema._parse_task_type_routing_row_key(
        "plan_pill_match|openrouter|openai/gpt-5.4-mini",
    ) == ("plan_pill_match", "openrouter", "openai/gpt-5.4-mini")


def test_parse_private_provider_api_job_allowlist_row_key_supports_implicit_llm_task_adapter() -> None:
    assert postgres_schema._parse_private_provider_api_job_allowlist_row_key(
        "scratch_agent.plan_pill_match.openrouter.openai/gpt-5.4-mini",
    ) == (
        "scratch_agent",
        "plan_pill_match",
        "llm_task",
        "openrouter",
        "openai/gpt-5.4-mini",
    )


def test_parse_private_provider_api_job_allowlist_row_key_supports_authority_dotted_shape() -> None:
    assert postgres_schema._parse_private_provider_api_job_allowlist_row_key(
        "praxis.plan_pill_match.openrouter.openai/gpt-5.4-mini",
    ) == (
        "praxis",
        "plan_pill_match",
        "llm_task",
        "openrouter",
        "openai/gpt-5.4-mini",
    )


def test_parse_private_provider_api_job_allowlist_row_key_supports_explicit_adapter_dotted_model() -> None:
    assert postgres_schema._parse_private_provider_api_job_allowlist_row_key(
        "scratch_agent.plan_pill_match.llm_task.openrouter.openai/gpt-5.4-mini",
    ) == (
        "scratch_agent",
        "plan_pill_match",
        "llm_task",
        "openrouter",
        "openai/gpt-5.4-mini",
    )


def test_parse_private_provider_api_job_allowlist_row_key_supports_legacy_pipe_syntax() -> None:
    assert postgres_schema._parse_private_provider_api_job_allowlist_row_key(
        "scratch_agent|plan_pill_match|llm_task|openrouter|openai/gpt-5.4-mini",
    ) == (
        "scratch_agent",
        "plan_pill_match",
        "llm_task",
        "openrouter",
        "openai/gpt-5.4-mini",
    )


def test_parse_task_type_routing_row_key_rejects_bad_shape() -> None:
    assert postgres_schema._parse_task_type_routing_row_key("not|enough") is None


def test_parse_private_provider_api_job_allowlist_row_key_rejects_bad_shape() -> None:
    assert (
        postgres_schema._parse_private_provider_api_job_allowlist_row_key("not|enough|parts")
        is None
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
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_audit",
        lambda _conn: asyncio.sleep(0, result=_migration_audit()),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert inspect_calls == [1]
    assert conn.executed == []
    assert conn.transaction_entries == 0
    assert conn.transaction_exits == 0


def test_bootstrap_workflow_schema_backfills_ledger_when_schema_is_ready(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    recorded: list[tuple[str, str]] = []

    async def _bootstrap_migration(_conn, filename: str) -> None:
        raise AssertionError(f"unexpected migration replay for {filename}")

    async def _record_migration_apply(_conn, filename: str, *, applied_by: str) -> None:
        recorded.append((filename, applied_by))

    monkeypatch.setattr(
        postgres_schema,
        "inspect_workflow_schema",
        lambda _conn: asyncio.sleep(0, result=_readiness(bootstrapped=True)),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_migration_audit",
        lambda _conn: asyncio.sleep(
            0,
            result=_migration_audit(
                missing=("198_remove_unbacked_anthropic_haiku_runtime_model.sql",)
            ),
        ),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(postgres_schema, "_record_migration_apply", _record_migration_apply)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert conn.transaction_entries == 1
    assert conn.transaction_exits == 1
    assert recorded == [
        (
            "198_remove_unbacked_anthropic_haiku_runtime_model.sql",
            "schema_ledger_backfill",
        )
    ]


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
    monkeypatch.setattr(
        postgres_schema,
        "inspect_control_plane_schema",
        lambda _conn: asyncio.sleep(0, result=_control_readiness(bootstrapped=True)),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_bootstrap_baseline_anchor_is_missing",
        lambda _conn: asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    acquire_calls: list[_FakeAsyncConn] = []

    async def _acquire_lock(_conn) -> float:
        acquire_calls.append(_conn)
        return 0.0

    monkeypatch.setattr(postgres_schema, "_acquire_schema_bootstrap_lock", _acquire_lock)
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_manifest_filenames",
        lambda: (
            "001_compile_artifacts.sql",
            "002_other.sql",
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
    monkeypatch.setattr(
        postgres_schema,
        "inspect_control_plane_schema",
        lambda _conn: asyncio.sleep(0, result=_control_readiness(bootstrapped=True)),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_bootstrap_baseline_anchor_is_missing",
        lambda _conn: asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_manifest_filenames",
        lambda: (
            "089_control_operator_frames.sql",
            "090_workflow_chain_cancellation_and_alignment.sql",
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
    monkeypatch.setattr(
        postgres_schema,
        "inspect_control_plane_schema",
        lambda _conn: asyncio.sleep(0, result=_control_readiness(bootstrapped=True)),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_bootstrap_baseline_anchor_is_missing",
        lambda _conn: asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_manifest_filenames",
        lambda: (
            "073_workflow_run_packet_inspection.sql",
            "074_provider_policy_multi_provider_refs.sql",
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert applied == ["074_provider_policy_multi_provider_refs.sql"]


def test_bootstrap_workflow_schema_uses_full_tree_for_fresh_cluster(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()
    readiness_states = iter(
        (
            _readiness(
                bootstrapped=False,
                missing_objects=(("table", "workflow_runs"),),
                missing_by_migration={
                    "001_v1_control_plane.sql": (("table", "workflow_runs"),),
                },
            ),
            _readiness(
                bootstrapped=False,
                missing_objects=(("table", "workflow_runs"),),
                missing_by_migration={
                    "001_v1_control_plane.sql": (("table", "workflow_runs"),),
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
    monkeypatch.setattr(
        postgres_schema,
        "inspect_control_plane_schema",
        lambda _conn: asyncio.sleep(0, result=_control_readiness(bootstrapped=False)),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_full_workflow_migration_filenames",
        lambda: (
            "001_v1_control_plane.sql",
            "002_registry_authority.sql",
            "032_triggers_and_events.sql",
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert applied == [
        "001_v1_control_plane.sql",
        "002_registry_authority.sql",
        "032_triggers_and_events.sql",
    ]


def test_bootstrap_workflow_schema_replays_full_tree_when_bootstrap_only_baseline_is_missing(
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
    monkeypatch.setattr(
        postgres_schema,
        "inspect_control_plane_schema",
        lambda _conn: asyncio.sleep(0, result=_control_readiness(bootstrapped=True)),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_bootstrap_baseline_anchor_is_missing",
        lambda _conn: asyncio.sleep(0, result=True),
    )
    monkeypatch.setattr(postgres_schema, "_bootstrap_migration", _bootstrap_migration)
    monkeypatch.setattr(
        postgres_schema,
        "_acquire_schema_bootstrap_lock",
        lambda _conn: asyncio.sleep(0, result=0.0),
    )
    monkeypatch.setattr(
        postgres_schema,
        "_full_workflow_migration_filenames",
        lambda: (
            "001_v1_control_plane.sql",
            "032_triggers_and_events.sql",
            "074_provider_policy_multi_provider_refs.sql",
        ),
    )

    asyncio.run(postgres_schema.bootstrap_workflow_schema(conn))

    assert applied == [
        "001_v1_control_plane.sql",
        "032_triggers_and_events.sql",
        "074_provider_policy_multi_provider_refs.sql",
    ]


def test_bootstrap_migration_skips_commented_transaction_wrappers(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()

    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_statements",
        lambda _filename: (
            "-- migration header\nBEGIN",
            "-- create the relation\nCREATE TABLE demo (id INT)",
            "-- migration footer\nCOMMIT",
        ),
    )

    asyncio.run(postgres_schema._bootstrap_migration(conn, "demo.sql"))

    # _bootstrap_migration now issues the schema_migrations ensure-table DDL
    # up front; the migration's own statement is the second execute call.
    # "demo.sql" is not in the generated manifest so the apply-tracking INSERT
    # is skipped (logged warning), leaving exactly two executes.
    assert len(conn.executed) == 2
    ensure_query, _ = conn.executed[0]
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in ensure_query
    executed_query, executed_params = conn.executed[1]
    assert "CREATE TABLE demo (id INT)" in executed_query
    assert "BEGIN" not in executed_query
    assert "COMMIT" not in executed_query
    assert executed_params == ()
    # Outer migration transaction + savepoint for the CREATE TABLE statement
    # (BUG-25C5319C: migration runs all-or-nothing under one outer transaction).
    assert conn.transaction_entries == 3
    assert conn.transaction_exits == 3


def test_bootstrap_migration_skips_comment_only_statements(
    monkeypatch,
) -> None:
    conn = _FakeAsyncConn()

    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_statements",
        lambda _filename: (
            "-- verification:\n-- SELECT count(*) FROM demo",
            "/* trailing notes */",
            "CREATE TABLE demo (id INT)",
        ),
    )

    asyncio.run(postgres_schema._bootstrap_migration(conn, "demo.sql"))

    executed_queries = [query for query, _params in conn.executed]
    assert len(executed_queries) == 2
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in executed_queries[0]
    assert executed_queries[1] == "CREATE TABLE demo (id INT)"
    # Outer migration transaction + savepoint for the CREATE TABLE statement
    # (BUG-25C5319C: migration runs all-or-nothing under one outer transaction).
    assert conn.transaction_entries == 3
    assert conn.transaction_exits == 3


def test_schema_migrations_bootstrap_contract_declares_constraints() -> None:
    statements = postgres_schema.workflow_bootstrap_migration_statements(
        "173_schema_migrations.sql"
    )
    migration_sql = "\n".join(statements)

    for constraint_name in (
        "schema_migrations_filename_nonblank",
        "schema_migrations_sha256_shape",
        "schema_migrations_bootstrap_role_check",
    ):
        assert constraint_name in postgres_schema._SCHEMA_MIGRATIONS_ENSURE_DDL
        assert f"ADD CONSTRAINT {constraint_name}" in migration_sql


class _FakeApplyTrackingConn(_FakeAsyncConn):
    """Fake conn that captures execute calls and serves a configurable schema_migrations state."""

    def __init__(self, *, schema_migrations_exists: bool = True) -> None:
        super().__init__()
        self._schema_migrations_exists = schema_migrations_exists
        self.rows: list[dict[str, object]] = []

    async def fetch(self, query: str, *params: object):
        if "FROM schema_migrations" in query:
            return list(self.rows)
        return []

    async def fetchval(self, query: str, *params: object):
        if "pg_class" in query and "schema_migrations" in query:
            return self._schema_migrations_exists
        return None


def test_bootstrap_migration_inserts_apply_tracking_row(monkeypatch) -> None:
    conn = _FakeApplyTrackingConn()

    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_statements",
        lambda filename: ("CREATE TABLE IF NOT EXISTS synthetic_example (id int);",),
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_sql_text",
        lambda filename: "-- synthetic\nCREATE TABLE IF NOT EXISTS synthetic_example (id int);\n",
    )

    asyncio.run(postgres_schema._bootstrap_migration(conn, "173_schema_migrations.sql"))

    # The applier should have issued: (1) ensure-table DDL, (2) the migration
    # statement, and (3) the apply-tracking INSERT. The first is idempotent
    # CREATE TABLE IF NOT EXISTS; the last is the INSERT ... ON CONFLICT.
    statements = [query for query, _ in conn.executed]
    assert any(
        "CREATE TABLE IF NOT EXISTS schema_migrations" in q for q in statements
    ), statements
    insert_calls = [
        (query, params)
        for query, params in conn.executed
        if "INSERT INTO schema_migrations" in query
    ]
    assert len(insert_calls) == 1, insert_calls
    insert_query, insert_params = insert_calls[0]
    assert "ON CONFLICT (filename) DO UPDATE" in insert_query
    filename_arg, sha_arg, applied_by_arg, policy_arg = insert_params
    assert filename_arg == "173_schema_migrations.sql"
    assert len(sha_arg) == 64 and all(c in "0123456789abcdef" for c in sha_arg)
    assert applied_by_arg == "schema_bootstrap"
    assert policy_arg in {"canonical", "bootstrap_only"}


def test_workflow_migration_audit_reports_empty_when_table_missing() -> None:
    conn = _FakeApplyTrackingConn(schema_migrations_exists=False)

    audit = asyncio.run(postgres_schema.workflow_migration_audit(conn))

    assert audit.applied == ()
    assert audit.declared and len(audit.missing) == len(audit.declared)
    assert audit.is_clean is False


def test_workflow_migration_audit_flags_drift_when_sha_mismatch(monkeypatch) -> None:
    conn = _FakeApplyTrackingConn(schema_migrations_exists=True)
    declared = ("173_schema_migrations.sql",)

    monkeypatch.setattr(
        postgres_schema,
        "_GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE",
        declared,
    )
    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_sql_text",
        lambda filename: "ON-DISK CONTENTS",
    )
    conn.rows = [
        {
            "filename": "173_schema_migrations.sql",
            "content_sha256": "0" * 64,  # deliberately does not match "ON-DISK CONTENTS"
            "applied_at": None,
            "applied_by": "schema_bootstrap",
            "bootstrap_role": "canonical",
        }
    ]

    audit = asyncio.run(postgres_schema.workflow_migration_audit(conn))

    assert len(audit.drifted) == 1
    assert audit.drifted[0].filename == "173_schema_migrations.sql"
    assert audit.missing == ()
    assert audit.extra == ()
    assert audit.is_clean is False


def test_workflow_migration_audit_reports_extra_rows(monkeypatch) -> None:
    conn = _FakeApplyTrackingConn(schema_migrations_exists=True)
    declared = ("173_schema_migrations.sql",)

    monkeypatch.setattr(
        postgres_schema,
        "_GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE",
        declared,
    )

    import hashlib as _hashlib

    def _sql(filename: str) -> str:
        if filename == "173_schema_migrations.sql":
            return "SQL"
        raise postgres_schema.WorkflowMigrationError("x", "y")

    monkeypatch.setattr(
        postgres_schema, "workflow_bootstrap_migration_sql_text", _sql
    )
    sha_current = _hashlib.sha256(b"SQL").hexdigest()
    conn.rows = [
        {
            "filename": "173_schema_migrations.sql",
            "content_sha256": sha_current,
            "applied_at": None,
            "applied_by": "schema_bootstrap",
            "bootstrap_role": "canonical",
        },
        {
            "filename": "999_retired.sql",
            "content_sha256": "a" * 64,
            "applied_at": None,
            "applied_by": "schema_bootstrap",
            "bootstrap_role": "canonical",
        },
    ]

    audit = asyncio.run(postgres_schema.workflow_migration_audit(conn))

    assert {r.filename for r in audit.extra} == {"999_retired.sql"}
    assert audit.missing == ()
    assert audit.drifted == ()
    assert audit.is_clean is False


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


class _FakeBootstrapConnEnsureThenFail(_FakeAsyncConn):
    """Runs schema_migrations ensure DDL, then raises on migration statements."""

    def __init__(self, exc: Exception) -> None:
        super().__init__()
        self._exc = exc

    async def execute(self, query: str, *params: object) -> str:
        if "CREATE TABLE IF NOT EXISTS schema_migrations" in query:
            return await super().execute(query, params)
        raise self._exc


def test_bootstrap_migration_postgres_error_message_includes_server_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeBootstrapConnEnsureThenFail(
        asyncpg.exceptions.CheckViolationError('check constraint "demo_check" violated'),
    )

    monkeypatch.setattr(
        postgres_schema,
        "workflow_bootstrap_migration_statements",
        lambda _filename: ("SELECT 1;",),
    )

    with pytest.raises(postgres_schema.PostgresSchemaError) as raised:
        asyncio.run(postgres_schema._bootstrap_migration(conn, "demo.sql"))

    assert 'check constraint "demo_check" violated' in str(raised.value)
    assert raised.value.details.get("sqlstate") == "23514"


def test_record_migration_apply_documented_public_export() -> None:
    """Regression: manual-apply path stays importable (BUG-431B3436)."""

    assert hasattr(postgres_schema, "record_migration_apply")
    assert postgres_schema.record_migration_apply is postgres_schema._record_migration_apply
