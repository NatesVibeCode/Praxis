from __future__ import annotations

import asyncio
from types import SimpleNamespace

from pydantic import BaseModel

import runtime.operation_catalog_gateway as gateway


class _ExampleCommand(BaseModel):
    value: str


class _FakeTransaction:
    def __init__(self, conn: "_FakeAuthorityConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeAuthorityConn":
        self.conn.transaction_enters += 1
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.transaction_commits += 1
        else:
            self.conn.transaction_rollbacks += 1


class _FakeAuthorityConn:
    def __init__(self, *, cached_result: dict[str, object] | None = None) -> None:
        self.cached_result = cached_result
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0
        self.raise_on_sql: str | None = None

    def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        if "FROM authority_operation_receipts" in query:
            return self.cached_result
        raise AssertionError(f"unexpected fetchrow: {query}")

    def execute(self, query: str, *args: object) -> list[dict[str, object]]:
        self.execute_calls.append((query, args))
        if self.raise_on_sql and self.raise_on_sql in query:
            raise RuntimeError("injected proof write failure")
        return []

    def executed_sql(self) -> str:
        return "\n".join(query for query, _args in self.execute_calls)

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


def test_execute_operation_from_subsystems_resolves_and_invokes_binding(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _handler(command: _ExampleCommand, subsystems: object) -> dict[str, object]:
        captured["command"] = command
        captured["subsystems"] = subsystems
        return {"value": command.value}

    binding = SimpleNamespace(
        operation_ref="operator.example",
        operation_name="operator.example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.operation.example.20260416",
        decision_ref="decision.operation.example.20260416",
    )

    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    subsystems = _Subsystems()
    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = gateway.execute_operation_from_subsystems(
        subsystems,
        operation_name="operator.example",
        payload={"value": "authoritative"},
    )

    assert result["value"] == "authoritative"
    receipt = result["operation_receipt"]
    assert receipt["operation_name"] == "operator.example"
    assert receipt["authority_domain_ref"] == "authority.example"
    assert receipt["storage_target_ref"] == "praxis.primary_postgres"
    assert receipt["execution_status"] == "completed"
    assert receipt["event_ids"]
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" in conn.executed_sql()
    assert "UPDATE authority_operation_receipts" not in conn.executed_sql()
    assert conn.transaction_enters == 1
    assert conn.transaction_commits == 1
    assert conn.transaction_rollbacks == 0
    assert captured["command"].value == "authoritative"
    assert captured["subsystems"] is subsystems


def test_execute_operation_from_env_builds_env_backed_subsystems(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeConn:
        pass

    monkeypatch.setattr(gateway, "get_workflow_pool", lambda env: object())
    monkeypatch.setattr(gateway, "SyncPostgresConnection", lambda pool: _FakeConn())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(gateway, "execute_operation_from_subsystems", _execute)

    result = gateway.execute_operation_from_env(
        env={"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"},
        operation_name="operator.example",
        payload={"value": "gateway"},
    )

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.example"
    assert captured["payload"] == {"value": "gateway"}
    assert captured["subsystems"].get_pg_conn().__class__ is _FakeConn


def test_execute_query_operation_also_attaches_operation_receipt(monkeypatch) -> None:
    binding = SimpleNamespace(
        operation_ref="operator.query_example",
        operation_name="operator.query_example",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=lambda command, _subsystems: {"value": command.value},
        authority_ref="authority.example_query",
        projection_ref="projection.example_query",
        posture="observe",
        idempotency_policy="read_only",
        binding_revision="binding.operation.query_example.20260416",
        decision_ref="decision.operation.query_example.20260416",
    )

    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = gateway.execute_operation_from_subsystems(
        _Subsystems(),
        operation_name="operator.query_example",
        payload={"value": "query"},
    )

    assert result["value"] == "query"
    assert result["operation_receipt"]["operation_name"] == "operator.query_example"
    assert result["operation_receipt"]["result_status"] is None
    assert result["operation_receipt"]["event_ids"] == []
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" not in conn.executed_sql()
    assert conn.transaction_commits == 1


def test_requested_query_mode_rejects_command_before_handler_or_receipt(monkeypatch) -> None:
    handled: list[object] = []
    binding = SimpleNamespace(
        operation_ref="operator.command_example",
        operation_name="operator.command_example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda command, _subsystems: handled.append(command) or {"value": command.value},
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.operation.command_example.20260416",
        decision_ref="decision.operation.command_example.20260416",
    )
    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    try:
        gateway.execute_operation_from_subsystems(
            _Subsystems(),
            operation_name="operator.command_example",
            payload={"value": "blocked"},
            requested_mode="query",
        )
    except gateway.OperationModeViolation as exc:
        assert exc.requested_mode == "query"
        assert exc.operation_kind == "command"
        assert exc.posture == "operate"
    else:
        raise AssertionError("query mode should reject command operations")

    assert handled == []
    assert conn.fetchrow_calls == []
    assert conn.execute_calls == []


def test_requested_query_mode_admits_read_only_query(monkeypatch) -> None:
    binding = SimpleNamespace(
        operation_ref="operator.query_example",
        operation_name="operator.query_example",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=lambda command, _subsystems: {"value": command.value},
        authority_ref="authority.example_query",
        projection_ref="projection.example_query",
        posture="observe",
        idempotency_policy="read_only",
        binding_revision="binding.operation.query_example.20260416",
        decision_ref="decision.operation.query_example.20260416",
    )
    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = gateway.execute_operation_from_subsystems(
        _Subsystems(),
        operation_name="operator.query_example",
        payload={"value": "query"},
        requested_mode="query",
    )

    assert result["value"] == "query"
    assert result["operation_receipt"]["operation_kind"] == "query"
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()


def test_aexecute_operation_from_subsystems_awaits_async_handlers(monkeypatch) -> None:
    async def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        return {"value": command.value}

    binding = SimpleNamespace(
        operation_ref="semantic_assertions.list",
        operation_name="semantic_assertions.list",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.semantic_assertions",
        projection_ref="projection.semantic_current_assertions",
        posture="observe",
        idempotency_policy="read_only",
        binding_revision="binding.operation.semantic_assertions.20260416",
        decision_ref="decision.operation.semantic_assertions.20260416",
    )

    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = asyncio.run(
        gateway.aexecute_operation_from_subsystems(
            _Subsystems(),
            operation_name="semantic_assertions.list",
            payload={"value": "async"},
        )
    )

    assert result["value"] == "async"
    assert result["operation_receipt"]["operation_name"] == "semantic_assertions.list"
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert conn.transaction_commits == 1


def test_command_receipt_event_persistence_rolls_back_as_one_proof_write(monkeypatch) -> None:
    binding = SimpleNamespace(
        operation_ref="operator.example",
        operation_name="operator.example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda command, _subsystems: {"value": command.value},
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.operation.example.20260416",
        decision_ref="decision.operation.example.20260416",
    )

    conn = _FakeAuthorityConn()
    conn.raise_on_sql = "INSERT INTO authority_events"

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    try:
        gateway.execute_operation_from_subsystems(
            _Subsystems(),
            operation_name="operator.example",
            payload={"value": "authoritative"},
        )
    except RuntimeError as exc:
        assert "injected proof write failure" in str(exc)
    else:
        raise AssertionError("operation proof write failure should propagate")

    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" in conn.executed_sql()
    assert conn.transaction_enters == 1
    assert conn.transaction_commits == 0
    assert conn.transaction_rollbacks == 1


def test_execute_operation_from_subsystems_runs_async_handlers_for_sync_surfaces(monkeypatch) -> None:
    async def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        return {"value": command.value}

    binding = SimpleNamespace(
        operation_ref="semantic_assertions.list",
        operation_name="semantic_assertions.list",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.semantic_assertions",
        projection_ref="projection.semantic_current_assertions",
        posture="observe",
        idempotency_policy="read_only",
        binding_revision="binding.operation.semantic_assertions.20260416",
        decision_ref="decision.operation.semantic_assertions.20260416",
    )

    conn = _FakeAuthorityConn()

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = gateway.execute_operation_from_subsystems(
        _Subsystems(),
        operation_name="semantic_assertions.list",
        payload={"value": "sync-surface"},
    )

    assert result["value"] == "sync-surface"
    assert result["operation_receipt"]["operation_name"] == "semantic_assertions.list"


def test_idempotent_operation_records_replay_receipt(monkeypatch) -> None:
    input_hash = gateway._stable_hash({"value": "same"})
    cached = {
        "receipt_id": "00000000-0000-0000-0000-000000000001",
        "input_hash": input_hash,
        "result_payload": {"value": "cached", "operation_receipt": {"execution_status": "completed"}},
    }
    conn = _FakeAuthorityConn(cached_result=cached)
    binding = SimpleNamespace(
        operation_ref="operator.idempotent_example",
        operation_name="operator.idempotent_example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda _command, _subsystems: {"value": "should-not-run"},
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="idempotent",
        idempotency_key_fields=["value"],
        binding_revision="binding.operation.idempotent_example.20260416",
        decision_ref="decision.operation.idempotent_example.20260416",
    )

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    result = gateway.execute_operation_from_subsystems(
        _Subsystems(),
        operation_name="operator.idempotent_example",
        payload={"value": "same"},
    )

    assert result["value"] == "cached"
    assert result["operation_receipt"]["execution_status"] == "replayed"
    assert result["operation_receipt"]["idempotency_key"]
    assert result["operation_receipt"]["receipt_id"] != cached["receipt_id"]
    assert result["operation_receipt"]["event_ids"] == []
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" not in conn.executed_sql()
    assert conn.transaction_enters == 1
    assert conn.transaction_commits == 1


def test_idempotent_operation_rejects_same_key_with_different_input(monkeypatch) -> None:
    cached = {
        "receipt_id": "00000000-0000-0000-0000-000000000001",
        "input_hash": gateway._stable_hash({"value": "original"}),
        "result_payload": {"value": "cached"},
    }
    conn = _FakeAuthorityConn(cached_result=cached)
    binding = SimpleNamespace(
        operation_ref="operator.idempotent_example",
        operation_name="operator.idempotent_example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda _command, _subsystems: {"value": "should-not-run"},
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        idempotency_key_fields=[],
        binding_revision="binding.operation.idempotent_example.20260416",
        decision_ref="decision.operation.idempotent_example.20260416",
    )

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return conn

    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda conn, operation_name: binding,
    )

    try:
        gateway.execute_operation_from_subsystems(
            _Subsystems(),
            operation_name="operator.idempotent_example",
            payload={"value": "different"},
            idempotency_key_override="caller-key-1",
        )
    except gateway.OperationIdempotencyConflict as exc:
        assert exc.operation_ref == "operator.idempotent_example"
        assert exc.idempotency_key == "caller-key-1"
    else:
        raise AssertionError("idempotency conflict should reject different input")

    assert conn.execute_calls == []
