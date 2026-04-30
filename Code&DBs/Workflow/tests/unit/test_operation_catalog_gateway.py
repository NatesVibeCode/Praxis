from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

from pydantic import BaseModel

import runtime.operation_catalog_gateway as gateway
from runtime.provider_authority import ProviderAuthorityError


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
    def __init__(
        self,
        *,
        cached_result: dict[str, object] | None = None,
        readback_overrides: dict[str, object] | None = None,
    ) -> None:
        self.cached_result = cached_result
        self.readback_overrides = dict(readback_overrides or {})
        self.receipts: dict[str, dict[str, object]] = {}
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0
        self.raise_on_sql: str | None = None

    def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if (
            "FROM authority_operation_receipts" in normalized
            and "WHERE receipt_id = $1::uuid" in normalized
        ):
            receipt = self.receipts.get(str(args[0]))
            if receipt is None:
                return None
            return {**receipt, **self.readback_overrides}
        if "FROM authority_operation_receipts" in normalized:
            return self.cached_result
        raise AssertionError(f"unexpected fetchrow: {query}")

    def execute(self, query: str, *args: object) -> list[dict[str, object]]:
        self.execute_calls.append((query, args))
        if self.raise_on_sql and self.raise_on_sql in query:
            raise RuntimeError("injected proof write failure")
        if "INSERT INTO authority_operation_receipts" in query:
            self.receipts[str(args[0])] = {
                "receipt_id": args[0],
                "operation_ref": args[1],
                "operation_name": args[2],
                "operation_kind": args[3],
                "authority_domain_ref": args[4],
                "authority_ref": args[5],
                "projection_ref": args[6],
                "storage_target_ref": args[7],
                "input_hash": args[8],
                "output_hash": args[9],
                "idempotency_key": args[10],
                "caller_ref": args[11],
                "transport_kind": args[12],
                "execution_status": args[13],
                "result_status": args[14],
                "error_code": args[15],
                "error_detail": args[16],
                "event_ids": args[17],
                "projection_freshness": args[18],
                "result_payload": args[19],
                "duration_ms": args[20],
                "binding_revision": args[21],
                "decision_ref": args[22],
                "cause_receipt_id": args[23],
                "correlation_id": args[24],
            }
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


def test_operation_receipt_response_is_durable_readback(monkeypatch) -> None:
    binding = SimpleNamespace(
        operation_ref="operator.example",
        operation_name="operator.example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda command, _subsystems: {"status": "recorded", "value": command.value},
        authority_ref="authority.example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.operation.example.20260416",
        decision_ref="decision.operation.example.20260416",
    )

    conn = _FakeAuthorityConn(readback_overrides={"duration_ms": 4242})

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
        operation_name="operator.example",
        payload={"value": "authoritative"},
    )

    receipt = result["operation_receipt"]
    assert receipt["duration_ms"] == 4242
    assert receipt["operation_name"] == "operator.example"
    assert receipt["source_kind"] == "operation_command"
    assert any(
        "WHERE receipt_id = $1::uuid" in " ".join(query.split())
        for query, _args in conn.fetchrow_calls
    )


def test_execute_operation_from_env_builds_env_backed_subsystems(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeConn:
        pass

    monkeypatch.setattr(gateway, "get_workflow_pool", lambda env: object())
    monkeypatch.setattr(gateway, "SyncPostgresConnection", lambda pool: _FakeConn())

    def _execute(subsystems, *, operation_name: str, payload, caller_context=None):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        captured["caller_context"] = caller_context
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
    assert captured["caller_context"].transport_kind == "cli"
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


def test_command_exception_returns_failed_receipt_without_event(monkeypatch) -> None:
    class _TypedMaterializeFailure(RuntimeError):
        reason_code = "compile.materialize.empty_graph"
        details = {"workflow_id": "wf_empty", "graph_summary": {"node_count": 0}}

    def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        raise _TypedMaterializeFailure(f"blocked {command.value}")

    binding = SimpleNamespace(
        operation_ref="compile.materialize",
        operation_name="compile_materialize",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.compile",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.operation.compile_materialize.20260429",
        decision_ref="decision.operation.compile_materialize.20260429",
        event_required=True,
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
        operation_name="compile_materialize",
        payload={"value": "empty graph"},
    )

    assert result["ok"] is False
    assert result["error_code"] == "compile.materialize.empty_graph"
    assert result["details"] == {"workflow_id": "wf_empty", "graph_summary": {"node_count": 0}}
    receipt = result["operation_receipt"]
    assert receipt["operation_name"] == "compile_materialize"
    assert receipt["execution_status"] == "failed"
    assert receipt["error_code"] == "compile.materialize.empty_graph"
    assert receipt["event_ids"] == []
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" not in conn.executed_sql()


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


def test_gateway_preserves_typed_reason_code_and_details(monkeypatch) -> None:
    binding = SimpleNamespace(
        operation_ref="operator.query_example",
        operation_name="operator.query_example",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=lambda _command, _subsystems: (_ for _ in ()).throw(
            ProviderAuthorityError(
                reason_code="provider_authority.transport_admission_missing",
                message="provider transport admission missing",
                details={"provider_slug": "openai", "adapter_type": "llm_task"},
            )
        ),
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

    assert result["ok"] is False
    assert result["error_code"] == "provider_authority.transport_admission_missing"
    assert result["details"] == {"provider_slug": "openai", "adapter_type": "llm_task"}


def test_sync_interactive_operation_timeout_returns_failed_receipt(monkeypatch) -> None:
    def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        time.sleep(0.05)
        return {"value": command.value}

    binding = SimpleNamespace(
        operation_ref="operator.interactive_example",
        operation_name="operator.interactive_example",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.example_query",
        projection_ref="projection.example_query",
        posture="observe",
        idempotency_policy="read_only",
        execution_lane="interactive",
        timeout_ms=1,
        binding_revision="binding.operation.interactive_example.20260430",
        decision_ref="decision.operation.interactive_example.20260430",
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
        operation_name="operator.interactive_example",
        payload={"value": "slow"},
        caller_context=gateway.CallerContext(
            cause_receipt_id=None,
            correlation_id="00000000-0000-0000-0000-000000000123",
            transport_kind="mcp",
        ),
    )

    assert result["ok"] is False
    assert result["error_code"] == "operation.interactive_timeout"
    assert result["details"] == {
        "operation_ref": "operator.interactive_example",
        "timeout_ms": 1,
    }
    receipt = result["operation_receipt"]
    assert receipt["execution_status"] == "failed"
    assert receipt["error_code"] == "operation.interactive_timeout"
    assert receipt["transport_kind"] == "mcp"


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


# ---------------------------------------------------------------------------
# Phase B — execution_lane + kickoff_required enforcement (migration 348).
# ---------------------------------------------------------------------------

def _kickoff_required_binding() -> SimpleNamespace:
    return SimpleNamespace(
        operation_ref="operator.background_example",
        operation_name="operator.background_example",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_ExampleCommand,
        handler=lambda _command, _subsystems: {"value": "should-not-run"},
        authority_ref="authority.background_example",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        execution_lane="background",
        kickoff_required=True,
        timeout_ms=15000,
        binding_revision="binding.operation.background_example.20260430",
        decision_ref="decision.operation.background_example.20260430",
    )


def test_kickoff_required_rejects_interactive_transport_sync(monkeypatch) -> None:
    """kickoff_required=true on a background op refuses direct sync dispatch
    from interactive transports (cli/mcp/http). The handler never runs and
    a failed receipt is persisted so the rejection is observable."""

    binding = _kickoff_required_binding()
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
        operation_name="operator.background_example",
        payload={"value": "interactive-cli"},
        caller_context=gateway.CallerContext(
            cause_receipt_id=None,
            correlation_id="00000000-0000-0000-0000-000000000200",
            transport_kind="cli",
        ),
    )

    assert result["ok"] is False
    assert result["error_code"] == "operation.kickoff_required"
    assert result["details"]["transport_kind"] == "cli"
    receipt = result["operation_receipt"]
    assert receipt["execution_status"] == "failed"
    assert receipt["error_code"] == "operation.kickoff_required"
    assert receipt["transport_kind"] == "cli"


def test_kickoff_required_rejects_interactive_transport_async(monkeypatch) -> None:
    """Same enforcement, async dispatch path."""

    binding = _kickoff_required_binding()
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
            operation_name="operator.background_example",
            payload={"value": "interactive-mcp"},
            caller_context=gateway.CallerContext(
                cause_receipt_id=None,
                correlation_id="00000000-0000-0000-0000-000000000201",
                transport_kind="mcp",
            ),
        )
    )

    assert result["ok"] is False
    assert result["error_code"] == "operation.kickoff_required"
    assert result["details"]["transport_kind"] == "mcp"
    receipt = result["operation_receipt"]
    assert receipt["execution_status"] == "failed"
    assert receipt["transport_kind"] == "mcp"


def test_kickoff_required_admits_workflow_transport(monkeypatch) -> None:
    """Worker/runtime callers still execute a kickoff_required operation —
    the gate is at the *interactive* edge, not on the operation entirely.
    A workflow-lane caller picking up the kicked-off work must not be
    blocked."""

    handler_calls: list[dict[str, Any]] = []

    def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        handler_calls.append({"value": command.value})
        return {"value": command.value, "ran": True}

    binding = _kickoff_required_binding()
    binding.handler = _handler

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
        operation_name="operator.background_example",
        payload={"value": "worker-claim"},
        caller_context=gateway.CallerContext(
            cause_receipt_id=None,
            correlation_id="00000000-0000-0000-0000-000000000202",
            transport_kind="workflow",
        ),
    )

    assert handler_calls == [{"value": "worker-claim"}]
    assert result["ok"] is True
    assert result["operation_receipt"]["transport_kind"] == "workflow"


def test_async_interactive_timeout_returns_failed_receipt(monkeypatch) -> None:
    """asyncio.wait_for around the handler enforces the interactive
    deadline on the async dispatch path, mirroring the sync path's
    thread+queue enforcement."""

    async def _handler(command: _ExampleCommand, _subsystems: object) -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"value": command.value}

    binding = SimpleNamespace(
        operation_ref="operator.async_interactive_example",
        operation_name="operator.async_interactive_example",
        source_kind="operation_query",
        operation_kind="query",
        command_class=_ExampleCommand,
        handler=_handler,
        authority_ref="authority.async_interactive",
        projection_ref="projection.async_interactive",
        posture="observe",
        idempotency_policy="read_only",
        execution_lane="interactive",
        kickoff_required=False,
        timeout_ms=1,
        binding_revision="binding.operation.async_interactive.20260430",
        decision_ref="decision.operation.async_interactive.20260430",
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
            operation_name="operator.async_interactive_example",
            payload={"value": "slow-async"},
            caller_context=gateway.CallerContext(
                cause_receipt_id=None,
                correlation_id="00000000-0000-0000-0000-000000000203",
                transport_kind="http",
            ),
        )
    )

    assert result["ok"] is False
    assert result["error_code"] == "operation.interactive_timeout"
    assert result["details"] == {
        "operation_ref": "operator.async_interactive_example",
        "timeout_ms": 1,
    }
    receipt = result["operation_receipt"]
    assert receipt["execution_status"] == "failed"
    assert receipt["error_code"] == "operation.interactive_timeout"
    assert receipt["transport_kind"] == "http"
