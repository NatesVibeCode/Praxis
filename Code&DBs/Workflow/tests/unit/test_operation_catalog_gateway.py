from __future__ import annotations

from types import SimpleNamespace

from pydantic import BaseModel

import runtime.operation_catalog_gateway as gateway


class _ExampleCommand(BaseModel):
    value: str


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

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return object()

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
    assert result["operation_receipt"] == {
        "operation_ref": "operator.example",
        "operation_name": "operator.example",
        "operation_kind": "command",
        "source_kind": "operation_command",
        "authority_ref": "authority.example",
        "projection_ref": None,
        "posture": "operate",
        "idempotency_policy": "non_idempotent",
        "binding_revision": "binding.operation.example.20260416",
        "decision_ref": "decision.operation.example.20260416",
        "execution_status": "completed",
        "result_status": None,
    }
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

    class _Subsystems:
        def get_pg_conn(self) -> object:
            return object()

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
