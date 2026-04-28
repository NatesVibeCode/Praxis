from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient
from pydantic import BaseModel

import runtime.operation_catalog_gateway as gateway
import surfaces.api.rest as rest
from surfaces.cli.main import main as workflow_cli_main


class _Subsystems:
    def get_pg_conn(self) -> object:
        return object()


class _GatewayCommand(BaseModel):
    text: str


class _FakeGatewayTransaction:
    def __init__(self, conn: "_FakeGatewayConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeGatewayConn":
        self.conn.transaction_enters += 1
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.transaction_commits += 1
        else:
            self.conn.transaction_rollbacks += 1


class _FakeGatewayConn:
    def __init__(self) -> None:
        self.receipts: dict[str, dict[str, Any]] = {}
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if (
            "FROM authority_operation_receipts" in normalized
            and "WHERE receipt_id = $1::uuid" in normalized
        ):
            return self.receipts.get(str(args[0]))
        if "FROM authority_operation_receipts" in normalized:
            return None
        raise AssertionError(f"unexpected fetchrow: {query}")

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
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
                "execution_status": args[12],
                "result_status": args[13],
                "error_code": args[14],
                "error_detail": args[15],
                "event_ids": args[16],
                "projection_freshness": args[17],
                "result_payload": args[18],
                "duration_ms": args[19],
                "binding_revision": args[20],
                "decision_ref": args[21],
                "cause_receipt_id": args[22],
                "correlation_id": args[23],
            }
        return []

    def transaction(self) -> _FakeGatewayTransaction:
        return _FakeGatewayTransaction(self)

    def executed_sql(self) -> str:
        return "\n".join(query for query, _args in self.execute_calls)


class _GatewaySubsystems:
    def __init__(self, conn: _FakeGatewayConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeGatewayConn:
        return self._conn


def test_operate_catalog_projects_db_operations(monkeypatch) -> None:
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())
    monkeypatch.setattr(
        rest,
        "build_operation_catalog_payload",
        lambda _conn: {
            "routed_to": "operation_catalog",
            "contract_version": 1,
            "operations": [
                {
                    "operation_name": "operator.echo",
                    "operation_ref": "operation.operator.echo",
                }
            ],
            "count": 1,
            "source_policies": [],
            "source_policy_count": 0,
        },
    )

    payload = rest.build_operate_catalog_payload()

    assert payload["routed_to"] == "operation_catalog_gateway"
    assert payload["call_path"] == "/api/operate"
    assert payload["catalog_path"] == "/api/catalog/operations"
    assert payload["operation_count"] == 1
    assert payload["operations"][0]["operation_name"] == "operator.echo"
    assert payload["authority"] == "operation_catalog_registry"


def test_operate_endpoint_delegates_to_db_operation_gateway(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any], str | None, str | None]] = []
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(
        _subsystems: object,
        *,
        operation_name: str,
        payload: dict[str, Any],
        idempotency_key_override: str | None = None,
        requested_mode: str | None = None,
    ) -> dict[str, Any]:
        calls.append((operation_name, payload, idempotency_key_override, requested_mode))
        return {
            "echo": payload,
            "operation_receipt": {
                "operation_name": operation_name,
                "authority_ref": "authority.echo",
                "execution_status": "completed",
                "idempotency_key": idempotency_key_override,
            },
        }

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "operator.echo",
                "input": {"text": "hello"},
                "idempotency_key": "idem-123",
                "trace": {"caller": "unit-test"},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["routed_to"] == "operation_catalog_gateway"
    assert payload["operation"] == "operator.echo"
    assert payload["result"] == {"echo": {"text": "hello"}}
    assert payload["operation_receipt"]["operation_name"] == "operator.echo"
    assert payload["operation_receipt"]["idempotency_key"] == "idem-123"
    assert calls == [("operator.echo", {"text": "hello"}, "idem-123", "call")]


def test_operate_endpoint_uses_idempotency_header_when_body_key_absent(monkeypatch) -> None:
    calls: list[tuple[str, str | None]] = []
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(
        _subsystems: object,
        *,
        operation_name: str,
        payload: dict[str, Any],
        idempotency_key_override: str | None = None,
        requested_mode: str | None = None,
    ) -> dict[str, Any]:
        calls.append((operation_name, idempotency_key_override))
        return {
            "echo": payload,
            "operation_receipt": {
                "operation_name": operation_name,
                "execution_status": "completed",
                "idempotency_key": idempotency_key_override,
                "requested_mode": requested_mode,
            },
        }

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            headers={"Idempotency-Key": "idem-header-456"},
            json={
                "operation": "operator.echo",
                "input": {"text": "hello"},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["idempotency_key"] == "idem-header-456"
    assert payload["operation_receipt"]["idempotency_key"] == "idem-header-456"
    assert calls == [("operator.echo", "idem-header-456")]


def test_workflow_operate_catalog_cli_uses_existing_gateway_catalog(monkeypatch) -> None:
    monkeypatch.setattr(
        rest,
        "build_operate_catalog_payload",
        lambda: {
            "ok": True,
            "authority": "operation_catalog_registry",
            "call_path": "/api/operate",
            "catalog_path": "/api/catalog/operations",
            "operation_count": 1,
            "operations": [{"operation_name": "operator.echo"}],
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["operate", "catalog", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["call_path"] == "/api/operate"
    assert payload["operations"][0]["operation_name"] == "operator.echo"


def test_workflow_operate_call_cli_delegates_to_existing_gateway(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_execute(body, *, header_idempotency_key=None, header_workflow_token=None):
        captured["operation"] = body.operation
        captured["mode"] = body.mode
        captured["input"] = body.input
        captured["idempotency_key"] = header_idempotency_key
        captured["workflow_token"] = header_workflow_token
        return 200, {"ok": True, "operation": body.operation, "result": {"echo": body.input}}

    monkeypatch.setattr(rest, "execute_operate_request", _fake_execute)

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "operate",
                "query",
                "operator.echo",
                "--input-json",
                '{"text":"hello"}',
                "--idempotency-key",
                "idem-cli-1",
            ],
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert payload["ok"] is True
    assert captured == {
        "operation": "operator.echo",
        "mode": "query",
        "input": {"text": "hello"},
        "idempotency_key": "idem-cli-1",
        "workflow_token": None,
    }


def test_operate_endpoint_persists_authority_receipt_through_gateway(monkeypatch) -> None:
    conn = _FakeGatewayConn()
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _GatewaySubsystems(conn))

    binding = SimpleNamespace(
        operation_ref="operator.echo",
        operation_name="operator.echo",
        source_kind="operation_command",
        operation_kind="command",
        command_class=_GatewayCommand,
        handler=lambda command, _subsystems: {"text": command.text},
        authority_ref="authority.operator",
        projection_ref=None,
        posture="operate",
        idempotency_policy="idempotent",
        idempotency_key_fields=["text"],
        binding_revision="binding.operator.echo.test",
        decision_ref="decision.operator.echo.test",
    )
    monkeypatch.setattr(
        gateway,
        "resolve_named_operation_binding",
        lambda _conn, operation_name: binding,
    )

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "operator.echo",
                "mode": "command",
                "input": {"text": "durable"},
                "idempotency_key": "idem-durable-1",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"] == {"ok": True, "text": "durable"}
    assert payload["operation_receipt"]["operation_name"] == "operator.echo"
    assert payload["operation_receipt"]["idempotency_key"] == "idem-durable-1"
    assert "INSERT INTO authority_operation_receipts" in conn.executed_sql()
    assert "INSERT INTO authority_events" in conn.executed_sql()
    assert conn.transaction_commits == 1


def test_operate_endpoint_rejects_unknown_operation(monkeypatch) -> None:
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise rest.OperationCatalogBoundaryError("Operation not found: praxis_missing", status_code=404)

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post("/api/operate", json={"operation": "praxis_missing", "input": {}})

    assert response.status_code == 404
    assert response.json()["reason_code"] == "operate.operation_not_found"


def test_operate_endpoint_reports_binding_resolution_failure(monkeypatch) -> None:
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise rest.OperationBindingResolutionError("missing command class")

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post("/api/operate", json={"operation": "broken.operation", "input": {}})

    assert response.status_code == 500
    assert response.json()["reason_code"] == "operate.binding_resolution_failed"


def test_operate_endpoint_reports_idempotency_conflict(monkeypatch) -> None:
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise rest.OperationIdempotencyConflict(
            operation_ref="operation.operator.echo",
            idempotency_key="idem-123",
        )

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "operator.echo",
                "input": {"text": "changed"},
                "idempotency_key": "idem-123",
            },
        )

    assert response.status_code == 409
    payload = response.json()
    assert payload["reason_code"] == "operate.idempotency_conflict"
    assert payload["idempotency_key"] == "idem-123"


def test_operate_endpoint_rejects_mode_that_does_not_admit_operation(monkeypatch) -> None:
    monkeypatch.setattr(rest, "mount_capabilities", lambda _app: None)
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: _Subsystems())

    def fake_execute_operation_from_subsystems(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise rest.OperationModeViolation(
            operation_ref="operation.operator.write",
            requested_mode="query",
            operation_kind="command",
            posture="operate",
        )

    monkeypatch.setattr(rest, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/operate",
            json={
                "operation": "operator.write",
                "mode": "query",
                "input": {"value": "mutating"},
            },
        )

    assert response.status_code == 422
    payload = response.json()
    assert payload["reason_code"] == "operate.mode_operation_mismatch"
    assert payload["requested_mode"] == "query"
    assert payload["operation_kind"] == "command"
