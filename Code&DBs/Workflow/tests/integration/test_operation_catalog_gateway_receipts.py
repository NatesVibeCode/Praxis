from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

import _pg_test_conn
import runtime.operation_catalog_gateway as gateway


class _ExampleCommand(BaseModel):
    value: str


class _RollbackGatewayConn:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def fetchrow(self, query: str, *args: Any) -> Any:
        return self._conn.fetchrow(query, *args)

    def execute(self, query: str, *args: Any) -> Any:
        return self._conn.execute(query, *args)

    @contextmanager
    def transaction(self):
        yield self


class _Subsystems:
    def __init__(self, conn: _RollbackGatewayConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _RollbackGatewayConn:
        return self._conn


def test_operation_gateway_receipt_response_matches_durable_rows() -> None:
    isolated_conn = _pg_test_conn.get_isolated_conn()
    conn = _RollbackGatewayConn(isolated_conn)
    operation_ref = f"test.operation_receipt.{uuid4()}"
    try:
        binding = SimpleNamespace(
            operation_ref=operation_ref,
            operation_name="test.operation_receipt.proof",
            source_kind="operation_command",
            operation_kind="command",
            command_class=_ExampleCommand,
            handler=lambda command, _subsystems: {
                "status": "recorded",
                "value": command.value,
            },
            authority_ref="authority.test",
            authority_domain_ref="authority.test",
            projection_ref=None,
            storage_target_ref="praxis.primary_postgres",
            posture="operate",
            idempotency_policy="non_idempotent",
            receipt_required=True,
            event_required=True,
            event_type="test.operation_receipt.proofed",
            binding_revision="binding.test.operation_receipt.proof",
            decision_ref="decision.test.operation_receipt.proof",
            idempotency_key_fields=[],
        )

        result = gateway.execute_operation_binding(
            binding,
            payload={"value": "durable"},
            subsystems=_Subsystems(conn),
        )

        receipt = result["operation_receipt"]
        persisted_receipt = conn.fetchrow(
            """
            SELECT receipt_id::text AS receipt_id,
                   operation_ref,
                   operation_name,
                   execution_status,
                   event_ids::text AS event_ids_json,
                   correlation_id::text AS correlation_id
              FROM authority_operation_receipts
             WHERE receipt_id = $1::uuid
            """,
            receipt["receipt_id"],
        )
        assert persisted_receipt is not None
        persisted_event_ids = json.loads(persisted_receipt["event_ids_json"])
        persisted_event = conn.fetchrow(
            """
            SELECT receipt_id::text AS receipt_id,
                   operation_ref,
                   event_type,
                   correlation_id::text AS correlation_id
              FROM authority_events
             WHERE event_id = $1::uuid
            """,
            receipt["event_ids"][0],
        )

        assert receipt["receipt_id"] == persisted_receipt["receipt_id"]
        assert receipt["operation_ref"] == persisted_receipt["operation_ref"]
        assert receipt["operation_name"] == persisted_receipt["operation_name"]
        assert receipt["execution_status"] == persisted_receipt["execution_status"]
        assert receipt["event_ids"] == persisted_event_ids
        assert persisted_event is not None
        assert persisted_event["receipt_id"] == receipt["receipt_id"]
        assert persisted_event["operation_ref"] == operation_ref
        assert persisted_event["event_type"] == "test.operation_receipt.proofed"
        assert persisted_event["correlation_id"] == receipt["correlation_id"]
    finally:
        isolated_conn.close()
