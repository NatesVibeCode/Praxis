"""End-to-end gateway dispatch test for the ``friction_record`` operation.

Proves that calling ``friction_record`` through ``execute_operation_from_subsystems``
records both a ``friction_events`` row AND an ``authority_events`` row with
``event_type='friction.recorded'`` linked by ``receipt_id`` — which is the
contract migration 332 sets up.

This is the proof Bug Praxis cared about: the JIT trigger-check hooks were
previously shelling into ``praxis_friction --action=record`` and getting
``{"error":"Unknown friction action: record"}`` back. Now the same call
flows through the catalog gateway and leaves durable receipts + events.
"""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import _pg_test_conn
import runtime.operation_catalog_gateway as gateway
from runtime.friction_ledger import FrictionLedger


class _GatewayConn:
    """Conn wrapper exposing the production ``SyncPostgresConnection`` surface
    on top of the rollback-on-close ``_IsolatedSyncPostgresConnection`` used
    by the test harness.

    The catalog-repository read paths call ``conn.fetch(...)`` while the
    isolated test conn only exposes ``execute`` / ``fetchrow`` / ``fetchval``.
    The gateway also uses ``conn.transaction()`` for receipt + event writes;
    nested-savepoint commits are pointless here because the outer isolated
    transaction rolls back on close, so we yield self (every write rides the
    outer transaction).
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def fetch(self, query: str, *args: Any) -> Any:
        return self._conn.execute(query, *args)

    def fetchrow(self, query: str, *args: Any) -> Any:
        return self._conn.fetchrow(query, *args)

    def fetchval(self, query: str, *args: Any) -> Any:
        return self._conn.fetchval(query, *args)

    def execute(self, query: str, *args: Any) -> Any:
        return self._conn.execute(query, *args)

    @contextmanager
    def transaction(self):
        yield self


class _Subsystems:
    """Minimal subsystems facade satisfying the gateway contract."""

    def __init__(self, conn: _GatewayConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _GatewayConn:
        return self._conn

    def get_friction_ledger(self) -> FrictionLedger:
        # Embedder=None: vector enrichment is best-effort and unrelated to
        # this proof. The ledger still writes the friction_events row.
        return FrictionLedger(self._conn, embedder=None)


def _read_friction_event(conn: Any, event_id: str) -> dict | None:
    return conn.fetchrow(
        """
        SELECT event_id, friction_type, source, job_label, message, task_mode, is_test
          FROM friction_events
         WHERE event_id = $1
        """,
        event_id,
    )


def _read_authority_event(conn, event_id: str) -> dict | None:
    return conn.fetchrow(
        """
        SELECT event_id::text         AS event_id,
               event_type,
               operation_ref,
               receipt_id::text       AS receipt_id,
               event_payload::text    AS event_payload_json,
               authority_domain_ref
          FROM authority_events
         WHERE event_id = $1::uuid
        """,
        event_id,
    )


def _read_operation_receipt(conn, receipt_id: str) -> dict | None:
    return conn.fetchrow(
        """
        SELECT receipt_id::text  AS receipt_id,
               operation_ref,
               operation_name,
               execution_status,
               event_ids::text    AS event_ids_json
          FROM authority_operation_receipts
         WHERE receipt_id = $1::uuid
        """,
        receipt_id,
    )


def test_friction_record_gateway_writes_friction_event_and_authority_event() -> None:
    isolated_conn = _pg_test_conn.get_isolated_conn()
    try:
        gateway_conn = _GatewayConn(isolated_conn)
        subsystems = _Subsystems(gateway_conn)

        unique_source = f"test.friction_record.{uuid4().hex[:8]}"
        decision_keys = [
            "architecture-policy::test::friction_record_gateway",
        ]
        payload = {
            "event_type": "WARN_ONLY",
            "source": unique_source,
            "subject_kind": "agent_action",
            "subject_ref": "Bash",
            "decision_keys": decision_keys,
            "metadata": {
                "subject": "echo hello",
                "matched_decisions": decision_keys,
                "harness": "claude_code",
            },
            "task_mode": "Build",  # capitalization is intentionally non-canonical
            "is_test": True,
        }

        result = gateway.execute_operation_from_subsystems(
            subsystems,
            operation_name="friction_record",
            payload=payload,
        )

        # Handler return contract. friction_type is the enum's lowercase value
        # (FrictionType.WARN_ONLY.value == "warn_only").
        assert result["ok"] is True
        assert result["friction_type"] == "warn_only"
        assert result["source"] == unique_source
        assert result["job_label"] == "Bash"
        assert result["task_mode"] == "build", "ledger normalizes task_mode to lower case"
        ledger_event_id = result["event_id"]
        assert ledger_event_id, "handler must surface the new ledger event_id"

        # Gateway-attached receipt envelope.
        receipt = result["operation_receipt"]
        assert receipt["operation_name"] == "friction_record"
        assert receipt["operation_ref"] == "friction-record"
        assert receipt["execution_status"] == "completed"
        assert receipt["event_ids"], "command operations with event_required must publish ≥1 event_id"
        authority_event_id = receipt["event_ids"][0]

        # Durable friction_events row.
        friction_row = _read_friction_event(isolated_conn, ledger_event_id)
        assert friction_row is not None, "friction_events row must persist"
        assert friction_row["friction_type"] == "warn_only"
        assert friction_row["source"] == unique_source
        assert friction_row["job_label"] == "Bash"
        assert friction_row["task_mode"] == "build"
        assert friction_row["is_test"] is True
        # message defaults to JSON envelope; should contain the matched decision key.
        assert decision_keys[0] in friction_row["message"]

        # Durable authority_events row, linked to the receipt.
        authority_row = _read_authority_event(isolated_conn, authority_event_id)
        assert authority_row is not None, "authority_events row must persist"
        assert authority_row["event_type"] == "friction.recorded"
        assert authority_row["operation_ref"] == "friction-record"
        assert authority_row["authority_domain_ref"] == "authority.friction_events"
        assert authority_row["receipt_id"] == receipt["receipt_id"]

        # Hoisted event_payload carries decision-relevant fields.
        event_payload = json.loads(authority_row["event_payload_json"])
        assert event_payload["event_id"] == ledger_event_id
        assert event_payload["friction_type"] == "warn_only"
        assert event_payload["source"] == unique_source
        assert event_payload["decision_keys"] == decision_keys
        assert event_payload["decision_match_count"] == len(decision_keys)
        assert event_payload["task_mode"] == "build"
        assert event_payload["is_test"] is True

        # The receipt's event_ids array must include the same UUID that the
        # authority_events row carries — that's the receipt↔event link.
        operation_receipt_row = _read_operation_receipt(
            isolated_conn, receipt["receipt_id"],
        )
        assert operation_receipt_row is not None
        assert operation_receipt_row["operation_ref"] == "friction-record"
        assert operation_receipt_row["execution_status"] == "completed"
        persisted_event_ids = json.loads(operation_receipt_row["event_ids_json"])
        assert authority_event_id in persisted_event_ids
    finally:
        isolated_conn.close()


def test_friction_record_handler_rejects_unknown_event_type() -> None:
    """Pydantic input validation refuses event_type values outside the enum."""

    from pydantic import ValidationError
    from runtime.operations.commands.friction_record import FrictionRecordInput

    try:
        FrictionRecordInput(event_type="MAYBE", source="test")
    except ValidationError:
        return
    raise AssertionError(
        "FrictionRecordInput must reject event_type values outside the friction enum",
    )


def test_friction_record_handler_defaults_job_label_to_subject_ref() -> None:
    """Handler defaults job_label to subject_ref when not supplied."""

    from runtime.operations.commands.friction_record import (
        FrictionRecordInput,
        handle_friction_record,
    )

    isolated_conn = _pg_test_conn.get_isolated_conn()
    try:
        subsystems = _Subsystems(_GatewayConn(isolated_conn))
        result = handle_friction_record(
            FrictionRecordInput(
                event_type="GUARDRAIL_BOUNCE",
                source=f"test.handler.{uuid4().hex[:8]}",
                subject_ref="Edit",
                is_test=True,
            ),
            subsystems,
        )
        assert result["ok"] is True
        assert result["job_label"] == "Edit"
        assert result["friction_type"] == "guardrail_bounce"
    finally:
        isolated_conn.close()
