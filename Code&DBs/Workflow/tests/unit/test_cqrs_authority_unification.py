from __future__ import annotations

from typing import Any
from uuid import uuid4

import runtime.operation_catalog_gateway as gateway
from runtime.authority_objects import (
    ListAuthorityAdoptionCommand,
    ListAuthorityDriftCommand,
    ListAuthorityObjectsCommand,
    handle_list_authority_adoption,
    handle_list_authority_drift,
    handle_list_authority_objects,
)
from runtime.feedback_authority import (
    ListAuthorityFeedbackCommand,
    RecordAuthorityFeedbackCommand,
    record_feedback_event,
)
from runtime.service_bus_authority import (
    RecordServiceBusMessageCommand,
    record_service_bus_message,
)


class _FakeTransaction:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeConn":
        self.conn.transaction_enters += 1
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.transaction_commits += 1
        else:
            self.conn.transaction_rollbacks += 1


class _FakeConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.feedback_event_id = str(uuid4())
        self.authority_event_id = str(uuid4())
        self.service_bus_message_id = str(uuid4())
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM authority_feedback_events" in normalized and "WHERE idempotency_key" in normalized:
            return None
        if "FROM authority_feedback_streams" in normalized:
            return {
                "feedback_stream_ref": args[0],
                "feedback_kind": "operator_review",
                "target_authority_domain_ref": None,
            }
        if "INSERT INTO authority_feedback_events" in normalized:
            return {
                "feedback_event_id": args[0],
                "feedback_stream_ref": args[1],
                "target_ref": args[2],
                "source_ref": args[3],
                "signal_kind": args[4],
                "signal_payload": args[5],
                "proposed_action": args[6],
                "recorded_by": args[7],
                "idempotency_key": args[8],
                "authority_event_id": None,
            }
        if normalized.startswith("UPDATE authority_feedback_events"):
            return {
                "feedback_event_id": args[0],
                "feedback_stream_ref": "feedback.operator_review",
                "target_ref": "authority.test",
                "source_ref": "unit",
                "signal_kind": "observation",
                "signal_payload": {},
                "proposed_action": {},
                "recorded_by": "unit",
                "idempotency_key": "feedback:test",
                "authority_event_id": args[1],
            }
        if "FROM service_bus_message_contracts" in normalized:
            return {
                "message_type_ref": args[0],
                "channel_ref": args[1],
                "authority_domain_ref": "authority.workflow_runs",
            }
        if "INSERT INTO service_bus_message_ledger" in normalized:
            return {
                "message_id": self.service_bus_message_id,
                "channel_ref": args[0],
                "message_type_ref": args[1],
                "correlation_ref": args[2],
                "command_ref": args[3],
                "receipt_id": args[4],
                "authority_domain_ref": args[5],
                "message_status": args[6],
                "payload": args[7],
                "recorded_by": args[8],
            }
        raise AssertionError(f"unexpected fetchrow query: {query}")

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        if "authority_object_ownership" in query:
            return [{"object_ref": "table.public.authority_object_registry"}]
        if "authority_object_drift_report" in query:
            return [{"object_ref": "table.public.unregistered", "drift_kind": "missing"}]
        if "authority_schema_adoption_report" in query:
            return [{
                "table_name": "legacy_table",
                "adoption_status": "legacy_inventory",
                "authority_domain_ref": "authority.legacy_schema",
            }]
        if "authority_feedback_event_projection" in query:
            return [{"feedback_event_id": self.feedback_event_id}]
        return []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
        return []

    def executed_sql(self) -> str:
        return "\n".join([q for q, _ in self.execute_calls] + [q for q, _ in self.fetchrow_calls])

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


class _Subsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_authority_object_runtime_lists_ownership_and_drift() -> None:
    conn = _FakeConn()
    subsystems = _Subsystems(conn)

    objects = handle_list_authority_objects(
        ListAuthorityObjectsCommand(object_kind="table"),
        subsystems,
    )
    drift = handle_list_authority_drift(
        ListAuthorityDriftCommand(object_kind="table"),
        subsystems,
    )

    assert objects["status"] == "listed"
    assert objects["objects"][0]["object_ref"] == "table.public.authority_object_registry"
    assert drift["drift"][0]["object_ref"] == "table.public.unregistered"
    assert "authority_object_ownership" in conn.fetch_calls[0][0]
    assert "authority_object_drift_report" in conn.fetch_calls[1][0]


def test_authority_adoption_runtime_reports_legacy_inventory() -> None:
    conn = _FakeConn()

    result = handle_list_authority_adoption(
        ListAuthorityAdoptionCommand(adoption_status="legacy_inventory"),
        _Subsystems(conn),
    )

    assert result["status"] == "listed"
    assert result["adoption"][0]["table_name"] == "legacy_table"
    assert result["adoption"][0]["authority_domain_ref"] == "authority.legacy_schema"
    assert "authority_schema_adoption_report" in conn.fetch_calls[0][0]


def test_feedback_authority_records_immutable_feedback_and_authority_event() -> None:
    conn = _FakeConn()

    result = record_feedback_event(
        conn,
        RecordAuthorityFeedbackCommand(
            feedback_stream_ref="feedback.operator_review",
            target_ref="authority.test",
            source_ref="unit",
            signal_kind="observation",
            signal_payload={"saw": "drift"},
            proposed_action={"kind": "inspect"},
            recorded_by="unit",
            idempotency_key="feedback:test",
        ),
    )

    assert result["status"] == "recorded"
    assert result["authority_event_ids"]
    sql = conn.executed_sql()
    assert "INSERT INTO authority_feedback_events" in sql
    assert "INSERT INTO authority_events" in sql
    assert "UPDATE authority_feedback_events" in sql


def test_service_bus_authority_records_transport_envelope_not_domain_truth() -> None:
    conn = _FakeConn()

    result = record_service_bus_message(
        conn,
        RecordServiceBusMessageCommand(
            channel_ref="service_bus.channel.workflow_command",
            message_type_ref="service_bus.message.workflow_submit",
            correlation_ref="control.command.test",
            command_ref="control.command.test",
            payload={"command_type": "workflow.submit"},
        ),
    )

    assert result["status"] == "recorded"
    assert result["message"]["authority_domain_ref"] == "authority.workflow_runs"
    assert "INSERT INTO service_bus_message_ledger" in conn.executed_sql()


def test_operation_gateway_reuses_domain_authority_event_ids(monkeypatch) -> None:
    event_id = str(uuid4())

    binding = type(
        "Binding",
        (),
        {
            "operation_ref": "feedback-record",
            "operation_name": "feedback.record",
            "source_kind": "operation_command",
            "operation_kind": "command",
            "authority_ref": "authority.feedback",
            "authority_domain_ref": "authority.feedback",
            "projection_ref": "projection.feedback.events",
            "storage_target_ref": "praxis.primary_postgres",
            "receipt_required": True,
            "event_required": True,
            "event_type": "feedback_recorded",
            "posture": "operate",
            "idempotency_policy": "non_idempotent",
            "binding_revision": "binding.feedback.test",
            "decision_ref": "decision.feedback.test",
            "idempotency_key_fields": [],
        },
    )()
    conn = _FakeConn()

    receipt = gateway._persist_operation_outcome(
        conn,
        binding,
        payload={"target_ref": "authority.test"},
        result={"status": "recorded", "authority_event_ids": [event_id]},
        input_hash="input",
        idempotency_key=None,
        started_ns=0,
    )

    assert receipt["event_ids"] == [event_id]
    sql = conn.executed_sql()
    assert "UPDATE authority_events" in sql
    assert "INSERT INTO authority_events" not in sql
