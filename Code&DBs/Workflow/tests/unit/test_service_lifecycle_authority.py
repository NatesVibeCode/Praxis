from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from runtime.service_lifecycle import (
    DeclareServiceDesiredStateCommand,
    RecordServiceLifecycleEventCommand,
    RegisterRuntimeTargetCommand,
    ServiceLifecycleError,
    declare_service_desired_state,
    normalize_substrate_kind,
    record_service_lifecycle_event,
    register_runtime_target,
)


class _FakeTransaction:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeConn":
        self.conn.transaction_opened = True
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        self.conn.transaction_closed = True


class _FakeConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.transaction_opened = False
        self.transaction_closed = False
        self.event_sequence = 0
        self.active_desired: dict[str, Any] | None = {
            "desired_state_ref": "service_desired.previous",
            "desired_status": "running",
        }
        self.projection: dict[str, Any] | None = None

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM service_definitions" in normalized and "enabled = TRUE" in normalized:
            return {"exists": 1}
        if "FROM runtime_targets" in normalized and "enabled = TRUE" in normalized:
            return {"exists": 1}
        if "WHERE idempotency_key = $1" in normalized:
            return None
        if normalized.startswith("UPDATE service_desired_states"):
            previous = self.active_desired
            self.active_desired = None
            return previous
        if "INSERT INTO runtime_targets" in normalized:
            return {
                "runtime_target_ref": args[0],
                "target_scope": args[1],
                "substrate_kind": args[2],
                "display_name": args[3],
                "workspace_ref": args[4],
                "base_path_ref": args[5],
                "host_ref": args[6],
                "endpoint_contract": args[7],
                "capability_contract": args[8],
                "secret_provider_ref": args[9],
                "enabled": args[10],
                "decision_ref": args[11],
            }
        if "INSERT INTO service_desired_states" in normalized:
            row = {
                "desired_state_ref": args[0],
                "service_ref": args[1],
                "runtime_target_ref": args[2],
                "desired_status": args[3],
                "desired_config": args[4],
                "environment_refs": args[5],
                "health_contract": args[6],
                "reconciler_ref": args[7],
                "declared_by": args[8],
                "declaration_reason": args[9],
                "idempotency_key": args[10],
                "supersedes_ref": args[11],
                "active": True,
            }
            self.active_desired = {
                "desired_state_ref": row["desired_state_ref"],
                "desired_status": row["desired_status"],
            }
            return row
        if "INSERT INTO service_instance_events" in normalized:
            self.event_sequence += 1
            if "desired_state_declared" in normalized:
                row = {
                    "event_id": uuid4(),
                    "event_sequence": self.event_sequence,
                    "service_ref": args[0],
                    "runtime_target_ref": args[1],
                    "desired_state_ref": args[2],
                    "event_type": "desired_state_declared",
                    "observed_status": "pending",
                    "event_payload": args[3],
                    "event_status": "recorded",
                    "observed_by": args[4],
                    "operation_ref": args[5],
                }
            else:
                row = {
                    "event_id": uuid4(),
                    "event_sequence": self.event_sequence,
                    "service_ref": args[0],
                    "runtime_target_ref": args[1],
                    "desired_state_ref": args[2],
                    "event_type": args[3],
                    "observed_status": args[4],
                    "event_payload": args[5],
                    "event_status": args[6],
                    "observed_by": args[7],
                    "operation_ref": args[8],
                }
            return row
        if "SELECT desired_state_ref, desired_status" in normalized:
            return self.active_desired
        if "INSERT INTO service_instance_projection" in normalized:
            is_declare_projection = len(args) == 6
            event_id = UUID(str(args[4] if is_declare_projection else args[6]))
            event_sequence = args[5] if is_declare_projection else args[7]
            observed_status = "pending" if is_declare_projection else args[4]
            endpoint_refs = "{}" if is_declare_projection else (args[5] or "{}")
            self.projection = {
                "service_ref": args[0],
                "runtime_target_ref": args[1],
                "active_desired_state_ref": args[2],
                "desired_status": args[3],
                "observed_status": observed_status,
                "endpoint_refs": endpoint_refs,
                "last_event_id": event_id,
                "last_event_sequence": event_sequence,
                "failure_reason": None
                if observed_status == "healthy"
                else args[8]
                if len(args) > 8
                else None,
                "projection_revision": 1,
            }
            return self.projection
        if "FROM service_instance_projection" in normalized:
            return self.projection
        raise AssertionError(f"unexpected fetchrow query: {query}")

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        return []

    def executed_sql(self) -> str:
        return "\n".join(query for query, _args in self.fetchrow_calls)


def test_runtime_target_substrate_kind_is_target_neutral() -> None:
    assert normalize_substrate_kind("browser") == "browser"
    assert normalize_substrate_kind("home_box") == "home_box"
    assert normalize_substrate_kind("saas_connector") == "saas_connector"

    with pytest.raises(ServiceLifecycleError):
        normalize_substrate_kind("windows_host")
    with pytest.raises(ServiceLifecycleError):
        normalize_substrate_kind("mac_mini")


def test_register_runtime_target_uses_registry_refs_not_local_paths() -> None:
    conn = _FakeConn()

    result = register_runtime_target(
        conn,
        RegisterRuntimeTargetCommand(
            runtime_target_ref="target.homebox.default",
            substrate_kind="home_box",
            workspace_ref="praxis",
            base_path_ref="workspace_base.praxis.default",
            host_ref="default",
            endpoint_contract={"api_url_ref": "service_endpoint.praxis.api"},
        ),
    )

    target = result["target"]
    assert target["runtime_target_ref"] == "target.homebox.default"
    assert target["base_path_ref"] == "workspace_base.praxis.default"
    assert "base_path" not in target
    assert target["endpoint_contract"] == {"api_url_ref": "service_endpoint.praxis.api"}


def test_declare_desired_state_records_command_event_and_projection() -> None:
    conn = _FakeConn()

    result = declare_service_desired_state(
        conn,
        DeclareServiceDesiredStateCommand(
            service_ref="praxis.workflow_app",
            runtime_target_ref="target.homebox.default",
            desired_status="running",
            desired_config={"app_url_ref": "service_endpoint.praxis.app"},
            declared_by="codex",
            declaration_reason="prove CQRS lifecycle foundation",
            idempotency_key="declare-app-homebox-running",
        ),
    )

    assert result["status"] == "declared"
    assert result["desired_state"]["supersedes_ref"] == "service_desired.previous"
    assert result["event"]["event_type"] == "desired_state_declared"
    assert result["projection"]["observed_status"] == "pending"
    assert conn.transaction_opened is True
    assert conn.transaction_closed is True
    sql = conn.executed_sql()
    assert "UPDATE service_desired_states" in sql
    assert "INSERT INTO service_instance_events" in sql
    assert "INSERT INTO service_instance_projection" in sql


def test_record_lifecycle_event_updates_projection_from_observed_evidence() -> None:
    conn = _FakeConn()

    result = record_service_lifecycle_event(
        conn,
        RecordServiceLifecycleEventCommand(
            service_ref="praxis.workflow_app",
            runtime_target_ref="target.homebox.default",
            event_type="health_check_passed",
            observed_by="health.probe",
            endpoint_refs={"app_url": "http://target.example:5173"},
        ),
    )

    assert result["status"] == "recorded"
    assert result["event"]["observed_status"] == "healthy"
    assert result["projection"]["observed_status"] == "healthy"
    assert result["projection"]["desired_status"] == "running"
