from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.routing import APIRoute
import pytest

import surfaces.api.rest as rest
from surfaces.api.handlers import workflow_query
from surfaces.api.handlers import workflow_query_routes
from surfaces.api.handlers import workflow_run


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
    def __init__(self) -> None:
        self.receipts: dict[str, dict] = {}
        self.events: dict[str, dict] = {}
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0

    def fetchrow(self, query, *args):
        normalized = " ".join(str(query).split())
        if (
            "FROM authority_operation_receipts" in normalized
            and "WHERE receipt_id = $1::uuid" in normalized
        ):
            return self.receipts.get(str(args[0]))
        if "FROM authority_operation_receipts" in normalized:
            idempotency_key = args[1]
            for receipt in self.receipts.values():
                if receipt.get("idempotency_key") == idempotency_key:
                    return receipt
        return None

    def execute(self, query, *args):
        if "INSERT INTO authority_operation_receipts" in query:
            receipt_id = args[0]
            self.receipts[receipt_id] = {
                "receipt_id": receipt_id,
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
            return "INSERT 0 1"
        if "INSERT INTO authority_events" in query:
            event_id = args[0]
            self.events[event_id] = {"event_id": event_id}
            return "INSERT 0 1"
        if "UPDATE authority_operation_receipts" in query:
            event_ids = args[0]
            receipt_id = args[1]
            if receipt_id in self.receipts:
                self.receipts[receipt_id]["event_ids"] = event_ids
            return "UPDATE 1"
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


def _fake_shared_subsystems():
    return SimpleNamespace(get_pg_conn=lambda: _FakeAuthorityConn())


def _binding(
    *,
    operation_name: str,
    http_method: str,
    http_path: str,
    command_class,
    handler,
):
    return SimpleNamespace(
        operation_ref=operation_name,
        operation_name=operation_name,
        source_kind="operation_command",
        operation_kind="command",
        http_method=http_method,
        http_path=http_path,
        command_class=command_class,
        handler=handler,
        authority_ref="authority.test",
        projection_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        binding_revision="binding.test.20260416",
        decision_ref="decision.test.20260416",
        summary=operation_name,
    )


def test_mount_capabilities_uses_operation_catalog_when_available(monkeypatch) -> None:
    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.mutate",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/{subpath:path}",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=object,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    rest.mount_capabilities(target_app)

    mounted = [
        route
        for route in target_app.routes
        if isinstance(route, APIRoute) and route.path == "/api/workflows/{workflow_id}/build/{subpath:path}"
    ]
    assert len(mounted) == 1
    assert mounted[0].openapi_extra["x-praxis-binding-source"] == "operation_catalog"
    assert mounted[0].name == "workflow_build.mutate"
    assert target_app.state.capabilities_mounted is True
    assert target_app.state.capabilities_mount_degraded is False


def test_mount_capabilities_skips_invalid_bindings_without_losing_valid_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class ValidCommand(BaseModel):
        body: dict = {}

    definitions = [
        SimpleNamespace(
            operation_ref="bad-op",
            operation_name="bad_operation",
            http_method="POST",
            http_path="/api/bad_operation",
            input_model_ref="runtime.missing.BadCommand",
            handler_ref="runtime.missing.handle_bad",
        ),
        SimpleNamespace(
            operation_ref="good-op",
            operation_name="good_operation",
            http_method="POST",
            http_path="/api/good_operation",
            input_model_ref="tests.ValidCommand",
            handler_ref="tests.handle_good",
        ),
    ]

    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: definitions,
    )

    def _resolve(definition):
        if definition.operation_name == "bad_operation":
            raise rest.OperationBindingResolutionError("missing runtime module")
        return _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=ValidCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        )

    monkeypatch.setattr(rest, "resolve_http_operation_binding", _resolve)

    rest.mount_capabilities(target_app)

    mounted_paths = [
        route.path
        for route in target_app.routes
        if isinstance(route, APIRoute)
    ]
    assert "/api/good_operation" in mounted_paths
    assert "/api/bad_operation" not in mounted_paths
    assert target_app.state.capabilities_mounted is True
    assert target_app.state.capabilities_mount_degraded is True
    assert target_app.state.capability_mount_errors == [
        {
            "operation_ref": "bad-op",
            "operation_name": "bad_operation",
            "http_method": "POST",
            "http_path": "/api/bad_operation",
            "input_model_ref": "runtime.missing.BadCommand",
            "handler_ref": "runtime.missing.handle_bad",
            "error": "missing runtime module",
        }
    ]


def test_list_api_routes_mounts_operation_catalog_routes_before_discovery(monkeypatch) -> None:
    target_app = FastAPI()

    class GoodCommand(BaseModel):
        pass

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="good-op",
                operation_name="good_operation",
                http_method="POST",
                http_path="/api/good_operation",
                input_model_ref="tests.GoodCommand",
                handler_ref="tests.handle_good",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=GoodCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(tag="operations", visibility="all")

    assert target_app.state.capabilities_mounted is True
    assert payload["count"] == 1
    assert payload["routes"][0]["path"] == "/api/good_operation"
    assert payload["routes"][0]["name"] == "good_operation"
    assert payload["routes"][0]["visibility"] == "internal"


def test_list_api_routes_surfaces_compile_family_through_operation_catalog(monkeypatch) -> None:
    target_app = FastAPI()

    class CompileCommand(BaseModel):
        intent: str

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="compile.materialize",
                operation_name="compile_materialize",
                http_method="POST",
                http_path="/api/compile/materialize",
                input_model_ref="runtime.operations.commands.compile_materialize.CompileMaterializeCommand",
                handler_ref="runtime.operations.commands.compile_materialize.handle_compile_materialize",
            ),
            SimpleNamespace(
                operation_ref="compile.preview",
                operation_name="compile_preview",
                http_method="POST",
                http_path="/api/compile/preview",
                input_model_ref="runtime.operations.queries.compile_preview.CompilePreviewQuery",
                handler_ref="runtime.operations.queries.compile_preview.handle_compile_preview",
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=CompileCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/compile",
        tag="operations",
        visibility="all",
    )

    assert target_app.state.capabilities_mounted is True
    paths = {route["path"]: route for route in payload["routes"]}
    assert target_app.state.capabilities_mounted is True
    assert payload["count"] == 2
    assert paths["/api/compile/materialize"]["name"] == "compile_materialize"
    assert paths["/api/compile/materialize"]["tags"] == ["operations"]
    assert paths["/api/compile/preview"]["name"] == "compile_preview"
    assert paths["/api/compile/preview"]["tags"] == ["operations"]


def test_list_api_routes_surfaces_client_operating_model_snapshot_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class SnapshotCommand(BaseModel):
        operator_view: dict = {}

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="client-operating-model-operator-view-snapshot-store",
                operation_name="client_operating_model_operator_view_snapshot_store",
                http_method="POST",
                http_path="/api/operator/client-operating-model/snapshots",
                input_model_ref=(
                    "runtime.operations.commands.client_operating_model."
                    "StoreOperatorViewSnapshotCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.client_operating_model."
                    "handle_store_operator_view_snapshot"
                ),
            ),
            SimpleNamespace(
                operation_ref="client-operating-model-operator-view-snapshot-read",
                operation_name="client_operating_model_operator_view_snapshot_read",
                http_method="GET",
                http_path="/api/operator/client-operating-model/snapshots",
                input_model_ref=(
                    "runtime.operations.queries.client_operating_model."
                    "QueryClientOperatingModelSnapshotRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.client_operating_model."
                    "handle_client_operating_model_snapshot_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SnapshotCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/operator/client-operating-model",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/operator/client-operating-model/snapshots",
        "client_operating_model_operator_view_snapshot_store",
    ) in route_keys
    assert (
        "GET",
        "/api/operator/client-operating-model/snapshots",
        "client_operating_model_operator_view_snapshot_read",
    ) in route_keys


def test_list_api_routes_surfaces_client_system_discovery_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class DiscoveryCommand(BaseModel):
        tenant_ref: str | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="client-system-discovery-census-record",
                operation_name="client_system_discovery_census_record",
                http_method="POST",
                http_path="/api/operator/client-system-discovery/census",
                input_model_ref=(
                    "runtime.operations.commands.client_system_discovery."
                    "RecordClientSystemCensusCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.client_system_discovery."
                    "handle_client_system_discovery_census_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="client-system-discovery-census-read",
                operation_name="client_system_discovery_census_read",
                http_method="GET",
                http_path="/api/operator/client-system-discovery/census",
                input_model_ref=(
                    "runtime.operations.queries.client_system_discovery."
                    "QueryClientSystemDiscoveryCensusRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.client_system_discovery."
                    "handle_client_system_discovery_census_read"
                ),
            ),
            SimpleNamespace(
                operation_ref="client-system-discovery-gap-record",
                operation_name="client_system_discovery_gap_record",
                http_method="POST",
                http_path="/api/operator/client-system-discovery/gaps",
                input_model_ref=(
                    "runtime.operations.commands.client_system_discovery."
                    "RecordClientSystemDiscoveryGapCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.client_system_discovery."
                    "handle_client_system_discovery_gap_record"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=DiscoveryCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/operator/client-system-discovery",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/operator/client-system-discovery/census",
        "client_system_discovery_census_record",
    ) in route_keys
    assert (
        "GET",
        "/api/operator/client-system-discovery/census",
        "client_system_discovery_census_read",
    ) in route_keys
    assert (
        "POST",
        "/api/operator/client-system-discovery/gaps",
        "client_system_discovery_gap_record",
    ) in route_keys


def test_list_api_routes_surfaces_object_truth_ingestion_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class IngestionCommand(BaseModel):
        client_ref: str | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="object_truth.command.ingestion_sample_record",
                operation_name="object_truth_ingestion_sample_record",
                http_method="POST",
                http_path="/api/object-truth/ingestion/samples",
                input_model_ref=(
                    "runtime.operations.commands.object_truth_ingestion."
                    "RecordObjectTruthIngestionSampleCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.object_truth_ingestion."
                    "handle_object_truth_ingestion_sample_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="object_truth.query.ingestion_sample_read",
                operation_name="object_truth_ingestion_sample_read",
                http_method="GET",
                http_path="/api/object-truth/ingestion/samples",
                input_model_ref=(
                    "runtime.operations.queries.object_truth_ingestion."
                    "QueryObjectTruthIngestionSampleRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.object_truth_ingestion."
                    "handle_object_truth_ingestion_sample_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=IngestionCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/object-truth/ingestion",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/object-truth/ingestion/samples",
        "object_truth_ingestion_sample_record",
    ) in route_keys
    assert (
        "GET",
        "/api/object-truth/ingestion/samples",
        "object_truth_ingestion_sample_read",
    ) in route_keys


def test_list_api_routes_surfaces_object_truth_mdm_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class MdmCommand(BaseModel):
        client_ref: str | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="object_truth.command.mdm_resolution_record",
                operation_name="object_truth_mdm_resolution_record",
                http_method="POST",
                http_path="/api/object-truth/mdm/resolutions",
                input_model_ref=(
                    "runtime.operations.commands.object_truth_mdm."
                    "RecordObjectTruthMdmResolutionCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.object_truth_mdm."
                    "handle_object_truth_mdm_resolution_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="object_truth.query.mdm_resolution_read",
                operation_name="object_truth_mdm_resolution_read",
                http_method="GET",
                http_path="/api/object-truth/mdm/resolutions",
                input_model_ref=(
                    "runtime.operations.queries.object_truth_mdm."
                    "QueryObjectTruthMdmResolutionRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.object_truth_mdm."
                    "handle_object_truth_mdm_resolution_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=MdmCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/object-truth/mdm",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/object-truth/mdm/resolutions",
        "object_truth_mdm_resolution_record",
    ) in route_keys
    assert (
        "GET",
        "/api/object-truth/mdm/resolutions",
        "object_truth_mdm_resolution_read",
    ) in route_keys


def test_list_api_routes_surfaces_task_environment_contract_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class ContractCommand(BaseModel):
        contract: dict | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="task_environment.command.contract_record",
                operation_name="task_environment_contract_record",
                http_method="POST",
                http_path="/api/task-environment/contracts",
                input_model_ref=(
                    "runtime.operations.commands.task_environment_contracts."
                    "RecordTaskEnvironmentContractCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.task_environment_contracts."
                    "handle_task_environment_contract_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="task_environment.query.contract_read",
                operation_name="task_environment_contract_read",
                http_method="GET",
                http_path="/api/task-environment/contracts",
                input_model_ref=(
                    "runtime.operations.queries.task_environment_contracts."
                    "QueryTaskEnvironmentContractRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.task_environment_contracts."
                    "handle_task_environment_contract_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=ContractCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/task-environment",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/task-environment/contracts",
        "task_environment_contract_record",
    ) in route_keys
    assert (
        "GET",
        "/api/task-environment/contracts",
        "task_environment_contract_read",
    ) in route_keys


def test_list_api_routes_surfaces_integration_action_contract_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class ContractCommand(BaseModel):
        contracts: list[dict] | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="integration_action.command.contract_record",
                operation_name="integration_action_contract_record",
                http_method="POST",
                http_path="/api/integration-action/contracts",
                input_model_ref=(
                    "runtime.operations.commands.integration_action_contracts."
                    "RecordIntegrationActionContractCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.integration_action_contracts."
                    "handle_integration_action_contract_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="integration_action.query.contract_read",
                operation_name="integration_action_contract_read",
                http_method="GET",
                http_path="/api/integration-action/contracts",
                input_model_ref=(
                    "runtime.operations.queries.integration_action_contracts."
                    "QueryIntegrationActionContractRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.integration_action_contracts."
                    "handle_integration_action_contract_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=ContractCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/integration-action",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/integration-action/contracts",
        "integration_action_contract_record",
    ) in route_keys
    assert (
        "GET",
        "/api/integration-action/contracts",
        "integration_action_contract_read",
    ) in route_keys


def test_list_api_routes_surfaces_virtual_lab_state_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class VirtualLabCommand(BaseModel):
        environment_revision: dict | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="virtual_lab.command.state_record",
                operation_name="virtual_lab_state_record",
                http_method="POST",
                http_path="/api/virtual-lab/state",
                input_model_ref=(
                    "runtime.operations.commands.virtual_lab_state."
                    "RecordVirtualLabStateCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.virtual_lab_state."
                    "handle_virtual_lab_state_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="virtual_lab.query.state_read",
                operation_name="virtual_lab_state_read",
                http_method="GET",
                http_path="/api/virtual-lab/state",
                input_model_ref=(
                    "runtime.operations.queries.virtual_lab_state."
                    "QueryVirtualLabStateRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.virtual_lab_state."
                    "handle_virtual_lab_state_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=VirtualLabCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/virtual-lab",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/virtual-lab/state",
        "virtual_lab_state_record",
    ) in route_keys
    assert (
        "GET",
        "/api/virtual-lab/state",
        "virtual_lab_state_read",
    ) in route_keys


def test_list_api_routes_surfaces_virtual_lab_simulation_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class VirtualLabSimulationCommand(BaseModel):
        scenario: dict | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="virtual_lab.command.simulation_run",
                operation_name="virtual_lab_simulation_run",
                http_method="POST",
                http_path="/api/virtual-lab/simulations",
                input_model_ref=(
                    "runtime.operations.commands.virtual_lab_simulation."
                    "RunVirtualLabSimulationCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.virtual_lab_simulation."
                    "handle_virtual_lab_simulation_run"
                ),
            ),
            SimpleNamespace(
                operation_ref="virtual_lab.query.simulation_read",
                operation_name="virtual_lab_simulation_read",
                http_method="GET",
                http_path="/api/virtual-lab/simulations",
                input_model_ref=(
                    "runtime.operations.queries.virtual_lab_simulation."
                    "QueryVirtualLabSimulationRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.virtual_lab_simulation."
                    "handle_virtual_lab_simulation_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=VirtualLabSimulationCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/virtual-lab",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/virtual-lab/simulations",
        "virtual_lab_simulation_run",
    ) in route_keys
    assert (
        "GET",
        "/api/virtual-lab/simulations",
        "virtual_lab_simulation_read",
    ) in route_keys


def test_list_api_routes_surfaces_virtual_lab_sandbox_promotion_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class VirtualLabSandboxPromotionCommand(BaseModel):
        manifest: dict | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="virtual_lab.command.sandbox_promotion_record",
                operation_name="virtual_lab_sandbox_promotion_record",
                http_method="POST",
                http_path="/api/virtual-lab/sandbox-promotions",
                input_model_ref=(
                    "runtime.operations.commands.virtual_lab_sandbox_promotion."
                    "RecordVirtualLabSandboxPromotionCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.virtual_lab_sandbox_promotion."
                    "handle_virtual_lab_sandbox_promotion_record"
                ),
            ),
            SimpleNamespace(
                operation_ref="virtual_lab.query.sandbox_promotion_read",
                operation_name="virtual_lab_sandbox_promotion_read",
                http_method="GET",
                http_path="/api/virtual-lab/sandbox-promotions",
                input_model_ref=(
                    "runtime.operations.queries.virtual_lab_sandbox_promotion."
                    "QueryVirtualLabSandboxPromotionRead"
                ),
                handler_ref=(
                    "runtime.operations.queries.virtual_lab_sandbox_promotion."
                    "handle_virtual_lab_sandbox_promotion_read"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=VirtualLabSandboxPromotionCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/virtual-lab",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/virtual-lab/sandbox-promotions",
        "virtual_lab_sandbox_promotion_record",
    ) in route_keys
    assert (
        "GET",
        "/api/virtual-lab/sandbox-promotions",
        "virtual_lab_sandbox_promotion_read",
    ) in route_keys


def test_list_api_routes_surfaces_portable_cartridge_routes(monkeypatch) -> None:
    target_app = FastAPI()

    class PortableCartridgeCommand(BaseModel):
        manifest: dict | None = None

    monkeypatch.setattr(rest, "app", target_app)
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_ref="authority-portable-cartridge-record",
                operation_name="authority.portable_cartridge.record",
                http_method="POST",
                http_path="/api/authority/portable-cartridges",
                input_model_ref=(
                    "runtime.operations.commands.portable_cartridge."
                    "RecordPortableCartridgeCommand"
                ),
                handler_ref=(
                    "runtime.operations.commands.portable_cartridge."
                    "handle_record_portable_cartridge"
                ),
            ),
            SimpleNamespace(
                operation_ref="authority-portable-cartridge-read",
                operation_name="authority.portable_cartridge.read",
                http_method="GET",
                http_path="/api/authority/portable-cartridges",
                input_model_ref=(
                    "runtime.operations.queries.portable_cartridge."
                    "ReadPortableCartridgeQuery"
                ),
                handler_ref=(
                    "runtime.operations.queries.portable_cartridge."
                    "handle_read_portable_cartridge"
                ),
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=PortableCartridgeCommand,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    payload = rest.list_api_routes(
        path_prefix="/api/authority",
        tag="operations",
        visibility="all",
    )

    route_keys = {
        (route["methods"][0], route["path"], route["name"])
        for route in payload["routes"]
    }
    assert (
        "POST",
        "/api/authority/portable-cartridges",
        "authority.portable_cartridge.record",
    ) in route_keys
    assert (
        "GET",
        "/api/authority/portable-cartridges",
        "authority.portable_cartridge.read",
    ) in route_keys


def test_compile_family_routes_are_not_static_wrappers() -> None:
    source = Path(rest.__file__).read_text(encoding="utf-8")

    assert '@app.post("/api/compile/materialize")' not in source
    assert '@app.post("/api/compile/preview")' not in source


def test_workflow_query_routes_do_not_import_dead_trampoline_modules() -> None:
    source = Path(workflow_query_routes.__file__).read_text(encoding="utf-8")

    for retired_module in (
        "_query_catalog",
        "_query_dashboard",
        "_query_files",
        "_query_objects",
        "_query_workflows",
    ):
        assert retired_module not in source


def test_workflow_query_routes_bind_directly_to_authoritative_handlers() -> None:
    matched_catalog = [
        handler
        for matcher, handler in workflow_query_routes.QUERY_GET_ROUTES
        if matcher("/api/catalog")
    ]
    matched_workflows = [
        handler
        for matcher, handler in workflow_query_routes.QUERY_GET_ROUTES
        if matcher("/api/workflows")
    ]
    matched_files = [
        handler
        for matcher, handler in workflow_query_routes.QUERY_GET_ROUTES
        if matcher("/api/files")
    ]
    matched_object_types = [
        handler
        for matcher, handler in workflow_query_routes.QUERY_GET_ROUTES
        if matcher("/api/object-types")
    ]
    matched_object_type_fields = [
        handler
        for matcher, handler in workflow_query_routes.QUERY_GET_ROUTES
        if matcher("/api/object-types/schema-123/fields")
    ]

    assert workflow_query._handle_catalog_get in matched_catalog
    assert workflow_query._handle_workflows_get in matched_workflows
    assert workflow_query._handle_files_get in matched_files
    assert workflow_query._handle_object_types_get in matched_object_types
    assert workflow_query._handle_object_fields_get in matched_object_type_fields
    assert workflow_query._handle_object_types_post in [
        handler
        for matcher, handler in workflow_query_routes.QUERY_POST_ROUTES
        if matcher("/api/object-types")
    ]
    assert workflow_query._handle_object_fields_post in [
        handler
        for matcher, handler in workflow_query_routes.QUERY_POST_ROUTES
        if matcher("/api/object-types/schema-123/fields")
    ]
    assert workflow_query._handle_object_types_put in [
        handler
        for matcher, handler in workflow_query_routes.QUERY_PUT_ROUTES
        if matcher("/api/object-types/schema-123")
    ]
    assert workflow_query._handle_object_fields_delete in [
        handler
        for matcher, handler in workflow_query_routes.QUERY_DELETE_ROUTES
        if matcher("/api/object-types/schema-123/fields/title")
    ]
    assert workflow_query._handle_object_types_delete in [
        handler
        for matcher, handler in workflow_query_routes.QUERY_DELETE_ROUTES
        if matcher("/api/object-types/schema-123")
    ]


def test_mount_capabilities_raises_when_catalog_load_fails(monkeypatch) -> None:
    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: (_ for _ in ()).throw(
            RuntimeError("db unavailable")
        ),
    )

    try:
        rest.mount_capabilities(target_app)
    except RuntimeError as exc:
        assert str(exc) == "db unavailable"
    else:  # pragma: no cover - defensive
        raise AssertionError("mount_capabilities should fail when the operation catalog cannot load")


def test_mount_capabilities_sorts_specific_routes_ahead_of_catchalls(monkeypatch) -> None:
    class GenericBuildCommand(BaseModel):
        workflow_id: str
        subpath: str
        body: dict

    class SuggestNextCommand(BaseModel):
        workflow_id: str
        body: dict

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.mutate",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/{subpath:path}",
            ),
            SimpleNamespace(
                operation_name="workflow_build.suggest_next",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            ),
        ],
    )

    def _resolve(definition):
        if definition.operation_name == "workflow_build.mutate":
            return _binding(
                operation_name=definition.operation_name,
                http_method=definition.http_method,
                http_path=definition.http_path,
                command_class=GenericBuildCommand,
                handler=lambda *_args, **_kwargs: {"route": "generic"},
            )
        return _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SuggestNextCommand,
            handler=lambda *_args, **_kwargs: {"route": "suggest-next"},
        )

    monkeypatch.setattr(rest, "resolve_http_operation_binding", _resolve)

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/workflows/wf_123/build/suggest-next",
        json={"node_id": "step-1"},
    )

    assert response.status_code == 200
    assert response.json()["route"] == "suggest-next"


def test_mount_capabilities_promotes_routes_ahead_of_legacy_rest_of_path_catchalls(monkeypatch) -> None:
    class GenericBuildCommand(BaseModel):
        workflow_id: str
        subpath: str
        body: dict

    class SuggestNextCommand(BaseModel):
        workflow_id: str
        body: dict

    target_app = FastAPI()

    @target_app.get("/api/workflows/{rest_of_path:path}")
    async def _legacy_workflow_get(rest_of_path: str):
        return {"route": "legacy", "path": rest_of_path}

    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.mutate",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/{subpath:path}",
            ),
            SimpleNamespace(
                operation_name="workflow_build.suggest_next",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            ),
        ],
    )

    def _resolve(definition):
        if definition.operation_name == "workflow_build.mutate":
            return _binding(
                operation_name=definition.operation_name,
                http_method=definition.http_method,
                http_path=definition.http_path,
                command_class=GenericBuildCommand,
                handler=lambda *_args, **_kwargs: {"route": "generic"},
            )
        return _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SuggestNextCommand,
            handler=lambda *_args, **_kwargs: {"route": "suggest-next"},
        )

    monkeypatch.setattr(rest, "resolve_http_operation_binding", _resolve)

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/workflows/wf_123/build/suggest-next",
        json={"node_id": "step-1"},
    )

    assert response.status_code == 200
    assert response.json()["route"] == "suggest-next"


def test_mount_capabilities_raises_on_duplicate_operation_route_bindings(monkeypatch) -> None:
    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.mutate",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            ),
            SimpleNamespace(
                operation_name="workflow_build.suggest_next",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            ),
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=object,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    with pytest.raises(RuntimeError, match="duplicate operation-catalog route binding"):
        rest.mount_capabilities(target_app)


def test_mount_capabilities_raises_when_existing_route_owns_binding(monkeypatch) -> None:
    target_app = FastAPI()

    @target_app.post("/api/workflows/{workflow_id}/build/suggest-next")
    async def _legacy_build_route(workflow_id: str):
        return {"route": "legacy", "workflow_id": workflow_id}

    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.suggest_next",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=object,
            handler=lambda *_args, **_kwargs: {"ok": True},
        ),
    )

    with pytest.raises(RuntimeError, match="capability route conflict"):
        rest.mount_capabilities(target_app)


def test_mount_capabilities_allows_legacy_compile_alias_beside_canonical_route(
    monkeypatch,
) -> None:
    class CompileCommand(BaseModel):
        intent: str

    target_app = FastAPI()

    @target_app.post("/api/compile_materialize")
    async def _legacy_compile_alias():
        return {"route": "legacy-alias"}

    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="compile_materialize",
                http_method="POST",
                http_path="/api/compile/materialize",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=CompileCommand,
            handler=lambda command, _subs: {"intent": command.intent},
        ),
    )

    rest.mount_capabilities(target_app)

    route_paths = {
        (route.path, tuple(sorted(item for item in (route.methods or set()) if item != "HEAD")))
        for route in target_app.routes
        if isinstance(route, APIRoute)
    }
    assert ("/api/compile_materialize", ("POST",)) in route_paths
    assert ("/api/compile/materialize", ("POST",)) in route_paths


def test_mount_capabilities_flattens_post_body_for_models_without_body_field(monkeypatch) -> None:
    class RoadmapWriteCommand(BaseModel):
        title: str
        intent_brief: str
        action: str = "preview"

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="operator.roadmap_write",
                http_method="POST",
                http_path="/api/operator/roadmap-write",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=RoadmapWriteCommand,
            handler=lambda command, _subs: {
                "title": command.title,
                "intent_brief": command.intent_brief,
                "action": command.action,
            },
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/operator/roadmap-write",
        json={
            "title": "One gateway",
            "intent_brief": "No nested body wrapper",
        },
    )

    assert response.status_code == 200
    assert response.json()["title"] == "One gateway"
    assert response.json()["intent_brief"] == "No nested body wrapper"
    assert response.json()["action"] == "preview"
    assert response.json()["operation_receipt"]["operation_name"] == "operator.roadmap_write"


def test_provider_onboarding_has_no_static_route_owner() -> None:
    assert not any(
        isinstance(route, APIRoute)
        and route.path == "/api/operator/provider-onboarding"
        and (route.openapi_extra or {}).get("x-praxis-binding-source") != "operation_catalog"
        for route in rest.app.routes
    )


def test_circuits_has_no_static_route_owner() -> None:
    assert not any(
        isinstance(route, APIRoute)
        and route.path == "/api/circuits"
        and (route.openapi_extra or {}).get("x-praxis-binding-source") != "operation_catalog"
        for route in rest.app.routes
    )


def test_status_has_no_static_route_owner() -> None:
    assert not any(
        matcher("/api/status") for matcher, _handler in workflow_query_routes.QUERY_GET_ROUTES
    )
    assert not any(
        matcher("/api/status") for matcher, _handler in workflow_run.RUN_GET_ROUTES
    )


def test_operator_view_has_no_static_route_owner() -> None:
    assert "/operator_view" not in workflow_query_routes.QUERY_ROUTES
    assert not any(
        isinstance(route, APIRoute)
        and route.path == "/operator_view"
        and (route.openapi_extra or {}).get("x-praxis-binding-source") != "operation_catalog"
        for route in rest.app.routes
    )


def test_mount_capabilities_json_encodes_datetime_results(monkeypatch) -> None:
    class ListObjectTypesQuery(BaseModel):
        pass

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="object_schema.type_list",
                http_method="GET",
                http_path="/api/object-types",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=ListObjectTypesQuery,
            handler=lambda *_args, **_kwargs: {
                "types": [
                    {
                        "type_id": "schema-smoke",
                        "created_at": datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
                    }
                ]
            },
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.get("/api/object-types")

    assert response.status_code == 200
    payload = response.json()
    assert payload["operation_receipt"]["operation_name"] == "object_schema.type_list"
    assert payload["types"][0]["created_at"] in {
        "2026-04-16T12:00:00Z",
        "2026-04-16T12:00:00+00:00",
    }


def test_mount_capabilities_awaits_async_handlers(monkeypatch) -> None:
    class QuerySemanticAssertions(BaseModel):
        pass

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="semantic_assertions.list",
                http_method="GET",
                http_path="/api/semantic/assertions",
            )
        ],
    )

    async def _handler(*_args, **_kwargs):
        return {"boundary": "async-handler", "semantic_assertions": []}

    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=QuerySemanticAssertions,
            handler=_handler,
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.get("/api/semantic/assertions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["operation_receipt"]["operation_name"] == "semantic_assertions.list"
    assert payload["boundary"] == "async-handler"


def test_mount_capabilities_accepts_raw_provider_onboarding_body(monkeypatch) -> None:
    from runtime.operations.commands.provider_onboarding import ProviderOnboardingCommand

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="operator.provider_onboarding",
                http_method="POST",
                http_path="/api/operator/provider-onboarding",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=ProviderOnboardingCommand,
            handler=lambda command, _subs: {
                "provider_slug": command.provider_slug,
                "selected_transport": command.model_extra["selected_transport"],
                "dry_run": command.dry_run,
            },
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/operator/provider-onboarding",
        json={
            "provider_slug": "openai",
            "selected_transport": "api",
            "dry_run": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["provider_slug"] == "openai"
    assert response.json()["selected_transport"] == "api"
    assert response.json()["dry_run"] is True
    assert response.json()["operation_receipt"]["operation_name"] == "operator.provider_onboarding"


def test_mount_capabilities_supports_circuit_query_params(monkeypatch) -> None:
    from runtime.operations.queries.circuits import QueryCircuitStates

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="operator.circuit_states",
                http_method="GET",
                http_path="/api/circuits",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=QueryCircuitStates,
            handler=lambda command, _subs: {
                "circuits": {"selected": command.provider_slug},
            },
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.get("/api/circuits", params={"provider_slug": "openai"})

    assert response.status_code == 200
    assert response.json()["circuits"]["selected"] == "openai"
    assert response.json()["operation_receipt"]["operation_name"] == "operator.circuit_states"


def test_mount_capabilities_supports_circuit_override_body(monkeypatch) -> None:
    from runtime.operations.commands.operator_control import CircuitOverrideCommand

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: _fake_shared_subsystems(),
    )
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="operator.circuit_override",
                http_method="POST",
                http_path="/api/circuits",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: _binding(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=CircuitOverrideCommand,
            handler=lambda command, _subs: {
                "provider_slug": command.provider_slug,
                "override_state": command.override_state,
            },
        ),
    )

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/circuits",
        json={
            "provider_slug": "openai",
            "override_state": "open",
        },
    )

    assert response.status_code == 200
    assert response.json()["provider_slug"] == "openai"
    assert response.json()["override_state"] == "open"
    assert response.json()["operation_receipt"]["operation_name"] == "operator.circuit_override"
