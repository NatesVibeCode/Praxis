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


class _FakeAuthorityConn:
    def __init__(self) -> None:
        self.receipts: dict[str, dict] = {}
        self.events: dict[str, dict] = {}

    def fetchrow(self, query, *args):
        if "FROM authority_operation_receipts" in query:
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
                "operation_name": args[1],
                "operation_kind": args[2],
                "authority_ref": args[3],
                "authority_domain_ref": args[4],
                "storage_target_ref": args[5],
                "caller_ref": args[6],
                "idempotency_key": args[7],
                "idempotency_policy": args[8],
                "status": args[9],
                "input_hash": args[10],
                "output_hash": args[11],
                "event_ids": args[12],
                "error": args[13],
                "handler_module": args[14],
                "handler_qualname": args[15],
                "latency_ms": args[16],
                "result_payload": args[17],
                "metadata": args[18],
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
