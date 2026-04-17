from __future__ import annotations
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.routing import APIRoute
import pytest

import surfaces.api.rest as rest
from surfaces.api.handlers import workflow_query_routes
from surfaces.api.handlers import workflow_run


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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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


def test_mount_capabilities_raises_when_catalog_load_fails(monkeypatch) -> None:
    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        isinstance(route, APIRoute) and route.path == "/api/operator/provider-onboarding"
        for route in rest.app.routes
    )


def test_circuits_has_no_static_route_owner() -> None:
    assert not any(
        isinstance(route, APIRoute) and route.path == "/api/circuits"
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
        isinstance(route, APIRoute) and route.path == "/operator_view"
        for route in rest.app.routes
    )


def test_mount_capabilities_json_encodes_datetime_results(monkeypatch) -> None:
    class ListObjectTypesQuery(BaseModel):
        pass

    target_app = FastAPI()
    monkeypatch.setattr(
        rest,
        "_ensure_shared_subsystems",
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
        lambda _app: SimpleNamespace(get_pg_conn=lambda: object()),
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
