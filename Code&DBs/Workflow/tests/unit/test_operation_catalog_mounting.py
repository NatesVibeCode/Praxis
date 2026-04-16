from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.routing import APIRoute

import surfaces.api.rest as rest


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
        lambda definition: SimpleNamespace(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=object,
            handler=lambda *_args, **_kwargs: {"ok": True},
            summary=definition.operation_name,
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


def test_mount_capabilities_falls_back_to_registry_when_catalog_load_fails(monkeypatch) -> None:
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

    rest.mount_capabilities(target_app)

    mounted = [
        route
        for route in target_app.routes
        if isinstance(route, APIRoute) and route.path == "/api/workflows/{workflow_id}/build/{subpath:path}"
    ]
    assert len(mounted) == 1
    assert mounted[0].openapi_extra["x-praxis-binding-source"] == "registry_fallback"


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
            return SimpleNamespace(
                operation_name=definition.operation_name,
                http_method=definition.http_method,
                http_path=definition.http_path,
                command_class=GenericBuildCommand,
                handler=lambda *_args, **_kwargs: {"route": "generic"},
                summary=definition.operation_name,
            )
        return SimpleNamespace(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SuggestNextCommand,
            handler=lambda *_args, **_kwargs: {"route": "suggest-next"},
            summary=definition.operation_name,
        )

    monkeypatch.setattr(rest, "resolve_http_operation_binding", _resolve)

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/workflows/wf_123/build/suggest-next",
        json={"node_id": "step-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"route": "suggest-next"}


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
            return SimpleNamespace(
                operation_name=definition.operation_name,
                http_method=definition.http_method,
                http_path=definition.http_path,
                command_class=GenericBuildCommand,
                handler=lambda *_args, **_kwargs: {"route": "generic"},
                summary=definition.operation_name,
            )
        return SimpleNamespace(
            operation_name=definition.operation_name,
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SuggestNextCommand,
            handler=lambda *_args, **_kwargs: {"route": "suggest-next"},
            summary=definition.operation_name,
        )

    monkeypatch.setattr(rest, "resolve_http_operation_binding", _resolve)

    rest.mount_capabilities(target_app)

    client = TestClient(target_app)
    response = client.post(
        "/api/workflows/wf_123/build/suggest-next",
        json={"node_id": "step-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"route": "suggest-next"}
