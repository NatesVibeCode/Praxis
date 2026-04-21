"""REST API surface for the Praxis Engine workflow platform.

Exposes all CLI-backed functions as HTTP endpoints so UIs and external
clients can access the same functionality without shelling out.

Dependencies are declared in ``requirements.runtime.txt`` and enforced by
``surfaces.api.server`` before the ASGI app boots.

Launch:
    python -m surfaces.api.server --host 0.0.0.0 --port 8420
    workflow api

The supported product front door remains ``./scripts/praxis launch``.

The module-level ``app`` object is the canonical ASGI app.
"""

from __future__ import annotations
from collections import Counter
from contextlib import asynccontextmanager
import dataclasses
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Request, Security
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

from registry.provider_execution_registry import (
    default_provider_slug,
)
from contracts.domain import validate_workflow_request
from runtime.native_authority import default_native_authority_refs
from runtime.operation_catalog_bindings import resolve_http_operation_binding
from runtime.operation_catalog_gateway import aexecute_operation_binding
from runtime.atlas_graph import build_atlas_payload
from runtime.workflow._status import summarize_run_health
from runtime.workflow_graph_compiler import compile_graph_workflow_request, spec_uses_graph_runtime
from surfaces.api.catalog_authority import build_catalog_payload
from surfaces.api.operation_catalog_authority import build_operation_catalog_payload
from surfaces.api import agent_sessions as agent_sessions_app
from surfaces.mcp.catalog import McpToolDefinition, canonical_tool_name, get_tool_catalog
from surfaces.mcp.invocation import ToolInvocationError, invoke_tool
from storage.postgres import PostgresWorkflowSurfaceUsageRepository
from .handlers._subsystems import _Subsystems
from .handlers._surface_usage import record_api_route_usage as _record_api_route_usage
from .handlers import (
    handle_delete_request,
    handle_get_request,
    handle_post_request,
    handle_put_request,
    path_is_known,
)
from .handlers._shared import REPO_ROOT
from .handlers import workflow_launcher as launcher_handlers
from .handlers.workflow_run import _submit_workflow_via_service_bus

__all__ = ["app"]

logger = logging.getLogger(__name__)

_PUBLIC_API_VERSION = "v1"
_PUBLIC_ROUTE_PREFIX = "/v1/"
_PUBLIC_AUTH_TOKEN_ENV = "PRAXIS_API_TOKEN"
_REQUEST_ID_HEADER = "X-Request-Id"
_CLIENT_VERSION_HEADERS = (
    "X-Client-Version",
    "X-Praxis-Client-Version",
    "User-Agent",
)


def _run_authority_unavailable(
    *,
    reason_code: str,
    message: str,
    run_id: str,
    exc: Exception | None = None,
) -> HTTPException:
    detail: dict[str, Any] = {
        "reason_code": reason_code,
        "message": message,
        "run_id": run_id,
    }
    if exc is not None:
        detail["error_type"] = type(exc).__name__
        detail["error_message"] = str(exc)
    return HTTPException(status_code=503, detail=detail)


_IDEMPOTENCY_HEADER = "Idempotency-Key"
_HTTP_BEARER = HTTPBearer(auto_error=False)


def _is_public_request_path(path: str) -> bool:
    return path == "/v1" or path.startswith(_PUBLIC_ROUTE_PREFIX)


def _route_visibility(route: APIRoute) -> str:
    openapi_extra = route.openapi_extra or {}
    visibility = str(openapi_extra.get("x-praxis-visibility") or "").strip().lower()
    if visibility in {"public", "internal"}:
        return visibility
    return "public" if _is_public_request_path(route.path) else "internal"


def _route_operation_id(route: APIRoute) -> str:
    operation_id = getattr(route, "operation_id", None)
    if isinstance(operation_id, str) and operation_id.strip():
        return operation_id
    return _unique_operation_id(route)


def _public_api_token() -> str | None:
    value = str(os.getenv(_PUBLIC_AUTH_TOKEN_ENV, "")).strip()
    return value or None


def _configured_cors_origins() -> list[str]:
    raw_value = str(os.getenv("PRAXIS_API_ALLOWED_ORIGINS", "*")).strip()
    origins = [part.strip() for part in raw_value.split(",") if part.strip()]
    return origins or ["*"]


def _request_id_from_request(request: Request) -> str:
    header_value = str(request.headers.get(_REQUEST_ID_HEADER, "")).strip()
    if header_value:
        return header_value
    return f"req_{uuid4().hex[:16]}"


def _client_version_from_request(request: Request) -> str | None:
    for header_name in _CLIENT_VERSION_HEADERS:
        value = str(request.headers.get(header_name, "")).strip()
        if value:
            return value
    return None


def _problem_type(error_code: str | None) -> str:
    normalized = str(error_code or "internal_error").strip().lower().replace(" ", "_")
    return f"urn:praxis:problem:{normalized}"


def _problem_response(
    request: Request,
    *,
    status_code: int,
    title: str,
    detail: str,
    error_code: str | None = None,
    invalid_params: list[dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    normalized_error_code = error_code or "request_failed"
    payload: dict[str, Any] = {
        "type": _problem_type(normalized_error_code),
        "title": title,
        "status": status_code,
        "detail": detail,
        "error_code": normalized_error_code,
        "request_id": getattr(request.state, "request_id", None),
    }
    if invalid_params:
        payload["invalid_params"] = invalid_params
    if extra:
        payload.update(extra)
    response = JSONResponse(
        status_code=status_code,
        content=payload,
        media_type="application/problem+json",
        headers=headers,
    )
    return response


async def _require_public_api_access(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(_HTTP_BEARER),
) -> str | None:
    expected_token = _public_api_token()
    if expected_token is None:
        return None
    if credentials is None or str(credentials.scheme).lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail={
                "message": "Bearer token required for the public API",
                "error_code": "public_api_auth_required",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )
    if credentials.credentials != expected_token:
        raise HTTPException(
            status_code=403,
            detail={
                "message": "Bearer token rejected for the public API",
                "error_code": "public_api_auth_rejected",
            },
        )
    request.state.authenticated_principal = "public_api_token"
    return "public_api_token"


def _slugify_identifier(value: str, *, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", ".", value.strip().lower())
    normalized = normalized.strip(".")
    return normalized or fallback


def _unique_operation_id(route: APIRoute) -> str:
    methods = "_".join(
        method.lower()
        for method in sorted(route.methods or set())
        if method != "HEAD"
    )
    path = route.path_format.strip("/") or "root"
    normalized_path = (
        path.replace("/", "_")
        .replace("{", "")
        .replace("}", "")
        .replace(":", "_")
    )
    return f"{route.name}_{normalized_path}_{methods}"


def _api_route_record(route: APIRoute) -> dict[str, Any]:
    methods = sorted(method for method in (route.methods or set()) if method != "HEAD")
    description = (route.description or "").strip()
    summary = (route.summary or "").strip()
    visibility = _route_visibility(route)
    return {
        "path": route.path,
        "name": route.name,
        "methods": methods,
        "summary": summary,
        "description": description,
        "tags": list(route.tags or ()),
        "include_in_schema": bool(route.include_in_schema),
        "operation_id": _route_operation_id(route),
        "visibility": visibility,
    }


def _route_facet_rows(items: Counter[str], *, field_name: str) -> list[dict[str, Any]]:
    return [
        {field_name: value, "count": count}
        for value, count in sorted(items.items(), key=lambda item: (-item[1], item[0]))
        if value
    ]


def _route_summary(routes: list[dict[str, Any]]) -> dict[str, Any]:
    method_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    for route in routes:
        for method in route.get("methods", []):
            method_text = str(method).strip().upper()
            if method_text:
                method_counts[method_text] += 1
        for tag in route.get("tags", []):
            tag_text = str(tag).strip()
            if tag_text:
                tag_counts[tag_text] += 1

    methods = _route_facet_rows(method_counts, field_name="method")
    tags = _route_facet_rows(tag_counts, field_name="tag")
    suggested_filters: dict[str, str] = {}
    if tags:
        suggested_filters["tag"] = str(tags[0]["tag"])
    if methods:
        suggested_filters["method"] = str(methods[0]["method"])

    return {
        "route_count": len(routes),
        "methods": methods,
        "tags": tags,
        "suggested_filters": suggested_filters,
    }


def _normalize_route_filter(value: str | None) -> str:
    return str(value or "").strip().lower()


def _route_matches(
    route: dict[str, Any],
    *,
    search: str | None = None,
    method: str | None = None,
    tag: str | None = None,
    path_prefix: str | None = None,
) -> bool:
    search_text = _normalize_route_filter(search)
    method_text = _normalize_route_filter(method)
    tag_text = _normalize_route_filter(tag)
    path_prefix_text = str(path_prefix or "").strip()

    if path_prefix_text and not str(route.get("path") or "").startswith(path_prefix_text):
        return False

    methods = {str(item).strip().lower() for item in route.get("methods", [])}
    if method_text and method_text not in methods:
        return False

    tags = {str(item).strip().lower() for item in route.get("tags", [])}
    if tag_text and tag_text not in tags:
        return False

    if search_text:
        searchable = " ".join(
            str(part)
            for part in (
                route.get("path"),
                route.get("name"),
                route.get("summary"),
                route.get("description"),
                " ".join(str(item) for item in route.get("tags", [])),
            )
            if part
        ).lower()
        if search_text not in searchable:
            return False

    return True


def list_api_routes(
    *,
    search: str | None = None,
    method: str | None = None,
    tag: str | None = None,
    path_prefix: str | None = None,
    visibility: str = "public",
) -> dict[str, Any]:
    """Return the live FastAPI route catalog for discovery surfaces."""

    normalized_visibility = str(visibility or "public").strip().lower() or "public"
    if normalized_visibility not in {"public", "internal", "all"}:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Unsupported route visibility filter: {visibility}",
                "error_code": "invalid_route_visibility",
                "supported": ["public", "internal", "all"],
            },
        )
    routes = [
        _api_route_record(route)
        for route in app.routes
        if isinstance(route, APIRoute)
    ]
    routes.sort(key=lambda item: (str(item["path"]), ",".join(item["methods"])))
    filtered_routes = [
        route
        for route in routes
        if (
            normalized_visibility == "all"
            or route.get("visibility") == normalized_visibility
        )
        if _route_matches(
            route,
            search=search,
            method=method,
            tag=tag,
            path_prefix=path_prefix,
        )
    ]
    filters = {
        key: value
        for key, value in {
            "search": str(search or "").strip() or None,
            "method": str(method or "").strip().upper() or None,
            "tag": str(tag or "").strip() or None,
            "path_prefix": str(path_prefix or "").strip() or None,
            "visibility": normalized_visibility if normalized_visibility != "public" else None,
        }.items()
        if value is not None
    }
    return {
        "count": len(filtered_routes),
        "docs_url": app.docs_url,
        "openapi_url": app.openapi_url,
        "redoc_url": app.redoc_url,
        "filters": filters,
        "summary": _route_summary(filtered_routes),
        "routes": filtered_routes,
    }


def _ensure_shared_subsystems(target_app: FastAPI) -> _Subsystems | None:
    """Instantiate the shared subsystem container once for API request handling."""
    subsystems = getattr(target_app.state, "shared_subsystems", None)
    if subsystems is not None:
        return subsystems
    try:
        subsystems = _Subsystems()
    except Exception:
        logger.exception("failed to initialize shared API subsystems")
        return None
    target_app.state.shared_subsystems = subsystems
    return subsystems


def _should_boot_shared_subsystems() -> bool:
    return "PYTEST_CURRENT_TEST" not in os.environ


def _boot_shared_subsystems(target_app: FastAPI) -> _Subsystems | None:
    """Run explicit shared subsystem boot during real API startup only."""
    subsystems = _ensure_shared_subsystems(target_app)
    if subsystems is None or not _should_boot_shared_subsystems():
        return subsystems
    try:
        subsystems.boot()
    except Exception:
        logger.exception("shared subsystem boot failed; API continues in degraded mode")
    return subsystems


def _json_response_payload(value: Any) -> Any:
    """Preserve repo-local UTC timestamp shape after generic JSON encoding."""
    if isinstance(value, datetime):
        text = value.isoformat()
        return text[:-6] + "Z" if text.endswith("+00:00") else text
    if isinstance(value, str) and value.endswith("+00:00") and "T" in value:
        try:
            datetime.fromisoformat(value)
        except ValueError:
            return value
        return value[:-6] + "Z"
    if isinstance(value, dict):
        return {key: _json_response_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_response_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_json_response_payload(item) for item in value]
    return value


@asynccontextmanager
async def _app_lifespan(target_app: FastAPI):
    _boot_shared_subsystems(target_app)
    try:
        mount_capabilities(target_app)
    except Exception:
        logger.exception("capability mount failed during startup; API continues in degraded mode")
    yield


# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Praxis Engine API",
    description="HTTP surface for Praxis Engine runtime functions.",
    version="1.0.0",
    lifespan=_app_lifespan,
    generate_unique_id_function=_unique_operation_id,
)

from .handlers.webhook_ingest import webhook_ingest_router
app.include_router(webhook_ingest_router)

from runtime.operation_catalog import list_resolved_operation_definitions


def _create_capability_endpoint(
    target_app: FastAPI,
    binding: Any,
):
    async def endpoint(request: Request):
        subsystems = _ensure_shared_subsystems(target_app)
        if subsystems is None:
            return JSONResponse({"error": "shared subsystems unavailable"}, status_code=503)

        body = {}
        if request.method in {"POST", "PUT", "PATCH"}:
            body_bytes = await request.body()
            try:
                body = json.loads(body_bytes) if body_bytes else {}
            except json.JSONDecodeError as e:
                return JSONResponse({"error": f"Invalid JSON: {e}"}, status_code=400)

        path_params = request.path_params
        query_params = dict(request.query_params)
        command_data = {**query_params, **path_params}
        if "body" in binding.command_class.model_fields:
            command_data["body"] = body
        else:
            command_data.update(body)

        try:
            result = await aexecute_operation_binding(
                binding,
                payload=command_data,
                subsystems=subsystems,
            )
            return JSONResponse(_json_response_payload(jsonable_encoder(result)))
        except Exception as e:
            if isinstance(e, (RequestValidationError, ValidationError)):
                return JSONResponse({"error": f"Validation Error: {e}"}, status_code=400)
            logger.error("Command failure: %s", e, exc_info=True)
            return JSONResponse({"error": str(e)}, status_code=500)

    return endpoint


def _capability_route_exists(target_app: FastAPI, *, path: str, method: str) -> bool:
    normalized_method = method.strip().upper()
    for route in target_app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path != path:
            continue
        methods = {item.upper() for item in (route.methods or set())}
        if normalized_method in methods:
            return True
    return False


def _assert_unique_capability_route_specs(route_specs: list[tuple[Any, str]]) -> None:
    seen: dict[tuple[str, str], str] = {}
    for binding, _mount_source in route_specs:
        signature = (str(binding.http_method).upper(), binding.http_path)
        existing_owner = seen.get(signature)
        if existing_owner is not None:
            method, path = signature
            raise RuntimeError(
                "duplicate operation-catalog route binding for "
                f"{method} {path}: {existing_owner} and {binding.operation_name}"
            )
        seen[signature] = binding.operation_name


def _assert_capability_routes_do_not_conflict_with_app(
    target_app: FastAPI,
    route_specs: list[tuple[Any, str]],
) -> None:
    for binding, _mount_source in route_specs:
        route_path = binding.http_path
        route_method = str(binding.http_method).upper()
        if not _capability_route_exists(target_app, path=route_path, method=route_method):
            continue
        raise RuntimeError(
            "capability route conflict for "
            f"{route_method} {route_path}; an existing route already owns this binding"
        )


def _capability_routes_from_operation_catalog(target_app: FastAPI) -> list[tuple[Any, str]]:
    subsystems = _ensure_shared_subsystems(target_app)
    if subsystems is None:
        raise RuntimeError("shared subsystems unavailable")
    resolved = list_resolved_operation_definitions(
        subsystems.get_pg_conn(),
        include_disabled=False,
        limit=500,
    )
    mounted: list[tuple[Any, str]] = []
    for definition in resolved:
        mounted.append((resolve_http_operation_binding(definition), "operation_catalog"))
    return mounted


def _capability_path_sort_key(route_path: str) -> tuple[int, int, int, str]:
    segments = [segment for segment in route_path.split("/") if segment]
    catch_all_count = sum(1 for segment in segments if ":path" in segment)
    parameter_count = sum(1 for segment in segments if segment.startswith("{") and segment.endswith("}"))
    literal_count = len(segments) - parameter_count
    return (
        catch_all_count,
        parameter_count,
        -literal_count,
        route_path,
    )


def _sort_capability_route_specs(route_specs: list[tuple[Any, str]]) -> list[tuple[Any, str]]:
    return sorted(
        route_specs,
        key=lambda item: (
            _capability_path_sort_key(item[0].http_path),
            str(item[0].http_method).upper(),
            item[0].operation_name,
        ),
    )


def _preferred_capability_route_insert_index(target_app: FastAPI) -> int:
    for index, route in enumerate(target_app.router.routes):
        if isinstance(route, APIRoute) and "{rest_of_path:path}" in route.path:
            return index
    return len(target_app.router.routes)


def _promote_last_capability_route(target_app: FastAPI) -> None:
    routes = target_app.router.routes
    if not routes:
        return
    last_index = len(routes) - 1
    insert_index = _preferred_capability_route_insert_index(target_app)
    if insert_index >= last_index:
        return
    routes.insert(insert_index, routes.pop(last_index))


def mount_capabilities(target_app: FastAPI) -> None:
    """Mount operation-catalog endpoints from the DB-backed operation catalog."""
    if getattr(target_app.state, "capabilities_mounted", False):
        return

    route_specs = _capability_routes_from_operation_catalog(target_app)
    _assert_unique_capability_route_specs(route_specs)
    _assert_capability_routes_do_not_conflict_with_app(target_app, route_specs)

    for binding, mount_source in _sort_capability_route_specs(route_specs):
        route_path = binding.http_path
        route_method = str(binding.http_method).upper()
        route_name = binding.operation_name
        endpoint_func = _create_capability_endpoint(target_app, binding)
        target_app.add_api_route(
            route_path,
            endpoint_func,
            methods=[route_method],
            summary=binding.summary,
            tags=["operations"],
            name=route_name,
            openapi_extra={
                "x-praxis-binding-source": mount_source,
                "x-praxis-operation-name": route_name,
            },
        )
        _promote_last_capability_route(target_app)

    target_app.state.capabilities_mounted = True
    _apply_route_visibility_policy()

app.add_middleware(
    CORSMiddleware,
    allow_origins=_configured_cors_origins(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _praxis_transport_middleware(request: Request, call_next):
    request.state.request_id = _request_id_from_request(request)
    request.state.client_version = _client_version_from_request(request)
    request.state.idempotency_key = str(request.headers.get(_IDEMPOTENCY_HEADER, "")).strip() or None

    response = await call_next(request)
    response.headers.setdefault(_REQUEST_ID_HEADER, request.state.request_id)
    if _is_public_request_path(request.url.path):
        response.headers.setdefault("X-Praxis-Api-Version", _PUBLIC_API_VERSION)
    return response


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException) -> Response:
    if _is_public_request_path(request.url.path):
        detail = exc.detail
        error_code = None
        extra: dict[str, Any] | None = None
        if isinstance(detail, dict):
            error_code = str(detail.get("error_code") or "").strip() or None
            extra = {
                key: value
                for key, value in detail.items()
                if key not in {"message", "detail", "error_code"}
            } or None
            detail_text = str(detail.get("message") or detail.get("detail") or exc.status_code)
        else:
            detail_text = str(detail)
        return _problem_response(
            request,
            status_code=exc.status_code,
            title="Request failed",
            detail=detail_text,
            error_code=error_code,
            extra=extra,
            headers=exc.headers,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)


@app.exception_handler(RequestValidationError)
async def _request_validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> Response:
    if _is_public_request_path(request.url.path):
        invalid_params = [
            {
                "name": ".".join(str(part) for part in error.get("loc", [])),
                "reason": error.get("msg"),
                "type": error.get("type"),
            }
            for error in exc.errors()
        ]
        return _problem_response(
            request,
            status_code=422,
            title="Validation failed",
            detail="The request body or parameters did not match the public API contract.",
            error_code="validation_error",
            invalid_params=invalid_params,
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> Response:
    logger.exception("unhandled API exception for %s", request.url.path)
    if _is_public_request_path(request.url.path):
        return _problem_response(
            request,
            status_code=500,
            title="Internal server error",
            detail="The API could not complete the request safely.",
            error_code="internal_error",
        )
    return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})

# Serve the launcher app assets from the built SPA bundle.
_APP_DIST_DIR = Path(__file__).resolve().parent.parent / "app" / "dist"
_APP_ASSETS_DIR = _APP_DIST_DIR / "assets"
app.mount(
    "/app/assets",
    StaticFiles(directory=str(_APP_ASSETS_DIR), check_dir=False),
    name="launcher-app-assets",
)
app.mount("/api/agent-sessions", agent_sessions_app.app)

def _default_workspace_ref() -> str:
    try:
        return default_native_authority_refs()[0]
    except Exception:
        return "native"


def _default_runtime_profile_ref() -> str:
    try:
        return default_native_authority_refs()[1]
    except Exception:
        return "native"


def _default_workflow_provider_slug() -> str:
    return default_provider_slug()


def _normalize_operate_mode(value: object) -> str:
    mode = str(value or "call").strip().lower().replace("-", "_")
    if mode in {"call", "command", "query"}:
        return mode
    raise HTTPException(
        status_code=422,
        detail={
            "message": "mode must be one of: call, command, query",
            "error_code": "operate.invalid_mode",
        },
    )


def _split_operate_operation(
    operation: str,
    catalog: dict[str, McpToolDefinition],
) -> tuple[McpToolDefinition, dict[str, Any], str]:
    """Resolve an operation id to a catalog tool and selector patch.

    Accepted forms:
    - ``praxis_bugs`` -> call the tool as-is.
    - ``praxis_bugs.file`` -> call ``praxis_bugs`` with ``{"action": "file"}``.
    - ``praxis_query.operator_graph`` -> call ``praxis_query`` with the tool's
      selector field when it declares one.
    """

    raw_operation = str(operation or "").strip()
    if not raw_operation:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "operation is required",
                "error_code": "operate.operation_required",
            },
        )

    canonical_operation = canonical_tool_name(raw_operation)
    direct = catalog.get(canonical_operation)
    if direct is not None:
        return direct, {}, direct.name

    if "." not in canonical_operation:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"unknown operation: {raw_operation}",
                "error_code": "operate.operation_not_found",
            },
        )

    tool_name, selector_value = canonical_operation.rsplit(".", 1)
    definition = catalog.get(tool_name)
    if definition is None:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"unknown operation: {raw_operation}",
                "error_code": "operate.operation_not_found",
            },
        )
    selector_field = definition.selector_field
    if selector_field is None:
        raise HTTPException(
            status_code=422,
            detail={
                "message": f"operation {raw_operation!r} names a sub-action, but {tool_name!r} has no selector field",
                "error_code": "operate.selector_not_supported",
            },
        )
    selector = selector_value.strip().replace("-", "_")
    if definition.selector_enum and selector not in definition.selector_enum:
        raise HTTPException(
            status_code=422,
            detail={
                "message": f"unknown {selector_field} {selector!r} for operation {tool_name!r}",
                "error_code": "operate.selector_not_found",
                "allowed": list(definition.selector_enum),
            },
        )
    return definition, {selector_field: selector}, f"{tool_name}.{selector}"


def _operate_catalog_tool(definition: McpToolDefinition) -> dict[str, Any]:
    return {
        "name": definition.name,
        "description": definition.description,
        "entrypoint": definition.cli_entrypoint,
        "describe_command": definition.cli_describe_command,
        "badges": list(definition.cli_badges),
        "surface": definition.cli_surface,
        "tier": definition.cli_tier,
        "selector_field": definition.selector_field,
        "selector_default": definition.selector_default,
        "selector_enum": list(definition.selector_enum),
        "risk_levels": list(definition.risk_levels),
        "requires_workflow_token": definition.requires_workflow_token,
        "input_schema": definition.input_schema,
        "example_input": definition.example_input(),
    }


def _operate_catalog_operation(definition: McpToolDefinition, action: str) -> dict[str, Any]:
    selector_field = definition.selector_field
    params = {selector_field: action} if selector_field else {}
    operation_name = f"{definition.name}.{action}" if selector_field else definition.name
    return {
        "operation": operation_name,
        "tool": definition.name,
        "selector_field": selector_field,
        "selector_value": action if selector_field else None,
        "risk": definition.risk_for_params(params),
        "surface": definition.cli_surface,
        "tier": definition.cli_tier,
        "description": definition.description,
        "input_schema": definition.input_schema,
    }


def build_operate_catalog_payload() -> dict[str, Any]:
    catalog = get_tool_catalog()
    tools = [_operate_catalog_tool(definition) for _, definition in sorted(catalog.items())]
    operations: list[dict[str, Any]] = []
    for _, definition in sorted(catalog.items()):
        capabilities = definition.capability_rows()
        if not capabilities:
            operations.append(_operate_catalog_operation(definition, definition.default_action))
            continue
        for capability in capabilities:
            action = str(capability.get("action") or definition.default_action).strip()
            if action:
                operations.append(_operate_catalog_operation(definition, action))
    return {
        "ok": True,
        "routed_to": "unified_operator_catalog",
        "contract_version": 1,
        "authority": "surfaces.mcp.catalog.get_tool_catalog",
        "call_path": "/api/operate",
        "catalog_path": "/api/operate/catalog",
        "tool_count": len(tools),
        "operation_count": len(operations),
        "tools": tools,
        "operations": operations,
    }


def execute_operate_request(
    body: OperateRequest,
    *,
    header_workflow_token: str | None = None,
) -> tuple[int, dict[str, Any]]:
    catalog = get_tool_catalog()
    definition, selector_patch, operation_name = _split_operate_operation(body.operation, catalog)
    mode = _normalize_operate_mode(body.mode)
    if body.input is None or not isinstance(body.input, dict):
        raise HTTPException(
            status_code=422,
            detail={
                "message": "input must be a JSON object",
                "error_code": "operate.invalid_input",
            },
        )
    tool_input = {**dict(body.input), **selector_patch}
    workflow_token = str(body.workflow_token or header_workflow_token or "").strip()
    try:
        result = invoke_tool(definition.name, tool_input, workflow_token=workflow_token)
    except ToolInvocationError as exc:
        status = 404 if exc.reason_code == "mcp.tool_not_found" else 400
        return (
            status,
            {
                "ok": False,
                "routed_to": "unified_operator_gateway",
                "operation": operation_name,
                "tool": definition.name,
                "error": exc.message,
                "reason_code": exc.reason_code,
                **({"details": exc.details} if exc.details else {}),
            },
        )
    if isinstance(result, dict) and result.get("error"):
        return (
            400,
            {
                "ok": False,
                "routed_to": "unified_operator_gateway",
                "operation": operation_name,
                "tool": definition.name,
                "result": result,
                "reason_code": str(result.get("reason_code") or "operate.tool_error"),
            },
        )
    return (
        200,
        {
            "ok": True,
            "routed_to": "unified_operator_gateway",
            "contract_version": 1,
            "operation": operation_name,
            "tool": definition.name,
            "mode": mode,
            "selector_field": definition.selector_field,
            "risk": definition.risk_for_params(tool_input),
            "idempotency_key": body.idempotency_key,
            "trace": body.trace,
            "result": result,
            "operation_receipt": {
                "operation_name": operation_name,
                "tool_name": definition.name,
                "authority_ref": "surfaces.mcp.invocation.invoke_tool",
                "catalog_ref": "surfaces.mcp.catalog.get_tool_catalog",
                "posture": "catalog_dispatched",
                "idempotency_policy": "caller_supplied" if body.idempotency_key else "tool_defined",
                "execution_status": "completed",
            },
        },
    )


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class WorkflowRunRequest(BaseModel):
    """Body for POST /api/workflow-runs."""

    prompt: str
    provider_slug: str | None = None
    model_slug: str | None = None
    tier: str | None = None
    adapter_type: str | None = None
    timeout: int = 300
    workdir: str | None = None
    max_tokens: int = 4096
    temperature: float = 0.0
    label: str | None = None
    workspace_ref: str = Field(default_factory=_default_workspace_ref)
    runtime_profile_ref: str = Field(default_factory=_default_runtime_profile_ref)
    system_prompt: str | None = None
    context_sections: list[dict[str, str]] | None = None
    max_retries: int = 0
    scope_read: list[str] | None = None
    scope_write: list[str] | None = None
    allowed_tools: list[str] | None = None
    verify_refs: list[str] | None = None
    definition_revision: str | None = None
    plan_revision: str | None = None
    packet_provenance: dict[str, Any] | None = None
    output_schema: dict | None = None
    authoring_contract: dict | None = None
    acceptance_contract: dict | None = None
    max_context_tokens: int | None = None
    persist: bool = True
    capabilities: list[str] | None = None
    use_cache: bool = False
    task_type: str | None = None
    prefer_cost: bool = False
    skip_auto_review: bool = False
    reviews_workflow_id: str | None = None
    review_target_modules: list[str] | None = None


class WorkflowBatchRequest(BaseModel):
    """Body for POST /api/workflow-runs/batch."""

    specs: list[WorkflowRunRequest]
    max_workers: int | None = None


class WorkflowStepRequest(BaseModel):
    """One step in a pipeline request."""

    name: str
    prompt: str
    adapter_type: str | None = None
    provider_slug: str | None = None
    model_slug: str | None = None
    tier: str | None = None
    max_tokens: int = 4096
    depends_on: list[str] = []
    loop: bool = False
    loop_prompt: str | None = None
    loop_max_parallel: int = 4


class PipelineRequest(BaseModel):
    """Body for POST /api/pipeline."""

    steps: list[WorkflowStepRequest]
    timeout: int = 600


class QueueSubmitRequest(BaseModel):
    """Body for POST /api/queue/submit."""

    spec: WorkflowRunRequest
    priority: int = 100
    max_attempts: int = 1


class LauncherRecoverRequest(BaseModel):
    """Body for POST /api/launcher/recover."""

    action: str
    service: str | None = None
    run_id: str | None = None
    open_browser: bool = False


class OperateRequest(BaseModel):
    """Catalog-backed unified operator gateway request."""

    operation: str
    input: dict[str, Any] = Field(default_factory=dict)
    mode: str = "call"
    idempotency_key: str | None = None
    workflow_token: str | None = None
    trace: dict[str, Any] = Field(default_factory=dict)


class PublicRunJobRequest(BaseModel):
    """One public API workflow job."""

    label: str
    prompt: str
    agent: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    read_scope: list[str] = Field(default_factory=list)
    write_scope: list[str] = Field(default_factory=list)
    max_attempts: int = 1


class PublicRunCreateRequest(BaseModel):
    """Body for POST /v1/runs."""

    name: str
    workflow_id: str | None = None
    phase: str = "build"
    workspace_ref: str = Field(default_factory=_default_workspace_ref)
    runtime_profile_ref: str = Field(default_factory=_default_runtime_profile_ref)
    jobs: list[PublicRunJobRequest]
    force_fresh_run: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec_from_request(req: WorkflowRunRequest):
    """Convert a WorkflowRunRequest pydantic model into a WorkflowSpec dataclass."""
    from runtime.workflow import WorkflowSpec

    return WorkflowSpec(
        prompt=req.prompt,
        provider_slug=req.provider_slug,
        model_slug=req.model_slug,
        tier=req.tier,
        adapter_type=req.adapter_type,
        timeout=req.timeout,
        workdir=req.workdir,
        max_tokens=req.max_tokens,
        temperature=req.temperature,
        label=req.label,
        workspace_ref=req.workspace_ref,
        runtime_profile_ref=req.runtime_profile_ref,
        system_prompt=req.system_prompt,
        context_sections=req.context_sections,
        max_retries=req.max_retries,
        scope_read=req.scope_read,
        scope_write=req.scope_write,
        allowed_tools=req.allowed_tools,
        verify_refs=req.verify_refs,
        definition_revision=req.definition_revision,
        plan_revision=req.plan_revision,
        packet_provenance=req.packet_provenance,
        output_schema=req.output_schema,
        authoring_contract=req.authoring_contract,
        acceptance_contract=req.acceptance_contract,
        max_context_tokens=req.max_context_tokens,
        persist=req.persist,
        capabilities=req.capabilities,
        use_cache=req.use_cache,
        task_type=req.task_type,
        prefer_cost=req.prefer_cost,
        skip_auto_review=req.skip_auto_review,
        reviews_workflow_id=req.reviews_workflow_id,
        review_target_modules=req.review_target_modules,
    )


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if value else None


def _public_run_links(run_id: str) -> dict[str, str]:
    return {
        "self": f"/v1/runs/{run_id}",
        "jobs": f"/v1/runs/{run_id}/jobs",
        "cancel": f"/v1/runs/{run_id}:cancel",
    }


def _public_run_summary_payload(
    *,
    conn: Any,
    run_status: dict[str, Any],
) -> dict[str, Any]:
    run_id = str(run_status.get("run_id") or "")
    jobs = [_serialize_run_job(dict(row)) for row in run_status.get("jobs", []) if isinstance(row, dict)]
    total_jobs = int(run_status.get("total_jobs") or len(jobs))
    payload = {
        "run_id": run_id,
        "workflow_id": run_status.get("workflow_id"),
        "request_id": run_status.get("request_id"),
        "status": run_status.get("current_state") or run_status.get("status"),
        "total_jobs": total_jobs,
        "completed_jobs": int(run_status.get("completed_jobs") or 0),
        "total_cost_usd": float(
            run_status.get("total_cost_usd")
            or run_status.get("total_cost")
            or 0.0
        ),
        "total_duration_ms": int(
            run_status.get("total_duration_ms")
            or run_status.get("duration_ms")
            or 0
        ),
        "created_at": _iso_or_none(run_status.get("requested_at") or run_status.get("created_at")),
        "finished_at": _iso_or_none(run_status.get("finished_at")),
        "summary": _build_run_summary(conn, run_id, jobs),
        "health": summarize_run_health({**run_status, "jobs": jobs}, datetime.now(timezone.utc)),
        "graph": _build_run_graph(conn, run_id, jobs),
        "links": _public_run_links(run_id),
    }
    if jobs:
        payload["jobs"] = jobs
    return payload


class _BufferedWriter:
    def __init__(self) -> None:
        self._chunks: list[bytes] = []

    def write(self, data: bytes) -> int:
        self._chunks.append(data)
        return len(data)

    def flush(self) -> None:
        return None

    def read(self) -> bytes:
        return b"".join(self._chunks)


class _FastAPIHandlerAdapter:
    """Bridge BaseHTTPRequestHandler-style route handlers onto FastAPI."""

    def __init__(self, request: Request, subsystems: _Subsystems, body: bytes) -> None:
        self.headers = request.headers
        self.path = request.url.path
        if request.url.query:
            self.path = f"{self.path}?{request.url.query}"
        self.rfile = io.BytesIO(body)
        self.subsystems = subsystems
        self._response_headers: dict[str, str] = {
            "Access-Control-Allow-Origin": "*",
        }
        self._status_code = 200
        self._writer = _BufferedWriter()

    @property
    def wfile(self) -> _BufferedWriter:
        return self._writer

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self._status_code = status
        self._response_headers["Content-Type"] = "application/json"
        self._response_headers["Content-Length"] = str(len(body))
        self._writer = _BufferedWriter()
        self._writer.write(body)

    def _send_bytes(
        self,
        status: int,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_disposition: str | None = None,
    ) -> None:
        self._status_code = status
        self._response_headers["Content-Type"] = content_type
        self._response_headers["Content-Length"] = str(len(payload))
        if content_disposition:
            self._response_headers["Content-Disposition"] = content_disposition
        self._writer = _BufferedWriter()
        self._writer.write(payload)

    def send_response(self, status: int) -> None:
        self._status_code = status

    def send_header(self, name: str, value: str) -> None:
        self._response_headers[name] = value

    def end_headers(self) -> None:
        return None

    def to_response(self) -> Response:
        body = self._writer.read()
        headers = dict(self._response_headers)
        media_type = headers.pop("Content-Type", None)
        headers.pop("Content-Length", None)
        return Response(
            content=body,
            status_code=self._status_code,
            media_type=media_type,
            headers=headers,
        )


def _shared_pg_conn():
    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")
    return subsystems.get_pg_conn()


async def _route_to_handler(request: Request) -> Response:
    """Dispatch a request through the unified handler system.

    This replaces the legacy bridge — same handler functions,
    cleaner dispatch. Every route that uses this is explicitly
    registered as a FastAPI endpoint (no catch-all wildcards).

    The handler is invoked with the URL-encoded path plus the query
    string — i.e. the raw request target. Two reasons it must be
    encoded rather than decoded:

    * Handlers that read query params (``?category=table``,
      ``?include_layers=1``) rely on ``urlparse(...).query`` — they'd
      get nothing if we handed them only ``request.url.path``.
    * Path segments can legitimately contain ``/`` once percent-
      decoded (e.g. ``object_kind='dataset:slm/review'`` becomes
      ``dataset%3Aslm%2Freview`` in the URL). Handlers split on ``/``
      before ``unquote``-ing each segment, so the path must reach
      them with ``%2F`` still intact or the split will produce the
      wrong segment count.
    """
    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")

    raw_path_bytes = request.scope.get("raw_path") or b""
    if raw_path_bytes:
        # raw_path is the pre-decode bytes for the path only — it does
        # not include the query string. Latin-1 is the ASGI-mandated
        # byte→str mapping and round-trips cleanly.
        raw_path = raw_path_bytes.decode("latin-1")
        # Strip the query fragment if one is embedded (some ASGI
        # servers include it, others don't).
        raw_path = raw_path.split("?", 1)[0]
    else:
        raw_path = request.url.path
    raw_path = raw_path.rstrip("/") or "/"
    query = request.url.query
    path = f"{raw_path}?{query}" if query else raw_path
    body = await request.body()
    adapter = _FastAPIHandlerAdapter(request, subsystems, body)

    if request.method == "GET":
        handled = handle_get_request(adapter, path)
    elif request.method == "POST":
        handled = handle_post_request(adapter, path)
    elif request.method == "PUT":
        handled = handle_put_request(adapter, path)
    elif request.method == "DELETE":
        handled = handle_delete_request(adapter, path)
    else:
        handled = False

    if not handled:
        adapter._send_json(404, {"error": f"Not found: {path}"})

    return adapter.to_response()


async def _dispatch_standard_route(request: Request) -> Response:
    """Dispatch a standard-route handler: handler(subsystems, body) -> dict."""
    from .handlers._shared import _ClientError

    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")

    from .handlers import ROUTES
    path = request.url.path.rstrip("/") or "/"
    handler = ROUTES.get(path)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"Not found: {path}")

    try:
        body_bytes = await request.body()
        body = json.loads(body_bytes) if body_bytes else {}
        if not isinstance(body, dict):
            payload = {"error": "Request body must be a JSON object"}
            _record_api_route_usage(
                subsystems,
                path=path,
                method="POST",
                status_code=400,
                response_payload=payload,
                headers=request.headers,
            )
            return JSONResponse({"error": "Request body must be a JSON object"}, status_code=400)
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        _record_api_route_usage(
            subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return JSONResponse({"error": f"Invalid JSON: {exc}"}, status_code=400)

    try:
        result = handler(subsystems, body)
        _record_api_route_usage(
            subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
        )
        return JSONResponse(result, status_code=200)
    except _ClientError as exc:
        payload = {"error": str(exc)}
        _record_api_route_usage(
            subsystems,
            path=path,
            method="POST",
            status_code=400,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:
        payload = {
            "error": f"{type(exc).__name__}: {exc}",
            "error_code": "internal_error",
        }
        _record_api_route_usage(
            subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
        return JSONResponse(
            payload,
            status_code=500,
        )


def _serialize_surface_usage_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "usage_date": (
            row.get("usage_date").isoformat() if row.get("usage_date") is not None else None
        ),
        "surface_kind": row.get("surface_kind"),
        "transport_kind": row.get("transport_kind"),
        "entrypoint_kind": row.get("entrypoint_kind"),
        "entrypoint_name": row.get("entrypoint_name"),
        "caller_kind": row.get("caller_kind"),
        "http_method": row.get("http_method") or None,
        "invocation_count": int(row.get("invocation_count") or 0),
        "success_count": int(row.get("success_count") or 0),
        "client_error_count": int(row.get("client_error_count") or 0),
        "server_error_count": int(row.get("server_error_count") or 0),
        "first_invoked_at": _iso_or_none(row.get("first_invoked_at")),
        "last_invoked_at": _iso_or_none(row.get("last_invoked_at")),
    }


def _json_or_none(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _serialize_surface_usage_event_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": int(row.get("event_id") or 0),
        "occurred_at": _iso_or_none(row.get("occurred_at")),
        "surface_kind": row.get("surface_kind"),
        "transport_kind": row.get("transport_kind"),
        "entrypoint_kind": row.get("entrypoint_kind"),
        "entrypoint_name": row.get("entrypoint_name"),
        "caller_kind": row.get("caller_kind"),
        "http_method": row.get("http_method") or None,
        "status_code": int(row.get("status_code") or 0),
        "result_state": row.get("result_state"),
        "reason_code": row.get("reason_code") or None,
        "routed_to": row.get("routed_to") or None,
        "workflow_id": row.get("workflow_id") or None,
        "run_id": row.get("run_id") or None,
        "job_label": row.get("job_label") or None,
        "request_id": row.get("request_id") or None,
        "client_version": row.get("client_version") or None,
        "payload_size_bytes": int(row.get("payload_size_bytes") or 0),
        "response_size_bytes": int(row.get("response_size_bytes") or 0),
        "prose_chars": int(row.get("prose_chars") or 0),
        "query_chars": int(row.get("query_chars") or 0),
        "result_count": int(row.get("result_count") or 0),
        "unresolved_count": int(row.get("unresolved_count") or 0),
        "capability_count": int(row.get("capability_count") or 0),
        "reference_count": int(row.get("reference_count") or 0),
        "compiled_job_count": int(row.get("compiled_job_count") or 0),
        "trigger_count": int(row.get("trigger_count") or 0),
        "definition_hash": row.get("definition_hash") or None,
        "definition_revision": row.get("definition_revision") or None,
        "task_class": row.get("task_class") or None,
        "planner_required": bool(row.get("planner_required")),
        "llm_used": bool(row.get("llm_used")),
        "has_current_plan": bool(row.get("has_current_plan")),
        "metadata": _json_or_none(row.get("metadata")) or {},
    }


def _read_job_output(output_path: str | None, stdout_preview: str | None) -> tuple[str, str]:
    preview = stdout_preview or ""
    if not output_path:
        return preview, "preview"

    path = Path(output_path)
    if not path.is_file():
        return preview, "preview"

    try:
        return path.read_text(encoding="utf-8"), "file"
    except OSError:
        return preview, "preview"


def _serialize_run_job(row: dict[str, Any]) -> dict[str, Any]:
    status = row["status"]
    output_preview = row.get("stdout_preview") or ""
    return {
        "id": int(row["id"]),
        "label": row["label"],
        "status": status,
        "job_type": row.get("job_type") or "workflow",
        "phase": row.get("phase") or "build",
        "agent_slug": row.get("agent_slug"),
        "resolved_agent": row.get("resolved_agent"),
        "integration_id": row.get("integration_id"),
        "integration_action": row.get("integration_action"),
        "integration_args": row.get("integration_args") or {},
        "attempt": int(row.get("attempt") or 0),
        "duration_ms": int(row.get("duration_ms") or 0),
        "cost_usd": float(row.get("cost_usd") or 0),
        "exit_code": row.get("exit_code"),
        "last_error_code": row.get("last_error_code"),
        "stdout_preview": output_preview,
        "has_output": bool(row.get("output_path") or output_preview),
        "started_at": _iso_or_none(row.get("started_at")),
        "finished_at": _iso_or_none(row.get("finished_at")),
        "created_at": _iso_or_none(row.get("created_at")),
    }


def _launcher_index_response() -> FileResponse | JSONResponse:
    index_path = _APP_DIST_DIR / "index.html"
    if not index_path.is_file():
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "launcher_build_missing",
                "detail": (
                    "Launcher build missing. Rebuild Code&DBs/Workflow/surfaces/app "
                    "with `npm run build`."
                ),
                "launch_url": None,
            },
        )
    return FileResponse(
        index_path,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


# ---------------------------------------------------------------------------
# Status endpoints (native FastAPI — richer than handler equivalents)
# ---------------------------------------------------------------------------


@app.post("/api/launcher/recover")
def launcher_recover(req: LauncherRecoverRequest) -> JSONResponse:
    """Run bounded launcher recovery through the preferred launcher command."""
    try:
        status_code, payload = launcher_handlers.launcher_recover_payload(
            action=req.action,
            service=req.service,
            run_id=req.run_id,
            open_browser=req.open_browser,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except launcher_handlers.LauncherAuthorityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/api/atlas.html", response_model=None)
def atlas_html() -> FileResponse | JSONResponse:
    """Serve the Praxis knowledge-graph atlas as a single self-contained page.

    The atlas is regenerated by ``scripts/praxis_atlas.py``; this endpoint
    streams whatever artifact currently exists on disk.
    """
    atlas_path = REPO_ROOT / "artifacts" / "atlas.html"
    if not atlas_path.is_file():
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "atlas_artifact_missing",
                "detail": (
                    "Atlas artifact not generated. Run "
                    "`python3 scripts/praxis_atlas.py` to build artifacts/atlas.html."
                ),
            },
        )
    return FileResponse(
        atlas_path,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/atlas/graph")
def atlas_graph() -> JSONResponse:
    """Return the canonical Atlas graph payload for native app rendering."""
    try:
        payload = build_atlas_payload()
    except Exception as exc:
        logger.exception("Atlas graph payload failed")
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "atlas_graph_unavailable",
                "detail": str(exc),
                "nodes": [],
                "edges": [],
                "areas": [],
                "metadata": {
                    "source_authority": "Praxis.db",
                    "node_count": 0,
                    "edge_count": 0,
                    "aggregate_edge_count": 0,
                },
                "warnings": [type(exc).__name__],
            },
        )
    return JSONResponse(
        content=jsonable_encoder(payload),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/leaderboard")
def get_leaderboard() -> list[dict[str, Any]]:
    """Return the agent leaderboard as a list of AgentScore dicts."""
    from runtime.leaderboard import build_leaderboard as _build_leaderboard

    scores = _build_leaderboard()
    return [dataclasses.asdict(s) for s in scores]


@app.get("/api/runs/recent")
def list_recent_runs(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """Return recent workflow runs with job progress summaries."""
    conn = _shared_pg_conn()
    rows = conn.execute(
        """SELECT r.run_id,
                  r.workflow_id,
                  COALESCE(r.request_envelope->>'name', r.workflow_id) AS spec_name,
                  r.current_state AS status,
                  COALESCE(NULLIF(r.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  r.requested_at AS created_at,
                  r.finished_at,
                  COUNT(j.id) FILTER (WHERE j.status IN ('succeeded','failed','dead_letter','blocked','cancelled')) as completed_jobs,
                  COALESCE(SUM(j.cost_usd), 0) as total_cost
           FROM workflow_runs r
           LEFT JOIN workflow_jobs j ON j.run_id = r.run_id
           GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at
           ORDER BY r.requested_at DESC
           LIMIT $1""",
        limit,
    )
    if not rows:
        return []
    return [
        {
            "run_id": r["run_id"],
            "workflow_id": r["workflow_id"],
            "spec_name": r["spec_name"],
            "status": r["status"],
            "total_jobs": r["total_jobs"],
            "completed_jobs": int(r["completed_jobs"]),
            "total_cost": float(r["total_cost"]),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
        }
        for r in rows
    ]


@app.get("/api/runs/{run_id}")
def get_run_detail(run_id: str) -> dict[str, Any]:
    """Return one workflow run with ordered job details."""
    conn = _shared_pg_conn()
    run_rows = conn.execute(
        """SELECT r.run_id,
                  r.workflow_id,
                  COALESCE(r.request_envelope->>'name', r.workflow_id) AS spec_name,
                  r.current_state AS status,
                  COALESCE(NULLIF(r.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  r.requested_at AS created_at,
                  r.finished_at,
                  COUNT(j.id) FILTER (WHERE j.status IN ('succeeded','failed','dead_letter','blocked','cancelled')) as completed_jobs,
                  COALESCE(SUM(j.cost_usd), 0) as total_cost,
                  COALESCE(SUM(j.duration_ms), 0) as total_duration_ms
           FROM workflow_runs r
           LEFT JOIN workflow_jobs j ON j.run_id = r.run_id
           WHERE r.run_id = $1
           GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at""",
        run_id,
    )
    if not run_rows:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    job_rows = conn.execute(
        """SELECT id, label, status, job_type, phase, agent_slug, resolved_agent,
                  integration_id, integration_action, integration_args, attempt,
                  duration_ms, cost_usd, exit_code, last_error_code, stdout_preview,
                  output_path, created_at, started_at, finished_at
           FROM workflow_jobs
           WHERE run_id = $1
           ORDER BY id""",
        run_id,
    )

    run = run_rows[0]
    jobs = [_serialize_run_job(dict(row)) for row in (job_rows or [])]
    if not jobs:
        jobs = _load_run_jobs_from_status_authority(conn, run_id)
    summary = _build_run_summary(conn, run_id, jobs)
    graph = _build_run_graph(conn, run_id, jobs)
    health = summarize_run_health(
        {
            **run,
            "jobs": jobs,
        },
        datetime.now(timezone.utc),
    )

    return {
        "run_id": run["run_id"],
        "workflow_id": run["workflow_id"],
        "spec_name": run["spec_name"],
        "status": run["status"],
        "total_jobs": int(run["total_jobs"] or len(jobs)),
        "completed_jobs": int(run["completed_jobs"] or 0),
        "total_cost": float(run["total_cost"] or 0),
        "total_duration_ms": int(run["total_duration_ms"] or 0),
        "created_at": _iso_or_none(run["created_at"]),
        "finished_at": _iso_or_none(run["finished_at"]),
        "jobs": jobs,
        "summary": summary,
        "graph": graph,
        "health": health,
    }


def _load_run_jobs_from_status_authority(conn: Any, run_id: str) -> list[dict[str, Any]]:
    try:
        from runtime.workflow.unified import get_run_status

        status = get_run_status(conn, run_id)
    except Exception as exc:
        raise _run_authority_unavailable(
            reason_code="run_detail.status_authority_query_failed",
            message="run status authority query failed",
            run_id=run_id,
            exc=exc,
        ) from exc

    if not isinstance(status, dict):
        raise _run_authority_unavailable(
            reason_code="run_detail.status_authority_invalid_payload",
            message="run status authority returned a non-object payload",
            run_id=run_id,
        )

    jobs = status.get("jobs")
    if not isinstance(jobs, list):
        raise _run_authority_unavailable(
            reason_code="run_detail.status_authority_missing_jobs",
            message="run status authority payload is missing jobs",
            run_id=run_id,
        )
    return [
        _serialize_run_job(dict(row))
        for row in jobs
        if isinstance(row, dict) and row.get("label")
    ]


def _parse_json_mapping(raw: object) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return dict(parsed)
    return None


def _load_run_spec_snapshot(conn: Any, run_id: str) -> dict[str, Any] | None:
    try:
        rows = conn.execute(
            "SELECT request_envelope->'spec_snapshot' AS spec_snapshot FROM workflow_runs WHERE run_id = $1",
            run_id,
        )
    except Exception as exc:
        raise _run_authority_unavailable(
            reason_code="run_detail.spec_snapshot_query_failed",
            message="run spec snapshot query failed",
            run_id=run_id,
            exc=exc,
        ) from exc
    if not rows:
        return None
    return _parse_json_mapping(rows[0].get("spec_snapshot"))


def _load_operator_frame_counts(conn: Any, run_id: str) -> dict[str, Counter[str]]:
    try:
        rows = conn.execute(
            """SELECT operator_frame_id, node_id, frame_state
               FROM run_operator_frames
               WHERE run_id = $1
               ORDER BY node_id, operator_frame_id""",
            run_id,
        )
    except Exception as exc:
        raise _run_authority_unavailable(
            reason_code="run_detail.operator_frame_query_failed",
            message="run operator frame query failed",
            run_id=run_id,
            exc=exc,
        ) from exc

    counts_by_node: dict[str, Counter[str]] = {}
    for row in rows or []:
        node_id = str(row.get("node_id") or "").strip()
        frame_state = str(row.get("frame_state") or "").strip()
        if not node_id or not frame_state:
            continue
        counts_by_node.setdefault(node_id, Counter())[frame_state] += 1
    return counts_by_node


def _graph_condition_for_branch(
    *,
    operator: dict[str, Any],
    branch: str,
) -> dict[str, Any] | None:
    kind = str(operator.get("kind") or "").strip()
    if kind == "if":
        predicate = operator.get("predicate")
        if not isinstance(predicate, dict):
            return None
        field = predicate.get("field")
        op = predicate.get("op", "equals")
        value = predicate.get("value")
        if not isinstance(field, str) or not field.strip() or not isinstance(op, str) or not op.strip():
            return None
        if branch == "then":
            return {"field": field, "op": op, "value": value}
        if branch == "else":
            inverse = {
                "equals": "not_equals",
                "not_equals": "equals",
                "in": "not_in",
                "not_in": "in",
                "eq": "neq",
                "neq": "eq",
                "gt": "lte",
                "gte": "lt",
                "lt": "gte",
                "lte": "gt",
            }.get(op)
            if inverse:
                return {"field": field, "op": inverse, "value": value}
            return {"branch": branch}
        return None

    if kind == "switch":
        field = operator.get("field")
        if not isinstance(field, str) or not field.strip():
            return None
        for case in operator.get("cases") or []:
            if not isinstance(case, dict):
                continue
            if str(case.get("branch") or "").strip() != branch:
                continue
            return {"field": field, "op": "equals", "value": case.get("value")}
        return {"branch": branch}

    return None


def _build_run_graph_from_graph_spec(
    *,
    conn: Any,
    run_id: str,
    jobs: list[dict[str, Any]],
    spec_snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        compiled_request = compile_graph_workflow_request(spec_snapshot, run_id=run_id)
        validation = validate_workflow_request(compiled_request)
    except Exception:
        return None

    if not validation.is_valid:
        return None

    visible_nodes = [
        node
        for node in compiled_request.nodes
        if not node.template_owner_node_id
    ]
    if not visible_nodes:
        return None

    visible_node_ids = {node.node_id for node in visible_nodes}
    job_by_label = {str(job.get("label") or ""): job for job in jobs}
    operator_frame_counts = _load_operator_frame_counts(conn, run_id)

    graph_nodes: list[dict[str, Any]] = []
    for node in sorted(visible_nodes, key=lambda item: (item.position_index, item.node_id)):
        job = job_by_label.get(node.node_id)
        frame_counts = operator_frame_counts.get(node.node_id, Counter())
        total_frames = sum(frame_counts.values())
        running_frames = frame_counts.get("created", 0) + frame_counts.get("running", 0)
        failed_frames = frame_counts.get("failed", 0) + frame_counts.get("cancelled", 0)
        succeeded_frames = frame_counts.get("succeeded", 0)

        status = "pending"
        if job is not None:
            status = str(job.get("status") or "pending")
        elif total_frames:
            if failed_frames:
                status = "failed"
            elif succeeded_frames == total_frames:
                status = "succeeded"
            elif running_frames:
                status = "running"

        node_payload: dict[str, Any] = {
            "id": node.node_id,
            "label": node.node_id,
            "type": "job",
            "adapter": node.adapter_type,
            "position": int(node.position_index),
            "status": status,
        }
        if job is not None:
            node_payload["cost_usd"] = float(job.get("cost_usd") or 0)
            node_payload["duration_ms"] = int(job.get("duration_ms") or 0)
            node_payload["agent"] = job.get("resolved_agent") or job.get("agent_slug")
            node_payload["attempt"] = int(job.get("attempt") or 0)
            if job.get("last_error_code"):
                node_payload["error_code"] = job.get("last_error_code")
        if total_frames:
            node_payload["loop"] = {
                "count": total_frames,
                "succeeded": succeeded_frames,
                "failed": failed_frames,
                "running": running_frames,
            }
        graph_nodes.append(node_payload)

    nodes_by_id = {node.node_id: node for node in visible_nodes}
    graph_edges: list[dict[str, Any]] = []
    for edge in compiled_request.edges:
        if edge.template_owner_node_id:
            continue
        if edge.from_node_id not in visible_node_ids or edge.to_node_id not in visible_node_ids:
            continue
        edge_type = edge.edge_type
        condition: dict[str, Any] | None = None
        release_condition = dict(edge.release_condition or {})
        branch = str(release_condition.get("branch") or "").strip()
        if branch:
            source_node = nodes_by_id.get(edge.from_node_id)
            operator = (
                dict(source_node.inputs.get("operator") or {})
                if source_node is not None and isinstance(source_node.inputs.get("operator"), dict)
                else {}
            )
            condition = _graph_condition_for_branch(operator=operator, branch=branch)
            edge_type = "conditional" if condition is not None else edge.edge_type
        graph_edges.append(
            {
                "id": edge.edge_id,
                "from": edge.from_node_id,
                "to": edge.to_node_id,
                "type": edge_type,
                "condition": condition,
                "data_mapping": dict(edge.payload_mapping or {}),
            }
        )

    return {"nodes": graph_nodes, "edges": graph_edges}


def _build_run_graph(conn: Any, run_id: str, jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Build a run graph from the spec_snapshot and runtime job status.

    Reads the original spec's jobs + depends_on from request_envelope.spec_snapshot,
    then annotates each node with runtime status from workflow_jobs.
    Loop jobs (replicate: labels like prefix_01, prefix_02) are collapsed.
    """
    spec_snapshot = _load_run_spec_snapshot(conn, run_id)
    if isinstance(spec_snapshot, dict) and spec_uses_graph_runtime(spec_snapshot):
        graph = _build_run_graph_from_graph_spec(
            conn=conn,
            run_id=run_id,
            jobs=jobs,
            spec_snapshot=spec_snapshot,
        )
        if graph is not None:
            return graph

    if not jobs:
        return None
    try:
        spec_jobs_raw = spec_snapshot.get("jobs") if isinstance(spec_snapshot, dict) else None
        if spec_jobs_raw is None:
            return _build_run_graph_from_jobs(jobs)

        spec_jobs = json.loads(spec_jobs_raw) if isinstance(spec_jobs_raw, str) else spec_jobs_raw
        if not isinstance(spec_jobs, list) or not spec_jobs:
            return _build_run_graph_from_jobs(jobs)

        # Build job status lookup
        job_by_label: dict[str, dict[str, Any]] = {}
        for j in jobs:
            job_by_label[j["label"]] = j

        # Detect loop groups from runtime jobs
        spec_labels = {sj.get("label") for sj in spec_jobs if sj.get("label")}
        loop_groups: dict[str, list[dict[str, Any]]] = {}
        for j in jobs:
            label = j["label"]
            parts = label.rsplit("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and parts[0] in spec_labels:
                loop_groups.setdefault(parts[0], []).append(j)

        # Build nodes from spec jobs
        graph_nodes: list[dict[str, Any]] = []
        for i, sj in enumerate(spec_jobs):
            label = sj.get("label") or f"step_{i}"
            node: dict[str, Any] = {
                "id": label,
                "label": label,
                "type": "job",
                "adapter": sj.get("agent") or "auto",
                "position": i,
            }

            if label in loop_groups:
                children = loop_groups[label]
                succeeded = sum(1 for c in children if c["status"] == "succeeded")
                failed = sum(1 for c in children if c["status"] in ("failed", "dead_letter"))
                running = sum(1 for c in children if c["status"] in ("running", "claimed"))
                node["loop"] = {"count": len(children), "succeeded": succeeded, "failed": failed, "running": running}
                node["cost_usd"] = sum(c["cost_usd"] for c in children)
                node["duration_ms"] = max((c["duration_ms"] for c in children), default=0)
                node["status"] = "succeeded" if succeeded == len(children) else "failed" if failed > 0 else "running" if running > 0 else "pending"
            elif label in job_by_label:
                j = job_by_label[label]
                node["status"] = j["status"]
                node["cost_usd"] = j["cost_usd"]
                node["duration_ms"] = j["duration_ms"]
                node["agent"] = j.get("resolved_agent") or j.get("agent_slug")
                node["attempt"] = j.get("attempt", 0)
                if j.get("last_error_code"):
                    node["error_code"] = j["last_error_code"]
            else:
                node["status"] = "pending"

            graph_nodes.append(node)

        # Build edges from depends_on
        graph_edges: list[dict[str, Any]] = []
        for sj in spec_jobs:
            label = sj.get("label") or ""
            deps = sj.get("depends_on") or []
            for dep in deps:
                graph_edges.append({
                    "id": f"edge-{dep}-{label}",
                    "from": dep,
                    "to": label,
                    "type": "after_success",
                })

        return {"nodes": graph_nodes, "edges": graph_edges}
    except Exception:
        return _build_run_graph_from_jobs(jobs)


def _build_run_graph_from_jobs(jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Fallback: build a simple chain graph from runtime jobs when no spec is available."""
    if len(jobs) < 2:
        return None
    nodes = []
    edges = []
    for i, j in enumerate(jobs):
        nodes.append({
            "id": j["label"],
            "label": j["label"],
            "type": "job",
            "adapter": j.get("agent_slug") or "auto",
            "position": i,
            "status": j["status"],
            "cost_usd": j["cost_usd"],
            "duration_ms": j["duration_ms"],
        })
        if i > 0:
            edges.append({
                "id": f"edge-{jobs[i-1]['label']}-{j['label']}",
                "from": jobs[i - 1]["label"],
                "to": j["label"],
                "type": "after_success",
            })
    return {"nodes": nodes, "edges": edges}


def _build_run_summary(conn: Any, run_id: str, jobs: list[dict[str, Any]]) -> str | None:
    """Build a human-readable summary from job submission summaries."""
    try:
        rows = conn.execute(
            """SELECT s.job_label, s.summary
               FROM workflow_job_submissions s
               WHERE s.run_id = $1 AND s.summary IS NOT NULL AND s.summary != ''
               ORDER BY s.attempt_no DESC, s.sealed_at DESC""",
            run_id,
        )
        if rows:
            # Collect the latest summary per job label
            seen: set[str] = set()
            parts: list[str] = []
            for row in rows:
                label = row.get("job_label") or ""
                if label in seen:
                    continue
                seen.add(label)
                summary = (row.get("summary") or "").strip()
                if summary:
                    parts.append(summary)
            if parts:
                return " ".join(parts) if len(parts) <= 2 else "\n".join(f"- {p}" for p in parts)
    except Exception:
        pass

    # Fallback: status-based summary
    if not jobs:
        return None
    done = [j for j in jobs if j.get("status") in ("succeeded", "failed", "dead_letter", "blocked", "cancelled")]
    if not done:
        return None
    succeeded = sum(1 for j in done if j.get("status") == "succeeded")
    failed = len(done) - succeeded
    labels = [f"{j.get('label', '?')} ({'done' if j.get('status') == 'succeeded' else 'failed'})" for j in done]
    return f"{succeeded} of {len(jobs)} steps completed: {', '.join(labels)}" if failed else f"All {len(jobs)} steps completed successfully."


@app.get("/api/runs/{run_id}/jobs/{job_id}")
def get_run_job_detail(run_id: str, job_id: int) -> dict[str, Any]:
    """Return one workflow job with best-available output content."""
    conn = _shared_pg_conn()
    rows = conn.execute(
        """SELECT id, run_id, label, status, job_type, phase, agent_slug, resolved_agent,
                  integration_id, integration_action, integration_args, attempt,
                  duration_ms, cost_usd, exit_code, last_error_code, stdout_preview,
                  output_path, receipt_id, created_at, started_at, finished_at
           FROM workflow_jobs
           WHERE run_id = $1 AND id = $2
           LIMIT 1""",
        run_id,
        job_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Job not found: {run_id}/{job_id}")

    row = dict(rows[0])
    output, output_source = _read_job_output(row.get("output_path"), row.get("stdout_preview"))
    job = _serialize_run_job(row)
    job["output"] = output
    job["output_source"] = output_source
    job["receipt_id"] = row.get("receipt_id")

    if output:
        try:
            job["output_json"] = json.loads(output)
        except json.JSONDecodeError:
            pass

    return job


@app.get("/v1/runs", tags=["public", "runs"])
def public_list_runs(
    limit: int = Query(default=20, ge=1, le=100),
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    runs = [
        {
            **row,
            "total_cost_usd": float(row.get("total_cost") or 0.0),
        }
        for row in list_recent_runs(limit=limit)
    ]
    for row in runs:
        row.pop("total_cost", None)
    return {
        "count": len(runs),
        "runs": runs,
    }


@app.post("/v1/runs", tags=["public", "runs"])
def public_create_run(
    req: PublicRunCreateRequest,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    _auth: str | None = Security(_require_public_api_access),
) -> JSONResponse:
    del _auth
    if not req.jobs:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "At least one job is required to create a workflow run.",
                "error_code": "workflow_jobs_required",
            },
        )

    labels = [job.label.strip() for job in req.jobs if job.label.strip()]
    if len(labels) != len(req.jobs) or len(set(labels)) != len(labels):
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Each public workflow job must have a unique non-empty label.",
                "error_code": "workflow_job_labels_invalid",
            },
        )

    phase = str(req.phase or "build").strip() or "build"
    workflow_id = req.workflow_id or f"workflow.api.v1.{_slugify_identifier(req.name, fallback='run')}"
    inline_spec = {
        "name": req.name,
        "workflow_id": workflow_id,
        "phase": phase,
        "workspace_ref": req.workspace_ref,
        "runtime_profile_ref": req.runtime_profile_ref,
        "jobs": [
            {
                "label": job.label,
                "agent": job.agent or f"auto/{phase}",
                "prompt": job.prompt,
                "depends_on": list(job.depends_on),
                "read_scope": list(job.read_scope),
                "write_scope": list(job.write_scope),
                "max_attempts": int(job.max_attempts),
            }
            for job in req.jobs
        ],
    }

    from runtime.control_commands import ControlCommandError, ControlCommandIdempotencyConflict
    from runtime.command_handlers import (
        render_workflow_submit_response,
        request_workflow_submit_command,
    )

    request_key = idempotency_key or request.state.idempotency_key
    try:
        command = request_workflow_submit_command(
            _shared_pg_conn(),
            requested_by_kind="http",
            requested_by_ref=f"public_api.runs.{request.state.request_id}",
            inline_spec=inline_spec,
            repo_root=str(REPO_ROOT),
            force_fresh_run=bool(req.force_fresh_run),
            idempotency_key=request_key,
        )
        result = render_workflow_submit_response(
            command,
            spec_name=inline_spec["name"],
            total_jobs=len(inline_spec["jobs"]),
        )
    except ControlCommandIdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "error_code": exc.reason_code,
                "details": exc.details,
            },
        ) from exc
    except ControlCommandError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "message": str(exc),
                "error_code": exc.reason_code,
                "details": exc.details,
            },
        ) from exc

    if result.get("status") == "failed":
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(result.get("error") or "Workflow run could not be queued"),
                "error_code": str(result.get("error_code") or "workflow_submit_failed"),
                "details": result,
            },
        )

    run_id = str(result.get("run_id") or "").strip()
    payload = {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "status": result.get("status"),
        "command_id": result.get("command_id"),
        "command_status": result.get("command_status"),
        "request_id": request.state.request_id,
        "idempotency_key": request_key,
        "links": _public_run_links(run_id) if run_id else {},
    }
    return JSONResponse(status_code=202, content=payload)


@app.get("/v1/runs/{run_id}", tags=["public", "runs"])
def public_get_run(
    run_id: str,
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    from runtime.workflow._status import get_run_status

    conn = _shared_pg_conn()
    status = get_run_status(conn, run_id)
    if status is None:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"Run not found: {run_id}",
                "error_code": "run_not_found",
            },
        )
    return _public_run_summary_payload(conn=conn, run_status=status)


@app.get("/v1/runs/{run_id}/jobs", tags=["public", "runs"])
def public_list_run_jobs(
    run_id: str,
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    from runtime.workflow._status import get_run_status

    conn = _shared_pg_conn()
    status = get_run_status(conn, run_id)
    if status is None:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"Run not found: {run_id}",
                "error_code": "run_not_found",
            },
        )
    jobs = [_serialize_run_job(dict(row)) for row in status.get("jobs", []) if isinstance(row, dict)]
    return {
        "run_id": run_id,
        "count": len(jobs),
        "jobs": jobs,
        "links": _public_run_links(run_id),
    }


@app.post("/v1/runs/{run_id}:cancel", tags=["public", "runs"])
def public_cancel_run(
    run_id: str,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    _auth: str | None = Security(_require_public_api_access),
) -> JSONResponse:
    del _auth
    from runtime.control_commands import (
        ControlCommandError,
        ControlCommandIdempotencyConflict,
        ControlCommandType,
        ControlIntent,
        execute_control_intent,
        render_control_command_response,
    )

    conn = _shared_pg_conn()
    request_key = (
        idempotency_key
        or request.state.idempotency_key
        or f"workflow.cancel.public_api.{run_id}"
    )
    try:
        command = execute_control_intent(
            conn,
            ControlIntent(
                command_type=ControlCommandType.WORKFLOW_CANCEL,
                requested_by_kind="http",
                requested_by_ref=f"public_api.cancel.{request.state.request_id}",
                idempotency_key=request_key,
                payload={"run_id": run_id, "include_running": True},
            ),
            approved_by="public_api.cancel",
        )
        result = render_control_command_response(conn, command, action="cancel", run_id=run_id)
    except ControlCommandIdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "error_code": exc.reason_code,
                "details": exc.details,
            },
        ) from exc
    except ControlCommandError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "error_code": exc.reason_code,
                "details": exc.details,
            },
        ) from exc

    status_code = 200
    if result.get("status") == "failed":
        if result.get("error_code") == "control.command.workflow_cancel_target_not_found":
            status_code = 404
        else:
            status_code = 409
    payload = {
        **result,
        "request_id": request.state.request_id,
        "idempotency_key": request_key,
        "links": _public_run_links(run_id),
    }
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/v1/events", tags=["public", "events"])
def public_get_events(
    type: str | None = Query(default=None, alias="type"),
    limit: int = Query(default=50, ge=1, le=1000),
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    payload = get_events(type=type, limit=limit)
    return {
        "count": payload["event_count"],
        "event_type_filter": payload["event_type_filter"],
        "limit": payload["limit"],
        "events": payload["events"],
    }


@app.get("/v1/receipts/{receipt_id}", tags=["public", "receipts"])
def public_get_receipt(
    receipt_id: str,
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    return get_receipt(receipt_id)


@app.get("/v1/catalog", tags=["public", "catalog"])
def public_get_catalog(
    _auth: str | None = Security(_require_public_api_access),
) -> dict[str, Any]:
    del _auth
    return {
        "version": _PUBLIC_API_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "auth": {
            "bearer_required": _public_api_token() is not None,
            "token_env": _PUBLIC_AUTH_TOKEN_ENV,
        },
        "routes": list_api_routes(visibility="public"),
        "runtime_catalog": build_catalog_payload(_shared_pg_conn()),
    }


@app.get("/api/costs")
def get_costs() -> dict[str, Any]:
    """Return the cost summary from the in-memory cost tracker."""
    from runtime.cost_tracker import get_cost_tracker

    return get_cost_tracker().summary()


# ---------------------------------------------------------------------------
# Handler-backed routes — explicit registrations, no catch-all wildcards.
# Each route calls into the unified handler system via _route_to_handler
# or _dispatch_standard_route.
# ---------------------------------------------------------------------------

# -- Workflows (CRUD, build, compile, refine, commit) --
@app.get("/api/workflows")
async def workflows_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflows")
async def workflows_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflows/{rest_of_path:path}")
async def workflows_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/dashboard")
async def dashboard_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflows/{rest_of_path:path}")
async def workflows_path_put(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/workflows/{rest_of_path:path}")
async def workflows_path_delete(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Files --
@app.get("/api/files")
async def files_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/files")
async def files_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/files/{rest_of_path:path}")
async def files_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/files/{rest_of_path:path}")
async def files_path_delete(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Objects --
@app.get("/api/objects")
async def objects_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/objects")
async def objects_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/objects/{rest_of_path:path}")
async def objects_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/objects/{rest_of_path:path}")
async def objects_path_put(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/objects/{rest_of_path:path}")
async def objects_path_delete(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/objects/update")
async def objects_update_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/objects/delete")
async def objects_delete(request: Request) -> Response:
    return await _route_to_handler(request)

# -- References, documents, search --
@app.get("/api/references")
async def references_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/documents")
async def documents_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/documents")
async def documents_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/documents/{doc_id}/attach")
async def documents_attach_post(request: Request, doc_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/source-options")
async def source_options_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/templates")
async def templates_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/search")
async def search_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Moon pickers (read-only datalist-backed dropdowns) --
@app.get("/api/moon/pickers/{rest_of_path:path}")
async def moon_pickers_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/registries/search")
async def registries_search_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/intent/analyze")
async def intent_analyze_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Models --
@app.get("/api/models")
async def models_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/models/market")
async def models_market_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/models/run")
async def models_run_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/models/runs/{rest_of_path:path}")
async def models_runs_path_post(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/models/runs/{rest_of_path:path}")
async def models_runs_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Integrations --
@app.get("/api/integrations")
async def integrations_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/integrations")
async def integrations_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/integrations/reload")
async def integrations_reload_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/integrations/{integration_id}")
async def integrations_describe_get(request: Request, integration_id: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/integrations/{integration_id}/secret")
async def integrations_secret_put(request: Request, integration_id: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/integrations/{integration_id}/test")
async def integrations_test_post(request: Request, integration_id: str) -> Response:
    return await _route_to_handler(request)

# -- Data Dictionary --
@app.get("/api/data-dictionary")
async def data_dictionary_list_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/reproject")
async def data_dictionary_reproject_post(request: Request) -> Response:
    return await _route_to_handler(request)

# Lineage routes must be declared BEFORE the generic
# `/api/data-dictionary/{object_kind:path}` wildcard so FastAPI's path-matcher
# routes them to the lineage handlers rather than the generic describe handler.
@app.get("/api/data-dictionary/lineage")
async def data_dictionary_lineage_summary_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/lineage/reproject")
async def data_dictionary_lineage_reproject_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/data-dictionary/lineage")
async def data_dictionary_lineage_set_edge_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/data-dictionary/lineage")
async def data_dictionary_lineage_clear_edge_delete(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/lineage/{object_kind:path}")
async def data_dictionary_lineage_describe_get(request: Request, object_kind: str) -> Response:
    return await _route_to_handler(request)

# Classifications routes: same ordering constraint as lineage — declare
# BEFORE the generic /api/data-dictionary/{object_kind:path} wildcard.
@app.get("/api/data-dictionary/classifications")
async def data_dictionary_classifications_summary_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/classifications/by-tag")
async def data_dictionary_classifications_by_tag_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/classifications/reproject")
async def data_dictionary_classifications_reproject_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/data-dictionary/classifications")
async def data_dictionary_classifications_set_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/data-dictionary/classifications")
async def data_dictionary_classifications_clear_delete(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/classifications/{object_kind:path}")
async def data_dictionary_classifications_describe_get(request: Request, object_kind: str) -> Response:
    return await _route_to_handler(request)

# Quality routes: declare BEFORE the generic /api/data-dictionary/{path} wildcard.
@app.get("/api/data-dictionary/quality")
async def data_dictionary_quality_summary_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/quality/rules")
async def data_dictionary_quality_rules_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/quality/runs")
async def data_dictionary_quality_runs_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/quality/runs/{object_kind}/{rule_kind}")
async def data_dictionary_quality_run_history_get(
    request: Request, object_kind: str, rule_kind: str,
) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/quality/reproject")
async def data_dictionary_quality_reproject_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/quality/evaluate")
async def data_dictionary_quality_evaluate_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/data-dictionary/quality")
async def data_dictionary_quality_set_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/data-dictionary/quality")
async def data_dictionary_quality_clear_delete(request: Request) -> Response:
    return await _route_to_handler(request)

# Stewardship axis — declared before the generic `/api/data-dictionary/{object_kind:path}`
# wildcard so these specific routes are not swallowed by the describe handler.
@app.get("/api/data-dictionary/stewardship")
async def data_dictionary_stewardship_summary_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/stewardship/by-steward")
async def data_dictionary_stewardship_by_steward_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/stewardship/{object_kind:path}")
async def data_dictionary_stewardship_describe_get(
    request: Request, object_kind: str,
) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/stewardship/reproject")
async def data_dictionary_stewardship_reproject_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/data-dictionary/stewardship")
async def data_dictionary_stewardship_set_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/data-dictionary/stewardship")
async def data_dictionary_stewardship_clear_delete(request: Request) -> Response:
    return await _route_to_handler(request)

# Impact axis (cross-axis blast-radius) — declared before the generic
# `/api/data-dictionary/{object_kind:path}` wildcard.
@app.get("/api/data-dictionary/impact/{object_kind:path}")
async def data_dictionary_impact_get(request: Request, object_kind: str) -> Response:
    return await _route_to_handler(request)

# Governance compliance axis — declared before the generic wildcard.
@app.get("/api/data-dictionary/governance")
async def data_dictionary_governance_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/scorecard")
async def data_dictionary_governance_scorecard_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/remediate")
async def data_dictionary_governance_remediate_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/clusters")
async def data_dictionary_governance_clusters_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/scans")
async def data_dictionary_governance_scans_list_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/scans/{scan_id}")
async def data_dictionary_governance_scan_detail_get(request: Request, scan_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/governance/pending")
async def data_dictionary_governance_pending_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/governance/drain")
async def data_dictionary_governance_drain_post(request: Request) -> Response:
    return await _route_to_handler(request)

# Drift axis (schema-snapshot detector) — declared before generic wildcard.
@app.get("/api/data-dictionary/drift")
async def data_dictionary_drift_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/drift/snapshots")
async def data_dictionary_drift_snapshots_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/drift/diff")
async def data_dictionary_drift_diff_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/drift/snapshot")
async def data_dictionary_drift_snapshot_post(request: Request) -> Response:
    return await _route_to_handler(request)

# Wiring audit — hard paths + unwired authorities. Declared before the
# generic /api/data-dictionary/{object_kind:path} wildcard.
@app.get("/api/data-dictionary/wiring-audit")
async def data_dictionary_wiring_audit_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/wiring-audit/hard-paths")
async def data_dictionary_wiring_audit_hard_paths_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/wiring-audit/decisions")
async def data_dictionary_wiring_audit_decisions_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/wiring-audit/orphans")
async def data_dictionary_wiring_audit_orphans_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/wiring-audit/trend")
async def data_dictionary_wiring_audit_trend_get(request: Request) -> Response:
    return await _route_to_handler(request)

# Audit primitive — generic scan/plan/apply surface accessible to jobs.
@app.get("/api/audit/playbook")
async def audit_playbook_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/audit/registered")
async def audit_registered_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/audit/plan")
async def audit_plan_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/audit/apply")
async def audit_apply_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/audit/contracts")
async def audit_contracts_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/audit/execute_contract")
async def audit_execute_contract_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/audit/execute_all_contracts")
async def audit_execute_all_contracts_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/data-dictionary/governance/enforce")
async def data_dictionary_governance_enforce_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/data-dictionary/{object_kind:path}")
async def data_dictionary_describe_get(request: Request, object_kind: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/data-dictionary/{object_kind}/{field_path:path}")
async def data_dictionary_set_override_put(request: Request, object_kind: str, field_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/data-dictionary/{object_kind}/{field_path:path}")
async def data_dictionary_clear_override_delete(request: Request, object_kind: str, field_path: str) -> Response:
    return await _route_to_handler(request)

# -- Chat --
@app.get("/api/chat/conversations")
async def chat_conversations_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/chat/conversations")
async def chat_conversations_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/chat/conversations/{conversation_id}")
async def chat_conversation_get(request: Request, conversation_id: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/chat/conversations/{conversation_id}/messages")
async def chat_messages_post(request: Request, conversation_id: str) -> Response:
    return await _route_to_handler(request)

# -- Triggers --
@app.get("/api/workflow-triggers")
async def workflow_triggers_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflow-triggers")
async def workflow_triggers_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflow-triggers")
async def workflow_triggers_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflow-triggers/{rest_of_path:path}")
async def workflow_triggers_path_put(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/trigger/{rest_of_path:path}")
async def trigger_post(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Manifests --
@app.post("/api/manifests/generate")
async def manifests_generate_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/generate-quick")
async def manifests_generate_quick_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/refine")
async def manifests_refine_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/save")
async def manifests_save_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/save-as")
async def manifests_save_as_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/manifests")
async def manifests_list(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/manifest-heads")
async def manifest_heads_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/manifests/history")
async def manifests_history_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/manifests/{manifest_id}")
async def manifests_get(request: Request, manifest_id: str) -> Response:
    return await _route_to_handler(request)


@app.get("/api/handoff/latest")
async def handoff_latest_get(request: Request) -> Response:
    return await _route_to_handler(request)


@app.get("/api/handoff/lineage")
async def handoff_lineage_get(request: Request) -> Response:
    return await _route_to_handler(request)


@app.get("/api/handoff/status")
async def handoff_status_get(request: Request) -> Response:
    return await _route_to_handler(request)


@app.get("/api/handoff/history")
async def handoff_history_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Checkpoints --
@app.post("/api/checkpoints")
async def checkpoints_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/checkpoints/{checkpoint_id}/approve")
async def checkpoints_approve_post(request: Request, checkpoint_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/checkpoints")
async def checkpoints_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/checkpoints/{checkpoint_id}")
async def checkpoints_detail_get(request: Request, checkpoint_id: str) -> Response:
    return await _route_to_handler(request)

# -- Bugs --
@app.get("/api/bugs")
async def bugs_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/bugs/replay-ready")
async def bugs_replay_ready_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Workflow execution (browser-initiated, routed through handler system) --
@app.post("/api/workflow-runs")
async def workflow_runs_handler_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflow-runs/spawn")
async def workflow_runs_spawn_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-runs/{run_id}/stream")
async def workflow_runs_stream_get(request: Request, run_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-runs/{run_id}/status")
async def workflow_runs_status_get(request: Request, run_id: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflows/run")
async def workflows_run_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflow-job")
async def workflow_job_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-status")
async def workflow_status_alias_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Launcher --
@app.get("/api/launcher/status")
async def launcher_status_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/agent-sessions")
def agent_sessions_index_get() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "agent_sessions",
        "base_path": "/api/agent-sessions",
        "standalone_port": 8421,
        "description": (
            "Persistent Claude session management mounted into the main Praxis API "
            "while the standalone 8421 service remains available for scripts."
        ),
        "routes": [
            "/api/agent-sessions/",
            "/api/agent-sessions/agents",
            "/api/agent-sessions/agents/{agent_id}/messages",
            "/api/agent-sessions/agents/{agent_id}/stream",
            "/api/agent-sessions/agents/{agent_id}",
        ],
    }

@app.get("/api/platform-overview")
async def platform_overview_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-templates")
async def workflow_templates_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- MCP JSON-RPC bridge --
@app.post("/mcp")
async def mcp_bridge(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Agent orient --
@app.post("/orient")
async def orient_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

# -- Subsystem APIs (standard routes: handler(subsystems, body) -> dict) --
@app.post("/query")
async def query_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/bugs")
async def bugs_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/recall")
async def recall_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/ingest")
async def ingest_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/graph")
async def graph_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/receipts")
async def receipts_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/constraints")
async def constraints_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/friction")
async def friction_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/heal")
async def heal_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/artifacts")
async def artifacts_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/decompose")
async def decompose_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/research")
async def research_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/health")
async def health_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/governance")
async def governance_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/workflow-runs")
async def workflow_runs_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/workflow-validate")
async def workflow_validate_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/status")
async def status_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/wave")
async def wave_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/generate")
async def manifest_generate_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/refine")
async def manifest_refine_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/get")
async def manifest_get_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/heartbeat")
async def heartbeat_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/session")
async def session_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.get("/api/trust")
def get_trust() -> list[dict[str, Any]]:
    """Return ELO-based trust scores for all (provider, model) pairs."""
    from runtime.trust_scoring import get_trust_scorer

    scorer = get_trust_scorer()
    scores = scorer.all_scores()
    return [
        {
            "provider_slug": s.provider_slug,
            "model_slug": s.model_slug,
            "elo_score": round(s.elo_score, 2),
            "total_runs": s.total_runs,
            "wins": s.wins,
            "losses": s.losses,
            "win_rate": round(s.win_rate, 4),
            "last_updated": s.last_updated.isoformat(),
        }
        for s in scores
    ]


@app.get("/api/reviews")
def get_reviews() -> dict[str, Any]:
    """Return author review summaries with dimension scores."""
    try:
        from runtime.review_tracker import get_review_tracker
        tracker = get_review_tracker()
        authors = tracker.author_summary()
        return {
            "authors": authors,
            "total_reviews": sum(a.get("total_reviews", 0) for a in authors),
        }
    except Exception:
        return {"authors": [], "total_reviews": 0}


@app.get("/api/fitness")
def get_fitness() -> dict[str, Any]:
    """Return capability fitness matrix."""
    try:
        from runtime.capability_router import compute_model_fitness
        fitness = compute_model_fitness()
        result = {}
        for (provider, model, cap), fit in fitness.items():
            key = f"{provider}/{model}"
            if key not in result:
                result[key] = {}
            result[key][cap] = {
                "success_rate": round(fit.success_rate, 3),
                "sample_count": fit.sample_count,
                "avg_latency_ms": fit.avg_latency_ms,
                "avg_cost_usd": round(fit.avg_cost_usd, 4),
                "fitness_score": round(fit.fitness_score, 2),
            }
        return {"models": result}
    except Exception:
        return {"models": {}}


# ---------------------------------------------------------------------------
# Job queue endpoints
# ---------------------------------------------------------------------------

@app.get("/api/queue/stats")
def queue_stats_endpoint() -> dict[str, Any]:
    """Return workflow job statistics grouped by status."""
    try:
        conn = _shared_pg_conn()
        rows = conn.execute(
            """SELECT status, COUNT(*) AS count
               FROM workflow_jobs
               GROUP BY status
               ORDER BY status"""
        )
        counts = {str(row["status"]): int(row["count"]) for row in (rows or [])}
        counts["total"] = sum(counts.values())
        return counts
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/queue/jobs")
def list_queue_jobs(
    status: str | None = Query(default=None, description="Filter by job status"),
    limit: int = Query(default=50, ge=1, le=1000),
) -> list[dict[str, Any]]:
    """List workflow jobs, optionally filtered by status."""
    try:
        conn = _shared_pg_conn()
        params: list[Any] = []
        where = ""
        if status:
            params.append(status)
            where = f"WHERE j.status = ${len(params)}"
        params.append(limit)
        rows = conn.execute(
            f"""SELECT j.id, j.run_id, j.label, j.status, j.agent_slug, j.resolved_agent,
                       j.attempt, j.max_attempts,
                       COALESCE(wr.request_envelope->>'name', wr.workflow_id) AS workflow_name,
                       j.created_at, j.ready_at, j.claimed_at, j.started_at, j.finished_at
                FROM workflow_jobs j
                LEFT JOIN workflow_runs wr ON wr.run_id = j.run_id
                {where}
                ORDER BY j.created_at DESC
                LIMIT ${len(params)}""",
            *params,
        )
        return [
            {
                "id": int(row["id"]),
                "run_id": row["run_id"],
                "workflow_name": row.get("workflow_name"),
                "label": row["label"],
                "status": row["status"],
                "agent_slug": row.get("agent_slug"),
                "resolved_agent": row.get("resolved_agent"),
                "attempt": int(row.get("attempt") or 0),
                "max_attempts": int(row.get("max_attempts") or 0),
                "created_at": _iso_or_none(row.get("created_at")),
                "ready_at": _iso_or_none(row.get("ready_at")),
                "claimed_at": _iso_or_none(row.get("claimed_at")),
                "started_at": _iso_or_none(row.get("started_at")),
                "finished_at": _iso_or_none(row.get("finished_at")),
            }
            for row in (rows or [])
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/api/queue/submit")
def submit_queue_job(req: QueueSubmitRequest) -> dict[str, Any]:
    """Submit a one-job workflow through the workflow command bus."""
    label = req.spec.label or "api_queue_job"
    task_type = req.spec.task_type or "build"
    if req.spec.model_slug and req.spec.provider_slug:
        agent = f"{req.spec.provider_slug}/{req.spec.model_slug}"
    elif req.spec.model_slug:
        agent = f"{_default_workflow_provider_slug()}/{req.spec.model_slug}"
    else:
        agent = f"auto/{task_type}"
    spec = {
        "name": label,
        "workflow_id": f"workflow.api.{label.lower().replace(' ', '.')}",
        "phase": task_type,
        "workspace_ref": req.spec.workspace_ref,
        "runtime_profile_ref": req.spec.runtime_profile_ref,
        "jobs": [
            {
                "label": label,
                "agent": agent,
                "prompt": req.spec.prompt,
                "read_scope": req.spec.scope_read or [],
                "write_scope": req.spec.scope_write or [],
                "max_attempts": req.max_attempts,
            }
        ],
    }

    try:
        conn = _shared_pg_conn()
        result = _submit_workflow_via_service_bus(
            SimpleNamespace(get_pg_conn=lambda: conn),
            inline_spec=spec,
            spec_name=label,
            total_jobs=len(spec["jobs"]),
            requested_by_kind="http",
            requested_by_ref="queue_submit",
        )

        if result.get("error"):
            raise RuntimeError(str(result["error"]))

        return {
            "run_id": result["run_id"],
            "status": result["status"],
            "command_id": result["command_id"],
            "priority": req.priority,
            "note": "priority is accepted for compatibility but scheduling is workflow-runtime driven",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/queue/cancel/{job_id}")
def cancel_queue_job(job_id: str) -> JSONResponse:
    """Cancel a queue-backed workflow through the workflow command bus."""
    run_id: str | None = None
    try:
        from runtime.control_commands import (
            ControlCommandType,
            ControlIntent,
            execute_control_intent,
            render_control_command_failure,
            render_control_command_response,
        )

        conn = _shared_pg_conn()
        rows = conn.execute(
            """SELECT run_id
               FROM workflow_jobs
               WHERE id = $1::bigint
               LIMIT 1""",
            job_id,
        )
    except Exception as exc:
        try:
            from runtime.control_commands import render_control_command_failure

            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=run_id,
                job_id=job_id,
            )
            return JSONResponse(status_code=500, content=failure)
        except Exception:
            raise HTTPException(status_code=500, detail=str(exc))

    if rows:
        run_id = str(rows[0]["run_id"])
        try:
            command = execute_control_intent(
                conn,
                ControlIntent(
                    command_type=ControlCommandType.WORKFLOW_CANCEL,
                    requested_by_kind="http",
                    requested_by_ref="queue_cancel",
                    idempotency_key=f"workflow.cancel.http.{job_id}",
                    payload={"run_id": run_id, "include_running": True},
                ),
                approved_by="http.queue_cancel",
            )
        except Exception as exc:
            details = getattr(exc, "details", None)
            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=run_id,
                job_id=job_id,
                details=details if isinstance(details, dict) else None,
            )
            return JSONResponse(status_code=500, content=failure)

        result = render_control_command_response(
            conn,
            command,
            action="cancel",
            run_id=run_id,
            job_id=job_id,
        )
        status_code = 200 if result.get("status") == "cancelled" else 409
        return JSONResponse(status_code=status_code, content=result)

    failure = render_control_command_failure(
        error_code="control.command.workflow_cancel_target_not_found",
        error_detail=f"Job {job_id!r} not found or already in a terminal state",
        job_id=job_id,
    )
    return JSONResponse(status_code=404, content=failure)


# ---------------------------------------------------------------------------
# Observability endpoints
# ---------------------------------------------------------------------------

@app.get("/api/metrics")
def get_metrics(days: int = Query(default=7, ge=1)) -> dict[str, Any]:
    """Return the core metrics summary for the last N days."""
    from runtime.observability import get_workflow_metrics_view

    view = get_workflow_metrics_view()
    return {
        "pass_rate_by_model": view.pass_rate_by_model(days=days),
        "cost_by_agent": view.cost_by_agent(days=days),
        "latency_percentiles": view.latency_percentiles(days=days),
        "efficiency_summary": view.efficiency_summary(days=days),
        "failure_category_breakdown": view.failure_category_breakdown(days=days),
        "hourly_workflow_volume": view.hourly_workflow_volume(days=days),
        "capability_distribution": view.capability_distribution(days=days),
    }


@app.get("/api/metrics/surface-usage")
def get_surface_usage_metrics(
    days: int = Query(default=30, ge=1),
    entrypoint: str | None = Query(default=None),
    event_limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    """Return durable frontdoor surface-usage counters for the last N days."""

    repo = PostgresWorkflowSurfaceUsageRepository(_shared_pg_conn())
    entries = [
        _serialize_surface_usage_row(row)
        for row in repo.list_usage_rollup(days=days, entrypoint_name=entrypoint)
    ]
    daily = [
        _serialize_surface_usage_row(row)
        for row in repo.list_usage_daily(days=days, entrypoint_name=entrypoint)
    ]
    recent_events = [
        _serialize_surface_usage_event_row(row)
        for row in repo.list_usage_events(
            days=days,
            entrypoint_name=entrypoint,
            limit=event_limit,
        )
    ]
    query_routing_quality = [
        {
            "entrypoint_name": row.get("entrypoint_name"),
            "caller_kind": row.get("caller_kind"),
            "routed_to": row.get("routed_to") or None,
            "result_state": row.get("result_state"),
            "reason_code": row.get("reason_code") or None,
            "invocation_count": int(row.get("invocation_count") or 0),
            "success_count": int(row.get("success_count") or 0),
            "average_query_chars": float(row.get("average_query_chars") or 0.0),
            "total_result_count": int(row.get("total_result_count") or 0),
        }
        for row in repo.summarize_query_routing(days=days)
    ]
    builder_funnels = repo.summarize_builder_funnels(days=days)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "filters": {
            key: value
            for key, value in {
                "entrypoint": (entrypoint or "").strip() or None,
                "event_limit": event_limit,
            }.items()
            if value is not None
        },
        "totals": {
            "entry_count": len(entries),
            "invocation_count": sum(row["invocation_count"] for row in entries),
            "success_count": sum(row["success_count"] for row in entries),
            "client_error_count": sum(row["client_error_count"] for row in entries),
            "server_error_count": sum(row["server_error_count"] for row in entries),
            "event_count": len(recent_events),
        },
        "entries": entries,
        "daily": daily,
        "recent_events": recent_events,
        "query_routing_quality": query_routing_quality,
        "builder_funnels": builder_funnels,
    }


@app.get("/api/metrics/heatmap")
def get_metrics_heatmap(days: int = Query(default=7, ge=1)) -> list[dict[str, Any]]:
    """Return the failure code x provider heatmap for the last N days."""
    from runtime.observability import get_workflow_metrics_view

    view = get_workflow_metrics_view()
    return view.failure_heatmap(days=days)


@app.get("/api/observability/code-hotspots")
def get_code_hotspots(
    limit: int = Query(default=20, ge=1, le=200),
    roots: str | None = Query(
        default=None,
        description="Comma-separated repo roots to scan (defaults to runtime,surfaces/api,surfaces/cli)",
    ),
    path_prefix: str | None = Query(
        default=None,
        description="Optional repo-relative path prefix to filter hotspot results",
    ),
) -> dict[str, Any]:
    """Return merged code hotspot rollups across static health, receipt risk, and bug packets."""
    from runtime.engineering_observability import build_code_hotspots

    subsystems = _ensure_shared_subsystems(app)
    bug_tracker = None
    if subsystems is not None:
        try:
            bug_tracker = subsystems.get_bug_tracker()
        except Exception:
            bug_tracker = None

    roots_list = [part.strip() for part in (roots or "").split(",") if part.strip()] or None
    return build_code_hotspots(
        repo_root=REPO_ROOT,
        bug_tracker=bug_tracker,
        limit=limit,
        roots=roots_list,
        path_prefix=path_prefix,
    )


@app.get("/api/observability/bug-scoreboard")
def get_bug_scoreboard(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    """Return aggregate bug observability focused on replay readiness, regressions, and recurrence."""
    from runtime.engineering_observability import build_bug_scoreboard

    subsystems = _ensure_shared_subsystems(app)
    bug_tracker = None
    if subsystems is not None:
        try:
            bug_tracker = subsystems.get_bug_tracker()
        except Exception:
            bug_tracker = None
    return build_bug_scoreboard(
        bug_tracker=bug_tracker,
        limit=limit,
        repo_root=REPO_ROOT,
    )


@app.get("/api/observability/platform")
def get_platform_observability() -> dict[str, Any]:
    """Return operator-facing platform probe status with lane cues and degraded causes."""
    from runtime.engineering_observability import build_platform_observability
    from surfaces.api.handlers.workflow_admin import _handle_health

    subsystems = _ensure_shared_subsystems(app)
    payload = None
    error = None
    if subsystems is not None:
        try:
            payload = _handle_health(subsystems, {})
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
    return build_platform_observability(platform_payload=payload, error=error)


@app.get("/api/events")
def get_events(
    type: str | None = Query(
        default=None,
        alias="type",
        description="Filter by event type (e.g. workflow.failed)",
    ),
    limit: int = Query(default=50, ge=1, le=1000),
) -> dict[str, Any]:
    """Return recent platform events from the durable event log."""
    from runtime.event_log import read_since, read_all_since
    from storage.dev_postgres import get_sync_connection

    try:
        conn = get_sync_connection()
        if type:
            # Filter by channel (event_type in old API maps to channel)
            events = read_since(conn, channel=type, limit=limit)
        else:
            events = read_all_since(conn, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return {
        "event_type_filter": type,
        "limit": limit,
        "event_count": len(events),
        "events": [e.to_dict() for e in events],
    }


@app.get("/api/receipts")
def list_receipts(limit: int = Query(default=20, ge=1, le=500)) -> list[dict[str, Any]]:
    """Return a listing of recent workflow receipts (metadata, not full content)."""
    from runtime.receipt_store import list_receipts as _list_receipts

    try:
        records = _list_receipts(limit=limit)
        return [
            {"id": r.id, "label": r.label, "agent": r.agent, "status": r.status,
             "timestamp": r.timestamp.isoformat() if r.timestamp else None,
             "run_id": r.run_id}
            for r in records
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    """Return the full JSON content of one receipt by id."""
    from runtime.receipt_store import load_receipt

    rec = load_receipt(receipt_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Receipt not found: {receipt_id}")
    return rec.to_dict()


# ---------------------------------------------------------------------------
# Operations endpoints
# ---------------------------------------------------------------------------

def _health_db_snapshot(*, timeout_s: float = 2.0) -> tuple[list[dict[str, Any]], str]:
    """Fast health probe that does not depend on shared API subsystems."""
    statement_timeout_ms = max(1, int(timeout_s * 1000))

    async def _collect() -> tuple[list[dict[str, Any]], str]:
        import asyncpg

        from storage.postgres.connection import resolve_workflow_database_url

        checks: list[dict[str, Any]] = []
        overall = "healthy"
        database_url = resolve_workflow_database_url()
        conn = await asyncpg.connect(
            database_url,
            timeout=timeout_s,
            command_timeout=timeout_s,
        )
        try:
            await conn.execute(f"SET statement_timeout = {statement_timeout_ms}")
            await conn.fetchval("SELECT 1")
            checks.append({"name": "postgres", "ok": True})

            active = int(
                await conn.fetchval(
                    "SELECT count(*) FROM workflow_jobs WHERE heartbeat_at > now() - interval '5 minutes'"
                )
                or 0
            )
            recent_claims = int(
                await conn.fetchval(
                    "SELECT count(*) FROM workflow_jobs WHERE claimed_at > now() - interval '10 minutes'"
                )
                or 0
            )
            ready_cnt = int(
                await conn.fetchval("SELECT count(*) FROM workflow_jobs WHERE status = 'ready'") or 0
            )
            worker_alive = active > 0 or recent_claims > 0 or ready_cnt == 0
            checks.append(
                {
                    "name": "worker",
                    "ok": worker_alive,
                    "active_jobs": active,
                    "ready_jobs": ready_cnt,
                }
            )
            if not worker_alive:
                overall = "degraded"

            row = await conn.fetchrow(
                """
                SELECT count(*) as total,
                       count(*) FILTER (WHERE status = 'succeeded') as passed,
                       count(*) FILTER (WHERE status IN ('failed', 'dead_letter')) as failed
                FROM workflow_jobs
                WHERE created_at > now() - interval '24 hours'
                """
            )
            total = int(row["total"]) if row else 0
            passed = int(row["passed"]) if row else 0
            failed = int(row["failed"]) if row else 0
            pass_rate = round(passed / total, 3) if total > 0 else 1.0
            checks.append(
                {
                    "name": "workflow",
                    "ok": True,
                    "total": total,
                    "passed": passed,
                    "failed": failed,
                    "pass_rate": pass_rate,
                }
            )
            return checks, overall
        finally:
            await conn.close()

    try:
        from storage.postgres.connection import _run_sync

        return _run_sync(_collect())
    except Exception as exc:
        return ([{"name": "postgres", "ok": False, "error": str(exc)[:200]}], "unhealthy")


@app.get("/api/health")
def health_check_endpoint() -> Any:
    """Platform health from bounded Postgres probes."""
    now = datetime.now(timezone.utc)
    checks, overall = _health_db_snapshot()
    if overall == "unhealthy":
        return JSONResponse(status_code=503, content={
            "status": overall, "checks": checks, "timestamp": now.isoformat(),
        })

    import shutil

    usage = shutil.disk_usage(str(Path(__file__).resolve().parents[3]))
    free_gb = round(usage.free / (1024**3), 1)
    checks.append({"name": "disk", "ok": free_gb > 5, "free_gb": free_gb})
    if free_gb <= 5:
        overall = "degraded"

    status_code = 200 if overall == "healthy" else (200 if overall == "degraded" else 503)
    return JSONResponse(status_code=status_code, content={
        "status": overall, "checks": checks, "timestamp": now.isoformat(),
    })


@app.get("/api/scope")
def resolve_scope_endpoint(
    files: list[str] = Query(
        description="Write-scope files to resolve (repeat param for multiple)"
    ),
    root: str = Query(default=".", description="Project root directory"),
) -> dict[str, Any]:
    """Resolve read scope, blast radius, and test scope for write-scope files."""
    from runtime.scope_resolver import resolve_scope as _resolve_scope

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    try:
        resolution = _resolve_scope(files, root_dir=root)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "write_scope": resolution.write_scope,
        "computed_read_scope": resolution.computed_read_scope,
        "test_scope": resolution.test_scope,
        "blast_radius": resolution.blast_radius,
        "context_sections": [
            {"name": s["name"], "content_length": len(s["content"])}
            for s in resolution.context_sections
        ],
    }


@app.get("/api/routes")
def get_routes(
    search: str | None = Query(default=None, description="Substring search across route path, name, summary, description, and tags."),
    method: str | None = Query(default=None, description="Filter to one HTTP method such as GET or POST."),
    tag: str | None = Query(default=None, description="Filter to routes carrying a specific FastAPI tag."),
    path_prefix: str | None = Query(default=None, description="Filter to routes whose path starts with this prefix."),
    visibility: str = Query(default="public", description="Route visibility slice: public, internal, or all."),
) -> dict[str, Any]:
    """Return the live HTTP route catalog for CLI and API discovery."""

    return list_api_routes(
        search=search,
        method=method,
        tag=tag,
        path_prefix=path_prefix,
        visibility=visibility,
    )


@app.get("/api/catalog")
def get_catalog() -> dict[str, Any]:
    """Return live catalog items from platform registries + static primitives."""
    return build_catalog_payload(_shared_pg_conn())


@app.get("/api/catalog/operations")
def get_operation_catalog() -> dict[str, Any]:
    """Return DB-backed CQRS operation definitions and source policies."""
    return build_operation_catalog_payload(_shared_pg_conn())


@app.get("/api/operate/catalog")
def get_operate_catalog() -> dict[str, Any]:
    """Return the unified operator gateway catalog."""
    return build_operate_catalog_payload()


@app.post("/api/operate")
def post_operate(
    body: OperateRequest,
    x_workflow_token: str | None = Header(default=None, alias="X-Workflow-Token"),
) -> JSONResponse:
    """Call one catalog-backed operator operation through the unified gateway."""
    status_code, payload = execute_operate_request(
        body,
        header_workflow_token=x_workflow_token,
    )
    return JSONResponse(status_code=status_code, content=jsonable_encoder(payload))


@app.get("/api/catalog/review-decisions")
async def catalog_review_decisions_get(request: Request) -> Response:
    return await _route_to_handler(request)


@app.post("/api/catalog/review-decisions")
async def catalog_review_decisions_post(request: Request) -> Response:
    return await _route_to_handler(request)


# No catch-all routes — every endpoint is explicitly registered above.


@app.get("/", response_model=None)
def root_redirect() -> Response:
    return RedirectResponse(url="/app")

@app.get("/app", response_model=None)
def launcher_app_root() -> Any:
    return _launcher_index_response()


@app.get("/app/", response_model=None)
def launcher_app_root_slash() -> Any:
    return _launcher_index_response()


@app.get("/app/{path:path}", response_model=None)
def launcher_app_path(path: str = "") -> Any:
    del path
    return _launcher_index_response()


def _apply_route_visibility_policy(target_app: FastAPI | None = None) -> None:
    resolved_app = target_app or app
    for route in resolved_app.routes:
        if not isinstance(route, APIRoute):
            continue
        visibility = "public" if _is_public_request_path(route.path) else "internal"
        openapi_extra = dict(route.openapi_extra or {})
        openapi_extra["x-praxis-visibility"] = visibility
        route.openapi_extra = openapi_extra
        route.include_in_schema = visibility == "public"

    resolved_app.openapi_schema = None


_apply_route_visibility_policy(app)
