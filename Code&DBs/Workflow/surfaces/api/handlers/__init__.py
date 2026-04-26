"""Grouped handler registries for the workflow HTTP API."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from typing import Any

from ._dispatch import _dispatch_dynamic, _dispatch_standard_post


def _record_api_route_usage(*args: Any, **kwargs: Any) -> Any:
    from ._surface_usage import record_api_route_usage

    return record_api_route_usage(*args, **kwargs)


_ROUTE_STATE: dict[str, object] | None = None


def _route_state() -> dict[str, object]:
    global _ROUTE_STATE
    if _ROUTE_STATE is not None:
        return _ROUTE_STATE

    from .audit_primitive_admin import AUDIT_PRIMITIVE_GET_ROUTES, AUDIT_PRIMITIVE_POST_ROUTES
    from .data_dictionary_admin import (
        DATA_DICTIONARY_DELETE_ROUTES,
        DATA_DICTIONARY_GET_ROUTES,
        DATA_DICTIONARY_POST_ROUTES,
        DATA_DICTIONARY_PUT_ROUTES,
    )
    from .data_dictionary_classifications_admin import (
        DATA_DICTIONARY_CLASSIFICATIONS_DELETE_ROUTES,
        DATA_DICTIONARY_CLASSIFICATIONS_GET_ROUTES,
        DATA_DICTIONARY_CLASSIFICATIONS_POST_ROUTES,
        DATA_DICTIONARY_CLASSIFICATIONS_PUT_ROUTES,
    )
    from .data_dictionary_drift_admin import (
        DATA_DICTIONARY_DRIFT_GET_ROUTES,
        DATA_DICTIONARY_DRIFT_POST_ROUTES,
    )
    from .data_dictionary_governance_admin import (
        DATA_DICTIONARY_GOVERNANCE_GET_ROUTES,
        DATA_DICTIONARY_GOVERNANCE_POST_ROUTES,
    )
    from .data_dictionary_impact_admin import DATA_DICTIONARY_IMPACT_GET_ROUTES
    from .data_dictionary_lineage_admin import (
        DATA_DICTIONARY_LINEAGE_DELETE_ROUTES,
        DATA_DICTIONARY_LINEAGE_GET_ROUTES,
        DATA_DICTIONARY_LINEAGE_POST_ROUTES,
        DATA_DICTIONARY_LINEAGE_PUT_ROUTES,
    )
    from .data_dictionary_quality_admin import (
        DATA_DICTIONARY_QUALITY_DELETE_ROUTES,
        DATA_DICTIONARY_QUALITY_GET_ROUTES,
        DATA_DICTIONARY_QUALITY_POST_ROUTES,
        DATA_DICTIONARY_QUALITY_PUT_ROUTES,
    )
    from .data_dictionary_stewardship_admin import (
        DATA_DICTIONARY_STEWARDSHIP_DELETE_ROUTES,
        DATA_DICTIONARY_STEWARDSHIP_GET_ROUTES,
        DATA_DICTIONARY_STEWARDSHIP_POST_ROUTES,
        DATA_DICTIONARY_STEWARDSHIP_PUT_ROUTES,
    )
    from .data_dictionary_wiring_audit_admin import DATA_DICTIONARY_WIRING_AUDIT_GET_ROUTES
    from .integrations_admin import (
        INTEGRATIONS_GET_ROUTES,
        INTEGRATIONS_POST_ROUTES,
        INTEGRATIONS_PUT_ROUTES,
    )
    from .moon_pickers import MOON_PICKERS_GET_ROUTES
    from .shell_routes_handler import SHELL_ROUTES_GET_ROUTES
    from .workflow_admin import ADMIN_GET_ROUTES, ADMIN_POST_ROUTES, ADMIN_ROUTES
    from .workflow_mcp import MCP_POST_ROUTES
    from .workflow_notify import NOTIFY_GET_ROUTES, NOTIFY_POST_ROUTES, NOTIFY_ROUTES
    from .workflow_query_routes import (
        QUERY_DELETE_ROUTES,
        QUERY_GET_ROUTES,
        QUERY_POST_ROUTES,
        QUERY_PUT_ROUTES,
        QUERY_ROUTES,
    )
    from .workflow_run import RUN_GET_ROUTES, RUN_POST_ROUTES, RUN_ROUTES

    routes: dict[str, object] = {}
    routes.update(ADMIN_ROUTES)
    routes.update(RUN_ROUTES)
    routes.update(QUERY_ROUTES)
    routes.update(NOTIFY_ROUTES)

    _ROUTE_STATE = {
        "admin_routes": ADMIN_ROUTES,
        "notify_routes": NOTIFY_ROUTES,
        "query_routes": QUERY_ROUTES,
        "run_routes": RUN_ROUTES,
        "routes": routes,
        "post": [
            *MCP_POST_ROUTES,
            *NOTIFY_POST_ROUTES,
            *RUN_POST_ROUTES,
            *QUERY_POST_ROUTES,
            # Lineage / classifications routes must precede the generic data-dictionary
            # prefix routes so specialized paths are not swallowed by describe handlers.
            *DATA_DICTIONARY_LINEAGE_POST_ROUTES,
            *DATA_DICTIONARY_CLASSIFICATIONS_POST_ROUTES,
            *DATA_DICTIONARY_QUALITY_POST_ROUTES,
            *DATA_DICTIONARY_STEWARDSHIP_POST_ROUTES,
            *DATA_DICTIONARY_GOVERNANCE_POST_ROUTES,
            *DATA_DICTIONARY_DRIFT_POST_ROUTES,
            *AUDIT_PRIMITIVE_POST_ROUTES,
            *DATA_DICTIONARY_POST_ROUTES,
            *INTEGRATIONS_POST_ROUTES,
            *ADMIN_POST_ROUTES,
        ],
        "put": [
            *DATA_DICTIONARY_LINEAGE_PUT_ROUTES,
            *DATA_DICTIONARY_CLASSIFICATIONS_PUT_ROUTES,
            *DATA_DICTIONARY_QUALITY_PUT_ROUTES,
            *DATA_DICTIONARY_STEWARDSHIP_PUT_ROUTES,
            *DATA_DICTIONARY_PUT_ROUTES,
            *INTEGRATIONS_PUT_ROUTES,
            *QUERY_PUT_ROUTES,
        ],
        "get": [
            *SHELL_ROUTES_GET_ROUTES,
            *MOON_PICKERS_GET_ROUTES,
            *QUERY_GET_ROUTES,
            *DATA_DICTIONARY_LINEAGE_GET_ROUTES,
            *DATA_DICTIONARY_CLASSIFICATIONS_GET_ROUTES,
            *DATA_DICTIONARY_QUALITY_GET_ROUTES,
            *DATA_DICTIONARY_STEWARDSHIP_GET_ROUTES,
            *DATA_DICTIONARY_IMPACT_GET_ROUTES,
            *DATA_DICTIONARY_GOVERNANCE_GET_ROUTES,
            *DATA_DICTIONARY_DRIFT_GET_ROUTES,
            *DATA_DICTIONARY_WIRING_AUDIT_GET_ROUTES,
            *AUDIT_PRIMITIVE_GET_ROUTES,
            *DATA_DICTIONARY_GET_ROUTES,
            *INTEGRATIONS_GET_ROUTES,
            *ADMIN_GET_ROUTES,
            *NOTIFY_GET_ROUTES,
            *RUN_GET_ROUTES,
        ],
        "delete": [
            *DATA_DICTIONARY_LINEAGE_DELETE_ROUTES,
            *DATA_DICTIONARY_CLASSIFICATIONS_DELETE_ROUTES,
            *DATA_DICTIONARY_QUALITY_DELETE_ROUTES,
            *DATA_DICTIONARY_STEWARDSHIP_DELETE_ROUTES,
            *DATA_DICTIONARY_DELETE_ROUTES,
            *QUERY_DELETE_ROUTES,
        ],
    }
    return _ROUTE_STATE


class _LazyRouteList(Sequence[Any]):
    def __init__(self, key: str) -> None:
        self._key = key

    def _routes(self) -> list[Any]:
        return list(_route_state()[self._key])

    def __getitem__(self, index):
        return self._routes()[index]

    def __len__(self) -> int:
        return len(self._routes())

    def __iter__(self) -> Iterator[Any]:
        return iter(self._routes())


class _LazyRouteMap(Mapping[str, object]):
    def __init__(self, key: str) -> None:
        self._key = key

    def _routes(self) -> Mapping[str, object]:
        return _route_state()[self._key]  # type: ignore[return-value]

    def _mutable_routes(self) -> MutableMapping[str, object]:
        return _route_state()[self._key]  # type: ignore[return-value]

    def __getitem__(self, key: str) -> object:
        return self._routes()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._routes())

    def __len__(self) -> int:
        return len(self._routes())

    def __contains__(self, key: object) -> bool:
        return key in self._routes()

    def __setitem__(self, key: str, value: object) -> None:
        self._mutable_routes()[key] = value

    def __delitem__(self, key: str) -> None:
        del self._mutable_routes()[key]


ADMIN_ROUTES = _LazyRouteMap("admin_routes")
DELETE_ROUTE_HANDLERS = _LazyRouteList("delete")
GET_ROUTE_HANDLERS = _LazyRouteList("get")
NOTIFY_ROUTES = _LazyRouteMap("notify_routes")
POST_ROUTE_HANDLERS = _LazyRouteList("post")
PUT_ROUTE_HANDLERS = _LazyRouteList("put")
QUERY_ROUTES = _LazyRouteMap("query_routes")
ROUTES = _LazyRouteMap("routes")
RUN_ROUTES = _LazyRouteMap("run_routes")
POST_ROUTES_REQUIRING_BODY = frozenset(
    {
        "/api/operator/roadmap-write",
        "/api/operator/native-primary-cutover-gate",
        "/api/operator/work-item-closeout",
        "/api/operator/provider-onboarding",
        "/api/operator/decision",
        "/api/operator/architecture-policy",
        "/api/operator/functional-area",
        "/api/operator/object-relation",
    }
)


def handle_post_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(POST_ROUTE_HANDLERS, request, path) or _dispatch_standard_post(
        request,
        path,
        ROUTES,
        record_api_route_usage=_record_api_route_usage,
        required_body_paths=POST_ROUTES_REQUIRING_BODY,
    )


def handle_get_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(GET_ROUTE_HANDLERS, request, path)


def handle_put_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(PUT_ROUTE_HANDLERS, request, path)


def handle_delete_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(DELETE_ROUTE_HANDLERS, request, path)


def path_is_known(path: str) -> bool:
    if path in ROUTES:
        return True
    return any(
        matches(path)
        for routes in (
            POST_ROUTE_HANDLERS,
            PUT_ROUTE_HANDLERS,
            GET_ROUTE_HANDLERS,
            DELETE_ROUTE_HANDLERS,
        )
        for matches, _handler in routes
    )


__all__ = [
    "ADMIN_ROUTES",
    "DELETE_ROUTE_HANDLERS",
    "GET_ROUTE_HANDLERS",
    "NOTIFY_ROUTES",
    "POST_ROUTE_HANDLERS",
    "PUT_ROUTE_HANDLERS",
    "QUERY_ROUTES",
    "ROUTES",
    "RUN_ROUTES",
    "handle_delete_request",
    "handle_get_request",
    "handle_post_request",
    "handle_put_request",
    "path_is_known",
]
