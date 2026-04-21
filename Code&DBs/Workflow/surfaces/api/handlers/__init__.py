"""Grouped handler registries for the workflow HTTP API."""

from __future__ import annotations

from typing import Any

from ._surface_usage import record_api_route_usage as _record_api_route_usage
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
from .data_dictionary_impact_admin import DATA_DICTIONARY_IMPACT_GET_ROUTES
from .data_dictionary_governance_admin import (
    DATA_DICTIONARY_GOVERNANCE_GET_ROUTES,
    DATA_DICTIONARY_GOVERNANCE_POST_ROUTES,
)
from .data_dictionary_drift_admin import (
    DATA_DICTIONARY_DRIFT_GET_ROUTES,
    DATA_DICTIONARY_DRIFT_POST_ROUTES,
)
from .data_dictionary_wiring_audit_admin import (
    DATA_DICTIONARY_WIRING_AUDIT_GET_ROUTES,
)
from .audit_primitive_admin import (
    AUDIT_PRIMITIVE_GET_ROUTES,
    AUDIT_PRIMITIVE_POST_ROUTES,
)
from .integrations_admin import (
    INTEGRATIONS_GET_ROUTES,
    INTEGRATIONS_POST_ROUTES,
    INTEGRATIONS_PUT_ROUTES,
)
from .moon_pickers import MOON_PICKERS_GET_ROUTES
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
from ._dispatch import _dispatch_dynamic, _dispatch_standard_post


ROUTES: dict[str, object] = {}
ROUTES.update(ADMIN_ROUTES)
ROUTES.update(RUN_ROUTES)
ROUTES.update(QUERY_ROUTES)
ROUTES.update(NOTIFY_ROUTES)

POST_ROUTE_HANDLERS = [
    *MCP_POST_ROUTES,
    *NOTIFY_POST_ROUTES,
    *RUN_POST_ROUTES,
    *QUERY_POST_ROUTES,
    # Lineage / classifications routes must precede the generic data-dictionary
    # prefix routes so `/api/data-dictionary/lineage/...` and
    # `/api/data-dictionary/classifications/...` are not swallowed by the
    # describe handler that prefix-matches on `/api/data-dictionary/`.
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
]

PUT_ROUTE_HANDLERS = [
    *DATA_DICTIONARY_LINEAGE_PUT_ROUTES,
    *DATA_DICTIONARY_CLASSIFICATIONS_PUT_ROUTES,
    *DATA_DICTIONARY_QUALITY_PUT_ROUTES,
    *DATA_DICTIONARY_STEWARDSHIP_PUT_ROUTES,
    *DATA_DICTIONARY_PUT_ROUTES,
    *INTEGRATIONS_PUT_ROUTES,
    *QUERY_PUT_ROUTES,
]

GET_ROUTE_HANDLERS = [
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
]

DELETE_ROUTE_HANDLERS = [
    *DATA_DICTIONARY_LINEAGE_DELETE_ROUTES,
    *DATA_DICTIONARY_CLASSIFICATIONS_DELETE_ROUTES,
    *DATA_DICTIONARY_QUALITY_DELETE_ROUTES,
    *DATA_DICTIONARY_STEWARDSHIP_DELETE_ROUTES,
    *DATA_DICTIONARY_DELETE_ROUTES,
    *QUERY_DELETE_ROUTES,
]


def handle_post_request(request: Any, path: str) -> bool:
    return _dispatch_dynamic(POST_ROUTE_HANDLERS, request, path) or _dispatch_standard_post(
        request,
        path,
        ROUTES,
        record_api_route_usage=_record_api_route_usage,
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
