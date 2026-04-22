"""Inspectable authority contract for mutating API routes.

This is a boundary guard, not another dispatcher. FastAPI routes may write
durable state only when their authority path is explicit here or when the
route is mounted from the DB-backed operation catalog.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class ApiAuthorityBoundaryError(RuntimeError):
    """Raised when a mutating API route lacks an authority declaration."""

    def __init__(self, message: str, *, drift: dict[str, Any]) -> None:
        super().__init__(message)
        self.drift = drift


@dataclass(frozen=True, slots=True)
class ApiRouteAuthority:
    method: str
    path: str
    boundary_kind: str
    authority_domain_ref: str
    owner_ref: str
    receipt_policy: str
    event_policy: str
    decision_ref: str
    operation_name: str | None = None
    projection_ref: str | None = None
    legacy_status: str = "active"
    migration_target: str | None = None
    notes: str = ""

    @property
    def route_key(self) -> str:
        return route_key(self.method, self.path)

    def to_payload(self, *, route_name: str | None = None, mounted: bool = True) -> dict[str, Any]:
        payload = asdict(self)
        payload["route_key"] = self.route_key
        payload["route_name"] = route_name
        payload["mounted"] = mounted
        return payload


def route_key(method: str, path: str) -> str:
    return f"{method.strip().upper()} {path.strip()}"


def _api(
    method: str,
    path: str,
    boundary_kind: str,
    authority_domain_ref: str,
    *,
    receipt_policy: str = "domain_authority",
    event_policy: str = "domain_authority",
    owner_ref: str = "praxis.engine.api",
    decision_ref: str = "decision.cqrs_authority_unification.20260422",
    operation_name: str | None = None,
    projection_ref: str | None = None,
    legacy_status: str = "active",
    migration_target: str | None = None,
    notes: str = "",
) -> ApiRouteAuthority:
    return ApiRouteAuthority(
        method=method,
        path=path,
        boundary_kind=boundary_kind,
        authority_domain_ref=authority_domain_ref,
        owner_ref=owner_ref,
        receipt_policy=receipt_policy,
        event_policy=event_policy,
        decision_ref=decision_ref,
        operation_name=operation_name,
        projection_ref=projection_ref,
        legacy_status=legacy_status,
        migration_target=migration_target,
        notes=notes,
    )


def _handler(
    method: str,
    path: str,
    authority_domain_ref: str,
    *,
    migration_target: str = "operation_catalog_gateway",
    notes: str = "Legacy handler-router route; explicit while adoption moves write paths into cataloged operations.",
) -> ApiRouteAuthority:
    return _api(
        method,
        path,
        "legacy_handler_router",
        authority_domain_ref,
        legacy_status="legacy_visible",
        migration_target=migration_target,
        notes=notes,
    )


def _standard(
    path: str,
    authority_domain_ref: str,
    *,
    notes: str = "Standard POST dispatcher route; explicit while it is migrated behind cataloged operation bindings.",
) -> ApiRouteAuthority:
    return _api(
        "POST",
        path,
        "legacy_standard_dispatch",
        authority_domain_ref,
        legacy_status="legacy_visible",
        migration_target="operation_catalog_gateway",
        notes=notes,
    )


_API_ROUTE_AUTHORITY_ROWS: tuple[ApiRouteAuthority, ...] = (
    _api("POST", "/api/webhooks/endpoints", "domain_authority", "authority.webhook_ingest"),
    _api("POST", "/api/webhooks/{slug}", "domain_authority", "authority.webhook_ingest"),
    _api("POST", "/api/launcher/recover", "domain_authority", "authority.launcher"),
    _api("POST", "/v1/runs", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.submit"),
    _api("POST", "/v1/runs/{run_id}:cancel", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.cancel"),
    _handler("POST", "/api/workflows", "authority.workflow_definitions"),
    _handler("PUT", "/api/workflows/{rest_of_path:path}", "authority.workflow_definitions"),
    _handler("DELETE", "/api/workflows/{rest_of_path:path}", "authority.workflow_definitions"),
    _handler("POST", "/api/files", "authority.files"),
    _handler("DELETE", "/api/files/{rest_of_path:path}", "authority.files"),
    _handler("POST", "/api/objects", "authority.object_schema", migration_target="object_schema.* operation bindings"),
    _handler("PUT", "/api/objects/{rest_of_path:path}", "authority.object_schema", migration_target="object_schema.* operation bindings"),
    _handler("DELETE", "/api/objects/{rest_of_path:path}", "authority.object_schema", migration_target="object_schema.* operation bindings"),
    _handler("PUT", "/api/objects/update", "authority.object_schema", migration_target="object_schema.field_upsert"),
    _handler("DELETE", "/api/objects/delete", "authority.object_schema", migration_target="object_schema.type_delete"),
    _handler("POST", "/api/documents", "authority.structured_documents", migration_target="structured_documents.* operation bindings"),
    _handler("POST", "/api/documents/{doc_id}/attach", "authority.structured_documents", migration_target="structured_documents.* operation bindings"),
    _handler("POST", "/api/models/run", "authority.model_execution"),
    _handler("POST", "/api/models/runs/{rest_of_path:path}", "authority.model_execution"),
    _handler("POST", "/api/integrations", "authority.integrations"),
    _handler("POST", "/api/integrations/reload", "authority.integrations"),
    _handler("PUT", "/api/integrations/{integration_id}/secret", "authority.secrets"),
    _handler("POST", "/api/integrations/{integration_id}/test", "authority.integrations"),
    _handler("POST", "/api/data-dictionary/reproject", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/lineage/reproject", "authority.data_dictionary"),
    _handler("PUT", "/api/data-dictionary/lineage", "authority.data_dictionary"),
    _handler("DELETE", "/api/data-dictionary/lineage", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/classifications/reproject", "authority.data_dictionary"),
    _handler("PUT", "/api/data-dictionary/classifications", "authority.data_dictionary"),
    _handler("DELETE", "/api/data-dictionary/classifications", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/quality/reproject", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/quality/evaluate", "authority.data_dictionary"),
    _handler("PUT", "/api/data-dictionary/quality", "authority.data_dictionary"),
    _handler("DELETE", "/api/data-dictionary/quality", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/stewardship/reproject", "authority.data_dictionary"),
    _handler("PUT", "/api/data-dictionary/stewardship", "authority.data_dictionary"),
    _handler("DELETE", "/api/data-dictionary/stewardship", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/governance/drain", "authority.data_dictionary"),
    _handler("POST", "/api/data-dictionary/drift/snapshot", "authority.data_dictionary"),
    _handler("POST", "/api/audit/apply", "authority.audit"),
    _handler("POST", "/api/audit/execute_contract", "authority.audit"),
    _handler("POST", "/api/audit/execute_all_contracts", "authority.audit"),
    _handler("POST", "/api/data-dictionary/governance/enforce", "authority.data_dictionary"),
    _handler("PUT", "/api/data-dictionary/{object_kind}/{field_path:path}", "authority.data_dictionary"),
    _handler("DELETE", "/api/data-dictionary/{object_kind}/{field_path:path}", "authority.data_dictionary"),
    _handler("POST", "/api/chat/conversations", "authority.chat"),
    _handler("POST", "/api/chat/conversations/{conversation_id}/messages", "authority.chat"),
    _handler("POST", "/api/workflow-triggers", "authority.workflow_triggers"),
    _handler("PUT", "/api/workflow-triggers", "authority.workflow_triggers"),
    _handler("PUT", "/api/workflow-triggers/{rest_of_path:path}", "authority.workflow_triggers"),
    _handler("POST", "/api/trigger/{rest_of_path:path}", "authority.workflow_triggers"),
    _handler("POST", "/api/manifests/generate", "authority.manifest"),
    _handler("POST", "/api/manifests/generate-quick", "authority.manifest"),
    _handler("POST", "/api/manifests/refine", "authority.manifest"),
    _handler("POST", "/api/manifests/save", "authority.manifest"),
    _handler("POST", "/api/manifests/save-as", "authority.manifest"),
    _handler("POST", "/api/checkpoints", "authority.approvals"),
    _handler("POST", "/api/checkpoints/{checkpoint_id}/approve", "authority.approvals"),
    _api("POST", "/api/workflow-runs", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.submit"),
    _api("POST", "/api/workflow-runs/spawn", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.spawn"),
    _api("POST", "/api/workflows/run", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.submit"),
    _api("POST", "/api/workflow-job", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.submit"),
    _standard("/mcp", "authority.workflow_mcp", notes="Workflow MCP bridge route; tool execution is catalog-gated by MCP token/tool policy."),
    _standard("/orient", "authority.orient"),
    _standard("/query", "authority.operator_query"),
    _standard("/bugs", "authority.bugs"),
    _standard("/recall", "authority.semantic_memory"),
    _standard("/ingest", "authority.ingest"),
    _standard("/graph", "authority.graph"),
    _standard("/receipts", "authority.receipts"),
    _standard("/constraints", "authority.constraints"),
    _standard("/friction", "authority.feedback"),
    _standard("/heal", "authority.repair"),
    _standard("/artifacts", "authority.artifacts"),
    _standard("/decompose", "authority.workflow_planning"),
    _standard("/research", "authority.research"),
    _standard("/health", "authority.health"),
    _standard("/governance", "authority.governance"),
    _standard("/workflow-runs", "authority.workflow_runs"),
    _standard("/workflow-validate", "authority.workflow_validation"),
    _standard("/status", "authority.operator_status"),
    _standard("/wave", "authority.workflow_orchestration"),
    _standard("/manifest/generate", "authority.manifest"),
    _standard("/manifest/refine", "authority.manifest"),
    _standard("/manifest/get", "authority.manifest"),
    _standard("/heartbeat", "authority.heartbeat"),
    _standard("/session", "authority.session"),
    _api("POST", "/api/auth/bootstrap/exchange", "domain_authority", "authority.mobile_access", operation_name="mobile.bootstrap.exchange", receipt_policy="mobile_session_receipt", event_policy="mobile_session_event"),
    _api("POST", "/auth/bootstrap/exchange", "domain_authority", "authority.mobile_access", operation_name="mobile.bootstrap.exchange", receipt_policy="mobile_session_receipt", event_policy="mobile_session_event"),
    _api("POST", "/api/approvals/{request_id}/ratify", "domain_authority", "authority.access_control", operation_name="approval.ratify", receipt_policy="capability_grant_receipt", event_policy="approval_event"),
    _api("POST", "/approvals/{request_id}/ratify", "domain_authority", "authority.access_control", operation_name="approval.ratify", receipt_policy="capability_grant_receipt", event_policy="approval_event"),
    _api("POST", "/api/devices/{device_id}/revoke", "domain_authority", "authority.mobile_access", operation_name="mobile.device.revoke", receipt_policy="capability_grant_receipt", event_policy="device_event"),
    _api("POST", "/devices/{device_id}/revoke", "domain_authority", "authority.mobile_access", operation_name="mobile.device.revoke", receipt_policy="capability_grant_receipt", event_policy="device_event"),
    _api("POST", "/api/queue/submit", "control_command_bus", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.submit"),
    _api("POST", "/api/queue/cancel/{job_id}", "control_command_bus_with_lookup", "authority.workflow_runs", receipt_policy="control_command", event_policy="system_event", operation_name="workflow.cancel", notes="Reads workflow_jobs to resolve run_id, then mutates only through control command bus."),
    _api("POST", "/api/operate", "operation_gateway", "authority.operation_catalog", receipt_policy="authority_operation_receipt", event_policy="catalog_operation_policy", operation_name="dynamic"),
    _handler("POST", "/api/catalog/review-decisions", "authority.catalog_review"),
    _api("POST", "/agents", "agent_session_runtime", "authority.agent_sessions", receipt_policy="agent_session_event", event_policy="agent_session_event", owner_ref="praxis.engine.agent_sessions"),
    _api("POST", "/agents/{agent_id}/messages", "agent_session_runtime", "authority.agent_sessions", receipt_policy="agent_session_event", event_policy="agent_session_event", owner_ref="praxis.engine.agent_sessions"),
    _api("DELETE", "/agents/{agent_id}", "agent_session_runtime", "authority.agent_sessions", receipt_policy="agent_session_event", event_policy="agent_session_event", owner_ref="praxis.engine.agent_sessions"),
)

API_ROUTE_AUTHORITY: dict[str, ApiRouteAuthority] = {
    row.route_key: row for row in _API_ROUTE_AUTHORITY_ROWS
}


def _route_methods(route: Any) -> list[str]:
    methods = getattr(route, "methods", None) or set()
    return sorted(str(method).upper() for method in methods if str(method).upper() in MUTATING_METHODS)


def _operation_catalog_record(route: Any, method: str) -> ApiRouteAuthority | None:
    extra = getattr(route, "openapi_extra", None)
    if not isinstance(extra, dict):
        return None
    operation_name = str(extra.get("x-praxis-operation-name") or "").strip()
    if not operation_name:
        return None
    return _api(
        method,
        str(getattr(route, "path", "")),
        "operation_catalog_gateway",
        str(extra.get("x-praxis-authority-domain") or "authority.operation_catalog"),
        receipt_policy="authority_operation_receipt",
        event_policy=str(extra.get("x-praxis-event-policy") or "catalog_operation_policy"),
        operation_name=operation_name,
        projection_ref=str(extra.get("x-praxis-projection-ref") or "") or None,
        notes="Mounted from DB-backed operation_catalog_registry.",
    )


def iter_mutating_routes(app: Any) -> list[dict[str, Any]]:
    """Return mutating FastAPI routes without importing FastAPI at runtime."""
    records: list[dict[str, Any]] = []
    for route in getattr(app, "routes", ()):
        path = str(getattr(route, "path", "") or "")
        if not path:
            continue
        for method in _route_methods(route):
            records.append(
                {
                    "method": method,
                    "path": path,
                    "route_key": route_key(method, path),
                    "route_name": str(getattr(route, "name", "") or ""),
                    "route": route,
                }
            )
    return sorted(records, key=lambda row: (row["path"], row["method"], row["route_name"]))


def classify_mutating_routes(app: Any) -> dict[str, Any]:
    routes = iter_mutating_routes(app)
    mounted_keys = {row["route_key"] for row in routes}
    route_records: list[dict[str, Any]] = []
    unknown_routes: list[dict[str, Any]] = []

    for row in routes:
        operation_record = _operation_catalog_record(row["route"], row["method"])
        record = operation_record or API_ROUTE_AUTHORITY.get(row["route_key"])
        if record is None:
            unknown = {key: row[key] for key in ("method", "path", "route_key", "route_name")}
            unknown_routes.append(unknown)
            route_records.append({**unknown, "boundary_kind": "unclassified", "mounted": True})
            continue
        route_records.append(record.to_payload(route_name=row["route_name"], mounted=True))

    stale_declarations = [
        record.to_payload(route_name=None, mounted=False)
        for key, record in sorted(API_ROUTE_AUTHORITY.items())
        if key not in mounted_keys
    ]
    duplicate_declarations: list[str] = []
    seen: set[str] = set()
    for row in _API_ROUTE_AUTHORITY_ROWS:
        if row.route_key in seen:
            duplicate_declarations.append(row.route_key)
        seen.add(row.route_key)

    return {
        "contract_version": 1,
        "mutating_route_count": len(routes),
        "classified_route_count": len(routes) - len(unknown_routes),
        "routes": route_records,
        "drift": {
            "unknown_routes": unknown_routes,
            "stale_declarations": stale_declarations,
            "duplicate_declarations": duplicate_declarations,
        },
    }


def assert_api_mutation_routes_classified(app: Any) -> None:
    payload = classify_mutating_routes(app)
    drift = payload["drift"]
    if drift["unknown_routes"] or drift["duplicate_declarations"]:
        raise ApiAuthorityBoundaryError(
            "mutating API routes must declare an authority boundary",
            drift=drift,
        )


def build_api_authority_payload(app: Any) -> dict[str, Any]:
    payload = classify_mutating_routes(app)
    drift = payload["drift"]
    payload["ok"] = not drift["unknown_routes"] and not drift["duplicate_declarations"]
    payload["routed_to"] = "api_authority_boundary"
    return payload


__all__ = [
    "API_ROUTE_AUTHORITY",
    "ApiAuthorityBoundaryError",
    "ApiRouteAuthority",
    "MUTATING_METHODS",
    "assert_api_mutation_routes_classified",
    "build_api_authority_payload",
    "classify_mutating_routes",
    "iter_mutating_routes",
    "route_key",
]
