from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from surfaces.api.catalog_authority import build_catalog_payload
from surfaces.api.handlers import workflow_admin, workflow_query


class _CatalogPg:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "FROM capability_catalog" in query:
            return [
                {
                    "capability_ref": "cap-task-debug",
                    "capability_slug": "debug",
                    "capability_kind": "task",
                    "title": "Debugging",
                    "summary": "Diagnose failures and trace problems.",
                    "description": "Debug work",
                    "route": "task/debug",
                }
            ]
        if "FROM integration_registry" in query:
            return [
                {
                    "id": "workflow",
                    "name": "Workflow",
                    "description": "Invoke workflows",
                    "provider": "praxis",
                    "capabilities": json.dumps(
                        [{"action": "invoke", "description": "Invoke a workflow"}]
                    ),
                    "auth_status": "connected",
                    "icon": "workflow",
                }
            ]
        if "FROM connector_registry" in query:
            return [
                {
                    "slug": "gmail",
                    "display_name": "Gmail",
                    "version": "1.2.3",
                    "auth_type": "oauth",
                    "base_url": "https://gmail.googleapis.com",
                    "status": "active",
                    "health_status": "healthy",
                }
            ]
        return []


class _WorkflowTemplatePg:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "FROM registry_workflows" in query:
            assert "registry_workflows" in query
            return [
                {
                    "id": "workflow_composition",
                    "name": "Workflow Composition",
                    "description": "Workflows that call and orchestrate other workflows",
                    "category": "automation",
                    "trigger_type": "manual",
                    "input_schema": {"type": "object"},
                    "output_schema": {"type": "object"},
                    "steps": ["draft", "review", "publish"],
                    "mcp_tool_refs": ["workflow.invoke"],
                }
            ]
        return []


class _RequestStub:
    def __init__(self, pg: _CatalogPg) -> None:
        self.headers = {"Content-Length": "2"}
        self.rfile = io.BytesIO(b"{}")
        self.path = "/api/catalog"
        self.subsystems = SimpleNamespace(get_pg_conn=lambda: pg)
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


def test_build_catalog_payload_surfaces_shared_static_and_connector_items() -> None:
    payload = build_catalog_payload(_CatalogPg())

    items_by_id = {item["id"]: item for item in payload["items"]}

    assert items_by_id["ctrl-branch"]["gateFamily"] == "conditional"
    assert items_by_id["ctrl-retry"]["gateFamily"] == "retry"
    assert items_by_id["ctrl-on-failure"]["gateFamily"] == "after_failure"
    assert items_by_id["think-fan-out"]["actionValue"] == "workflow.fanout"
    assert items_by_id["think-fan-out-legacy"]["actionValue"] == "auto/fan-out"
    assert items_by_id["conn-gmail"]["actionValue"] == "@gmail"
    assert items_by_id["conn-gmail"]["status"] == "ready"
    assert items_by_id["cap-debug"]["actionValue"] == "task/debug"
    assert items_by_id["int-workflow-invoke"]["actionValue"] == "@workflow/invoke"
    assert items_by_id["ctrl-branch"]["truth"]["category"] == "runtime"
    assert items_by_id["ctrl-branch"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-approval"]["truth"]["category"] == "runtime"
    assert items_by_id["ctrl-approval"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-validation"]["truth"]["category"] == "runtime"
    assert items_by_id["ctrl-validation"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-review"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["ctrl-retry"]["truth"]["category"] == "runtime"
    assert items_by_id["ctrl-retry"]["surfacePolicy"]["tier"] == "advanced"
    assert items_by_id["gather-docs"]["truth"]["category"] == "alias"
    assert items_by_id["gather-docs"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["think-fan-out"]["truth"]["category"] == "runtime"
    assert items_by_id["think-fan-out"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["think-fan-out-legacy"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["int-workflow-invoke"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["cap-debug"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["conn-gmail"]["truth"]["category"] == "runtime"
    assert payload["sources"]["connectors"] == 1
    assert payload["fetched_at"]


def test_workflow_template_handler_reads_registry_workflows() -> None:
    request = _RequestStub(_WorkflowTemplatePg())
    request.path = "/api/workflow-templates?q=workflow"

    workflow_admin._handle_workflow_templates_get(request, "/api/workflow-templates")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["query"] == "workflow"
    assert payload["templates"] == [
        {
            "id": "workflow_composition",
            "name": "Workflow Composition",
            "description": "Workflows that call and orchestrate other workflows",
            "category": "automation",
            "trigger_type": "manual",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "steps": ["draft", "review", "publish"],
            "mcp_tool_refs": ["workflow.invoke"],
        }
    ]


def test_legacy_catalog_handler_uses_shared_catalog_authority() -> None:
    request = _RequestStub(_CatalogPg())

    workflow_query._handle_catalog_get(request, "/api/catalog")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    items_by_id = {item["id"]: item for item in payload["items"]}
    assert items_by_id["ctrl-branch"]["gateFamily"] == "conditional"
    assert items_by_id["ctrl-retry"]["gateFamily"] == "retry"
    assert items_by_id["ctrl-on-failure"]["gateFamily"] == "after_failure"
    assert items_by_id["think-fan-out"]["actionValue"] == "workflow.fanout"
    assert items_by_id["think-fan-out-legacy"]["actionValue"] == "auto/fan-out"
    assert items_by_id["conn-gmail"]["actionValue"] == "@gmail"
    assert items_by_id["think-fan-out"]["truth"]["category"] == "runtime"
    assert items_by_id["think-fan-out"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-approval"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-review"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["ctrl-retry"]["surfacePolicy"]["tier"] == "advanced"
