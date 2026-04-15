from __future__ import annotations

import io
import json
from types import SimpleNamespace
from typing import Any

from surfaces.api.catalog_authority import build_catalog_payload
from surfaces.api.handlers import workflow_admin, workflow_query


def _surface_registry_rows() -> list[dict[str, Any]]:
    return [
        {
            "catalog_item_id": "trigger-manual",
            "label": "Manual",
            "icon": "trigger",
            "family": "trigger",
            "status": "ready",
            "drop_kind": "node",
            "action_value": "trigger",
            "gate_family": None,
            "description": "User-initiated run",
            "truth_category": "runtime",
            "truth_badge": "Runs on release",
            "truth_detail": "Creates trigger intent that is preserved into compiled triggers.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "Primary trigger primitive with real compile and release authority.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "gather-docs",
            "label": "Docs",
            "icon": "research",
            "family": "gather",
            "status": "ready",
            "drop_kind": "node",
            "action_value": "auto/research",
            "gate_family": None,
            "description": "Read and extract from documents",
            "truth_category": "alias",
            "truth_badge": "Alias",
            "truth_detail": "Uses the same `auto/research` route as Web Research today.",
            "surface_tier": "hidden",
            "surface_badge": "Merged",
            "surface_detail": "Merged into Web Research because both buttons point at the same route today.",
            "hard_choice": "Merged into Web Research. One route gets one obvious button.",
        },
        {
            "catalog_item_id": "think-fan-out",
            "label": "Fan Out",
            "icon": "classify",
            "family": "think",
            "status": "ready",
            "drop_kind": "node",
            "action_value": "workflow.fanout",
            "gate_family": None,
            "description": "Split into parallel sub-tasks and aggregate",
            "truth_category": "runtime",
            "truth_badge": "Runs on release",
            "truth_detail": "Fan-out now has a verified runtime lane and compiles into the same release path as other core step routes.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "Fan-out now has a verified runtime lane, so Moon can surface it as a core builder primitive.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "think-fan-out-legacy",
            "label": "Fan Out (Legacy)",
            "icon": "classify",
            "family": "think",
            "status": "ready",
            "drop_kind": "node",
            "action_value": "auto/fan-out",
            "gate_family": None,
            "description": "Legacy fan-out token kept for older saved graphs",
            "truth_category": "alias",
            "truth_badge": "Alias",
            "truth_detail": "Legacy fan-out token kept for existing saved graphs; Moon uses `workflow.fanout` now.",
            "surface_tier": "hidden",
            "surface_badge": "Alias",
            "surface_detail": "Legacy token only, kept so older graphs still open cleanly.",
            "hard_choice": "Compatibility alias for saved graphs only.",
        },
        {
            "catalog_item_id": "ctrl-approval",
            "label": "Approval",
            "icon": "gate",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "approval",
            "description": "Human approval gate",
            "truth_category": "runtime",
            "truth_badge": "Executes",
            "truth_detail": "Compiled into dependency edges that change runtime flow today.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "Pauses the downstream step behind a human approval checkpoint before execution continues.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "ctrl-review",
            "label": "Human Review",
            "icon": "review",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "human_review",
            "description": "Manual review before proceeding",
            "truth_category": "persisted",
            "truth_badge": "Saved only",
            "truth_detail": "Stored in edge metadata now, but not enforced by the planner yet.",
            "surface_tier": "hidden",
            "surface_badge": "Removed",
            "surface_detail": "Folded into Approval so Moon keeps one obvious human gate concept.",
            "hard_choice": "Collapsed into Approval. Two human gate names for one future concept would be noise.",
        },
        {
            "catalog_item_id": "ctrl-validation",
            "label": "Validation",
            "icon": "gate",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "validation",
            "description": "Automated verification command gate",
            "truth_category": "runtime",
            "truth_badge": "Executes",
            "truth_detail": "Runs the configured verification command before the downstream step can continue.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "Executes the configured verification command before the downstream step proceeds.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "ctrl-branch",
            "label": "Branch",
            "icon": "gate",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "conditional",
            "description": "Conditional path (equals, in, not_equals, not_in)",
            "truth_category": "runtime",
            "truth_badge": "Executes",
            "truth_detail": "Compiled into dependency edges that change runtime flow today.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "This is one of the few gate types that changes execution today.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "ctrl-retry",
            "label": "Retry",
            "icon": "gate",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "retry",
            "description": "Retry with backoff + provider failover chain",
            "truth_category": "runtime",
            "truth_badge": "Executes",
            "truth_detail": "Sets the downstream job's max_attempts so failed work can requeue through the runtime retry loop.",
            "surface_tier": "advanced",
            "surface_badge": "Later",
            "surface_detail": "Feeds retry policy into downstream job max_attempts, but stays outside the core gate set.",
            "hard_choice": None,
        },
        {
            "catalog_item_id": "ctrl-on-failure",
            "label": "On Failure",
            "icon": "gate",
            "family": "control",
            "status": "ready",
            "drop_kind": "edge",
            "action_value": None,
            "gate_family": "after_failure",
            "description": "Run only if upstream step failed",
            "truth_category": "runtime",
            "truth_badge": "Executes",
            "truth_detail": "Compiled into dependency edges that change runtime flow today.",
            "surface_tier": "primary",
            "surface_badge": "Core now",
            "surface_detail": "This is one of the few gate types that changes execution today.",
            "hard_choice": None,
        },
    ]


def _source_policy_rows() -> list[dict[str, Any]]:
    return [
        {
            "source_kind": "capability",
            "truth_category": "runtime",
            "truth_badge": "Runs on release",
            "truth_detail": "Capability routes persist into the build graph and become planned runtime routes at release.",
            "surface_tier": "hidden",
            "surface_badge": "Hidden",
            "surface_detail": "Capability catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.",
            "hard_choice": None,
        },
        {
            "source_kind": "integration",
            "truth_category": "runtime",
            "truth_badge": "Runs on release",
            "truth_detail": "Integration actions persist into the build graph and become planned runtime routes at release.",
            "surface_tier": "hidden",
            "surface_badge": "Hidden",
            "surface_detail": "Live integration catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.",
            "hard_choice": None,
        },
        {
            "source_kind": "connector",
            "truth_category": "runtime",
            "truth_badge": "Runs on release",
            "truth_detail": "Connector actions persist into the build graph and become planned runtime routes at release.",
            "surface_tier": "hidden",
            "surface_badge": "Hidden",
            "surface_detail": "Connector catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.",
            "hard_choice": None,
        },
    ]


class _CatalogPg:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "FROM surface_catalog_registry" in query:
            return _surface_registry_rows()
        if "FROM surface_catalog_source_policy_registry" in query:
            return _source_policy_rows()
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


class _NoSurfaceCatalogPg(_CatalogPg):
    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "FROM surface_catalog_registry" in query:
            return []
        if "FROM surface_catalog_source_policy_registry" in query:
            return []
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
    assert items_by_id["trigger-manual"]["source"] == "surface_registry"
    assert items_by_id["ctrl-branch"]["source"] == "surface_registry"
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
    assert items_by_id["int-workflow-invoke"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["int-workflow-invoke"]["truth"]["category"] == "runtime"
    assert items_by_id["cap-debug"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["cap-debug"]["truth"]["category"] == "runtime"
    assert items_by_id["conn-gmail"]["truth"]["category"] == "runtime"
    assert payload["sources"]["surface_registry"] == len(_surface_registry_rows())
    assert payload["sources"]["source_policy_registry"] == len(_source_policy_rows())
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


def test_build_catalog_payload_does_not_fabricate_surface_rows_when_registry_is_empty() -> None:
    payload = build_catalog_payload(_NoSurfaceCatalogPg())

    items_by_id = {item["id"]: item for item in payload["items"]}

    assert "trigger-manual" not in items_by_id
    assert "ctrl-branch" not in items_by_id
    assert payload["sources"]["surface_registry"] == 0
    assert payload["sources"]["source_policy_registry"] == 0
    assert items_by_id["cap-debug"]["actionValue"] == "task/debug"
    assert items_by_id["cap-debug"]["truth"]["category"] == "partial"
    assert items_by_id["cap-debug"]["surfacePolicy"]["tier"] == "hidden"


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
    assert items_by_id["trigger-manual"]["source"] == "surface_registry"
    assert items_by_id["conn-gmail"]["actionValue"] == "@gmail"
    assert items_by_id["think-fan-out"]["truth"]["category"] == "runtime"
    assert items_by_id["think-fan-out"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-approval"]["surfacePolicy"]["tier"] == "primary"
    assert items_by_id["ctrl-review"]["surfacePolicy"]["tier"] == "hidden"
    assert items_by_id["ctrl-retry"]["surfacePolicy"]["tier"] == "advanced"
    assert items_by_id["int-workflow-invoke"]["surfacePolicy"]["tier"] == "hidden"
