from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from runtime import canonical_workflows
from surfaces.api.handlers import workflow_query
from tests.unit.test_workflow_query_handlers import _MutableWorkflowPg, _RequestStub


def _graph_payload() -> dict[str, Any]:
    return {
        "nodes": [
            {
                "node_id": "trigger-001",
                "kind": "step",
                "title": "Manual",
                "route": "trigger",
                "trigger": {"event_type": "manual", "filter": {}},
            },
            {
                "node_id": "step-001",
                "kind": "step",
                "title": "Draft",
                "route": "auto/draft",
                "prompt": "Draft a support response.",
                "outputs": ["draft_response"],
            },
            {
                "node_id": "step-002",
                "kind": "step",
                "title": "Validate",
                "route": "auto/review",
                "prompt": "Validate response quality.",
                "outputs": ["quality_report"],
            },
        ],
        "edges": [
            {
                "edge_id": "edge-trigger-step",
                "kind": "sequence",
                "from_node_id": "trigger-001",
                "to_node_id": "step-001",
            },
            {
                "edge_id": "edge-step-validate",
                "kind": "sequence",
                "from_node_id": "step-001",
                "to_node_id": "step-002",
                "release": {
                    "family": "validation",
                    "edge_type": "validation",
                    "release_condition": {"kind": "always"},
                    "config": {"verify_refs": ["verify.support_response_quality.v1"]},
                },
            },
        ],
    }


def _harden_graph(pg: _MutableWorkflowPg) -> tuple[int, dict[str, Any]]:
    request = _RequestStub(
        {"title": "Graph Dispatch", "build_graph": _graph_payload()},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_graph_dispatch/build/harden",
    )
    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_graph_dispatch/build/harden",
    )
    assert request.sent is not None
    return request.sent


def _pg() -> _MutableWorkflowPg:
    return _MutableWorkflowPg(
        workflow_rows={
            "wf_graph_dispatch": {
                "id": "wf_graph_dispatch",
                "name": "Graph Dispatch",
                "description": "Graph Dispatch",
                "definition": {},
                "compiled_spec": None,
                "version": 1,
                "updated_at": "2026-04-15T10:00:00Z",
            }
        }
    )


def test_graph_first_harden_requires_review_before_execution_manifest() -> None:
    status, payload = _harden_graph(_pg())

    assert status == 200
    assert payload["compiled_spec"] is None
    assert payload["execution_manifest"] is None
    assert payload["candidate_resolution_manifest"]["execution_readiness"] == "review_required"
    assert payload["candidate_resolution_manifest"]["projection_status"] == {
        "state": "ready",
        "blocking_issue_ids": [],
        "issue_count": 0,
        "compiled_spec_available": False,
    }
    assert "Workflow shape requires explicit approval" in payload["planning_notes"][0]


def test_graph_first_trigger_waits_for_approved_execution_manifest(tmp_path, monkeypatch) -> None:
    pg = _pg()
    status, _payload = _harden_graph(pg)
    assert status == 200

    monkeypatch.setattr(workflow_query, "REPO_ROOT", tmp_path)

    trigger_request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))
    with patch.object(canonical_workflows, "_submit_spec_via_service_bus") as bus_mock:
        workflow_query._handle_trigger_post(trigger_request, "/api/trigger/wf_graph_dispatch")

    assert trigger_request.sent == (
        400,
        {"error": "Workflow 'Graph Dispatch' has no approved execution manifest. Review and harden the workflow first."},
    )
    bus_mock.assert_not_called()
