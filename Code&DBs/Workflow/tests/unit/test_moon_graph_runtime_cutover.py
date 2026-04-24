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


def test_graph_first_harden_persists_current_execution_manifest() -> None:
    status, payload = _harden_graph(_pg())

    assert status == 200
    assert payload["compiled_spec"]["workflow_id"] == "wf_graph_dispatch"
    assert payload["compiled_spec"]["jobs"][0]["verify_refs"] == ["verify.support_response_quality.v1"]
    assert payload["compiled_spec"]["jobs"][1]["depends_on"] == ["draft"]
    assert "dependency_edges" not in payload["compiled_spec"]["jobs"][1]
    assert payload["execution_manifest"]["execution_manifest_ref"].startswith(
        "execution_manifest:wf_graph_dispatch:"
    )
    assert payload["execution_manifest"]["verify_refs"] == ["verify.support_response_quality.v1"]


def test_graph_first_manifest_dispatches_through_trigger(tmp_path, monkeypatch) -> None:
    pg = _pg()
    status, _payload = _harden_graph(pg)
    assert status == 200

    monkeypatch.setattr(workflow_query, "REPO_ROOT", tmp_path)
    captured_spec: dict[str, Any] = {}

    def _fake_submit(*_args, **kwargs):
        captured_spec.update(kwargs["spec"])
        return {
            "run_id": "dispatch_graph_123",
            "status": "queued",
            "spec_name": "Graph Dispatch",
            "total_jobs": 2,
        }

    trigger_request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))
    with patch.object(canonical_workflows, "_submit_spec_via_service_bus", side_effect=_fake_submit):
        workflow_query._handle_trigger_post(trigger_request, "/api/trigger/wf_graph_dispatch")

    assert trigger_request.sent == (
        200,
        {
            "triggered": True,
            "workflow_id": "wf_graph_dispatch",
            "workflow_name": "Graph Dispatch",
            "run_id": "dispatch_graph_123",
        },
    )
    assert captured_spec["execution_manifest_ref"].startswith("execution_manifest:wf_graph_dispatch:")
