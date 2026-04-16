from fastapi.testclient import TestClient
from typing import Any
import sys
from pathlib import Path

# Ensure paths are set up correctly
_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest
from runtime.cqrs import CommandBus
from runtime.cqrs.queries.roadmap_tree import QueryRoadmapTree

def test_cqrs_dynamic_route_mounting_and_dispatch(monkeypatch: Any) -> None:
    """
    Proves that the CapabilityRegistry successfully mounts HTTP endpoints,
    validates Pydantic schemas, and routes through the CommandBus.
    """
    class MockConn:
        def execute(self, *args, **kwargs):
            return []
        def fetchrow(self, *args, **kwargs):
            return {"id": "test_wf", "name": "Test", "version": 1}
    class MockSubsystems:
        def get_pg_conn(self):
            return MockConn()

    # Stub the shared subsystems so the CommandBus has a mock DB
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda app: MockSubsystems())

    # We need to stub mutate_workflow_build because the real one does a lot of DB queries
    from runtime import canonical_workflows
    monkeypatch.setattr(
        canonical_workflows, 
        "mutate_workflow_build", 
        lambda *args, **kwargs: {
            "row": {"id": "test_wf", "name": "Test", "version": 1},
            "definition": {"type": "test"},
            "compiled_spec": {"compiled": True},
            "build_bundle": {"status": "ready"},
            "planning_notes": [],
        }
    )

    client = TestClient(rest.app)
    
    # Send a request to the dynamically mounted CQRS route
    response = client.post(
        "/api/workflows/wf_123/build/attachments",
        json={
            "node_id": "step-1",
            "authority_kind": "integration",
            "authority_ref": "test"
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["workflow"]["id"] == "test_wf"
    assert data["compiled_spec"] == {"compiled": True}


def test_cqrs_query_roadmap_tree_dispatch(monkeypatch: Any) -> None:
    class MockSubsystems:
        pass

    captured: dict[str, Any] = {}

    def _query_roadmap_tree(**kwargs):
        captured.update(kwargs)
        return {"kind": "roadmap_tree", "root_roadmap_item_id": kwargs["root_roadmap_item_id"]}

    from surfaces.api import operator_read

    monkeypatch.setattr(operator_read, "query_roadmap_tree", _query_roadmap_tree)
    bus = CommandBus(MockSubsystems())

    result = bus.dispatch(QueryRoadmapTree(root_roadmap_item_id="roadmap_item.test.root"))

    assert result["kind"] == "roadmap_tree"
    assert captured["root_roadmap_item_id"] == "roadmap_item.test.root"
    assert captured["semantic_neighbor_limit"] == 5
    assert captured["include_completed_nodes"] is True

    captured.clear()
    result = bus.dispatch(
        QueryRoadmapTree(
            root_roadmap_item_id="roadmap_item.test.root",
            include_completed_nodes=False,
        )
    )

    assert result["kind"] == "roadmap_tree"
    assert captured["include_completed_nodes"] is False
