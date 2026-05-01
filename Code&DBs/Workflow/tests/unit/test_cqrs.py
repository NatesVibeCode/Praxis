from fastapi.testclient import TestClient
from fastapi import FastAPI
from typing import Any
import sys
from pathlib import Path
from types import SimpleNamespace

# Ensure paths are set up correctly
_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest
from runtime.operation_catalog_bindings import resolve_python_reference
from runtime.operations.queries.roadmap_tree import QueryRoadmapTree, handle_query_roadmap_tree

def test_cqrs_dynamic_route_mounting_and_dispatch(monkeypatch: Any) -> None:
    """
    Proves that the operation catalog mounts HTTP endpoints and dispatches
    through the resolved binding handler.
    """
    class MockConn:
        def transaction(self):
            class _Transaction:
                def __enter__(_self):
                    return self

                def __exit__(_self, exc_type, exc, tb):
                    return False

            return _Transaction()
        def execute(self, *args, **kwargs):
            return []
        def fetch(self, *args, **kwargs):
            return []
        def fetchrow(self, *args, **kwargs):
            return {"id": "test_wf", "name": "Test", "version": 1}
    class MockSubsystems:
        def get_pg_conn(self):
            return MockConn()

    # Stub the shared subsystems so the mounted binding has a mock DB
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda app: MockSubsystems())

    # We need to stub mutate_workflow_build because the real one does a lot of DB queries
    from runtime import canonical_workflows
    monkeypatch.setattr(
        canonical_workflows, 
        "mutate_workflow_build", 
        lambda *args, **kwargs: {
            "row": {"id": "test_wf", "name": "Test", "version": 1},
            "definition": {"type": "test"},
            "materialized_spec": {"compiled": True},
            "build_bundle": {"status": "ready"},
            "planning_notes": [],
        }
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
            operation_ref="workflow_build.mutate",
            operation_name=definition.operation_name,
            source_kind="operation_command",
            operation_kind="command",
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=resolve_python_reference(
                "runtime.operations.commands.workflow_build.MutateWorkflowBuildCommand"
            ),
            handler=resolve_python_reference(
                "runtime.operations.commands.workflow_build.handle_mutate_workflow_build"
            ),
            authority_ref="authority.workflow_build",
            projection_ref=None,
            posture="operate",
            idempotency_policy="non_idempotent",
            binding_revision="binding.test.20260416",
            decision_ref="decision.test.20260416",
            summary=definition.operation_name,
        ),
    )

    target_app = FastAPI()
    rest.mount_capabilities(target_app)
    client = TestClient(target_app)
    
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
    assert data["materialized_spec"] == {"compiled": True}


def test_operation_query_roadmap_tree_dispatch(monkeypatch: Any) -> None:
    class MockSubsystems:
        def _postgres_env(self):
            return {"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"}

    captured: dict[str, Any] = {}

    class _FakeFrontdoor:
        def query_roadmap_tree(self, **kwargs):
            captured.update(kwargs)
            return {
                "kind": "roadmap_tree",
                "root_roadmap_item_id": kwargs["root_roadmap_item_id"],
            }

    from surfaces.api import operator_read

    monkeypatch.setattr(operator_read, "NativeOperatorQueryFrontdoor", _FakeFrontdoor)

    result = handle_query_roadmap_tree(
        QueryRoadmapTree(root_roadmap_item_id="roadmap_item.test.root"),
        MockSubsystems(),
    )

    assert result["kind"] == "roadmap_tree"
    assert captured["root_roadmap_item_id"] == "roadmap_item.test.root"
    assert captured["semantic_neighbor_limit"] == 5
    assert captured["include_completed_nodes"] is True
    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"}

    captured.clear()
    result = handle_query_roadmap_tree(
        QueryRoadmapTree(
            root_roadmap_item_id="roadmap_item.test.root",
            include_completed_nodes=False,
        ),
        MockSubsystems(),
    )

    assert result["kind"] == "roadmap_tree"
    assert captured["include_completed_nodes"] is False
