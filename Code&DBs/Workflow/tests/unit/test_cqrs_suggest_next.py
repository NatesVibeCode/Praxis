from fastapi.testclient import TestClient
from typing import Any
import sys
from pathlib import Path

# Ensure paths are set up correctly
_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest

def test_cqrs_suggest_next(monkeypatch: Any) -> None:
    """
    Proves that the Context-Aware Graph Autocomplete route works
    via the CQRS Command Bus.
    """
    class MockConn:
        def execute(self, *args, **kwargs):
            return []
            
    class MockSubsystems:
        def get_pg_conn(self):
            return MockConn()

    # Stub the shared subsystems so the CommandBus has a mock DB
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda app: MockSubsystems())

    # Stub the capability catalog load
    from runtime import capability_catalog
    mock_catalog = [
        {"capability_kind": "task", "capability_slug": "task/analyze", "title": "Analyze Data"},
        {"capability_kind": "task", "capability_slug": "task/draft", "title": "Draft Email"},
        {"capability_kind": "integration", "capability_slug": "tool/github/create_issue", "title": "Create GitHub Issue"},
    ]
    monkeypatch.setattr(capability_catalog, "load_capability_catalog", lambda conn: mock_catalog)

    with TestClient(rest.app) as client:
        # 1. Simulate finding something: Should boost analysis/drafting
        response = client.post(
            "/api/workflows/wf_123/build/suggest-next",
            json={
                "node_id": "step-1",
                "build_graph": {
                    "nodes": [
                        {"node_id": "step-1", "title": "Search Google for news", "summary": "Search the web"}
                    ],
                    "edges": []
                }
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        # "Analyze" and "Draft" should be boosted because of the word "search"
        likely_titles = [cap["title"] for cap in data["likely_next_steps"]]
        assert "Analyze Data" in likely_titles
        assert "Draft Email" in likely_titles

        # 2. Simulate drafting something: Should boost reviewing/notifying
        response = client.post(
            "/api/workflows/wf_123/build/suggest-next",
            json={
                "node_id": "step-2",
                "build_graph": {
                    "nodes": [
                        {"node_id": "step-2", "title": "Draft the announcement", "summary": "Write the blog post"}
                    ],
                    "edges": []
                }
            }
        )

        assert response.status_code == 200
        data2 = response.json()
        likely_titles2 = [cap["title"] for cap in data2["likely_next_steps"]]
        # GitHub issue matches the notify/review heuristics
        assert "Create GitHub Issue" in likely_titles2
