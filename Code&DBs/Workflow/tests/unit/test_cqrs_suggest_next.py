from fastapi.testclient import TestClient
from typing import Any
import sys
from pathlib import Path
from types import SimpleNamespace

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
        def transaction(self):
            class _Tx:
                def __enter__(_self):
                    return self

                def __exit__(_self, exc_type, exc, tb):
                    return False

            return _Tx()

        def fetchrow(self, *args, **kwargs):
            return None

        def execute(self, *args, **kwargs):
            return []

        def fetch(self, *args, **kwargs):
            return []
            
    class MockSubsystems:
        def get_pg_conn(self):
            return MockConn()

    # Stub the shared subsystems so the CommandBus has a mock DB
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda app: MockSubsystems())

    # Stub the capability catalog load
    from runtime import capability_catalog
    from runtime.operations.commands.suggest_next import (
        SuggestNextNodesCommand,
        handle_suggest_next_nodes,
    )
    mock_catalog = [
        {"capability_kind": "task", "capability_slug": "task/analyze", "title": "Analyze Data"},
        {"capability_kind": "task", "capability_slug": "task/draft", "title": "Draft Email"},
        {"capability_kind": "integration", "capability_slug": "tool/github/create_issue", "title": "Create GitHub Issue"},
    ]
    monkeypatch.setattr(capability_catalog, "load_capability_catalog", lambda conn: mock_catalog)
    monkeypatch.setattr(
        rest,
        "list_resolved_operation_definitions",
        lambda _conn, include_disabled=False, limit=500: [
            SimpleNamespace(
                operation_name="workflow_build.suggest_next",
                http_method="POST",
                http_path="/api/workflows/{workflow_id}/build/suggest-next",
            )
        ],
    )
    monkeypatch.setattr(
        rest,
        "resolve_http_operation_binding",
        lambda definition: SimpleNamespace(
            operation_ref="workflow_build.suggest_next",
            operation_name=definition.operation_name,
            source_kind="operation_query",
            operation_kind="query",
            http_method=definition.http_method,
            http_path=definition.http_path,
            command_class=SuggestNextNodesCommand,
            handler=handle_suggest_next_nodes,
            authority_ref="authority.capability_catalog",
            projection_ref="projection.capability_catalog",
            posture="observe",
            idempotency_policy="read_only",
            binding_revision="binding.test.20260416",
            decision_ref="decision.test.20260416",
            summary=definition.operation_name,
        ),
    )

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


def test_suggest_next_filters_by_accumulated_type_contract(monkeypatch: Any) -> None:
    from runtime import capability_catalog
    from runtime.operations.commands.suggest_next import (
        SuggestNextNodesCommand,
        handle_suggest_next_nodes,
    )

    class MockSubsystems:
        def get_pg_conn(self):
            return object()

    mock_catalog = [
        {
            "capability_ref": "cap-bug-provenance",
            "capability_kind": "task",
            "capability_slug": "bug/replay-provenance",
            "title": "Backfill replay provenance",
            "route": "praxis_bug_replay_provenance_backfill",
            "consumes": ["ReplayReadyBugSet"],
            "produces": ["BugEvidencePack"],
        },
        {
            "capability_ref": "cap-receipts",
            "capability_kind": "task",
            "capability_slug": "receipts/search",
            "title": "Search receipts",
            "route": "praxis_receipts",
            "consumes": ["BugEvidencePack"],
            "produces": ["ReceiptSet"],
        },
    ]
    monkeypatch.setattr(capability_catalog, "load_capability_catalog", lambda conn: mock_catalog)

    result = handle_suggest_next_nodes(
        SuggestNextNodesCommand(
            workflow_id="wf_bug_lifecycle",
            body={
                "node_id": "replay-ready",
                "build_graph": {
                    "nodes": [
                        {
                            "node_id": "replay-ready",
                            "title": "Read replay-ready bugs",
                            "outputs": ["ReplayReadyBugSet"],
                        }
                    ],
                    "edges": [],
                },
            },
        ),
        MockSubsystems(),
    )

    assert result["status"] == "success"
    assert result["type_context"]["available_types"] == ["replay_ready_bug_set"]
    assert [cap["capability_ref"] for cap in result["likely_next_steps"]] == [
        "cap-bug-provenance"
    ]
    assert [cap["capability_ref"] for cap in result["blocked_next_steps"]] == [
        "cap-receipts"
    ]
    assert result["blocked_next_steps"][0]["type_satisfaction"]["missing"] == [
        "bug_evidence_pack"
    ]
