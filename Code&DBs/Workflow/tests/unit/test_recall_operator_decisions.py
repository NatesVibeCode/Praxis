from __future__ import annotations

from types import SimpleNamespace

from surfaces._recall import search_recall_results
from surfaces.api.handlers import workflow_query_core
from surfaces.mcp.tools import knowledge


class _EmptyKnowledgeGraph:
    def search(self, query: str, entity_type: str | None = None, limit: int = 20):
        return []


class _FakeSubsystems:
    def get_knowledge_graph(self):
        return _EmptyKnowledgeGraph()

    def _postgres_env(self):
        return {"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}


def _operator_decision_row() -> dict[str, object]:
    return {
        "operator_decision_id": "operator_decision.architecture_policy.provider_onboarding.registry_owned_catalog_exposed",
        "decision_key": "architecture-policy::provider-onboarding::registry-owned-catalog-exposed",
        "decision_kind": "architecture_policy",
        "decision_status": "decided",
        "title": "Provider onboarding stays registry-owned and catalog-exposed",
        "rationale": "Canonical onboarding includes post-onboarding sync.",
        "decided_by": "codex",
        "decision_source": "implementation",
        "decision_scope_kind": "authority_domain",
        "decision_scope_ref": "provider_onboarding",
        "effective_from": "2026-04-17T02:28:58.868527+00:00",
        "effective_to": None,
        "decided_at": "2026-04-17T02:28:58.868527+00:00",
        "created_at": "2026-04-17T02:28:58.868527+00:00",
        "updated_at": "2026-04-17T02:28:58.868970+00:00",
    }


def test_search_recall_results_includes_operator_decisions(monkeypatch) -> None:
    monkeypatch.setattr(
        "surfaces._recall.operator_control.list_operator_decisions",
        lambda **kwargs: {"operator_decisions": [_operator_decision_row()]},
    )

    results = search_recall_results(
        _FakeSubsystems(),
        query="provider onboarding authority",
        entity_type=None,
        limit=10,
    )

    assert len(results) == 1
    assert results[0]["entity_id"] == _operator_decision_row()["operator_decision_id"]
    assert results[0]["source"] == "operator_decisions"
    assert results[0]["found_via"] == "authority_scan"


def test_handle_recall_surfaces_operator_decisions(monkeypatch) -> None:
    monkeypatch.setattr(
        "surfaces._recall.operator_control.list_operator_decisions",
        lambda **kwargs: {"operator_decisions": [_operator_decision_row()]},
    )

    payload = workflow_query_core.handle_recall(
        _FakeSubsystems(),
        {"query": "provider onboarding authority"},
    )

    assert payload["count"] == 1
    assert payload["results"][0]["entity_id"] == _operator_decision_row()["operator_decision_id"]
    assert payload["results"][0]["source"] == "operator_decisions"
    assert "post-onboarding sync" in payload["results"][0]["content_preview"]


def test_tool_praxis_recall_includes_operator_decisions(monkeypatch) -> None:
    monkeypatch.setattr(
        knowledge,
        "_subs",
        _FakeSubsystems(),
    )
    monkeypatch.setattr(
        "surfaces._recall.operator_control.list_operator_decisions",
        lambda **kwargs: {"operator_decisions": [_operator_decision_row()]},
    )

    payload = knowledge.tool_praxis_recall({"query": "provider onboarding authority"})

    assert payload["count"] == 1
    assert payload["results"][0]["id"] == _operator_decision_row()["operator_decision_id"]
    assert payload["results"][0]["source"] == "operator_decisions"
