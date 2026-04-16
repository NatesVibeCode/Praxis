from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

from surfaces.api.handlers import workflow_query


class _RequestStub:
    def __init__(self, *, path: str, subsystems: Any) -> None:
        self.path = path
        self.subsystems = subsystems
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


class _PgStub:
    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        if "FROM app_manifests" in normalized:
            return [
                {
                    "id": "manifest_support",
                    "name": "Support Workspace",
                    "description": "Workspace for support operations",
                }
            ]
        if "FROM integration_registry" in normalized:
            return [
                {
                    "id": "gmail",
                    "name": "Gmail",
                    "description": "Email integration",
                    "icon": "mail",
                    "capabilities": [{"action": "search", "description": "Search messages"}],
                }
            ]
        raise AssertionError(f"unexpected SQL: {normalized}")


def test_intent_analyze_uses_intent_matcher_and_returns_analysis() -> None:
    matcher = Mock()
    matcher.match.return_value = SimpleNamespace(
        ui_components=(
            SimpleNamespace(
                id="search-panel",
                name="Search Panel",
                description="Search across records",
                category="display",
                rank=0.9,
                metadata={"props_schema": {"placeholder": "Search..."}},
            ),
        ),
        calculations=(
            SimpleNamespace(
                id="calc-aggregate",
                name="Aggregate",
                description="Aggregate records",
                category="logic",
                rank=0.7,
                metadata={},
            ),
        ),
        workflows=(),
        coverage_score=0.8,
        gaps=("no matching workflows",),
    )
    matcher.compose.return_value = SimpleNamespace(
        components=("search-panel",),
        calculations=("calc-aggregate",),
        workflows=(),
        bindings=(
            SimpleNamespace(
                source_id="calc-aggregate",
                source_type="calculation",
                target_id="search-panel",
                target_type="ui_component",
                rationale="feeds the search panel",
            ),
        ),
        layout_suggestion="main=[search-panel]",
        confidence=0.75,
    )

    subsystems = SimpleNamespace(
        get_pg_conn=lambda: _PgStub(),
        get_intent_matcher=lambda: matcher,
    )
    request = _RequestStub(path="/api/intent/analyze?q=support dashboard", subsystems=subsystems)

    workflow_query._handle_intent_analyze_get(request, "/api/intent/analyze")

    assert matcher.match.called
    assert matcher.match.call_args.args == ("support dashboard",)
    assert matcher.match.call_args.kwargs == {"limit": 5}
    assert request.sent is not None

    status, payload = request.sent
    assert status == 200
    assert payload["intent"] == "support dashboard"
    assert payload["templates"][0]["id"] == "manifest_support"
    assert payload["integrations"][0]["display_name"] == "Gmail"
    assert payload["analysis"]["source"] == "intent_matcher"
    assert payload["analysis"]["matches"]["total_count"] == 2
    assert payload["analysis"]["matches"]["ui_components"][0]["id"] == "search-panel"
    assert payload["analysis"]["composition"]["layout_suggestion"] == "main=[search-panel]"
    assert payload["can_generate"] is False
