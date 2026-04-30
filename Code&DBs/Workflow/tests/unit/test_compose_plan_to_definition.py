from __future__ import annotations

from runtime.compose_plan_to_definition import packets_to_definition


class _Dictish:
    def __init__(self, payload: dict):
        self._payload = payload

    def to_dict(self) -> dict:
        return self._payload


class _ComposeResult:
    ok = True
    intent = "Build an app integration workflow."
    notes: list[str] = []
    reason_code = None
    error = None
    synthesis = _Dictish({"packet_seeds": []})
    validation = _Dictish({"passed": True, "findings": []})
    pill_triage = None
    plan_packets = [
        {
            "label": "search_sources",
            "stage": "research",
            "description": "Search official docs and catalog records.",
            "write": ["research.search_results"],
            "depends_on": [],
            "agent": "auto/research",
            "task_type": "research",
            "capabilities": ["search", "catalog_lookup"],
            "consumes": ["app_target"],
            "produces": ["search_results"],
            "gates": [],
            "parameters": {},
            "prompt": "Search the app docs.",
        }
    ]

    def usage_summary(self) -> dict:
        return {"calls": 1}


def test_packets_to_definition_emits_capability_objects() -> None:
    definition = packets_to_definition(
        workflow_id="wf_test",
        intent="Build an app integration workflow.",
        compose_result=_ComposeResult(),
    )

    assert definition["capabilities"] == [
        {
            "slug": "catalog_lookup",
            "label": "Catalog Lookup",
            "route": "catalog_lookup",
            "signals": ["catalog_lookup"],
            "reference_slugs": [],
        },
        {
            "slug": "search",
            "label": "Search",
            "route": "search",
            "signals": ["search"],
            "reference_slugs": [],
        },
    ]
