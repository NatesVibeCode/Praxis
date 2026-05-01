from __future__ import annotations

from runtime.materializer import (
    _detect_triggers,
    _enrich_binding_ledger,
    _extract_references,
    _generate_jobs,
    _resolve_references,
)


def test_extract_references_detects_all_reference_types() -> None:
    prose = (
        "When @gmail/search receives a message, triage-agent inspects "
        "#contact/{sender} and sets {priority: P1|P2|P3} before review-agent acts."
    )

    refs = _extract_references(prose)

    assert [ref["type"] for ref in refs] == [
        "integration",
        "agent",
        "object",
        "variable",
        "agent",
    ]
    assert refs[0]["slug"] == "@gmail/search"
    assert refs[2]["slug"] == "#contact/{sender}"
    assert refs[3]["config"] == {"name": "priority", "options": ["P1", "P2", "P3"]}


def test_resolve_references_handles_exact_base_and_agent_route_fallback() -> None:
    refs = [
        {
            "id": "ref-001",
            "type": "integration",
            "slug": "@gmail/search",
            "span": [0, 13],
            "raw": "@gmail/search",
            "config": {},
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        },
        {
            "id": "ref-002",
            "type": "object",
            "slug": "#contact/{sender}",
            "span": [14, 31],
            "raw": "#contact/{sender}",
            "config": {},
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        },
        {
            "id": "ref-003",
            "type": "integration",
            "slug": "@slack/post",
            "span": [32, 43],
            "raw": "@slack/post",
            "config": {},
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        },
        {
            "id": "ref-004",
            "type": "variable",
            "slug": "{priority}",
            "span": [44, 54],
            "raw": "{priority}",
            "config": {"name": "priority", "options": ["P1", "P2"]},
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        },
        {
            "id": "ref-005",
            "type": "agent",
            "slug": "review-agent",
            "span": [55, 67],
            "raw": "review-agent",
            "config": {},
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        },
    ]
    catalog = [
        {
            "slug": "@gmail/search",
            "ref_type": "integration",
            "display_name": "Gmail Search",
            "resolved_id": "gmail",
            "resolved_table": "integration_registry",
            "description": "Search Gmail",
        },
        {
            "slug": "#contact",
            "ref_type": "object",
            "display_name": "Contact",
            "resolved_id": "contact",
            "resolved_table": "object_types",
            "description": "Contact record",
        },
    ]

    resolved, unresolved = _resolve_references(refs, catalog)

    assert resolved[0]["resolved_to"] == "integration_registry:gmail/search"
    assert resolved[1]["resolved_to"] == "object_types:contact"
    assert resolved[1]["config"]["type_id"] == "contact"
    assert resolved[3]["resolved_to"] == "{priority}"
    assert resolved[4]["resolved_to"] == "task_type_routing:auto/review"
    assert resolved[4]["config"]["route"] == "auto/review"
    assert unresolved == ["@slack/post"]


def test_generate_jobs_builds_agent_chain() -> None:
    prose = "When @gmail/search receives a message, triage-agent drafts a reply. review-agent validates it."
    refs = [
        {
            "id": "ref-001",
            "type": "agent",
            "slug": "triage-agent",
            "span": [39, 51],
            "raw": "triage-agent",
            "config": {"route": "auto/build"},
            "resolved": True,
            "resolved_to": "task_type_routing:auto/build",
            "display_name": "Triage Agent",
            "description": "Drafts a reply",
        },
        {
            "id": "ref-002",
            "type": "agent",
            "slug": "review-agent",
            "span": [69, 81],
            "raw": "review-agent",
            "config": {"route": "auto/review"},
            "resolved": True,
            "resolved_to": "task_type_routing:auto/review",
            "display_name": "Review Agent",
            "description": "Validates output",
        },
    ]

    jobs = _generate_jobs(prose, refs)

    assert jobs[0]["label"] == "triage"
    assert jobs[0]["agent"] == "auto/build"
    assert jobs[1]["label"] == "review"
    assert jobs[1]["agent"] == "auto/review"
    assert jobs[1]["depends_on"] == ["triage"]


def test_detect_triggers_maps_integration_and_schedule_language() -> None:
    prose = "When @gmail/search receives a message, triage-agent responds. Nightly, review-agent summarizes on schedule."
    refs = _extract_references(prose)
    resolved, _ = _resolve_references(
        refs,
        [
            {
                "slug": "@gmail/search",
                "ref_type": "integration",
                "display_name": "Gmail Search",
                "resolved_id": "gmail",
                "resolved_table": "integration_registry",
                "description": "Search Gmail",
            }
        ],
    )

    triggers = _detect_triggers(prose, resolved)

    assert triggers[0]["event_type"] == "integration.event"
    assert triggers[0]["source_ref"] == "@gmail/search"
    assert triggers[1]["event_type"] == "schedule"
    assert triggers[1]["cron_expression"] == "0 2 * * *"


def test_enrich_binding_ledger_brands_praxis_tool_targets() -> None:
    class _Conn:
        def execute(self, query: str) -> list[dict[str, str]]:
            assert "FROM integration_registry" in query
            return [
                {
                    "id": "praxis_workflow",
                    "name": "Workflow",
                    "description": "Run and inspect workflows",
                    "provider": "mcp",
                    "auth_status": "connected",
                }
            ]

    ledger = [
        {
            "binding_id": "binding-1",
            "accepted_target": {"target_ref": "@praxis_workflow/kickoff"},
        }
    ]

    enriched = _enrich_binding_ledger(ledger, _Conn())

    assert (
        enriched[0]["accepted_target"]["enrichment"]["integration_name"]
        == "Praxis: Workflow"
    )
