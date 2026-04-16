from __future__ import annotations

from runtime import task_assembler, task_type_router


class _FakeAssemblerConn:
    def execute(self, query: str, *args):
        if "FROM integration_registry" in query:
            return [{
                "id": "gmail",
                "name": "Gmail",
                "description": "Mail provider",
                "capabilities": [{"action": "search", "description": "Search inbox"}],
                "icon": "mail",
                "score": 0.93,
            }]
        if "FROM registry_ui_components" in query:
            return [{
                "id": "data-table",
                "name": "Data Table",
                "description": "Tabular data browser",
                "category": "display",
                "props_schema": {},
                "score": 0.91,
            }]
        if "FROM registry_calculations" in query:
            return [{
                "id": "common_calcs",
                "name": "Common Calculations",
                "description": "Common calculations - averages, sums, percentages, growth rates",
                "category": "formula",
                "input_schema": {},
                "output_schema": {},
                "execution_type": "sql",
                "score": 0.88,
            }]
        if "FROM registry_workflows" in query:
            return [{
                "id": "workflow_composition",
                "name": "Workflow Composition",
                "description": "Workflows that call and orchestrate other workflows",
                "category": "automation",
                "trigger_type": "manual",
                "input_schema": {},
                "output_schema": {},
                "steps": [],
                "mcp_tool_refs": [],
                "score": 0.89,
            }]
        return []


class _FakeRouterConn:
    def execute(self, query: str, *args):
        if "FROM route_policy_registry" in query:
            return [{
                "task_rank_weight": 0.35,
                "route_health_weight": 0.40,
                "cost_weight": 0.10,
                "benchmark_weight": 0.15,
                "prefer_cost_task_rank_weight": 0.25,
                "prefer_cost_route_health_weight": 0.35,
                "prefer_cost_cost_weight": 0.30,
                "prefer_cost_benchmark_weight": 0.10,
                "claim_route_health_weight": 0.55,
                "claim_rank_weight": 0.30,
                "claim_load_weight": 0.15,
                "claim_internal_failure_penalty_step": 0.08,
                "claim_priority_penalty_step": 0.01,
                "neutral_benchmark_score": 0.50,
                "mixed_benchmark_score": 0.55,
                "neutral_route_health": 0.65,
                "min_route_health": 0.05,
                "max_route_health": 1.0,
                "success_health_bump": 0.04,
                "review_success_bump": 0.02,
                "consecutive_failure_penalty_step": 0.08,
                "consecutive_failure_penalty_cap": 0.20,
                "internal_failure_penalties": {"verification_failed": 0.25, "unknown": 0.10},
                "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
            }]
        if "FROM failure_category_zones" in query:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM task_type_route_profiles" in query:
            return [{
                "task_type": "support",
                "affinity_labels": {
                    "primary": ["support", "chat", "analysis"],
                    "secondary": ["review", "research", "build"],
                    "specialized": [],
                    "fallback": ["multimodal"],
                    "avoid": ["audio", "image", "image-generation", "image-editing"],
                },
                "affinity_weights": {"primary": 1.0, "secondary": 0.72, "specialized": 0.35, "fallback": 0.30, "unclassified": 0.25, "avoid": 0.0},
                "task_rank_weights": {"affinity": 0.45, "route_tier": 0.15, "latency": 0.40},
                "benchmark_metric_weights": {},
                "route_tier_preferences": ["medium", "low", "high"],
                "latency_class_preferences": ["instant", "reasoning"],
                "allow_unclassified_candidates": True,
                "rationale": "support profile",
            }]
        if "FROM market_benchmark_metric_registry" in query:
            return []
        if "FROM provider_model_candidates" in query:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 1,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["support", "chat", "analysis"],
                    "task_affinities": {"primary": ["support", "chat"], "secondary": ["analysis"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                }
            ]
        if "FROM task_type_routing" in query:
            return [
                {
                    "model_slug": "gpt-5.4-mini",
                    "provider_slug": "openai",
                    "rank": 1,
                    "benchmark_score": 0.91,
                    "benchmark_name": "routing-smoke",
                    "cost_per_m_tokens": 0.25,
                    "rationale": "support lane",
                    "route_health_score": 0.92,
                    "consecutive_internal_failures": 0,
                }
            ]
        return []


def test_assemble_short_circuits_to_support_ticket_drafts(monkeypatch):
    assembler = task_assembler.TaskAssembler(_FakeAssemblerConn())
    drafts = [
        {
            "ticket_id": "ticket-1",
            "subject": "Re: Billing delay",
            "body": "We are looking into this now.",
            "tone": "calm",
        }
    ]

    monkeypatch.setattr(task_assembler, "looks_like_ticket_drafting_task", lambda task: True)
    monkeypatch.setattr(task_assembler, "draft_ticket_responses", lambda **kwargs: drafts)
    monkeypatch.setattr(
        task_assembler.TaskAssembler,
        "_support_ticket_draft_manifest_id",
        lambda self, *, task, drafts: "support-ticket-drafts-1234567890abcdef",
    )
    monkeypatch.setattr(task_assembler, "load_app_manifest_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        task_assembler,
        "upsert_app_manifest",
        lambda *args, **kwargs: {
            "id": "support-ticket-drafts-1234567890abcdef",
            "name": "Draft a support ticket reply",
            "description": "Deterministic support ticket drafts (1 responses)",
            "manifest": {
                "kind": "support_reply_drafts",
                "manifest_family": "support",
                "manifest_type": "reply_drafts",
                "status": "draft",
                "task": "Draft a support ticket reply",
                "drafts": drafts,
                "draft_count": 1,
                "source": "deterministic_support_fallback",
            },
            "version": 1,
        },
    )
    monkeypatch.setattr(
        task_assembler,
        "record_app_manifest_history",
        lambda *args, **kwargs: {"id": "history-1"},
    )
    monkeypatch.setattr(
        task_assembler.TaskAssembler,
        "_pre_suggest",
        lambda self, task: (_ for _ in ()).throw(AssertionError("_pre_suggest should not run")),
    )

    result = assembler.assemble("Draft a support ticket reply")

    assert result == {
        "manifest_id": "support-ticket-drafts-1234567890abcdef",
        "plan_summary": "Drafted 1 support ticket response(s) deterministically",
        "drafts": drafts,
        "source": "deterministic_support_fallback",
    }


def test_pre_suggest_includes_workflow_registry_matches():
    assembler = task_assembler.TaskAssembler(_FakeAssemblerConn())

    suggestions = assembler._pre_suggest("invoice workflow approval")
    prompt = assembler._build_planner_prompt("invoice workflow approval", suggestions)

    assert [item["id"] for item in suggestions.integrations] == ["gmail"]
    assert [item["id"] for item in suggestions.modules] == ["data-table"]
    assert [item["id"] for item in suggestions.calculations] == ["common_calcs"]
    assert [item["id"] for item in suggestions.workflows] == ["workflow_composition"]
    assert "MATCHED CALCULATIONS:" in prompt
    assert "MATCHED WORKFLOWS:" in prompt


def test_task_type_router_resolves_auto_support():
    router = task_type_router.TaskTypeRouter(_FakeRouterConn())

    decision = router.resolve("auto/support")

    assert decision.task_type == "support"
    assert decision.provider_slug == "openai"
    assert decision.model_slug == "gpt-5.4-mini"
    assert decision.was_auto is True
