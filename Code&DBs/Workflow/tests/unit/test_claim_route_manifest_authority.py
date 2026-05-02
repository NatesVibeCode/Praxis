from __future__ import annotations

from runtime.workflow._routing import _select_claim_route


class _ClaimConn:
    def execute(self, query: str, *args):
        if "route_plan_manifest" in query:
            return [
                {
                    "route_plan_manifest": {
                        "jobs": {
                            "phase_build": {
                                "route_candidates": [
                                    {
                                        "slug": "openai/gpt-5.4",
                                        "provider_slug": "openai",
                                        "model_slug": "gpt-5.4",
                                        "transport_type": "API",
                                        "adapter_type": "llm_task",
                                    },
                                    {
                                        "slug": "anthropic/claude-sonnet-4-6",
                                        "provider_slug": "anthropic",
                                        "model_slug": "claude-sonnet-4-6",
                                        "transport_type": "CLI",
                                        "adapter_type": "cli_llm",
                                    },
                                ],
                            }
                        }
                    }
                }
            ]
        if "FROM route_policy_registry" in query:
            return [
                {
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
                }
            ]
        if "FROM provider_model_candidates" in query:
            return [
                {
                    "candidate_ref": "cand-openai",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "transport_type": "API",
                    "priority": 0,
                },
                {
                    "candidate_ref": "cand-anthropic",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "transport_type": "CLI",
                    "priority": 0,
                },
            ]
        if "FROM failure_category_zones" in query:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM task_type_route_profiles" in query:
            return [{
                "task_type": "build",
                "affinity_labels": {
                    "primary": ["build", "coding"],
                    "secondary": ["review", "analysis"],
                    "specialized": [],
                    "fallback": ["chat"],
                    "avoid": [],
                },
                "affinity_weights": {
                    "primary": 1.0,
                    "secondary": 0.7,
                    "specialized": 0.4,
                    "fallback": 0.2,
                    "unclassified": 0.1,
                    "avoid": 0.0,
                },
                "task_rank_weights": {"affinity": 0.6, "route_tier": 0.25, "latency": 0.15},
                "benchmark_metric_weights": {},
                "route_tier_preferences": ["high", "medium", "low"],
                "latency_class_preferences": ["reasoning", "instant"],
                "allow_unclassified_candidates": True,
                "rationale": "build profile",
            }]
        if "FROM market_benchmark_metric_registry" in query:
            return []
        if "FROM workflow_runs" in query:
            return [{"runtime_profile_ref": "nate-private"}]
        if "FROM effective_private_provider_job_catalog" in query:
            return [
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "model_version": "gpt-5.4",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {},
                    "reason_code": "catalog.available",
                    "candidate_ref": "cand-openai",
                    "provider_ref": "provider.openai",
                    "source_refs": [],
                    "projected_at": None,
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
                {
                    "runtime_profile_ref": "nate-private",
                    "job_type": "build",
                    "transport_type": "CLI",
                    "adapter_type": "cli_llm",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "model_version": "claude-sonnet-4-6",
                    "cost_structure": "subscription_included",
                    "cost_metadata": {},
                    "reason_code": "catalog.available",
                    "candidate_ref": "cand-anthropic",
                    "provider_ref": "provider.anthropic",
                    "source_refs": [],
                    "projected_at": None,
                    "projection_ref": "projection.private_provider_control_plane_snapshot",
                },
            ]
        if "FROM registry_runtime_profile_authority" in query:
            return []
        if "GROUP BY 1" in query:
            return []
        if "FROM task_type_routing" in query:
            if "permitted = true" in query:
                return [
                    {
                        "provider_slug": "openai",
                        "model_slug": "gpt-5.4",
                        "rank": 1,
                        "route_health_score": 0.80,
                        "consecutive_internal_failures": 0,
                    },
                    {
                        "provider_slug": "anthropic",
                        "model_slug": "claude-sonnet-4-6",
                        "rank": 2,
                        "route_health_score": 0.80,
                        "consecutive_internal_failures": 0,
                    },
                ]
            return []
        raise AssertionError(query)


class _CandidateRefClaimConn(_ClaimConn):
    def execute(self, query: str, *args):
        if "route_plan_manifest" in query:
            return [
                {
                    "route_plan_manifest": {
                        "jobs": {
                            "phase_build": {
                                "route_candidates": [
                                    {
                                        "slug": "openai/gpt-5.4",
                                        "candidate_ref": "cand-openai-stale",
                                        "provider_slug": "openai",
                                        "model_slug": "gpt-5.4",
                                        "transport_type": "CLI",
                                        "adapter_type": "cli_llm",
                                    },
                                    {
                                        "slug": "anthropic/claude-sonnet-4-6",
                                        "candidate_ref": "cand-anthropic",
                                        "provider_slug": "anthropic",
                                        "model_slug": "claude-sonnet-4-6",
                                        "transport_type": "CLI",
                                        "adapter_type": "cli_llm",
                                    },
                                ],
                            }
                        }
                    }
                }
            ]
        return super().execute(query, *args)


def test_select_claim_route_uses_manifest_transport_but_current_catalog_authority() -> None:
    selected = _select_claim_route(
        _ClaimConn(),
        {
            "run_id": "run-with-stale-manifest",
            "label": "phase_build",
            "agent_slug": "openai/gpt-5.4",
            "failover_chain": ["openai/gpt-5.4", "anthropic/claude-sonnet-4-6"],
            "route_task_type": "build",
        },
    )

    assert selected == "anthropic/claude-sonnet-4-6"


def test_select_claim_route_uses_manifest_candidate_ref_when_present() -> None:
    selected = _select_claim_route(
        _CandidateRefClaimConn(),
        {
            "run_id": "run-with-candidate-ref-manifest",
            "label": "phase_build",
            "agent_slug": "openai/gpt-5.4",
            "failover_chain": ["openai/gpt-5.4", "anthropic/claude-sonnet-4-6"],
            "route_task_type": "build",
        },
    )

    assert selected == "anthropic/claude-sonnet-4-6"
