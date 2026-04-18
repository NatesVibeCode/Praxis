from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from _pg_test_conn import ensure_test_database_ready
from registry.domain import RuntimeProfile
from registry.model_routing import ModelRouter, ModelRoutingError
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from storage.postgres import connect_workflow_database


_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)


async def _seed_route_catalog(conn, *, suffix: str) -> tuple[str, str, tuple[str, ...]]:
    model_profile_id = f"model_profile.{suffix}"
    provider_policy_id = f"provider_policy.{suffix}"
    candidate_refs = (
        f"candidate.openai.{suffix}.gpt54mini",
        f"candidate.openai.{suffix}.gpt54",
        f"candidate.anthropic.{suffix}.sonnet",
    )

    clock = _fixed_clock()
    await conn.execute(
        """
        INSERT INTO model_profiles (
            model_profile_id,
            profile_name,
            provider_name,
            model_name,
            schema_version,
            status,
            budget_policy,
            routing_policy,
            default_parameters,
            effective_from,
            effective_to,
            supersedes_model_profile_id,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11, $12, $13)
        """,
        model_profile_id,
        f"profile.{suffix}",
        "openai",
        "gpt-5.4",
        1,
        "active",
        json.dumps({"tier": "baseline"}),
        json.dumps({"selection": "binding_order"}),
        json.dumps({"temperature": 0}),
        clock,
        None,
        None,
        clock,
    )
    await conn.execute(
        """
        INSERT INTO provider_policies (
            provider_policy_id,
            policy_name,
            provider_name,
            scope,
            schema_version,
            status,
            allowed_models,
            retry_policy,
            budget_policy,
            routing_rules,
            allowed_provider_refs,
            preferred_provider_ref,
            effective_from,
            effective_to,
            decision_ref
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11::jsonb,
            $12,
            $13,
            $14,
            $15
        )
        """,
        provider_policy_id,
        f"policy.{suffix}",
        "openai",
        "runtime",
        1,
        "active",
        json.dumps(["gpt-5.4", "gpt-5.4-mini"]),
        json.dumps({"retry": 0}),
        json.dumps({"budget": "standard"}),
        json.dumps({"mode": "provider_catalog"}),
        json.dumps(["provider.openai"]),
        "provider.openai",
        clock,
        None,
        f"decision.{suffix}",
    )
    await conn.execute(
        """
        INSERT INTO provider_model_candidates (
            candidate_ref,
            provider_ref,
            provider_name,
            provider_slug,
            model_slug,
            status,
            priority,
            balance_weight,
            capability_tags,
            cli_config,
            route_tier,
            route_tier_rank,
            latency_class,
            latency_rank,
            reasoning_control,
            task_affinities,
            benchmark_profile,
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12,
            $13,
            $14,
            $15::jsonb,
            $16::jsonb,
            $17::jsonb,
            $18::jsonb,
            $19,
            $20,
            $21,
            $22
        )
        """,
        candidate_refs[0],
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4-mini",
        "active",
        20,
        1,
        json.dumps(["fallback", "latency"]),
        json.dumps({}),
        "medium",
        1,
        "instant",
        1,
        json.dumps({"default": "medium", "kind": "openai_reasoning_effort"}),
        json.dumps({"primary": ["build"], "secondary": ["review"], "avoid": []}),
        json.dumps({"positioning": "Fast mini route", "source_refs": ["test_seed"]}),
        json.dumps({"temperature": 0.2}),
        clock,
        None,
        f"decision.{suffix}.mini",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO provider_model_candidates (
            candidate_ref,
            provider_ref,
            provider_name,
            provider_slug,
            model_slug,
            status,
            priority,
            balance_weight,
            capability_tags,
            cli_config,
            route_tier,
            route_tier_rank,
            latency_class,
            latency_rank,
            reasoning_control,
            task_affinities,
            benchmark_profile,
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12,
            $13,
            $14,
            $15::jsonb,
            $16::jsonb,
            $17::jsonb,
            $18::jsonb,
            $19,
            $20,
            $21,
            $22
        )
        """,
        candidate_refs[1],
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4",
        "active",
        5,
        3,
        json.dumps(["primary", "reasoning"]),
        json.dumps({}),
        "high",
        1,
        "reasoning",
        2,
        json.dumps({"default": "high", "kind": "openai_reasoning_effort"}),
        json.dumps({"primary": ["architecture"], "secondary": ["build"], "avoid": []}),
        json.dumps({"positioning": "Primary reasoning route", "source_refs": ["test_seed"]}),
        json.dumps({"temperature": 0.0}),
        clock,
        None,
        f"decision.{suffix}.primary",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO provider_model_candidates (
            candidate_ref,
            provider_ref,
            provider_name,
            provider_slug,
            model_slug,
            status,
            priority,
            balance_weight,
            capability_tags,
            cli_config,
            route_tier,
            route_tier_rank,
            latency_class,
            latency_rank,
            reasoning_control,
            task_affinities,
            benchmark_profile,
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12,
            $13,
            $14,
            $15::jsonb,
            $16::jsonb,
            $17::jsonb,
            $18::jsonb,
            $19,
            $20,
            $21,
            $22
        )
        """,
        candidate_refs[2],
        "provider.anthropic",
        "anthropic",
        "anthropic",
        "claude-sonnet-4-5",
        "active",
        10,
        1,
        json.dumps(["blocked"]),
        json.dumps({}),
        "high",
        2,
        "reasoning",
        4,
        json.dumps({"default": "adaptive", "kind": "anthropic_thinking"}),
        json.dumps({"primary": ["research"], "secondary": ["review"], "avoid": []}),
        json.dumps({"positioning": "Blocked cross-provider route", "source_refs": ["test_seed"]}),
        json.dumps({"temperature": 0.5}),
        clock,
        None,
        f"decision.{suffix}.blocked",
        clock,
    )
    await conn.execute(
        """
        DELETE FROM model_profile_candidate_bindings
        WHERE candidate_ref = ANY($1::text[])
        """,
        candidate_refs,
    )
    await conn.execute(
        """
        INSERT INTO model_profile_candidate_bindings (
            model_profile_candidate_binding_id,
            model_profile_id,
            candidate_ref,
            binding_role,
            position_index,
            effective_from,
            effective_to,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        f"binding.{suffix}.mini",
        model_profile_id,
        candidate_refs[0],
        "fallback",
        0,
        clock,
        None,
        clock,
    )
    await conn.execute(
        """
        INSERT INTO model_profile_candidate_bindings (
            model_profile_candidate_binding_id,
            model_profile_id,
            candidate_ref,
            binding_role,
            position_index,
            effective_from,
            effective_to,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        f"binding.{suffix}.primary",
        model_profile_id,
        candidate_refs[1],
        "primary",
        1,
        clock,
        None,
        clock,
    )
    await conn.execute(
        """
        INSERT INTO model_profile_candidate_bindings (
            model_profile_candidate_binding_id,
            model_profile_id,
            candidate_ref,
            binding_role,
            position_index,
            effective_from,
            effective_to,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        f"binding.{suffix}.blocked",
        model_profile_id,
        candidate_refs[2],
        "blocked",
        2,
        clock,
        None,
        clock,
    )

    return model_profile_id, provider_policy_id, candidate_refs


def test_route_catalog_repository_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_route_catalog_repository())


async def _exercise_route_catalog_repository() -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL}
    )
    try:
        suffix = _unique_suffix()
        repository = PostgresRouteCatalogRepository(conn)
        await repository.bootstrap_route_catalog_schema()
        model_profile_id, provider_policy_id, candidate_refs = await _seed_route_catalog(
            conn,
            suffix=suffix,
        )

        catalog = await repository.load_route_catalog(
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=candidate_refs,
        )

        assert tuple(catalog.model_profiles.keys()) == (model_profile_id,)
        assert tuple(catalog.provider_policies.keys()) == (provider_policy_id,)
        assert set(catalog.provider_model_candidates.keys()) == set(candidate_refs)
        assert tuple(
            binding.position_index
            for binding in catalog.model_profile_candidate_bindings[model_profile_id]
        ) == (0, 1, 2)
        assert catalog.model_profiles[model_profile_id][0].candidate_refs == (
            candidate_refs[0],
            candidate_refs[1],
            candidate_refs[2],
        )
        assert catalog.provider_policies[provider_policy_id][0].provider_name == "openai"
        assert catalog.provider_policies[provider_policy_id][0].allowed_provider_refs == (
            "provider.openai",
        )
        assert catalog.provider_policies[provider_policy_id][0].preferred_provider_ref == (
            "provider.openai"
        )

        route_authority = ProviderRouteAuthority(
            provider_route_health_windows={
                candidate_ref: (
                    ProviderRouteHealthWindowAuthorityRecord(
                        provider_route_health_window_id=f"health.{suffix}.{candidate_ref}",
                        candidate_ref=candidate_ref,
                        provider_ref="provider.openai"
                        if candidate_ref != candidate_refs[2]
                        else "provider.anthropic",
                        health_status="healthy" if candidate_ref != candidate_refs[2] else "degraded",
                        health_score=0.99 if candidate_ref != candidate_refs[2] else 0.2,
                        sample_count=16,
                        failure_rate=0.0 if candidate_ref != candidate_refs[2] else 0.8,
                        latency_p95_ms=120 if candidate_ref != candidate_refs[2] else 900,
                        observed_window_started_at=_fixed_clock() - timedelta(minutes=20),
                        observed_window_ended_at=_fixed_clock(),
                        observation_ref=f"observation.{suffix}.{candidate_ref}",
                        created_at=_fixed_clock(),
                    ),
                )
                for candidate_ref in candidate_refs
            },
            provider_budget_windows={
                provider_policy_id: (
                    ProviderBudgetWindowAuthorityRecord(
                        provider_budget_window_id=f"budget.{suffix}.{provider_policy_id}",
                        provider_policy_id=provider_policy_id,
                        provider_ref="provider.openai",
                        budget_scope="runtime",
                        budget_status="available",
                        window_started_at=_fixed_clock() - timedelta(minutes=30),
                        window_ended_at=_fixed_clock() + timedelta(minutes=30),
                        request_limit=1000,
                        requests_used=0,
                        token_limit=500_000,
                        tokens_used=0,
                        spend_limit_usd=None,
                        spend_used_usd=None,
                        decision_ref=f"decision.{suffix}.budget",
                        created_at=_fixed_clock(),
                    ),
                ),
            },
            route_eligibility_states={
                candidate_ref: (
                    RouteEligibilityStateAuthorityRecord(
                        route_eligibility_state_id=f"eligibility.{suffix}.{candidate_ref}",
                        model_profile_id=model_profile_id,
                        provider_policy_id=provider_policy_id,
                        candidate_ref=candidate_ref,
                        eligibility_status="eligible"
                        if candidate_ref != candidate_refs[2]
                        else "rejected",
                        reason_code=(
                            "provider_route_authority.healthy_budget_available"
                            if candidate_ref != candidate_refs[2]
                            else "provider_route_authority.manual_hold"
                        ),
                        source_window_refs=(
                            f"health.{suffix}.{candidate_ref}",
                            f"budget.{suffix}.{provider_policy_id}",
                        ),
                        evaluated_at=_fixed_clock() - timedelta(minutes=10),
                        expires_at=None,
                        decision_ref=f"decision.{suffix}.eligibility.{candidate_ref}",
                        created_at=_fixed_clock() - timedelta(minutes=10),
                    ),
                )
                for candidate_ref in candidate_refs
            },
        )
        router = ModelRouter(route_catalog=catalog, route_authority=route_authority)
        runtime_profile = RuntimeProfile(
            runtime_profile_ref=f"runtime_profile.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
        )

        allowed_candidates = router.resolve_candidates(runtime_profile=runtime_profile)
        assert tuple(candidate.candidate_ref for candidate in allowed_candidates) == (
            candidate_refs[0],
            candidate_refs[1],
        )
        assert tuple(
            (candidate.provider_ref, candidate.model_slug)
            for candidate in allowed_candidates
        ) == (
            ("provider.openai", "gpt-5.4-mini"),
            ("provider.openai", "gpt-5.4"),
        )

        route_zero = router.decide_route(runtime_profile=runtime_profile, balance_slot=0)
        route_zero_repeat = router.decide_route(runtime_profile=runtime_profile, balance_slot=0)
        route_three = router.decide_route(runtime_profile=runtime_profile, balance_slot=3)

        assert route_zero == route_zero_repeat
        assert route_zero.route_decision_id == route_zero_repeat.route_decision_id
        assert route_zero.allowed_candidate_refs == (
            candidate_refs[0],
            candidate_refs[1],
        )
        assert route_zero.selected_candidate_ref == candidate_refs[0]
        assert route_zero.provider_slug == "openai"
        assert route_zero.model_slug == "gpt-5.4-mini"
        assert route_three.selected_candidate_ref == candidate_refs[1]
        assert route_three.model_slug == "gpt-5.4"
        assert route_zero.route_decision_id != route_three.route_decision_id

        unknown_profile = RuntimeProfile(
            runtime_profile_ref=f"runtime_profile.missing.{suffix}",
            model_profile_id=f"model_profile.missing.{suffix}",
            provider_policy_id=provider_policy_id,
        )
        with pytest.raises(ModelRoutingError) as exc:
            router.resolve_candidates(runtime_profile=unknown_profile)
        assert exc.value.reason_code == "routing.model_profile_unknown"
    finally:
        await conn.close()
