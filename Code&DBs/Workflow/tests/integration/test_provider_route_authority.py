from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from registry.domain import RuntimeProfile
from registry.model_routing import ModelRouter, ModelRoutingError
from registry.provider_routing import PostgresProviderRouteAuthorityRepository
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from storage.postgres import connect_workflow_database


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
            effective_from,
            effective_to,
            decision_ref
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11, $12, $13)
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
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14)
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
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14)
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
            default_parameters,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14)
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
        json.dumps({"temperature": 0.5}),
        clock,
        None,
        f"decision.{suffix}.blocked",
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


async def _seed_provider_route_authority(
    conn,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    candidate_refs: tuple[str, ...],
) -> None:
    clock = _fixed_clock()
    later_clock = clock + timedelta(minutes=1)
    mini_health_window_id = f"health.{suffix}.mini"
    primary_health_window_id = f"health.{suffix}.primary"
    budget_window_id = f"budget.{suffix}.primary"

    await conn.execute(
        """
        INSERT INTO provider_route_health_windows (
            provider_route_health_window_id,
            candidate_ref,
            provider_ref,
            health_status,
            health_score,
            sample_count,
            failure_rate,
            latency_p95_ms,
            observed_window_started_at,
            observed_window_ended_at,
            observation_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        mini_health_window_id,
        candidate_refs[0],
        "provider.openai",
        "healthy",
        0.98,
        24,
        0.01,
        125,
        clock - timedelta(minutes=10),
        clock,
        f"observation.{suffix}.mini",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO provider_route_health_windows (
            provider_route_health_window_id,
            candidate_ref,
            provider_ref,
            health_status,
            health_score,
            sample_count,
            failure_rate,
            latency_p95_ms,
            observed_window_started_at,
            observed_window_ended_at,
            observation_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        primary_health_window_id,
        candidate_refs[1],
        "provider.openai",
        "degraded",
        0.42,
        12,
        0.47,
        810,
        clock - timedelta(minutes=10),
        clock,
        f"observation.{suffix}.primary",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO provider_budget_windows (
            provider_budget_window_id,
            provider_policy_id,
            provider_ref,
            budget_scope,
            budget_status,
            window_started_at,
            window_ended_at,
            request_limit,
            requests_used,
            token_limit,
            tokens_used,
            spend_limit_usd,
            spend_used_usd,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        """,
        budget_window_id,
        provider_policy_id,
        "provider.openai",
        "runtime",
        "available",
        clock - timedelta(hours=1),
        clock,
        1000,
        120,
        200000,
        5000,
        "500.000000",
        "120.000000",
        f"decision.{suffix}.budget",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO route_eligibility_states (
            route_eligibility_state_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            eligibility_status,
            reason_code,
            source_window_refs,
            evaluated_at,
            expires_at,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
        """,
        f"eligibility.{suffix}.mini",
        model_profile_id,
        provider_policy_id,
        candidate_refs[0],
        "eligible",
        "provider_routing.healthy_budget_available",
        json.dumps([mini_health_window_id, budget_window_id]),
        clock,
        None,
        f"decision.{suffix}.eligibility.mini",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO route_eligibility_states (
            route_eligibility_state_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            eligibility_status,
            reason_code,
            source_window_refs,
            evaluated_at,
            expires_at,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
        """,
        f"eligibility.{suffix}.primary.older",
        model_profile_id,
        provider_policy_id,
        candidate_refs[1],
        "eligible",
        "provider_routing.healthy_budget_available",
        json.dumps([primary_health_window_id, budget_window_id]),
        clock,
        None,
        f"decision.{suffix}.eligibility.primary.older",
        clock,
    )
    await conn.execute(
        """
        INSERT INTO route_eligibility_states (
            route_eligibility_state_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            eligibility_status,
            reason_code,
            source_window_refs,
            evaluated_at,
            expires_at,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
        """,
        f"eligibility.{suffix}.primary.newer",
        model_profile_id,
        provider_policy_id,
        candidate_refs[1],
        "rejected",
        "provider_routing.health_degraded",
        json.dumps([primary_health_window_id, budget_window_id]),
        later_clock,
        None,
        f"decision.{suffix}.eligibility.primary.newer",
        later_clock,
    )


def test_provider_route_authority_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_provider_route_authority())


async def _exercise_provider_route_authority() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    try:
        suffix = _unique_suffix()
        catalog_repository = PostgresRouteCatalogRepository(conn)
        authority_repository = PostgresProviderRouteAuthorityRepository(conn)
        await catalog_repository.bootstrap_route_catalog_schema()
        await authority_repository.bootstrap_provider_route_authority_schema()
        model_profile_id, provider_policy_id, candidate_refs = await _seed_route_catalog(
            conn,
            suffix=suffix,
        )
        await _seed_provider_route_authority(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_refs=candidate_refs,
        )

        catalog = await catalog_repository.load_route_catalog(
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=candidate_refs,
        )
        authority = await authority_repository.load_provider_route_authority(
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=candidate_refs,
        )
        authority_repeat = await authority_repository.load_provider_route_authority(
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=candidate_refs,
        )

        assert authority == authority_repeat
        assert authority.route_eligibility_states[candidate_refs[1]][0].eligibility_status == "rejected"

        router = ModelRouter(route_catalog=catalog, route_authority=authority)
        runtime_profile = RuntimeProfile(
            runtime_profile_ref=f"runtime_profile.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
        )

        allowed_candidates = router.resolve_candidates(runtime_profile=runtime_profile)
        assert tuple(candidate.candidate_ref for candidate in allowed_candidates) == (
            candidate_refs[0],
        )
        assert tuple(
            (candidate.provider_ref, candidate.provider_slug, candidate.model_slug)
            for candidate in allowed_candidates
        ) == (
            ("provider.openai", "openai", "gpt-5.4-mini"),
        )

        route_zero = router.decide_route(runtime_profile=runtime_profile, balance_slot=0)
        route_zero_repeat = router.decide_route(runtime_profile=runtime_profile, balance_slot=0)
        route_nine = router.decide_route(runtime_profile=runtime_profile, balance_slot=9)

        assert route_zero == route_zero_repeat
        assert route_zero.route_decision_id == route_zero_repeat.route_decision_id
        assert route_zero.allowed_candidate_refs == (candidate_refs[0],)
        assert route_zero.selected_candidate_ref == candidate_refs[0]
        assert route_nine.selected_candidate_ref == candidate_refs[0]
        assert route_zero.provider_slug == "openai"
        assert route_zero.model_slug == "gpt-5.4-mini"
        assert route_zero.route_decision_id != route_nine.route_decision_id

        fail_closed_authority = replace(
            authority,
            route_eligibility_states={
                candidate_refs[1]: authority.route_eligibility_states[candidate_refs[1]],
            },
        )
        fail_closed_router = ModelRouter(
            route_catalog=catalog,
            route_authority=fail_closed_authority,
        )
        with pytest.raises(ModelRoutingError) as exc:
            fail_closed_router.resolve_candidates(runtime_profile=runtime_profile)
        assert exc.value.reason_code == "routing.no_allowed_candidates"
    finally:
        await conn.close()
