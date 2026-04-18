from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from _pg_test_conn import ensure_test_database_ready
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from registry.provider_routing import (
    PostgresProviderRouteAuthorityRepository,
    load_provider_route_authority,
)
from storage.postgres import connect_workflow_database


_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)


async def _seed_route_catalog_prereqs(
    conn,
    *,
    suffix: str,
) -> tuple[str, str, str]:
    clock = _fixed_clock()
    model_profile_id = f"model_profile.{suffix}"
    provider_policy_id = f"provider_policy.{suffix}"
    candidate_ref = f"candidate.openai.{suffix}.gpt54mini"

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
            created_at,
            route_tier,
            route_tier_rank,
            latency_class,
            latency_rank,
            reasoning_control,
            task_affinities,
            benchmark_profile
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14,
            $15, $16, $17, $18, $19::jsonb, $20::jsonb, $21::jsonb
        )
        """,
        candidate_ref,
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
        f"decision.{suffix}.candidate",
        clock,
        "medium",
        1,
        "instant",
        1,
        json.dumps({"default": "medium"}),
        json.dumps(
            {
                "primary": ["build", "wiring"],
                "secondary": ["review", "chat"],
                "avoid": [],
            }
        ),
        json.dumps(
            {
                "evidence_level": "test_seed",
                "positioning": "Fast OpenAI route for control-tower authority tests.",
            }
        ),
    )

    return model_profile_id, provider_policy_id, candidate_ref


async def _seed_provider_route_control_tower_rows(
    conn,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    candidate_ref: str,
) -> tuple[str, str, str, str]:
    clock = _fixed_clock()
    later_clock = clock + timedelta(minutes=1)
    health_window_healthy_id = f"health.{suffix}.healthy"
    health_window_degraded_id = f"health.{suffix}.degraded"
    budget_window_id = f"budget.{suffix}.runtime"
    eligible_state_id = f"eligibility.{suffix}.eligible"
    rejected_state_id = f"eligibility.{suffix}.rejected"

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
        health_window_healthy_id,
        candidate_ref,
        "provider.openai",
        "healthy",
        0.98,
        24,
        0.01,
        125,
        clock - timedelta(minutes=10),
        clock,
        f"observation.{suffix}.healthy",
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
        health_window_degraded_id,
        candidate_ref,
        "provider.openai",
        "degraded",
        0.5,
        12,
        0.4,
        800,
        clock - timedelta(minutes=5),
        later_clock,
        f"observation.{suffix}.degraded",
        later_clock,
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
        later_clock,
        1000,
        120,
        200000,
        5000,
        "500.000000",
        "120.000000",
        f"decision.{suffix}.budget",
        later_clock,
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
        eligible_state_id,
        model_profile_id,
        provider_policy_id,
        candidate_ref,
        "eligible",
        "provider_fallback.healthy_budget_available",
        json.dumps([health_window_healthy_id, budget_window_id]),
        clock,
        None,
        f"decision.{suffix}.eligibility.eligible",
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
        rejected_state_id,
        model_profile_id,
        provider_policy_id,
        candidate_ref,
        "rejected",
        "provider_fallback.health_degraded",
        json.dumps([health_window_degraded_id, budget_window_id]),
        later_clock,
        None,
        f"decision.{suffix}.eligibility.rejected",
        later_clock,
    )

    return health_window_healthy_id, health_window_degraded_id, budget_window_id, rejected_state_id


def test_provider_route_control_tower_repository_is_deterministic_and_fail_closed() -> None:
    asyncio.run(_exercise_provider_route_control_tower_repository())


async def _exercise_provider_route_control_tower_repository() -> None:
    conn = None
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL},
    )

    try:
        suffix = _unique_suffix()
        catalog_repository = PostgresRouteCatalogRepository(conn)
        repository = PostgresProviderRouteAuthorityRepository(conn)
        await catalog_repository.bootstrap_route_catalog_schema()
        await repository.bootstrap_provider_route_authority_schema()
        model_profile_id, provider_policy_id, candidate_ref = await _seed_route_catalog_prereqs(
            conn,
            suffix=suffix,
        )
        health_window_healthy_id, health_window_degraded_id, budget_window_id, rejected_state_id = (
            await _seed_provider_route_control_tower_rows(
                conn,
                suffix=suffix,
                model_profile_id=model_profile_id,
                provider_policy_id=provider_policy_id,
                candidate_ref=candidate_ref,
            )
        )

        control_tower = await repository.load_provider_route_authority(
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=(candidate_ref,),
        )
        control_tower_repeat = await load_provider_route_authority(
            conn,
            model_profile_ids=(model_profile_id,),
            provider_policy_ids=(provider_policy_id,),
            candidate_refs=(candidate_ref,),
        )

        assert control_tower == control_tower_repeat
        assert tuple(control_tower.provider_route_health_windows.keys()) == (candidate_ref,)
        assert tuple(control_tower.provider_budget_windows.keys()) == (provider_policy_id,)
        assert tuple(control_tower.route_eligibility_states.keys()) == (candidate_ref,)

        health_windows = control_tower.provider_route_health_windows[candidate_ref]
        budget_windows = control_tower.provider_budget_windows[provider_policy_id]
        eligibility_states = control_tower.route_eligibility_states[candidate_ref]

        assert tuple(window.provider_route_health_window_id for window in health_windows) == (
            health_window_degraded_id,
            health_window_healthy_id,
        )
        assert health_windows[0].health_status == "degraded"
        assert health_windows[1].health_status == "healthy"
        assert tuple(window.provider_budget_window_id for window in budget_windows) == (
            budget_window_id,
        )
        assert budget_windows[0].budget_status == "available"
        assert tuple(state.route_eligibility_state_id for state in eligibility_states) == (
            rejected_state_id,
            f"eligibility.{suffix}.eligible",
        )
        assert eligibility_states[0].eligibility_status == "rejected"
        assert eligibility_states[0].reason_code == "provider_fallback.health_degraded"
        assert eligibility_states[1].eligibility_status == "eligible"
    finally:
        if conn is not None:
            await conn.close()
