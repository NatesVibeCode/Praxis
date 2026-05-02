from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from _pg_test_conn import ensure_test_database_ready
from registry.domain import RuntimeProfile
from registry.provider_routing import PostgresProviderRouteAuthorityRepository
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from runtime.provider_route_runtime import (
    ProviderRouteRuntimeError,
    resolve_provider_route_runtime,
)
from storage.postgres import connect_workflow_database


_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


async def _seed_route_catalog(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[RuntimeProfile, str, str, str]:
    runtime_profile = RuntimeProfile(
        runtime_profile_ref=f"runtime_profile.provider-route-runtime.{suffix}",
        model_profile_id=f"model_profile.provider-route-runtime.{suffix}",
        provider_policy_id=f"provider_policy.provider-route-runtime.{suffix}",
    )
    primary_candidate_ref = f"candidate.openai.provider-route-runtime.{suffix}.gpt54"
    fallback_candidate_ref = f"candidate.openai.provider-route-runtime.{suffix}.gpt54mini"
    future_candidate_ref = f"candidate.openai.provider-route-runtime.{suffix}.gpt54nano"

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
        runtime_profile.model_profile_id,
        f"provider-route-runtime.{suffix}",
        "openai",
        "gpt-5.4",
        1,
        "active",
        _jsonb({"tier": "bounded"}),
        _jsonb({"selection": "binding_order"}),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        None,
        as_of,
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
        runtime_profile.provider_policy_id,
        f"provider-route-runtime.{suffix}",
        "openai",
        "runtime",
        1,
        "active",
        _jsonb(["gpt-5.4", "gpt-5.4-mini"]),
        _jsonb({"retry": 0}),
        _jsonb({"budget": "bounded"}),
        _jsonb({"mode": "provider_catalog"}),
        as_of - timedelta(hours=1),
        None,
        f"decision:provider-policy:{suffix}",
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
        primary_candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4",
        "active",
        5,
        1,
        _jsonb(["primary", "bounded-runtime"]),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        f"decision:candidate-primary:{suffix}",
        as_of,
        "high",
        1,
        "reasoning",
        2,
        _jsonb({"default": "high", "kind": "openai_reasoning_effort"}),
        _jsonb({"primary": ["build"], "secondary": ["review"], "avoid": []}),
        _jsonb({"positioning": "provider route runtime primary seed", "source_refs": ["integration_test"]}),
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
        fallback_candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4-mini",
        "active",
        10,
        1,
        _jsonb(["fallback", "bounded-runtime"]),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        f"decision:candidate-fallback:{suffix}",
        as_of,
        "medium",
        2,
        "instant",
        1,
        _jsonb({"default": "medium", "kind": "openai_reasoning_effort"}),
        _jsonb({"primary": ["wiring"], "secondary": ["review"], "avoid": []}),
        _jsonb({"positioning": "provider route runtime fallback seed", "source_refs": ["integration_test"]}),
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
        future_candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4-nano",
        "active",
        1,
        1,
        _jsonb(["future", "bounded-runtime"]),
        _jsonb({"temperature": 0}),
        as_of + timedelta(minutes=5),
        None,
        f"decision:candidate-future:{suffix}",
        as_of + timedelta(minutes=5),
        "low",
        3,
        "instant",
        3,
        _jsonb({"default": "low", "kind": "openai_reasoning_effort"}),
        _jsonb({"primary": ["batch"], "secondary": ["wiring"], "avoid": []}),
        _jsonb({"positioning": "provider route runtime future seed", "source_refs": ["integration_test"]}),
    )
    await conn.execute(
        """
        DELETE FROM model_profile_candidate_bindings
        WHERE candidate_ref = ANY($1::text[])
        """,
        [primary_candidate_ref, fallback_candidate_ref, future_candidate_ref],
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
        f"binding.provider-route-runtime.primary.{suffix}",
        runtime_profile.model_profile_id,
        primary_candidate_ref,
        "primary",
        0,
        as_of - timedelta(hours=1),
        None,
        as_of,
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
        f"binding.provider-route-runtime.fallback.{suffix}",
        runtime_profile.model_profile_id,
        fallback_candidate_ref,
        "fallback",
        1,
        as_of - timedelta(hours=1),
        None,
        as_of,
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
        f"binding.provider-route-runtime.future.{suffix}",
        runtime_profile.model_profile_id,
        future_candidate_ref,
        "future",
        2,
        as_of + timedelta(minutes=5),
        None,
        as_of + timedelta(minutes=5),
    )
    return (
        runtime_profile,
        primary_candidate_ref,
        fallback_candidate_ref,
        future_candidate_ref,
    )


async def _seed_control_tower(
    conn,
    *,
    suffix: str,
    runtime_profile: RuntimeProfile,
    primary_candidate_ref: str,
    fallback_candidate_ref: str,
    future_candidate_ref: str,
    as_of: datetime,
) -> tuple[str, str, str]:
    primary_health_window_id = f"health.provider-route-runtime.primary.{suffix}"
    fallback_health_window_id = f"health.provider-route-runtime.fallback.{suffix}"
    future_health_window_id = f"health.provider-route-runtime.future.{suffix}"
    budget_window_id = f"budget.provider-route-runtime.{suffix}"
    primary_eligible_state_id = f"eligibility.provider-route-runtime.primary.eligible.{suffix}"

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
        primary_candidate_ref,
        "provider.openai",
        "healthy",
        0.99,
        18,
        0.0,
        110,
        as_of - timedelta(minutes=30),
        as_of - timedelta(minutes=1),
        f"observation:primary:{suffix}",
        as_of,
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
        fallback_health_window_id,
        fallback_candidate_ref,
        "provider.openai",
        "healthy",
        0.97,
        22,
        0.01,
        130,
        as_of - timedelta(minutes=30),
        as_of - timedelta(minutes=1),
        f"observation:fallback:{suffix}",
        as_of,
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
        future_health_window_id,
        future_candidate_ref,
        "provider.openai",
        "healthy",
        0.995,
        30,
        0.0,
        70,
        as_of - timedelta(minutes=30),
        as_of - timedelta(minutes=1),
        f"observation:future:{suffix}",
        as_of,
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
        runtime_profile.provider_policy_id,
        "provider.openai",
        "runtime",
        "available",
        as_of - timedelta(hours=1),
        as_of + timedelta(hours=1),
        500,
        12,
        100000,
        2200,
        "250.000000",
        "11.000000",
        f"decision:budget:{suffix}",
        as_of,
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
        primary_eligible_state_id,
        runtime_profile.model_profile_id,
        runtime_profile.provider_policy_id,
        primary_candidate_ref,
        "eligible",
        "provider_fallback.healthy_budget_available",
        _jsonb((primary_health_window_id, budget_window_id)),
        as_of - timedelta(minutes=10),
        None,
        f"decision:primary-eligible:{suffix}",
        as_of - timedelta(minutes=10),
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
        f"eligibility.provider-route-runtime.primary.future-hold.{suffix}",
        runtime_profile.model_profile_id,
        runtime_profile.provider_policy_id,
        primary_candidate_ref,
        "ineligible",
        "provider_fallback.future_hold",
        _jsonb((primary_health_window_id, budget_window_id)),
        as_of + timedelta(minutes=5),
        None,
        f"decision:primary-future-hold:{suffix}",
        as_of + timedelta(minutes=5),
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
        f"eligibility.provider-route-runtime.fallback.eligible.{suffix}",
        runtime_profile.model_profile_id,
        runtime_profile.provider_policy_id,
        fallback_candidate_ref,
        "eligible",
        "provider_fallback.healthy_budget_available",
        _jsonb((fallback_health_window_id, budget_window_id)),
        as_of - timedelta(minutes=10),
        None,
        f"decision:fallback-eligible:{suffix}",
        as_of - timedelta(minutes=10),
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
        f"eligibility.provider-route-runtime.future.eligible.{suffix}",
        runtime_profile.model_profile_id,
        runtime_profile.provider_policy_id,
        future_candidate_ref,
        "eligible",
        "provider_fallback.healthy_budget_available",
        _jsonb((future_health_window_id, budget_window_id)),
        as_of - timedelta(minutes=10),
        None,
        f"decision:future-eligible:{suffix}",
        as_of - timedelta(minutes=10),
    )
    return primary_eligible_state_id, primary_health_window_id, budget_window_id


def test_provider_route_runtime_wires_control_tower_authority_on_one_bounded_native_path() -> None:
    asyncio.run(_exercise_provider_route_runtime_wiring())


async def _exercise_provider_route_runtime_wiring() -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL}
    )

    transaction = conn.transaction()
    await transaction.start()
    try:
        suffix = _unique_suffix()
        as_of = _fixed_clock()

        route_catalog_repository = PostgresRouteCatalogRepository(conn)
        control_tower_repository = PostgresProviderRouteAuthorityRepository(conn)
        await route_catalog_repository.bootstrap_route_catalog_schema()
        await control_tower_repository.bootstrap_provider_route_authority_schema()

        (
            runtime_profile,
            primary_candidate_ref,
            fallback_candidate_ref,
            future_candidate_ref,
        ) = await _seed_route_catalog(conn, suffix=suffix, as_of=as_of)
        primary_eligible_state_id, primary_health_window_id, budget_window_id = (
            await _seed_control_tower(
                conn,
                suffix=suffix,
                runtime_profile=runtime_profile,
                primary_candidate_ref=primary_candidate_ref,
                fallback_candidate_ref=fallback_candidate_ref,
                future_candidate_ref=future_candidate_ref,
                as_of=as_of,
            )
        )

        catalog_snapshot = await route_catalog_repository.load_route_catalog(
            model_profile_ids=(runtime_profile.model_profile_id,),
            provider_policy_ids=(runtime_profile.provider_policy_id,),
            candidate_refs=(
                primary_candidate_ref,
                fallback_candidate_ref,
                future_candidate_ref,
            ),
            as_of=as_of,
        )

        assert catalog_snapshot.model_profiles[runtime_profile.model_profile_id][0].candidate_refs == (
            primary_candidate_ref,
            fallback_candidate_ref,
        )
        assert set(catalog_snapshot.provider_model_candidates.keys()) == {
            primary_candidate_ref,
            fallback_candidate_ref,
        }

        resolution = await resolve_provider_route_runtime(
            conn,
            runtime_profile=runtime_profile,
            as_of=as_of,
            preferred_candidate_ref=primary_candidate_ref,
        )
        payload = resolution.to_json()

        assert resolution.selected_candidate_ref == primary_candidate_ref
        assert resolution.route_eligibility_state.route_eligibility_state_id == primary_eligible_state_id
        assert resolution.route_eligibility_state.reason_code == (
            "provider_fallback.healthy_budget_available"
        )
        assert resolution.route_decision.decision_reason_code == "routing.preferred_candidate"
        assert resolution.route_decision.allowed_candidate_refs == (
            primary_candidate_ref,
            fallback_candidate_ref,
        )
        assert [
            record.provider_route_health_window_id
            for record in resolution.source_provider_route_health_windows
        ] == [primary_health_window_id]
        assert [
            record.provider_budget_window_id
            for record in resolution.source_provider_budget_windows
        ] == [budget_window_id]
        assert payload == {
            "as_of": as_of.isoformat(),
            "authorities": {
                "route_catalog": "registry.route_catalog_repository",
                "route": "registry.provider_routing",
            },
            "runtime_profile": {
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
            },
            "route": {
                "route_decision_id": resolution.route_decision.route_decision_id,
                "selected_candidate_ref": primary_candidate_ref,
                "provider_ref": "provider.openai",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "balance_slot": 0,
                "decision_reason_code": "routing.preferred_candidate",
                "allowed_candidate_refs": [
                    primary_candidate_ref,
                    fallback_candidate_ref,
                ],
            },
            "route_eligibility_state": {
                "route_eligibility_state_id": primary_eligible_state_id,
                "eligibility_status": "eligible",
                "reason_code": "provider_fallback.healthy_budget_available",
                "source_window_refs": [
                    primary_health_window_id,
                    budget_window_id,
                ],
                "evaluated_at": (as_of - timedelta(minutes=10)).isoformat(),
                "decision_ref": f"decision:primary-eligible:{suffix}",
                "source_provider_route_health_window_ids": [
                    primary_health_window_id,
                ],
                "source_provider_budget_window_ids": [
                    budget_window_id,
                ],
            },
        }

        with pytest.raises(ProviderRouteRuntimeError) as future_catalog_exc_info:
            await resolve_provider_route_runtime(
                conn,
                runtime_profile=runtime_profile,
                as_of=as_of,
                preferred_candidate_ref=future_candidate_ref,
            )

        assert future_catalog_exc_info.value.reason_code == "provider_route_runtime.routing_failed"
        assert future_catalog_exc_info.value.details["reason_code"] == "routing.preference_unknown"
        assert future_catalog_exc_info.value.details["metadata"] == {
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "preferred_candidate_ref": future_candidate_ref,
        }

        with pytest.raises(ProviderRouteRuntimeError) as exc_info:
            await resolve_provider_route_runtime(
                conn,
                runtime_profile=runtime_profile,
                as_of=as_of + timedelta(minutes=10),
                preferred_candidate_ref=primary_candidate_ref,
            )

        assert exc_info.value.reason_code == "provider_route_runtime.routing_failed"
        assert exc_info.value.details["reason_code"] == "routing.preference_unknown"
        assert exc_info.value.details["metadata"] == {
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "preferred_candidate_ref": primary_candidate_ref,
        }
    finally:
        await transaction.rollback()
        await conn.close()
