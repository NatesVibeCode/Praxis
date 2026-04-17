from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from registry.domain import RuntimeProfile
from registry.provider_routing import PostgresProviderRouteAuthorityRepository
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from runtime.provider_route_runtime import (
    ProviderRouteRuntimeError,
    resolve_provider_route_runtime,
)
from storage.postgres import PostgresConfigurationError, connect_workflow_database


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 3, 20, 30, tzinfo=timezone.utc)


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


async def _create_failover_authority_table(conn) -> None:
    await conn.execute(
        """
        CREATE TEMP TABLE provider_failover_bindings (
            provider_failover_binding_id text PRIMARY KEY,
            model_profile_id text NOT NULL,
            provider_policy_id text NOT NULL,
            candidate_ref text NOT NULL,
            binding_scope text NOT NULL,
            failover_role text NOT NULL,
            trigger_rule text NOT NULL,
            position_index integer NOT NULL,
            effective_from timestamptz NOT NULL,
            effective_to timestamptz,
            decision_ref text NOT NULL,
            created_at timestamptz NOT NULL
        )
        """
    )


async def _seed_route_catalog(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[RuntimeProfile, str, str]:
    runtime_profile = RuntimeProfile(
        runtime_profile_ref=f"runtime_profile.bounded-failover-runtime.{suffix}",
        model_profile_id=f"model_profile.bounded-failover-runtime.{suffix}",
        provider_policy_id=f"provider_policy.bounded-failover-runtime.{suffix}",
    )
    primary_candidate_ref = f"candidate.openai.bounded-failover-runtime.{suffix}.gpt54"
    fallback_candidate_ref = f"candidate.openai.bounded-failover-runtime.{suffix}.gpt54mini"

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
        f"bounded-failover-runtime.{suffix}",
        "openai",
        "gpt-5.4",
        1,
        "active",
        _jsonb({"tier": "bounded"}),
        _jsonb({"selection": "catalog"}),
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
        f"bounded-failover-runtime.{suffix}",
        "openai",
        "runtime",
        1,
        "active",
        _jsonb(["gpt-5.4", "gpt-5.4-mini"]),
        _jsonb({"retry": 0}),
        _jsonb({"budget": "bounded"}),
        _jsonb({"mode": "bounded_failover"}),
        as_of - timedelta(hours=1),
        None,
        f"decision:provider-policy:{suffix}",
    )

    for candidate_ref, model_slug, priority, route_tier, route_tier_rank, latency_class, latency_rank in (
        (primary_candidate_ref, "gpt-5.4", 5, "high", 1, "reasoning", 2),
        (fallback_candidate_ref, "gpt-5.4-mini", 10, "medium", 2, "instant", 1),
    ):
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
            candidate_ref,
            "provider.openai",
            "openai",
            "openai",
            model_slug,
            "active",
            priority,
            1,
            _jsonb(["bounded", "failover-runtime"]),
            _jsonb({}),
            route_tier,
            route_tier_rank,
            latency_class,
            latency_rank,
            _jsonb({"default": "medium", "kind": "openai_reasoning_effort"}),
            _jsonb({"primary": ["build"], "secondary": ["review"], "avoid": []}),
            _jsonb({"positioning": "bounded failover runtime seed", "source_refs": ["integration_test"]}),
            _jsonb({"temperature": 0}),
            as_of - timedelta(hours=1),
            None,
            f"decision:{candidate_ref}",
            as_of,
        )

    for binding_id, candidate_ref, binding_role, position_index in (
        (
            f"binding.bounded-failover-runtime.primary.{suffix}",
            primary_candidate_ref,
            "primary",
            0,
        ),
        (
            f"binding.bounded-failover-runtime.fallback.{suffix}",
            fallback_candidate_ref,
            "fallback",
            1,
        ),
    ):
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
            binding_id,
            runtime_profile.model_profile_id,
            candidate_ref,
            binding_role,
            position_index,
            as_of - timedelta(hours=1),
            None,
            as_of,
        )

    return runtime_profile, primary_candidate_ref, fallback_candidate_ref


async def _seed_control_tower(
    conn,
    *,
    suffix: str,
    runtime_profile: RuntimeProfile,
    primary_candidate_ref: str,
    fallback_candidate_ref: str,
    as_of: datetime,
) -> tuple[str, str]:
    budget_window_id = f"budget.bounded-failover-runtime.{suffix}"
    fallback_route_state_id = f"eligibility.bounded-failover-runtime.fallback.{suffix}"

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
        8,
        100000,
        3000,
        "250.000000",
        "10.000000",
        f"decision:budget:{suffix}",
        as_of,
    )

    route_state_seed = (
        (
            primary_candidate_ref,
            f"health.bounded-failover-runtime.primary.{suffix}",
            f"eligibility.bounded-failover-runtime.primary.{suffix}",
            "ineligible",
            "provider_fallback.health_degraded",
        ),
        (
            fallback_candidate_ref,
            f"health.bounded-failover-runtime.fallback.{suffix}",
            fallback_route_state_id,
            "eligible",
            "provider_fallback.healthy_budget_available",
        ),
    )
    for (
        candidate_ref,
        health_window_id,
        route_eligibility_state_id,
        eligibility_status,
        reason_code,
    ) in route_state_seed:
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
            health_window_id,
            candidate_ref,
            "provider.openai",
            "healthy" if eligibility_status == "eligible" else "degraded",
            0.98 if eligibility_status == "eligible" else 0.41,
            24,
            0.0 if eligibility_status == "eligible" else 0.42,
            120 if eligibility_status == "eligible" else 480,
            as_of - timedelta(minutes=30),
            as_of - timedelta(minutes=1),
            f"observation:{candidate_ref}",
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
            route_eligibility_state_id,
            runtime_profile.model_profile_id,
            runtime_profile.provider_policy_id,
            candidate_ref,
            eligibility_status,
            reason_code,
            _jsonb((health_window_id, budget_window_id)),
            as_of - timedelta(minutes=10),
            None,
            f"decision:{route_eligibility_state_id}",
            as_of - timedelta(minutes=10),
        )

    return fallback_route_state_id, budget_window_id


async def _insert_failover_slice(
    conn,
    *,
    runtime_profile: RuntimeProfile,
    primary_candidate_ref: str,
    fallback_candidate_ref: str,
    suffix: str,
    decision_suffix: str,
    effective_from: datetime,
    effective_to: datetime | None,
) -> tuple[str, str, str]:
    decision_ref = f"decision:failover:{decision_suffix}:{suffix}"
    primary_binding_id = f"provider_failover_binding.primary.{decision_suffix}.{suffix}"
    fallback_binding_id = f"provider_failover_binding.fallback.{decision_suffix}.{suffix}"

    for binding_id, candidate_ref, failover_role, position_index in (
        (primary_binding_id, primary_candidate_ref, "primary", 0),
        (fallback_binding_id, fallback_candidate_ref, "fallback", 1),
    ):
        await conn.execute(
            """
            INSERT INTO provider_failover_bindings (
                provider_failover_binding_id,
                model_profile_id,
                provider_policy_id,
                candidate_ref,
                binding_scope,
                failover_role,
                trigger_rule,
                position_index,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            binding_id,
            runtime_profile.model_profile_id,
            runtime_profile.provider_policy_id,
            candidate_ref,
            "native_runtime",
            failover_role,
            "health_degraded",
            position_index,
            effective_from,
            effective_to,
            decision_ref,
            effective_from,
        )

    return primary_binding_id, fallback_binding_id, decision_ref


def test_bounded_failover_runtime_selection_adopts_failover_authority_on_one_route_runtime_seam() -> None:
    asyncio.run(_exercise_bounded_failover_runtime_selection())


async def _exercise_bounded_failover_runtime_selection() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for bounded failover runtime selection integration test: "
            f"{exc.reason_code}"
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
        await _create_failover_authority_table(conn)

        runtime_profile, primary_candidate_ref, fallback_candidate_ref = await _seed_route_catalog(
            conn,
            suffix=suffix,
            as_of=as_of,
        )
        fallback_route_state_id, budget_window_id = await _seed_control_tower(
            conn,
            suffix=suffix,
            runtime_profile=runtime_profile,
            primary_candidate_ref=primary_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            as_of=as_of,
        )
        (
            _active_primary_binding_id,
            active_fallback_binding_id,
            active_decision_ref,
        ) = await _insert_failover_slice(
            conn,
            runtime_profile=runtime_profile,
            primary_candidate_ref=primary_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            suffix=suffix,
            decision_suffix="active",
            effective_from=as_of - timedelta(hours=1),
            effective_to=None,
        )

        resolution = await resolve_provider_route_runtime(
            conn,
            runtime_profile=runtime_profile,
            as_of=as_of,
            failover_binding_scope="native_runtime",
        )
        payload = resolution.to_json()

        assert resolution.selected_candidate_ref == fallback_candidate_ref
        assert resolution.route_eligibility_state.route_eligibility_state_id == fallback_route_state_id
        assert resolution.route_decision.decision_reason_code == "routing.preferred_candidate"
        assert resolution.route_decision.allowed_candidate_refs == (fallback_candidate_ref,)
        assert payload["authorities"] == {
            "route_catalog": "registry.route_catalog_repository",
            "route": "registry.provider_routing",
            "failover": "registry.endpoint_failover",
        }
        assert payload["route"]["selected_candidate_ref"] == fallback_candidate_ref
        assert payload["route_eligibility_state"] == {
            "route_eligibility_state_id": fallback_route_state_id,
            "eligibility_status": "eligible",
            "reason_code": "provider_fallback.healthy_budget_available",
            "source_window_refs": [
                f"health.bounded-failover-runtime.fallback.{suffix}",
                budget_window_id,
            ],
            "evaluated_at": (as_of - timedelta(minutes=10)).isoformat(),
            "decision_ref": f"decision:{fallback_route_state_id}",
        }
        assert payload["failover"] == {
            "binding_scope": "native_runtime",
            "selected_provider_failover_binding_id": active_fallback_binding_id,
            "selected_candidate_ref": fallback_candidate_ref,
            "failover_role": "fallback",
            "trigger_rule": "health_degraded",
            "position_index": 1,
            "slice_candidate_refs": [
                primary_candidate_ref,
                fallback_candidate_ref,
            ],
            "decision_ref": active_decision_ref,
        }

        await conn.execute(
            """
            UPDATE provider_failover_bindings
            SET effective_to = $2
            WHERE decision_ref = $1
            """,
            active_decision_ref,
            as_of - timedelta(minutes=5),
        )
        (
            _stale_primary_binding_id,
            _stale_fallback_binding_id,
            stale_decision_ref,
        ) = await _insert_failover_slice(
            conn,
            runtime_profile=runtime_profile,
            primary_candidate_ref=primary_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            suffix=suffix,
            decision_suffix="stale",
            effective_from=as_of - timedelta(minutes=5),
            effective_to=None,
        )

        with pytest.raises(ProviderRouteRuntimeError) as stale_exc_info:
            await resolve_provider_route_runtime(
                conn,
                runtime_profile=runtime_profile,
                as_of=as_of,
                failover_binding_scope="native_runtime",
            )

        assert stale_exc_info.value.reason_code == "provider_route_runtime.failover_slice_stale"
        assert stale_exc_info.value.details == {
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "model_profile_id": runtime_profile.model_profile_id,
            "provider_policy_id": runtime_profile.provider_policy_id,
            "candidate_ref": fallback_candidate_ref,
            "binding_scope": "native_runtime",
            "as_of": as_of.isoformat(),
            "failover_slice_key": (
                f"effective_from={(as_of - timedelta(minutes=5)).isoformat()},"
                "effective_to=,"
                f"decision_ref={stale_decision_ref}"
            ),
            "route_eligibility_state_id": fallback_route_state_id,
            "route_evaluated_at": (as_of - timedelta(minutes=10)).isoformat(),
        }

        await _insert_failover_slice(
            conn,
            runtime_profile=runtime_profile,
            primary_candidate_ref=primary_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            suffix=suffix,
            decision_suffix="overlap",
            effective_from=as_of - timedelta(minutes=3),
            effective_to=None,
        )

        with pytest.raises(ProviderRouteRuntimeError) as ambiguous_exc_info:
            await resolve_provider_route_runtime(
                conn,
                runtime_profile=runtime_profile,
                as_of=as_of,
                failover_binding_scope="native_runtime",
            )

        assert ambiguous_exc_info.value.reason_code == (
            "provider_route_runtime.failover_slice_ambiguous"
        )
        assert ambiguous_exc_info.value.details == {
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "model_profile_id": runtime_profile.model_profile_id,
            "provider_policy_id": runtime_profile.provider_policy_id,
            "binding_scope": "native_runtime",
            "as_of": as_of.isoformat(),
            "slice_keys": (
                (
                    f"effective_from={(as_of - timedelta(minutes=5)).isoformat()},"
                    f"effective_to=,decision_ref={stale_decision_ref}"
                ),
                (
                    f"effective_from={(as_of - timedelta(minutes=3)).isoformat()},"
                    f"effective_to=,decision_ref=decision:failover:overlap:{suffix}"
                ),
            ),
        }
    finally:
        await transaction.rollback()
        await conn.close()
