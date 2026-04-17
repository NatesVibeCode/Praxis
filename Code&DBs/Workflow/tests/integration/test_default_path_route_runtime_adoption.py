from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from policy.workflow_lanes import (
    admit_native_workflow_lane_catalog,
    bootstrap_workflow_lane_catalog_schema,
)
from registry.provider_routing import PostgresProviderRouteAuthorityRepository
from registry.route_catalog_repository import PostgresRouteCatalogRepository
from runtime.default_path_pilot import (
    DefaultPathPilotError,
    DefaultPathPilotRequest,
    resolve_default_path_pilot,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import PostgresConfigurationError, connect_workflow_database

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in {"42P07", "42710"}


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 20, 30, tzinfo=timezone.utc)


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise


async def _create_authority_tables(conn) -> None:
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
    await conn.execute(
        """
        CREATE TEMP TABLE provider_endpoint_bindings (
            provider_endpoint_binding_id text PRIMARY KEY,
            provider_policy_id text NOT NULL,
            candidate_ref text NOT NULL,
            binding_scope text NOT NULL,
            endpoint_ref text NOT NULL,
            endpoint_kind text NOT NULL,
            transport_kind text NOT NULL,
            endpoint_uri text NOT NULL,
            auth_ref text NOT NULL,
            binding_status text NOT NULL,
            request_policy jsonb NOT NULL,
            circuit_breaker_policy jsonb NOT NULL,
            effective_from timestamptz NOT NULL,
            effective_to timestamptz,
            decision_ref text NOT NULL,
            created_at timestamptz NOT NULL
        )
        """
    )


async def _seed_failover_and_endpoint_authority(
    conn,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    requested_candidate_ref: str,
    fallback_candidate_ref: str,
    as_of: datetime,
) -> None:
    decision_ref = f"decision:failover:{suffix}"
    for binding_id, candidate_ref, failover_role, position_index in (
        (f"provider_failover_binding.requested.{suffix}", requested_candidate_ref, "primary", 0),
        (f"provider_failover_binding.fallback.{suffix}", fallback_candidate_ref, "fallback", 1),
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
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            "native_runtime",
            failover_role,
            "health_degraded",
            position_index,
            as_of - timedelta(hours=1),
            None,
            decision_ref,
            as_of - timedelta(hours=1),
        )

    await conn.execute(
        """
        INSERT INTO provider_endpoint_bindings (
            provider_endpoint_binding_id,
            provider_policy_id,
            candidate_ref,
            binding_scope,
            endpoint_ref,
            endpoint_kind,
            transport_kind,
            endpoint_uri,
            auth_ref,
            binding_status,
            request_policy,
            circuit_breaker_policy,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16
        )
        """,
        f"provider_endpoint_binding.requested.{suffix}",
        provider_policy_id,
        requested_candidate_ref,
        "native_runtime",
        f"endpoint.default-path-adoption.{suffix}",
        "chat_completions",
        "https",
        "https://api.example.test/v1/chat/completions",
        f"secret.default-path-adoption.{suffix}",
        "active",
        _jsonb({"timeout_ms": 30000}),
        _jsonb({"threshold": 3, "window_s": 60}),
        as_of - timedelta(hours=1),
        None,
        f"decision:endpoint:{suffix}",
        as_of - timedelta(hours=1),
    )


async def _seed_route_catalog(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[str, str, str, str, str]:
    model_profile_id = f"model_profile.default-path-adoption.{suffix}"
    provider_policy_id = f"provider_policy.default-path-adoption.{suffix}"
    requested_candidate_ref = f"candidate.openai.default-path-adoption.{suffix}.gpt54"
    fallback_candidate_ref = f"candidate.openai.default-path-adoption.{suffix}.gpt54mini"
    requested_binding_id = f"binding.default-path-adoption.requested.{suffix}"

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
        f"default-path-adoption.{suffix}",
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
        provider_policy_id,
        f"default-path-adoption.{suffix}",
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
        requested_candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4",
        "active",
        5,
        1,
        _jsonb(["default-path", "adoption"]),
        _jsonb({}),
        "high",
        1,
        "reasoning",
        2,
        _jsonb({"default": "medium", "kind": "openai_reasoning_effort"}),
        _jsonb({"primary": ["build"], "secondary": ["review"], "avoid": []}),
        _jsonb({"positioning": "default path requested seed", "source_refs": ["integration_test"]}),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        f"decision:candidate-requested:{suffix}",
        as_of,
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
        fallback_candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4-mini",
        "active",
        10,
        1,
        _jsonb(["default-path", "adoption"]),
        _jsonb({}),
        "medium",
        2,
        "instant",
        1,
        _jsonb({"default": "medium", "kind": "openai_reasoning_effort"}),
        _jsonb({"primary": ["build"], "secondary": ["review"], "avoid": []}),
        _jsonb({"positioning": "default path fallback seed", "source_refs": ["integration_test"]}),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        f"decision:candidate-fallback:{suffix}",
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
        requested_binding_id,
        model_profile_id,
        requested_candidate_ref,
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
        f"binding.default-path-adoption.fallback.{suffix}",
        model_profile_id,
        fallback_candidate_ref,
        "fallback",
        1,
        as_of - timedelta(hours=1),
        None,
        as_of,
    )

    return (
        model_profile_id,
        provider_policy_id,
        requested_candidate_ref,
        fallback_candidate_ref,
        requested_binding_id,
    )


async def _seed_control_tower(
    conn,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    requested_candidate_ref: str,
    fallback_candidate_ref: str,
    as_of: datetime,
) -> str:
    requested_health_window_id = f"health.default-path-adoption.requested.{suffix}"
    fallback_health_window_id = f"health.default-path-adoption.fallback.{suffix}"
    budget_window_id = f"budget.default-path-adoption.{suffix}"
    requested_state_id = f"eligibility.default-path-adoption.requested.{suffix}"

    for health_window_id, candidate_ref, observation_ref in (
        (
            requested_health_window_id,
            requested_candidate_ref,
            f"observation:default-path-adoption:requested:{suffix}",
        ),
        (
            fallback_health_window_id,
            fallback_candidate_ref,
            f"observation:default-path-adoption:fallback:{suffix}",
        ),
    ):
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
            "healthy",
            0.99,
            24,
            0.0,
            115,
            as_of - timedelta(minutes=30),
            as_of,
            observation_ref,
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
        provider_policy_id,
        "provider.openai",
        "runtime",
        "available",
        as_of - timedelta(hours=1),
        as_of + timedelta(hours=1),
        1000,
        5,
        200000,
        4000,
        "500.000000",
        "15.000000",
        f"decision:budget:{suffix}",
        as_of,
    )

    for route_state_id, candidate_ref, health_window_id in (
        (requested_state_id, requested_candidate_ref, requested_health_window_id),
        (
            f"eligibility.default-path-adoption.fallback.{suffix}",
            fallback_candidate_ref,
            fallback_health_window_id,
        ),
    ):
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
            route_state_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            "eligible",
            "provider_fallback.healthy_budget_available",
            _jsonb((health_window_id, budget_window_id)),
            as_of - timedelta(minutes=10),
            None,
            f"decision:{route_state_id}",
            as_of - timedelta(minutes=10),
        )

    return requested_state_id


async def _seed_workflow_class(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> str:
    workflow_class_id = f"workflow_class.default-path-adoption.{suffix}"

    await conn.execute(
        """
        INSERT INTO workflow_classes (
            workflow_class_id,
            class_name,
            class_kind,
            workflow_lane_id,
            status,
            queue_shape,
            throttle_policy,
            review_required,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
        )
        """,
        workflow_class_id,
        f"default-path-adoption-{suffix}",
        "smoke",
        "workflow_lane.smoke",
        "active",
        _jsonb({"max_parallel": 1, "route_kind": "smoke"}),
        _jsonb({"dispatch_limit": 1}),
        False,
        as_of - timedelta(minutes=15),
        None,
        f"decision:workflow-class:{suffix}",
        as_of,
    )

    return workflow_class_id


async def _seed_schedule_window(
    conn,
    *,
    suffix: str,
    workflow_class_id: str,
    target_ref: str,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO schedule_definitions (
            schedule_definition_id,
            workflow_class_id,
            schedule_name,
            schedule_kind,
            status,
            cadence_policy,
            throttle_policy,
            target_ref,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
        )
        """,
        f"schedule_definition.default-path-adoption.{suffix}",
        workflow_class_id,
        f"default-path-adoption-{suffix}",
        "smoke",
        "active",
        _jsonb({"bounded": True, "cadence": "manual"}),
        _jsonb({"capacity_limit": 1}),
        target_ref,
        as_of - timedelta(minutes=10),
        None,
        f"decision:schedule:{suffix}",
        as_of,
    )
    await conn.execute(
        """
        INSERT INTO recurring_run_windows (
            recurring_run_window_id,
            schedule_definition_id,
            window_started_at,
            window_ended_at,
            window_status,
            capacity_limit,
            capacity_used,
            last_workflow_at,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        )
        """,
        f"recurring_run_window.default-path-adoption.{suffix}",
        f"schedule_definition.default-path-adoption.{suffix}",
        as_of - timedelta(minutes=5),
        as_of + timedelta(minutes=55),
        "active",
        1,
        0,
        None,
        as_of,
    )


def test_default_path_pilot_adopts_provider_route_runtime_seam_on_bounded_native_path() -> None:
    asyncio.run(_exercise_default_path_route_runtime_adoption())


async def _exercise_default_path_route_runtime_adoption() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for default-path route-runtime adoption integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        suffix = _unique_suffix()
        as_of = _fixed_clock()
        target_ref = f"workspace.default-path-adoption.{suffix}"

        route_catalog_repository = PostgresRouteCatalogRepository(conn)
        control_tower_repository = PostgresProviderRouteAuthorityRepository(conn)

        await route_catalog_repository.bootstrap_route_catalog_schema()
        await control_tower_repository.bootstrap_provider_route_authority_schema()
        await bootstrap_workflow_lane_catalog_schema(conn)
        await admit_native_workflow_lane_catalog(conn, as_of=as_of)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _create_authority_tables(conn)

        (
            model_profile_id,
            provider_policy_id,
            requested_candidate_ref,
            fallback_candidate_ref,
            requested_binding_id,
        ) = await _seed_route_catalog(conn, suffix=suffix, as_of=as_of)
        await _seed_failover_and_endpoint_authority(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            requested_candidate_ref=requested_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            as_of=as_of,
        )
        requested_state_id = await _seed_control_tower(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            requested_candidate_ref=requested_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
            as_of=as_of,
        )
        workflow_class_id = await _seed_workflow_class(conn, suffix=suffix, as_of=as_of)
        await _seed_schedule_window(
            conn,
            suffix=suffix,
            workflow_class_id=workflow_class_id,
            target_ref=target_ref,
            as_of=as_of,
        )

        request = DefaultPathPilotRequest(
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=requested_candidate_ref,
            target_ref=target_ref,
            schedule_kind="smoke",
        )

        resolution = await resolve_default_path_pilot(conn, request=request, as_of=as_of)

        assert resolution.route.route_eligibility_state_id == requested_state_id
        assert resolution.workflow_class_id == workflow_class_id
        assert resolution.to_json()["authorities"] == {
            "route": "registry.provider_routing",
            "dispatch": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
        }

        await conn.execute(
            """
            UPDATE model_profile_candidate_bindings
            SET effective_to = $2
            WHERE model_profile_candidate_binding_id = $1
            """,
            requested_binding_id,
            as_of - timedelta(minutes=1),
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(conn, request=request, as_of=as_of)

        assert exc_info.value.reason_code == "default_path_pilot.route_runtime_failed"
        assert exc_info.value.details["provider_route_runtime_reason_code"] == (
            "provider_route_runtime.routing_failed"
        )
        assert exc_info.value.details["provider_route_runtime_details"]["reason_code"] == (
            "routing.preference_unknown"
        )
        assert exc_info.value.details["provider_route_runtime_details"]["metadata"] == {
            "runtime_profile_ref": (
                "runtime_profile.default_path_pilot."
                f"{model_profile_id}."
                f"{provider_policy_id}"
            ),
            "preferred_candidate_ref": requested_candidate_ref,
        }
    finally:
        await transaction.rollback()
        await conn.close()
