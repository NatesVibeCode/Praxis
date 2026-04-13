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
    return datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)


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
    candidate_ref: str,
    as_of: datetime,
) -> None:
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
        f"provider_failover_binding.default-path.{suffix}",
        model_profile_id,
        provider_policy_id,
        candidate_ref,
        "native_runtime",
        "primary",
        "health_degraded",
        0,
        as_of - timedelta(hours=1),
        None,
        f"decision:failover:{suffix}",
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
        f"provider_endpoint_binding.default-path.{suffix}",
        provider_policy_id,
        candidate_ref,
        "native_runtime",
        f"endpoint.default-path.{suffix}",
        "chat_completions",
        "https",
        "https://api.example.test/v1/chat/completions",
        f"secret.default-path.{suffix}",
        "active",
        _jsonb({"timeout_ms": 30000}),
        _jsonb({"threshold": 3, "window_s": 60}),
        as_of - timedelta(hours=1),
        None,
        f"decision:endpoint:{suffix}",
        as_of - timedelta(hours=1),
    )


async def _seed_route_catalog_prereqs(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[str, str, str]:
    model_profile_id = f"model_profile.default-path.{suffix}"
    provider_policy_id = f"provider_policy.default-path.{suffix}"
    candidate_ref = f"candidate.openai.default-path.{suffix}.gpt54mini"

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
        f"default-path.{suffix}",
        "openai",
        "gpt-5.4",
        1,
        "active",
        _jsonb({"tier": "baseline"}),
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
        f"default-path.{suffix}",
        "openai",
        "runtime",
        1,
        "active",
        _jsonb(["gpt-5.4", "gpt-5.4-mini"]),
        _jsonb({"retry": 0}),
        _jsonb({"budget": "standard"}),
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
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14)
        """,
        candidate_ref,
        "provider.openai",
        "openai",
        "openai",
        "gpt-5.4-mini",
        "active",
        10,
        1,
        _jsonb(["default-path", "pilot"]),
        _jsonb({"temperature": 0}),
        as_of - timedelta(hours=1),
        None,
        f"decision:candidate:{suffix}",
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
        f"binding.default-path.{suffix}",
        model_profile_id,
        candidate_ref,
        "primary",
        0,
        as_of - timedelta(hours=1),
        None,
        as_of,
    )

    return model_profile_id, provider_policy_id, candidate_ref


async def _seed_route_control_tower_rows(
    conn,
    *,
    suffix: str,
    provider_policy_id: str,
    candidate_ref: str,
    as_of: datetime,
) -> tuple[str, str]:
    health_window_id = f"health.default-path.{suffix}"
    budget_window_id = f"budget.default-path.{suffix}"

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
        32,
        0.0,
        120,
        as_of - timedelta(minutes=30),
        as_of,
        f"observation:default-path:{suffix}",
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
        10,
        200000,
        5000,
        "500.000000",
        "25.000000",
        f"decision:budget:{suffix}",
        as_of,
    )

    return health_window_id, budget_window_id


async def _insert_route_eligibility_state(
    conn,
    *,
    route_eligibility_state_id: str,
    model_profile_id: str,
    provider_policy_id: str,
    candidate_ref: str,
    eligibility_status: str,
    reason_code: str,
    source_window_refs: tuple[str, ...],
    evaluated_at: datetime,
) -> str:
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
        model_profile_id,
        provider_policy_id,
        candidate_ref,
        eligibility_status,
        reason_code,
        _jsonb(source_window_refs),
        evaluated_at,
        None,
        f"decision:{route_eligibility_state_id}",
        evaluated_at,
    )
    return route_eligibility_state_id


async def _seed_workflow_class(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[str, str]:
    workflow_class_id = f"workflow_class.default-path.{suffix}"
    class_name = f"default-path-smoke-{suffix}"

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
        class_name,
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

    return workflow_class_id, class_name


async def _seed_schedule_window(
    conn,
    *,
    suffix: str,
    workflow_class_id: str,
    target_ref: str,
    as_of: datetime,
) -> tuple[str, str]:
    schedule_definition_id = f"schedule_definition.default-path.{suffix}"
    recurring_run_window_id = f"recurring_run_window.default-path.{suffix}"

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
        schedule_definition_id,
        workflow_class_id,
        f"default-path-{suffix}",
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
        recurring_run_window_id,
        schedule_definition_id,
        as_of - timedelta(minutes=5),
        as_of + timedelta(minutes=55),
        "active",
        1,
        0,
        None,
        as_of,
    )

    return schedule_definition_id, recurring_run_window_id


def test_default_path_pilot_wires_one_bounded_native_default_path() -> None:
    asyncio.run(_exercise_default_path_pilot_wiring())


async def _exercise_default_path_pilot_wiring() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for default-path pilot wiring integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        suffix = _unique_suffix()
        as_of = _fixed_clock()
        target_ref = f"workspace.default-path.{suffix}"

        route_catalog_repository = PostgresRouteCatalogRepository(conn)
        route_control_tower_repository = PostgresProviderRouteAuthorityRepository(conn)

        await route_catalog_repository.bootstrap_route_catalog_schema()
        await route_control_tower_repository.bootstrap_provider_route_authority_schema()
        await bootstrap_workflow_lane_catalog_schema(conn)
        await admit_native_workflow_lane_catalog(conn, as_of=as_of)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _create_authority_tables(conn)

        model_profile_id, provider_policy_id, candidate_ref = await _seed_route_catalog_prereqs(
            conn,
            suffix=suffix,
            as_of=as_of,
        )
        health_window_id, budget_window_id = await _seed_route_control_tower_rows(
            conn,
            suffix=suffix,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            as_of=as_of,
        )
        await _seed_failover_and_endpoint_authority(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            as_of=as_of,
        )
        initial_route_eligibility_state_id = await _insert_route_eligibility_state(
            conn,
            route_eligibility_state_id=f"eligibility.default-path.initial.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            eligibility_status="eligible",
            reason_code="provider_fallback.healthy_budget_available",
            source_window_refs=(health_window_id, budget_window_id),
            evaluated_at=as_of - timedelta(minutes=20),
        )
        await _insert_route_eligibility_state(
            conn,
            route_eligibility_state_id=f"eligibility.default-path.future-ineligible.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            eligibility_status="ineligible",
            reason_code="provider_fallback.future_budget_hold",
            source_window_refs=(health_window_id, budget_window_id),
            evaluated_at=as_of + timedelta(minutes=5),
        )

        workflow_class_id, _ = await _seed_workflow_class(
            conn,
            suffix=suffix,
            as_of=as_of,
        )
        schedule_definition_id, recurring_run_window_id = await _seed_schedule_window(
            conn,
            suffix=suffix,
            workflow_class_id=workflow_class_id,
            target_ref=target_ref,
            as_of=as_of,
        )

        request = DefaultPathPilotRequest(
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            target_ref=target_ref,
            schedule_kind="smoke",
        )

        resolution = await resolve_default_path_pilot(
            conn,
            request=request,
            as_of=as_of,
        )
        payload = resolution.to_json()

        assert resolution.as_of == as_of
        assert resolution.workflow_class_id == workflow_class_id
        assert resolution.workflow_lane_id == "workflow_lane.smoke"
        assert resolution.recurring_run_window_id == recurring_run_window_id
        assert resolution.capacity_remaining == 1
        assert payload["authorities"] == {
            "route": "registry.provider_routing",
            "dispatch": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
        }
        assert payload["request"] == {
            "model_profile_id": model_profile_id,
            "provider_policy_id": provider_policy_id,
            "candidate_ref": candidate_ref,
            "target_ref": target_ref,
            "schedule_kind": "smoke",
        }
        assert payload["route"]["route_eligibility_state_id"] == initial_route_eligibility_state_id
        assert payload["route"]["eligibility_status"] == "eligible"
        assert payload["route"]["health_window"] == {
            "provider_route_health_window_id": health_window_id,
            "health_status": "healthy",
            "health_score": 0.99,
            "observation_ref": f"observation:default-path:{suffix}",
        }
        assert payload["route"]["budget_window"] == {
            "provider_budget_window_id": budget_window_id,
            "budget_status": "available",
            "decision_ref": f"decision:budget:{suffix}",
        }
        assert payload["dispatch"]["workflow_class_id"] == workflow_class_id
        assert payload["dispatch"]["workflow_lane_policy_id"] == "workflow_lane_policy.smoke"
        assert payload["dispatch"]["queue_shape"] == {
            "max_parallel": 1,
            "route_kind": "smoke",
        }
        assert payload["schedule"] == {
            "schedule_definition_id": schedule_definition_id,
            "schedule_name": f"default-path-{suffix}",
            "schedule_kind": "smoke",
            "target_ref": target_ref,
            "recurring_run_window_id": recurring_run_window_id,
            "window_status": "active",
            "capacity_limit": 1,
            "capacity_used": 0,
            "capacity_remaining": 1,
            "decision_ref": f"decision:schedule:{suffix}",
        }

        latest_ineligible_route_state_id = await _insert_route_eligibility_state(
            conn,
            route_eligibility_state_id=f"eligibility.default-path.latest-ineligible.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            eligibility_status="ineligible",
            reason_code="provider_fallback.manual_hold",
            source_window_refs=(health_window_id, budget_window_id),
            evaluated_at=as_of - timedelta(minutes=1),
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(
                conn,
                request=request,
                as_of=as_of,
            )

        assert exc_info.value.reason_code == "default_path_pilot.route_ineligible"
        assert exc_info.value.details == {
            "route_eligibility_state_id": latest_ineligible_route_state_id,
            "eligibility_status": "ineligible",
            "reason_code": "provider_fallback.manual_hold",
            "evaluated_at": (as_of - timedelta(minutes=1)).isoformat(),
            "as_of": as_of.isoformat(),
        }

        latest_eligible_route_state_id = await _insert_route_eligibility_state(
            conn,
            route_eligibility_state_id=f"eligibility.default-path.latest-eligible.{suffix}",
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            eligibility_status="eligible",
            reason_code="provider_fallback.released",
            source_window_refs=(health_window_id, budget_window_id),
            evaluated_at=as_of,
        )

        resolution = await resolve_default_path_pilot(
            conn,
            request=request,
            as_of=as_of,
        )

        assert resolution.route.route_eligibility_state_id == latest_eligible_route_state_id

        await conn.execute(
            """
            UPDATE recurring_run_windows
            SET capacity_used = 1
            WHERE recurring_run_window_id = $1
            """,
            recurring_run_window_id,
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(
                conn,
                request=request,
                as_of=as_of,
            )

        assert exc_info.value.reason_code == "default_path_pilot.window_capacity_exhausted"
        assert exc_info.value.details == {
            "schedule_definition_id": schedule_definition_id,
            "recurring_run_window_id": recurring_run_window_id,
            "capacity_limit": 1,
            "capacity_used": 1,
        }
    finally:
        await transaction.rollback()
        await conn.close()
