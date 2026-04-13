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

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 3, 19, 0, tzinfo=timezone.utc)


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


async def _bootstrap_workflow_migration(conn: asyncpg.Connection, filename: str) -> None:
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
                if getattr(exc, "sqlstate", None) in _DUPLICATE_SQLSTATES:
                    continue
                raise


async def _create_authority_tables(conn: asyncpg.Connection) -> None:
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


async def _seed_route_catalog(
    conn: asyncpg.Connection,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[str, str, str, str]:
    model_profile_id = f"model_profile.default-path-failover.{suffix}"
    provider_policy_id = f"provider_policy.default-path-failover.{suffix}"
    requested_candidate_ref = f"candidate.openai.default-path-failover.{suffix}.gpt54"
    fallback_candidate_ref = f"candidate.openai.default-path-failover.{suffix}.gpt54mini"

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
        f"default-path-failover.{suffix}",
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
        f"default-path-failover.{suffix}",
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

    for candidate_ref, model_slug, priority in (
        (requested_candidate_ref, "gpt-5.4", 5),
        (fallback_candidate_ref, "gpt-5.4-mini", 10),
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
            model_slug,
            "active",
            priority,
            1,
            _jsonb(["default-path", "adoption"]),
            _jsonb({"temperature": 0}),
            as_of - timedelta(hours=1),
            None,
            f"decision:{candidate_ref}",
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
        f"binding.default-path-failover.requested.{suffix}",
        model_profile_id,
        requested_candidate_ref,
        "primary",
        0,
        as_of - timedelta(hours=1),
        None,
        as_of,
    )

    return (
        model_profile_id,
        provider_policy_id,
        requested_candidate_ref,
        fallback_candidate_ref,
    )


async def _seed_control_tower(
    conn: asyncpg.Connection,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    candidate_ref: str,
    as_of: datetime,
) -> tuple[str, str, str]:
    health_window_id = f"health.default-path-failover.{suffix}"
    budget_window_id = f"budget.default-path-failover.{suffix}"
    route_eligibility_state_id = f"eligibility.default-path-failover.{suffix}"

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
        0.995,
        64,
        0.0,
        110,
        as_of - timedelta(minutes=30),
        as_of,
        f"observation:default-path-failover:{suffix}",
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
        250000,
        8000,
        "500.000000",
        "12.500000",
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
        route_eligibility_state_id,
        model_profile_id,
        provider_policy_id,
        candidate_ref,
        "eligible",
        "provider_fallback.healthy_budget_available",
        _jsonb((health_window_id, budget_window_id)),
        as_of - timedelta(minutes=5),
        None,
        f"decision:{route_eligibility_state_id}",
        as_of - timedelta(minutes=5),
    )

    return route_eligibility_state_id, health_window_id, budget_window_id


async def _seed_dispatch_and_schedule(
    conn: asyncpg.Connection,
    *,
    suffix: str,
    target_ref: str,
    as_of: datetime,
) -> tuple[str, str]:
    workflow_class_id = f"workflow_class.default-path-failover.{suffix}"
    recurring_run_window_id = f"recurring_run_window.default-path-failover.{suffix}"

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
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12)
        """,
        workflow_class_id,
        f"default-path-failover-{suffix}",
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
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12)
        """,
        f"schedule_definition.default-path-failover.{suffix}",
        workflow_class_id,
        f"default-path-failover-{suffix}",
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
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        recurring_run_window_id,
        f"schedule_definition.default-path-failover.{suffix}",
        as_of - timedelta(minutes=5),
        as_of + timedelta(minutes=55),
        "active",
        1,
        0,
        None,
        as_of,
    )

    return workflow_class_id, recurring_run_window_id


async def _seed_failover_and_endpoint_authority(
    conn: asyncpg.Connection,
    *,
    suffix: str,
    model_profile_id: str,
    provider_policy_id: str,
    requested_candidate_ref: str,
    fallback_candidate_ref: str,
    as_of: datetime,
) -> tuple[str, str]:
    failover_decision_ref = f"decision:failover:{suffix}"
    endpoint_binding_id = f"provider_endpoint_binding.default-path-failover.{suffix}"

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
        f"provider_failover_binding.default-path-failover.primary.{suffix}",
        model_profile_id,
        provider_policy_id,
        requested_candidate_ref,
        "native_runtime",
        "primary",
        "health_degraded",
        0,
        as_of - timedelta(hours=1),
        None,
        failover_decision_ref,
        as_of - timedelta(hours=1),
    )
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
        f"provider_failover_binding.default-path-failover.fallback.{suffix}",
        model_profile_id,
        provider_policy_id,
        fallback_candidate_ref,
        "native_runtime",
        "fallback",
        "health_degraded",
        1,
        as_of - timedelta(hours=1),
        None,
        failover_decision_ref,
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
        endpoint_binding_id,
        provider_policy_id,
        requested_candidate_ref,
        "native_runtime",
        f"endpoint.default-path-failover.{suffix}",
        "chat_completions",
        "https",
        "https://api.example.test/v1/chat/completions",
        f"secret.default-path-failover.{suffix}",
        "active",
        _jsonb({"timeout_ms": 30000}),
        _jsonb({"threshold": 3, "window_s": 60}),
        as_of - timedelta(hours=1),
        None,
        f"decision:endpoint:{suffix}",
        as_of - timedelta(hours=1),
    )

    return failover_decision_ref, endpoint_binding_id


def test_default_path_pilot_adopts_failover_and_endpoint_authority_on_one_bounded_default_path() -> None:
    asyncio.run(_exercise_default_path_failover_endpoint_adoption())


async def _exercise_default_path_failover_endpoint_adoption() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for default-path failover/endpoint adoption integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        suffix = _unique_suffix()
        as_of = _fixed_clock()
        target_ref = f"workspace.default-path-failover.{suffix}"

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
        ) = await _seed_route_catalog(conn, suffix=suffix, as_of=as_of)
        route_eligibility_state_id, health_window_id, budget_window_id = await _seed_control_tower(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=requested_candidate_ref,
            as_of=as_of,
        )
        workflow_class_id, recurring_run_window_id = await _seed_dispatch_and_schedule(
            conn,
            suffix=suffix,
            target_ref=target_ref,
            as_of=as_of,
        )
        failover_decision_ref, endpoint_binding_id = await _seed_failover_and_endpoint_authority(
            conn,
            suffix=suffix,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            requested_candidate_ref=requested_candidate_ref,
            fallback_candidate_ref=fallback_candidate_ref,
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
        payload = resolution.to_json()

        assert resolution.route.route_eligibility_state_id == route_eligibility_state_id
        assert resolution.workflow_class_id == workflow_class_id
        assert resolution.recurring_run_window_id == recurring_run_window_id
        assert resolution.failover.selected_candidate_ref == requested_candidate_ref
        assert tuple(
            binding.candidate_ref for binding in resolution.failover.provider_failover_bindings
        ) == (requested_candidate_ref, fallback_candidate_ref)
        assert resolution.endpoint.provider_endpoint_binding_id == endpoint_binding_id
        assert resolution.endpoint.endpoint_kind == "chat_completions"
        assert resolution.endpoint.endpoint_uri == "https://api.example.test/v1/chat/completions"
        assert payload["authorities"] == {
            "route": "registry.provider_routing",
            "dispatch": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
        }
        assert payload["route"]["route_eligibility_state_id"] == route_eligibility_state_id
        assert payload["route"]["health_window"]["provider_route_health_window_id"] == health_window_id
        assert payload["route"]["budget_window"]["provider_budget_window_id"] == budget_window_id
        assert payload["failover_endpoint_authority"] == "registry.endpoint_failover"
        assert payload["failover"] == {
            "binding_scope": "native_runtime",
            "selected_provider_failover_binding_id": (
                f"provider_failover_binding.default-path-failover.primary.{suffix}"
            ),
            "selected_candidate_ref": requested_candidate_ref,
            "failover_role": "primary",
            "trigger_rule": "health_degraded",
            "position_index": 0,
            "slice_candidate_refs": [requested_candidate_ref, fallback_candidate_ref],
            "decision_ref": failover_decision_ref,
        }
        assert payload["endpoint"] == {
            "binding_scope": "native_runtime",
            "provider_endpoint_binding_id": endpoint_binding_id,
            "endpoint_ref": f"endpoint.default-path-failover.{suffix}",
            "endpoint_kind": "chat_completions",
            "transport_kind": "https",
            "endpoint_uri": "https://api.example.test/v1/chat/completions",
            "auth_ref": f"secret.default-path-failover.{suffix}",
            "binding_status": "active",
            "request_policy": {"timeout_ms": 30000},
            "circuit_breaker_policy": {"threshold": 3, "window_s": 60},
            "decision_ref": f"decision:endpoint:{suffix}",
        }
        first_party_runtime = resolution.to_first_party_runtime_contract()
        assert first_party_runtime["kind"] == "default_path_first_party_runtime_contract"
        assert first_party_runtime["authorities"] == {
            "route": "registry.provider_routing",
            "dispatch": "authority.workflow_class_resolution",
            "schedule": "runtime.scheduler_window_repository",
            "provider_adapter": "adapters.provider_registry",
        }
        assert first_party_runtime["route_runtime"] == {
            "route_decision_id": resolution.route_runtime.route_decision_id,
            "selected_candidate_ref": requested_candidate_ref,
            "provider_ref": "provider.openai",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "balance_slot": 0,
            "decision_reason_code": "routing.preferred_candidate",
            "allowed_candidate_refs": [requested_candidate_ref],
        }
        assert first_party_runtime["provider_adapter_contract"]["adapter_type"] == "llm_task"
        assert first_party_runtime["provider_adapter_contract"]["transport_kind"] == "http"
        assert (
            first_party_runtime["provider_adapter_contract"]["prompt_envelope"]["protocol_family"]
            == "openai_chat_completions"
        )
        assert first_party_runtime["provider_adapter_contract"]["failover_failure_codes"] == [
            "adapter.timeout",
            "adapter.http_error",
            "adapter.network_error",
        ]
        assert first_party_runtime["llm_task_input_payload"] == {
            "adapter_type": "llm_task",
            "route_contract_required": True,
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "endpoint_uri": "https://api.example.test/v1/chat/completions",
            "auth_ref": f"secret.default-path-failover.{suffix}",
            "timeout_seconds": 30,
            "provider_adapter_contract": first_party_runtime["provider_adapter_contract"],
            "runtime_route": {
                "route_decision_id": resolution.route_runtime.route_decision_id,
                "selected_candidate_ref": requested_candidate_ref,
                "provider_ref": "provider.openai",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "balance_slot": 0,
                "decision_reason_code": "routing.preferred_candidate",
                "allowed_candidate_refs": [requested_candidate_ref],
                "failover_role": "primary",
                "failover_trigger_rule": "health_degraded",
                "failover_position_index": 0,
                "failover_slice_candidate_refs": [
                    requested_candidate_ref,
                    fallback_candidate_ref,
                ],
                "endpoint_kind": "chat_completions",
                "endpoint_transport_kind": "https",
                "route_eligibility_state_id": route_eligibility_state_id,
                "selected_provider_failover_binding_id": (
                    f"provider_failover_binding.default-path-failover.primary.{suffix}"
                ),
                "provider_endpoint_binding_id": endpoint_binding_id,
                "route_authority": "registry.provider_routing",
                "failover_endpoint_authority": "registry.endpoint_failover",
                "as_of": as_of.isoformat(),
            },
        }

        mismatch_request = DefaultPathPilotRequest(
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            candidate_ref=fallback_candidate_ref,
            target_ref=target_ref,
            schedule_kind="smoke",
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(conn, request=mismatch_request, as_of=as_of)

        assert exc_info.value.reason_code == "default_path_pilot.request_candidate_mismatch"
        assert exc_info.value.details == {
            "model_profile_id": model_profile_id,
            "provider_policy_id": provider_policy_id,
            "requested_candidate_ref": fallback_candidate_ref,
            "authoritative_candidate_ref": requested_candidate_ref,
            "binding_scope": "native_runtime",
            "as_of": as_of.isoformat(),
            "slice_candidate_refs": f"{requested_candidate_ref},{fallback_candidate_ref}",
        }

        await conn.execute(
            """
            UPDATE provider_endpoint_bindings
            SET effective_to = $2
            WHERE provider_endpoint_binding_id = $1
            """,
            endpoint_binding_id,
            as_of - timedelta(seconds=1),
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
            f"provider_endpoint_binding.default-path-failover.refresh.{suffix}",
            provider_policy_id,
            requested_candidate_ref,
            "native_runtime",
            f"endpoint.default-path-failover.refresh.{suffix}",
            "chat_completions",
            "https",
            "https://api.example.test/v1/chat/completions",
            f"secret.default-path-failover.refresh.{suffix}",
            "active",
            _jsonb({"timeout_ms": 30000}),
            _jsonb({"threshold": 3, "window_s": 60}),
            as_of - timedelta(minutes=20),
            None,
            f"decision:endpoint-refresh:{suffix}",
            as_of - timedelta(minutes=20),
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(conn, request=request, as_of=as_of)

        assert exc_info.value.reason_code == "default_path_pilot.failover_endpoint_slice_stale"
        assert exc_info.value.details == {
            "model_profile_id": model_profile_id,
            "provider_policy_id": provider_policy_id,
            "candidate_ref": requested_candidate_ref,
            "requested_candidate_ref": requested_candidate_ref,
            "binding_scope": "native_runtime",
            "endpoint_kind": "chat_completions",
            "as_of": as_of.isoformat(),
            "failover_slice_key": (
                f"effective_from={(as_of - timedelta(hours=1)).isoformat()},"
                "effective_to="
            ),
            "endpoint_slice_key": (
                f"effective_from={(as_of - timedelta(minutes=20)).isoformat()},"
                "effective_to="
            ),
        }

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
            f"provider_failover_binding.default-path-failover.overlap.{suffix}",
            model_profile_id,
            provider_policy_id,
            fallback_candidate_ref,
            "native_runtime",
            "fallback",
            "health_degraded",
            2,
            as_of - timedelta(minutes=10),
            None,
            f"decision:failover-overlap:{suffix}",
            as_of - timedelta(minutes=10),
        )

        with pytest.raises(DefaultPathPilotError) as exc_info:
            await resolve_default_path_pilot(conn, request=request, as_of=as_of)

        assert exc_info.value.reason_code == "default_path_pilot.failover_slice_ambiguous"
        assert exc_info.value.details == {
            "model_profile_id": model_profile_id,
            "provider_policy_id": provider_policy_id,
            "candidate_ref": requested_candidate_ref,
            "requested_candidate_ref": requested_candidate_ref,
            "binding_scope": "native_runtime",
            "endpoint_kind": "chat_completions",
            "as_of": as_of.isoformat(),
            "slice_keys": (
                f"effective_from={(as_of - timedelta(hours=1)).isoformat()},"
                f"effective_to=,decision_ref={failover_decision_ref}",
                f"effective_from={(as_of - timedelta(minutes=10)).isoformat()},"
                f"effective_to=,decision_ref=decision:failover-overlap:{suffix}",
            ),
        }
    finally:
        await transaction.rollback()
        await conn.close()
