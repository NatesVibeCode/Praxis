from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from _pg_test_conn import ensure_test_database_ready
from registry.endpoint_failover import (
    PostgresProviderFailoverAndEndpointAuthorityRepository,
    ProviderEndpointAuthoritySelector,
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    ProviderFailoverAuthoritySelector,
)
from storage.postgres import connect_workflow_database


_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)


def _json_payload(value: object) -> str:
    return json.dumps(value, sort_keys=True)


@dataclass(frozen=True, slots=True)
class SeededAuthorityContext:
    as_of: datetime
    model_profile_id: str
    provider_policy_id: str
    binding_scope: str
    primary_candidate_ref: str
    fallback_candidate_ref: str
    endpoint_ref: str
    endpoint_kind: str


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


async def _seed_authority_rows(
    conn,
    *,
    suffix: str,
) -> SeededAuthorityContext:
    as_of = _fixed_clock()
    active_from = as_of - timedelta(hours=1)
    model_profile_id = f"model_profile.{suffix}"
    provider_policy_id = f"provider_policy.{suffix}"
    binding_scope = "native_runtime"
    primary_candidate_ref = f"candidate.{suffix}.openai"
    fallback_candidate_ref = f"candidate.{suffix}.anthropic"
    endpoint_ref = f"endpoint.{suffix}.chat"
    endpoint_kind = "chat_completions"

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
        f"failover.{suffix}.primary",
        model_profile_id,
        provider_policy_id,
        primary_candidate_ref,
        binding_scope,
        "primary",
        "health_degraded",
        0,
        active_from,
        None,
        f"decision.{suffix}.failover.active",
        active_from,
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
        f"failover.{suffix}.fallback",
        model_profile_id,
        provider_policy_id,
        fallback_candidate_ref,
        binding_scope,
        "fallback",
        "health_degraded",
        1,
        active_from,
        None,
        f"decision.{suffix}.failover.active",
        active_from,
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
        f"endpoint.{suffix}.active",
        provider_policy_id,
        primary_candidate_ref,
        binding_scope,
        endpoint_ref,
        endpoint_kind,
        "https",
        "https://api.example.test/v1/chat/completions",
        f"secret.{suffix}.openai",
        "active",
        _json_payload({"timeout_ms": 30000}),
        _json_payload({"threshold": 3, "window_s": 60}),
        active_from,
        None,
        f"decision.{suffix}.endpoint.active",
        active_from,
    )

    return SeededAuthorityContext(
        as_of=as_of,
        model_profile_id=model_profile_id,
        provider_policy_id=provider_policy_id,
        binding_scope=binding_scope,
        primary_candidate_ref=primary_candidate_ref,
        fallback_candidate_ref=fallback_candidate_ref,
        endpoint_ref=endpoint_ref,
        endpoint_kind=endpoint_kind,
    )


async def _with_seeded_repository(exercise) -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL},
    )

    try:
        await _create_authority_tables(conn)
        context = await _seed_authority_rows(conn, suffix=_unique_suffix())
        repository = PostgresProviderFailoverAndEndpointAuthorityRepository(conn)
        await exercise(conn, repository, context)
    finally:
        await conn.close()


def test_failover_and_endpoint_authority_repository_requires_explicit_selectors() -> None:
    asyncio.run(_exercise_explicit_selector_contract())


async def _exercise_explicit_selector_contract() -> None:
    async def exercise(conn, repository, context: SeededAuthorityContext) -> None:
        del conn
        failover_selector = ProviderFailoverAuthoritySelector(
            model_profile_id=context.model_profile_id,
            provider_policy_id=context.provider_policy_id,
            binding_scope=context.binding_scope,
            as_of=context.as_of,
        )
        endpoint_selector = ProviderEndpointAuthoritySelector(
            provider_policy_id=context.provider_policy_id,
            candidate_ref=context.primary_candidate_ref,
            binding_scope=context.binding_scope,
            endpoint_ref=context.endpoint_ref,
            as_of=context.as_of,
        )
        endpoint_kind_selector = ProviderEndpointAuthoritySelector(
            provider_policy_id=context.provider_policy_id,
            candidate_ref=context.primary_candidate_ref,
            binding_scope=context.binding_scope,
            endpoint_kind=context.endpoint_kind,
            as_of=context.as_of,
        )

        with pytest.raises(ProviderFailoverAndEndpointAuthorityRepositoryError) as exc_info:
            await repository.load_provider_failover_and_endpoint_authority()
        assert exc_info.value.reason_code == "endpoint_failover.invalid_selector"

        authority = await repository.load_provider_failover_and_endpoint_authority(
            failover_selectors=(failover_selector,),
            endpoint_selectors=(endpoint_selector,),
        )
        authority_again = await repository.load_provider_failover_and_endpoint_authority(
            failover_selectors=(failover_selector,),
            endpoint_selectors=(endpoint_selector,),
        )

        assert authority == authority_again
        assert authority.provider_policy_ids == (context.provider_policy_id,)
        assert authority.endpoint_refs == (context.endpoint_ref,)

        failover_bindings = authority.resolve_provider_failover_bindings(
            selector=failover_selector
        )
        endpoint_binding = authority.resolve_endpoint_binding(selector=endpoint_selector)

        assert tuple(binding.candidate_ref for binding in failover_bindings) == (
            context.primary_candidate_ref,
            context.fallback_candidate_ref,
        )
        assert tuple(binding.failover_role for binding in failover_bindings) == (
            "primary",
            "fallback",
        )
        assert endpoint_binding.endpoint_kind == context.endpoint_kind
        assert endpoint_binding.endpoint_uri == "https://api.example.test/v1/chat/completions"
        assert await repository.fetch_endpoint_binding(selector=endpoint_kind_selector) == endpoint_binding

    await _with_seeded_repository(exercise)


def test_failover_and_endpoint_authority_repository_fails_closed_on_ambiguous_failover_slice() -> None:
    asyncio.run(_exercise_ambiguous_failover_slice())


async def _exercise_ambiguous_failover_slice() -> None:
    async def exercise(conn, repository, context: SeededAuthorityContext) -> None:
        failover_selector = ProviderFailoverAuthoritySelector(
            model_profile_id=context.model_profile_id,
            provider_policy_id=context.provider_policy_id,
            binding_scope=context.binding_scope,
            as_of=context.as_of,
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
            f"failover.{_unique_suffix()}.ambiguous",
            context.model_profile_id,
            context.provider_policy_id,
            f"candidate.{_unique_suffix()}.groq",
            context.binding_scope,
            "fallback",
            "health_degraded",
            2,
            context.as_of - timedelta(minutes=15),
            None,
            f"decision.{_unique_suffix()}.failover.overlap",
            context.as_of - timedelta(minutes=15),
        )

        with pytest.raises(ProviderFailoverAndEndpointAuthorityRepositoryError) as exc_info:
            await repository.fetch_provider_failover_bindings(selector=failover_selector)

        assert (
            exc_info.value.reason_code
            == "endpoint_failover.ambiguous_failover_slice"
        )

    await _with_seeded_repository(exercise)


def test_failover_and_endpoint_authority_repository_fails_closed_on_ambiguous_endpoint_kind_selector() -> None:
    asyncio.run(_exercise_ambiguous_endpoint_kind_selector())


async def _exercise_ambiguous_endpoint_kind_selector() -> None:
    async def exercise(conn, repository, context: SeededAuthorityContext) -> None:
        endpoint_kind_selector = ProviderEndpointAuthoritySelector(
            provider_policy_id=context.provider_policy_id,
            candidate_ref=context.primary_candidate_ref,
            binding_scope=context.binding_scope,
            endpoint_kind=context.endpoint_kind,
            as_of=context.as_of,
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
            f"endpoint.{_unique_suffix()}.alt",
            context.provider_policy_id,
            context.primary_candidate_ref,
            context.binding_scope,
            f"{context.endpoint_ref}.alt",
            context.endpoint_kind,
            "https",
            "https://api.example.test/v1/responses",
            f"secret.{_unique_suffix()}.alt",
            "active",
            _json_payload({"timeout_ms": 15000}),
            _json_payload({"threshold": 5, "window_s": 120}),
            context.as_of - timedelta(minutes=10),
            None,
            f"decision.{_unique_suffix()}.endpoint.overlap",
            context.as_of - timedelta(minutes=10),
        )

        with pytest.raises(ProviderFailoverAndEndpointAuthorityRepositoryError) as exc_info:
            await repository.fetch_endpoint_binding(selector=endpoint_kind_selector)

        assert (
            exc_info.value.reason_code
            == "endpoint_failover.ambiguous_endpoint_slice"
        )

    await _with_seeded_repository(exercise)
