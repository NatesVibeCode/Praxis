from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from _pg_test_conn import ensure_test_database_ready
from adapters.protocol_endpoint_runtime import (
    MCPProtocolEndpointRequest,
    ProtocolEndpointRuntimeError,
    resolve_mcp_protocol_endpoint,
)
from adapters.protocol_events import ProtocolMessage, ProtocolMetadata
from storage.postgres import connect_workflow_database


_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 3, 21, 0, tzinfo=timezone.utc)


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class SeededEndpointAuthorityContext:
    as_of: datetime
    provider_policy_id: str
    candidate_ref: str
    binding_scope: str
    endpoint_kind: str


async def _create_authority_tables(conn: asyncpg.Connection) -> None:
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


async def _seed_active_binding(
    conn: asyncpg.Connection,
    *,
    context: SeededEndpointAuthorityContext,
    provider_endpoint_binding_id: str,
    endpoint_ref: str,
    endpoint_uri: str,
    decision_ref: str,
) -> None:
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
            $1, $2, $3, $4, $5, $6, $7, $8, $9, 'active', $10::jsonb, $11::jsonb, $12, $13, $14, $15
        )
        """,
        provider_endpoint_binding_id,
        context.provider_policy_id,
        context.candidate_ref,
        context.binding_scope,
        endpoint_ref,
        context.endpoint_kind,
        "streamable_http",
        endpoint_uri,
        f"secret.mcp-endpoint.{provider_endpoint_binding_id}",
        _jsonb({"timeout_ms": 15000}),
        _jsonb({"threshold": 2, "window_s": 30}),
        context.as_of - timedelta(hours=1),
        None,
        decision_ref,
        context.as_of - timedelta(hours=1),
    )


def _mcp_tools_call_message() -> ProtocolMessage:
    return ProtocolMessage(
        direction="egress",
        metadata=ProtocolMetadata(
            protocol_kind="mcp",
            transport_kind="streamable_http",
            correlation_ids={
                "session_id": "session-1",
                "tool_call_id": "call-1",
            },
        ),
        body={
            "method": "tools/call",
            "params": {
                "name": "inspect_run",
                "arguments": {"run_id": "run-1"},
            },
        },
    )


async def _with_seeded_connection(exercise) -> None:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL},
    )

    try:
        await _create_authority_tables(conn)
        suffix = _unique_suffix()
        context = SeededEndpointAuthorityContext(
            as_of=_fixed_clock(),
            provider_policy_id=f"provider_policy.mcp-endpoint.{suffix}",
            candidate_ref=f"candidate.openai.mcp-endpoint.{suffix}.gpt54",
            binding_scope="protocol_mcp",
            endpoint_kind="mcp_tools_call",
        )
        await exercise(conn, context)
    finally:
        await conn.close()


def test_mcp_endpoint_authority_adoption_resolves_one_binding_and_fails_closed() -> None:
    asyncio.run(_exercise_mcp_endpoint_authority_adoption())


async def _exercise_mcp_endpoint_authority_adoption() -> None:
    async def exercise(conn: asyncpg.Connection, context: SeededEndpointAuthorityContext) -> None:
        request = MCPProtocolEndpointRequest(
            provider_policy_id=context.provider_policy_id,
            candidate_ref=context.candidate_ref,
            message=_mcp_tools_call_message(),
        )

        await _seed_active_binding(
            conn,
            context=context,
            provider_endpoint_binding_id="binding.primary",
            endpoint_ref="endpoint.mcp.primary",
            endpoint_uri="https://mcp.example.test/primary",
            decision_ref="decision:mcp-endpoint:primary",
        )

        resolution = await resolve_mcp_protocol_endpoint(
            conn,
            request=request,
            as_of=context.as_of,
        )

        assert resolution.authority == "registry.endpoint_failover"
        assert resolution.protocol_path == "tools/call"
        assert resolution.provider_endpoint_binding_id == "binding.primary"
        assert resolution.endpoint_ref == "endpoint.mcp.primary"
        assert resolution.endpoint_uri == "https://mcp.example.test/primary"
        assert resolution.transport_kind == "streamable_http"
        assert resolution.tool_name == "inspect_run"

        await conn.execute("DELETE FROM provider_endpoint_bindings")

        with pytest.raises(ProtocolEndpointRuntimeError) as missing_exc:
            await resolve_mcp_protocol_endpoint(
                conn,
                request=request,
                as_of=context.as_of,
            )
        assert missing_exc.value.reason_code == "protocol_endpoint_runtime.endpoint_missing"
        assert missing_exc.value.details["provider_policy_id"] == context.provider_policy_id
        assert missing_exc.value.details["candidate_ref"] == context.candidate_ref

        await _seed_active_binding(
            conn,
            context=context,
            provider_endpoint_binding_id="binding.primary",
            endpoint_ref="endpoint.mcp.primary",
            endpoint_uri="https://mcp.example.test/primary",
            decision_ref="decision:mcp-endpoint:primary",
        )
        await _seed_active_binding(
            conn,
            context=context,
            provider_endpoint_binding_id="binding.shadow",
            endpoint_ref="endpoint.mcp.shadow",
            endpoint_uri="https://mcp.example.test/shadow",
            decision_ref="decision:mcp-endpoint:shadow",
        )

        with pytest.raises(ProtocolEndpointRuntimeError) as ambiguous_exc:
            await resolve_mcp_protocol_endpoint(
                conn,
                request=request,
                as_of=context.as_of,
            )
        assert ambiguous_exc.value.reason_code == "protocol_endpoint_runtime.endpoint_ambiguous"
        assert ambiguous_exc.value.details["binding_scope"] == context.binding_scope
        assert ambiguous_exc.value.details["endpoint_kind"] == context.endpoint_kind

    await _with_seeded_connection(exercise)
