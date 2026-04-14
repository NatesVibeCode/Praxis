from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from storage.migrations import (
    workflow_migration_expected_objects,
    workflow_migration_manifest,
    workflow_migration_statements,
)
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    resolve_workflow_database_url,
)

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_runtime_breadth_authority_schema_is_the_canonical_tail_migration() -> None:
    filenames = [entry.filename for entry in workflow_migration_manifest()]
    assert "011_runtime_breadth_authority.sql" in filenames
    assert "071_repo_snapshots_runtime_breadth_repair.sql" in filenames
    assert filenames[-1] == "106_acceptance_status_index.sql"


def test_runtime_breadth_authority_schema_expected_objects_are_declared() -> None:
    objects = workflow_migration_expected_objects("011_runtime_breadth_authority.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "provider_failover_bindings",
            "provider_endpoint_bindings",
            "persona_profiles",
            "persona_context_bindings",
            "fork_profiles",
            "fork_worktree_bindings",
            "provider_failover_bindings_scope_idx",
            "provider_endpoint_bindings_policy_status_idx",
            "persona_profiles_name_status_idx",
            "persona_context_bindings_profile_idx",
            "fork_profiles_name_status_idx",
            "fork_worktree_bindings_profile_status_idx",
        }
    )


def test_runtime_breadth_authority_schema_rows_can_be_inserted_against_canonical_dependencies() -> None:
    asyncio.run(
        _exercise_runtime_breadth_authority_schema_rows_can_be_inserted_against_canonical_dependencies()
    )


async def _exercise_runtime_breadth_authority_schema_rows_can_be_inserted_against_canonical_dependencies() -> None:
    try:
        database_url = resolve_workflow_database_url(
            env={"WORKFLOW_DATABASE_URL": os.environ.get("WORKFLOW_DATABASE_URL", "")}
        )
        conn = await connect_workflow_database(
            env={"WORKFLOW_DATABASE_URL": database_url},
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for runtime breadth schema integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        as_of = datetime(2026, 4, 3, 18, 0, tzinfo=timezone.utc)
        schema_name = f"runtime_breadth_{uuid.uuid4().hex[:10]}"

        await conn.execute(f'CREATE SCHEMA "{schema_name}"')
        await conn.execute(f'SET search_path TO "{schema_name}"')

        await bootstrap_control_plane_schema(conn)
        await _bootstrap_migration(conn, "004_claim_lease_proposal_runtime.sql")
        await _bootstrap_migration(conn, "006_platform_authority_schema.sql")
        await _bootstrap_migration(conn, "011_runtime_breadth_authority.sql")

        await _seed_definition_and_run(conn, as_of=as_of)
        await _seed_provider_authority(conn, as_of=as_of)
        await _seed_sandbox_authority(conn, as_of=as_of)

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
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            """,
            "provider_failover_binding.runtime-breadth.1",
            "model_profile.runtime-breadth.1",
            "provider_policy.runtime-breadth.1",
            "candidate.runtime-breadth.1",
            "native_runtime",
            "primary",
            "health_degraded",
            0,
            as_of,
            None,
            "decision.runtime-breadth.failover.1",
            as_of,
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
            "provider_endpoint_binding.runtime-breadth.1",
            "provider_policy.runtime-breadth.1",
            "candidate.runtime-breadth.1",
            "native_runtime",
            "endpoint.runtime-breadth.1",
            "chat_completions",
            "https",
            "https://api.example.test/v1/chat/completions",
            "secret.runtime-breadth.1",
            "active",
            _json_payload({"timeout_ms": 30000}),
            _json_payload({"threshold": 3, "window_s": 60}),
            as_of,
            None,
            "decision.runtime-breadth.endpoint.1",
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO persona_profiles (
                persona_profile_id,
                persona_name,
                persona_kind,
                status,
                instruction_contract,
                response_contract,
                tool_policy,
                runtime_hints,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11, $12
            )
            """,
            "persona_profile.runtime-breadth.1",
            "runtime-breadth-operator",
            "operator",
            "active",
            "Keep runtime-breadth control explicit and refuse hidden defaults.",
            _json_payload({"format": "structured", "verbosity": "tight"}),
            _json_payload({"shell_authority": "forbidden"}),
            _json_payload({"preferred_workflow_class": "review"}),
            as_of,
            None,
            "decision.runtime-breadth.persona.1",
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO persona_context_bindings (
                persona_context_binding_id,
                persona_profile_id,
                binding_scope,
                workspace_ref,
                runtime_profile_ref,
                model_profile_id,
                provider_policy_id,
                context_selector,
                binding_status,
                position_index,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14
            )
            """,
            "persona_context_binding.runtime-breadth.1",
            "persona_profile.runtime-breadth.1",
            "workspace_runtime",
            "workspace.runtime-breadth.1",
            "runtime-profile.runtime-breadth.1",
            "model_profile.runtime-breadth.1",
            "provider_policy.runtime-breadth.1",
            _json_payload({"dispatch_kind": "native", "route_kind": "bounded"}),
            "active",
            0,
            as_of,
            None,
            "decision.runtime-breadth.persona-context.1",
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO fork_profiles (
                fork_profile_id,
                profile_name,
                orchestration_kind,
                status,
                fork_mode,
                worktree_strategy,
                sandbox_policy,
                retention_policy,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12
            )
            """,
            "fork_profile.runtime-breadth.1",
            "bounded-native-build",
            "build",
            "active",
            "bounded_fork",
            "ephemeral_worktree",
            _json_payload({"share_mode": "exclusive"}),
            _json_payload({"retire_after": "promotion_or_reject"}),
            as_of,
            None,
            "decision.runtime-breadth.fork.1",
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO fork_worktree_bindings (
                fork_worktree_binding_id,
                fork_profile_id,
                sandbox_session_id,
                workflow_run_id,
                binding_scope,
                binding_status,
                workspace_ref,
                runtime_profile_ref,
                base_ref,
                fork_ref,
                worktree_ref,
                created_at,
                retired_at,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            """,
            "fork_worktree_binding.runtime-breadth.1",
            "fork_profile.runtime-breadth.1",
            "sandbox_session.runtime-breadth.1",
            "run.runtime-breadth.1",
            "proposal",
            "active",
            "workspace.runtime-breadth.1",
            "runtime-profile.runtime-breadth.1",
            "refs/heads/main",
            "refs/heads/runtime-breadth/fork-1",
            "worktree.runtime-breadth.1",
            as_of,
            None,
            "decision.runtime-breadth.fork-binding.1",
        )

        row = await conn.fetchrow(
            """
            SELECT
                failover.binding_scope,
                endpoint.endpoint_ref,
                persona.persona_name,
                persona_binding.runtime_profile_ref,
                fork_profile.profile_name,
                fork_binding.worktree_ref
            FROM provider_failover_bindings AS failover
            JOIN provider_endpoint_bindings AS endpoint
                ON endpoint.candidate_ref = failover.candidate_ref
            JOIN persona_profiles AS persona
                ON persona.persona_profile_id = $1
            JOIN persona_context_bindings AS persona_binding
                ON persona_binding.persona_profile_id = persona.persona_profile_id
            JOIN fork_profiles AS fork_profile
                ON fork_profile.fork_profile_id = $2
            JOIN fork_worktree_bindings AS fork_binding
                ON fork_binding.fork_profile_id = fork_profile.fork_profile_id
            WHERE failover.provider_failover_binding_id = $3
              AND endpoint.provider_endpoint_binding_id = $4
              AND persona_binding.persona_context_binding_id = $5
              AND fork_binding.fork_worktree_binding_id = $6
            """,
            "persona_profile.runtime-breadth.1",
            "fork_profile.runtime-breadth.1",
            "provider_failover_binding.runtime-breadth.1",
            "provider_endpoint_binding.runtime-breadth.1",
            "persona_context_binding.runtime-breadth.1",
            "fork_worktree_binding.runtime-breadth.1",
        )
        assert row is not None
        assert row["binding_scope"] == "native_runtime"
        assert row["endpoint_ref"] == "endpoint.runtime-breadth.1"
        assert row["persona_name"] == "runtime-breadth-operator"
        assert row["runtime_profile_ref"] == "runtime-profile.runtime-breadth.1"
        assert row["profile_name"] == "bounded-native-build"
        assert row["worktree_ref"] == "worktree.runtime-breadth.1"
    finally:
        await transaction.rollback()
        await conn.close()


async def _bootstrap_migration(conn: asyncpg.Connection, filename: str) -> None:
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


async def _seed_definition_and_run(conn: asyncpg.Connection, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10
        )
        """,
        "workflow_definition.runtime-breadth.1",
        "workflow.runtime-breadth.1",
        1,
        1,
        "sha256:runtime-breadth-definition-1",
        "admitted",
        _json_payload({"kind": "native_build"}),
        _json_payload({"nodes": [], "edges": []}),
        as_of,
        None,
    )
    await conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        """,
        "admission_decision.runtime-breadth.1",
        "workflow.runtime-breadth.1",
        "request.runtime-breadth.1",
        "admit",
        "policy_satisfied",
        as_of,
        "operator.console",
        "policy.runtime-breadth.1",
        "validation.runtime-breadth.1",
        "authority_context.runtime-breadth.1",
    )
    await conn.execute(
        """
        INSERT INTO context_bundles (
            context_bundle_id,
            workflow_id,
            run_id,
            workspace_ref,
            runtime_profile_ref,
            model_profile_id,
            provider_policy_id,
            bundle_version,
            bundle_hash,
            bundle_payload,
            source_decision_refs,
            resolved_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12
        )
        """,
        "context_bundle.runtime-breadth.1",
        "workflow.runtime-breadth.1",
        "run.runtime-breadth.1",
        "workspace.runtime-breadth.1",
        "runtime-profile.runtime-breadth.1",
        "model_profile.runtime-breadth.1",
        "provider_policy.runtime-breadth.1",
        1,
        "sha256:context-bundle-runtime-breadth-1",
        _json_payload({"route_kind": "native"}),
        _json_payload(["decision.runtime-breadth.bootstrap"]),
        as_of,
    )
    await conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        """,
        "run.runtime-breadth.1",
        "workflow.runtime-breadth.1",
        "request.runtime-breadth.1",
        "sha256:request-runtime-breadth-1",
        "sha256:authority-runtime-breadth-1",
        "workflow_definition.runtime-breadth.1",
        "sha256:runtime-breadth-definition-1",
        "runtime-breadth-idempotency-1",
        1,
        _json_payload({"objective": "runtime breadth schema seed"}),
        "context_bundle.runtime-breadth.1",
        "admission_decision.runtime-breadth.1",
        "claim_received",
        None,
        as_of,
        as_of,
        None,
        None,
        None,
    )


async def _seed_provider_authority(conn: asyncpg.Connection, *, as_of: datetime) -> None:
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
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11, $12, $13
        )
        """,
        "model_profile.runtime-breadth.1",
        "runtime-breadth-default",
        "openai",
        "gpt-5.4",
        1,
        "active",
        _json_payload({"daily_usd": 100}),
        _json_payload({"strategy": "deterministic"}),
        _json_payload({"temperature": 0}),
        as_of,
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
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11, $12, $13
        )
        """,
        "provider_policy.runtime-breadth.1",
        "runtime-breadth-policy",
        "openai",
        "native_runtime",
        1,
        "active",
        _json_payload(["gpt-5.4"]),
        _json_payload({"max_attempts": 2}),
        _json_payload({"monthly_usd": 1000}),
        _json_payload({"mode": "fail_closed"}),
        as_of,
        None,
        "decision.runtime-breadth.provider-policy.1",
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
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14
        )
        """,
        "candidate.runtime-breadth.1",
        "provider.runtime-breadth.openai",
        "openai",
        "openai",
        "gpt-5.4",
        "active",
        1,
        1,
        _json_payload(["chat", "tools"]),
        _json_payload({"temperature": 0}),
        as_of,
        None,
        "decision.runtime-breadth.candidate.1",
        as_of,
    )


async def _seed_sandbox_authority(conn: asyncpg.Connection, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_claim_lease_proposal_runtime (
            run_id,
            workflow_id,
            request_id,
            authority_context_ref,
            authority_context_digest,
            claim_id,
            lease_id,
            proposal_id,
            promotion_decision_id,
            attempt_no,
            transition_seq,
            sandbox_group_id,
            sandbox_session_id,
            share_mode,
            reuse_reason_code,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        """,
        "run.runtime-breadth.1",
        "workflow.runtime-breadth.1",
        "request.runtime-breadth.1",
        "authority_context.runtime-breadth.1",
        "sha256:authority-runtime-breadth-1",
        "claim.runtime-breadth.1",
        None,
        None,
        None,
        1,
        0,
        "sandbox_group.runtime-breadth.1",
        None,
        "exclusive",
        None,
        as_of,
        as_of,
    )
    await conn.execute(
        """
        INSERT INTO sandbox_sessions (
            sandbox_session_id,
            sandbox_group_id,
            workspace_ref,
            runtime_profile_ref,
            base_ref,
            base_digest,
            authority_context_digest,
            shared_compatibility_key,
            sandbox_root,
            share_mode,
            opened_at,
            expires_at,
            closed_at,
            closed_reason_code,
            owner_route_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
        """,
        "sandbox_session.runtime-breadth.1",
        "sandbox_group.runtime-breadth.1",
        "workspace.runtime-breadth.1",
        "runtime-profile.runtime-breadth.1",
        "refs/heads/main",
        "sha256:base-runtime-breadth-1",
        "sha256:authority-runtime-breadth-1",
        None,
        "/tmp/runtime-breadth-worktree-1",
        "exclusive",
        as_of,
        as_of,
        None,
        None,
        "run.runtime-breadth.1",
    )


def _json_payload(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
