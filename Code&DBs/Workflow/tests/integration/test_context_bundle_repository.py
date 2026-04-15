from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from registry.context_bundle_repository import (
    ContextBundleRepositoryError,
    PostgresContextBundleRepository,
    bootstrap_context_bundle_schema,
)
from registry.domain import (
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from registry.model_routing import (
    ModelProfileAuthorityRecord,
    ModelRouter,
    ProviderModelCandidateAuthorityRecord,
    ProviderPolicyAuthorityRecord,
)
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from registry.route_catalog_repository import (
    ModelProfileCandidateBindingAuthorityRecord,
    RouteCatalogAuthority,
)
from registry.repository import (
    PostgresRegistryAuthorityRepository,
    bootstrap_registry_authority_schema,
    load_registry_resolver,
)
from runtime.context_compiler import (
    ContextAuthorityRecord,
    ContextCompilationError,
    ContextCompiler,
)
from storage.postgres import connect_workflow_database


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _build_router() -> ModelRouter:
    route_catalog = RouteCatalogAuthority(
        model_profiles={
            "model_profile.alpha": (
                ModelProfileAuthorityRecord(
                    model_profile_id="model_profile.alpha",
                    candidate_refs=("candidate.openai.gpt54",),
                ),
            ),
        },
        provider_policies={
            "provider_policy.alpha": (
                ProviderPolicyAuthorityRecord(
                    provider_policy_id="provider_policy.alpha",
                    allowed_provider_refs=("provider.openai",),
                ),
            ),
        },
        provider_model_candidates={
            "candidate.openai.gpt54": (
                ProviderModelCandidateAuthorityRecord(
                    candidate_ref="candidate.openai.gpt54",
                    provider_ref="provider.openai",
                    provider_slug="openai",
                    model_slug="gpt-5.4",
                    priority=10,
                    balance_weight=1,
                    capability_tags=("primary",),
                ),
            ),
        },
        model_profile_candidate_bindings={
            "model_profile.alpha": (
                ModelProfileCandidateBindingAuthorityRecord(
                    model_profile_candidate_binding_id="binding.model_profile.alpha.0",
                    model_profile_id="model_profile.alpha",
                    candidate_ref="candidate.openai.gpt54",
                    binding_role="admitted",
                    position_index=0,
                ),
            ),
        },
    )
    candidate_ref = "candidate.openai.gpt54"
    health_window_id = "health_window.model_profile.alpha"
    budget_window_id = "budget_window.provider_policy.alpha"
    route_authority = ProviderRouteAuthority(
        provider_route_health_windows={
            candidate_ref: (
                ProviderRouteHealthWindowAuthorityRecord(
                    provider_route_health_window_id=health_window_id,
                    candidate_ref=candidate_ref,
                    provider_ref="provider.openai",
                    health_status="healthy",
                    health_score=1.0,
                    sample_count=8,
                    failure_rate=0.0,
                    latency_p95_ms=75,
                    observed_window_started_at=_fixed_clock() - timedelta(minutes=15),
                    observed_window_ended_at=_fixed_clock(),
                    observation_ref="observation.context_bundle_repository",
                    created_at=_fixed_clock(),
                ),
            ),
        },
        provider_budget_windows={
            "provider_policy.alpha": (
                ProviderBudgetWindowAuthorityRecord(
                    provider_budget_window_id=budget_window_id,
                    provider_policy_id="provider_policy.alpha",
                    provider_ref="provider.openai",
                    budget_scope="runtime",
                    budget_status="available",
                    window_started_at=_fixed_clock() - timedelta(minutes=30),
                    window_ended_at=_fixed_clock() + timedelta(minutes=30),
                    request_limit=100,
                    requests_used=0,
                    token_limit=10_000,
                    tokens_used=0,
                    spend_limit_usd=None,
                    spend_used_usd=None,
                    decision_ref="decision.context_bundle_repository.budget",
                    created_at=_fixed_clock(),
                ),
            ),
        },
        route_eligibility_states={
            candidate_ref: (
                RouteEligibilityStateAuthorityRecord(
                    route_eligibility_state_id="eligibility.context_bundle_repository",
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                    candidate_ref=candidate_ref,
                    eligibility_status="eligible",
                    reason_code="provider_route_authority.healthy_budget_available",
                    source_window_refs=(health_window_id, budget_window_id),
                    evaluated_at=_fixed_clock() - timedelta(minutes=5),
                    expires_at=None,
                    decision_ref="decision.context_bundle_repository.eligibility",
                    created_at=_fixed_clock() - timedelta(minutes=5),
                ),
            ),
        },
    )
    return ModelRouter(route_catalog=route_catalog, route_authority=route_authority)


def _context_records() -> dict[str, tuple[ContextAuthorityRecord, ...]]:
    return {
        "context.policy.alpha": (
            ContextAuthorityRecord(
                context_ref="context.policy.alpha",
                authority_kind="policy",
                content_hash="sha256:policy-alpha",
                payload={
                    "instructions": [
                        "Follow admitted routing only.",
                        "Fail closed on unknown refs.",
                    ],
                    "max_tokens": 800,
                },
            ),
        ),
        "context.workflow.alpha": (
            ContextAuthorityRecord(
                context_ref="context.workflow.alpha",
                authority_kind="workflow_definition",
                content_hash="sha256:workflow-alpha",
                payload={
                    "summary": "Compile bounded authority only.",
                    "tools": ["rg", "pytest"],
                },
            ),
        ),
    }


async def _prepare_context_bundle_authority(
    *,
    database_url: str,
) -> tuple[object, object, object, str]:
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": database_url},
    )
    try:
        await conn.execute("DROP TABLE IF EXISTS context_bundle_anchors CASCADE")
        await conn.execute("DROP TABLE IF EXISTS context_bundles CASCADE")
        await bootstrap_registry_authority_schema(conn)
        await bootstrap_context_bundle_schema(conn)

        suffix = _unique_suffix()
        workspace_ref = f"workspace.{suffix}"
        runtime_profile_ref = f"runtime_profile.{suffix}"
        workspace_record = WorkspaceAuthorityRecord(
            workspace_ref=workspace_ref,
            repo_root=f"/tmp/{workspace_ref}",
            workdir=f"/tmp/{workspace_ref}/workdir",
        )
        runtime_profile_record = RuntimeProfileAuthorityRecord(
            runtime_profile_ref=runtime_profile_ref,
            model_profile_id="model_profile.alpha",
            provider_policy_id="provider_policy.alpha",
            sandbox_profile_ref=runtime_profile_ref,
        )

        registry_repository = PostgresRegistryAuthorityRepository(conn)
        await registry_repository.upsert_workspace_authority(workspace_record)
        await registry_repository.upsert_runtime_profile_authority(runtime_profile_record)

        resolver = await load_registry_resolver(
            conn,
            workspace_refs=(workspace_ref,),
            runtime_profile_refs=(runtime_profile_ref,),
        )
        workspace = resolver.resolve_workspace(workspace_ref=workspace_ref)
        runtime_profile = resolver.resolve_runtime_profile(
            runtime_profile_ref=runtime_profile_ref,
        )
        router = _build_router()
        route_decision = router.decide_route(
            runtime_profile=runtime_profile,
            balance_slot=0,
        )
        return workspace, runtime_profile, route_decision, suffix
    finally:
        await conn.close()


def test_context_bundle_repository_persists_and_replays_canonical_bundle_rows() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    workspace, runtime_profile, route_decision, suffix = asyncio.run(
        _prepare_context_bundle_authority(database_url=database_url),
    )

    bundle_repository = PostgresContextBundleRepository(database_url=database_url)
    run_id = f"run.{suffix}"
    context_bundle_id = f"context:{run_id}"
    source_decision_refs = ("decision.admission.alpha", route_decision.route_decision_id)
    seeded_context_records = tuple(
        _context_records()[context_ref][0]
        for context_ref in ("context.policy.alpha", "context.workflow.alpha")
    )
    seeded_bundle = ContextCompiler._build_context_bundle(
        workflow_id="workflow.alpha",
        run_id=run_id,
        workspace=workspace,
        runtime_profile=runtime_profile,
        bundle_version=1,
        source_decision_refs=source_decision_refs,
        resolved_at=_fixed_clock(),
    )
    bundle_repository.persist_context_bundle(
        bundle=seeded_bundle,
        anchors=ContextCompiler._anchors_from_context_records(seeded_context_records),
    )

    compiler = ContextCompiler(
        context_records={},
        context_bundle_repository=bundle_repository,
        model_router=_build_router(),
        clock=_fixed_clock,
    )

    packet_one = compiler.compile_packet(
        workflow_id="workflow.alpha",
        run_id=run_id,
        workspace=workspace,
        runtime_profile=runtime_profile,
        route_decision=route_decision,
        context_refs=("context.policy.alpha", "context.workflow.alpha"),
        source_decision_refs=source_decision_refs,
        context_bundle_id=context_bundle_id,
    )

    snapshot = bundle_repository.load_context_bundle(
        context_bundle_id=context_bundle_id,
    )
    assert snapshot.bundle.context_bundle_id == context_bundle_id
    assert snapshot.bundle.workflow_id == "workflow.alpha"
    assert snapshot.bundle.run_id == run_id
    assert snapshot.bundle.sandbox_profile_ref == runtime_profile.sandbox_profile_ref
    assert snapshot.bundle.bundle_hash == packet_one.packet_payload["context_bundle"]["bundle_hash"]
    assert snapshot.bundle.bundle_payload["workspace"]["repo_root"] == workspace.repo_root
    assert snapshot.bundle.bundle_payload["workspace"]["workdir"] == workspace.workdir
    assert (
        snapshot.bundle.bundle_payload["runtime_profile"]["sandbox_profile_ref"]
        == runtime_profile.sandbox_profile_ref
    )
    assert tuple(anchor.anchor_ref for anchor in snapshot.anchors) == (
        "context.policy.alpha",
        "context.workflow.alpha",
    )
    assert tuple(anchor.position_index for anchor in snapshot.anchors) == (0, 1)
    assert tuple(anchor.anchor_kind for anchor in snapshot.anchors) == (
        "policy",
        "workflow_definition",
    )

    load_only_compiler = ContextCompiler(
        context_records={},
        context_bundle_repository=bundle_repository,
        model_router=_build_router(),
        clock=_fixed_clock,
    )
    packet_two = load_only_compiler.compile_packet(
        workflow_id="workflow.alpha",
        run_id=run_id,
        workspace=workspace,
        runtime_profile=runtime_profile,
        route_decision=route_decision,
        context_refs=(),
        source_decision_refs=source_decision_refs,
        context_bundle_id=context_bundle_id,
    )

    assert packet_one == packet_two
    assert packet_two.packet_payload["context_bundle"]["context_bundle_id"] == context_bundle_id
    assert tuple(entry.context_ref for entry in packet_two.entries) == (
        "context.policy.alpha",
        "context.workflow.alpha",
    )

    with pytest.raises(ContextCompilationError) as mismatch_exc_info:
        load_only_compiler.compile_packet(
            workflow_id="workflow.alpha",
            run_id=run_id,
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=route_decision,
            context_refs=("context.policy.alpha",),
            source_decision_refs=source_decision_refs,
            context_bundle_id=context_bundle_id,
        )
    assert mismatch_exc_info.value.reason_code == "context.bundle_anchor_mismatch"

    with pytest.raises(ContextBundleRepositoryError) as exc_info:
        bundle_repository.load_context_bundle(
            context_bundle_id="context:missing",
        )
    assert exc_info.value.reason_code == "context.bundle_unknown"

    with pytest.raises(ContextCompilationError) as exc_info:
        load_only_compiler.compile_packet(
            workflow_id="workflow.alpha",
            run_id=f"run.{suffix}.missing",
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=route_decision,
            context_refs=("context.policy.alpha",),
            source_decision_refs=source_decision_refs,
            context_bundle_id="context:missing",
        )
    assert exc_info.value.reason_code == "context.bundle_unknown"
