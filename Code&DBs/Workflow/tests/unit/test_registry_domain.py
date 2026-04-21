from __future__ import annotations

import pytest

from registry.domain import (
    RegistryBoundaryError,
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)


def test_registry_resolver_resolves_context_bundle_with_explicit_sandbox_profile() -> None:
    resolver = RegistryResolver(
        workspace_records={
            "workspace.alpha": (
                WorkspaceAuthorityRecord(
                    workspace_ref="workspace.alpha",
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            "runtime_profile.alpha": (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref="runtime_profile.alpha",
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                    sandbox_profile_ref="sandbox_profile.alpha",
                ),
            ),
        },
    )

    workspace = resolver.resolve_workspace(workspace_ref="workspace.alpha")
    runtime_profile = resolver.resolve_runtime_profile(
        runtime_profile_ref="runtime_profile.alpha",
    )
    bundle = resolver.resolve_context_bundle(
        workflow_id="workflow.alpha",
        run_id="run.alpha",
        workspace=workspace,
        runtime_profile=runtime_profile,
        bundle_version=1,
        source_decision_refs=("decision.alpha",),
    )

    assert runtime_profile.sandbox_profile_ref == "sandbox_profile.alpha"
    assert bundle.sandbox_profile_ref == "sandbox_profile.alpha"
    assert bundle.bundle_payload["runtime_profile"]["sandbox_profile_ref"] == "sandbox_profile.alpha"


def test_registry_resolver_fails_closed_when_sandbox_profile_ref_is_missing() -> None:
    resolver = RegistryResolver(
        runtime_profile_records={
            "runtime_profile.alpha": (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref="runtime_profile.alpha",
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                ),
            ),
        },
    )

    with pytest.raises(RegistryBoundaryError) as exc_info:
        resolver.resolve_runtime_profile(runtime_profile_ref="runtime_profile.alpha")

    assert exc_info.value.reason_code == "registry.boundary_violation"
    assert "sandbox_profile_ref missing" in exc_info.value.details
