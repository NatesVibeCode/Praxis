"""Registry authority.

Owns workspace, runtime profile, and boundary resolution. The boundary fails
closed instead of guessing roots or contexts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from runtime.crypto_authority import canonical_digest_hex


class RegistryBoundaryError(RuntimeError):
    """Raised when path or authority context resolution is ambiguous."""

    def __init__(self, reason_code: str, details: str):
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details


class RegistryResolutionError(RuntimeError):
    """Raised when registry data cannot be resolved into a usable bundle."""

    def __init__(self, reason_code: str, details: str):
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details


@dataclass(frozen=True, slots=True)
class WorkspaceIdentity:
    """Resolved workspace boundary for a run."""

    workspace_ref: str
    repo_root: str
    workdir: str


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    """Resolved runtime profile boundary for a run."""

    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    sandbox_profile_ref: str = ""


@dataclass(frozen=True, slots=True)
class WorkspaceAuthorityRecord:
    """Authoritative workspace record used to resolve workspace identity."""

    workspace_ref: str
    repo_root: str
    workdir: str


@dataclass(frozen=True, slots=True)
class RuntimeProfileAuthorityRecord:
    """Authoritative runtime profile record used to resolve profile identity."""

    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    sandbox_profile_ref: str = ""


@dataclass(frozen=True, slots=True)
class SandboxProfileAuthorityRecord:
    """Canonical sandbox profile consumed by runtime execution."""

    sandbox_profile_ref: str
    sandbox_provider: str
    docker_image: str | None = None
    docker_cpus: str | None = None
    docker_memory: str | None = None
    network_policy: str = "provider_only"
    workspace_materialization: str = "copy"
    secret_allowlist: tuple[str, ...] = ()
    auth_mount_policy: str = "provider_scoped"
    timeout_profile: str = "default"


@dataclass(frozen=True, slots=True)
class ContextBundle:
    """Immutable admitted context bundle for one run."""

    context_bundle_id: str
    workflow_id: str
    run_id: str
    workspace_ref: str
    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    sandbox_profile_ref: str
    bundle_version: int
    bundle_hash: str
    bundle_payload: Mapping[str, Any]
    source_decision_refs: tuple[str, ...]
    resolved_at: datetime


@dataclass(frozen=True, slots=True)
class UnresolvedAuthorityContext:
    """Typed reject-path authority context for unresolved intake outcomes."""

    context_bundle_id: str
    workflow_id: str
    run_id: str
    workspace_ref: str
    runtime_profile_ref: str
    bundle_version: int
    bundle_hash: str
    bundle_payload: Mapping[str, Any]
    source_decision_refs: tuple[str, ...]
    unresolved_reason_code: str
    derived_at: datetime


AuthorityContext = ContextBundle | UnresolvedAuthorityContext


class RegistryResolver:
    """Boundary resolver for workspace and runtime context.

    Callers provide authoritative refs only. The resolver owns the mapping from
    those refs to concrete workspace/profile records and fails closed on
    missing or ambiguous input.
    """

    def __init__(
        self,
        *,
        workspace_records: Mapping[str, Sequence[WorkspaceAuthorityRecord]] | None = None,
        runtime_profile_records: Mapping[str, Sequence[RuntimeProfileAuthorityRecord]] | None = None,
    ) -> None:
        self._workspace_records = {
            workspace_ref: tuple(records)
            for workspace_ref, records in (workspace_records or {}).items()
        }
        self._runtime_profile_records = {
            runtime_profile_ref: tuple(records)
            for runtime_profile_ref, records in (runtime_profile_records or {}).items()
        }

    @staticmethod
    def _select_one(
        *,
        reason_code: str,
        ref_name: str,
        ref_value: str,
        candidates: Sequence[object],
    ) -> object:
        if not candidates:
            raise RegistryResolutionError(
                reason_code,
                f"missing authoritative {ref_name} for ref={ref_value!r}",
            )
        if len(candidates) > 1:
            raise RegistryBoundaryError(
                "registry.ambiguity",
                f"ambiguous authoritative {ref_name} for ref={ref_value!r}",
            )
        return candidates[0]

    def resolve_workspace(
        self,
        *,
        workspace_ref: str,
    ) -> WorkspaceIdentity:
        candidate = self._select_one(
            reason_code="registry.workspace_unknown",
            ref_name="workspace",
            ref_value=workspace_ref,
            candidates=self._workspace_records.get(workspace_ref, ()),
        )
        if not isinstance(candidate, WorkspaceAuthorityRecord):
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"workspace record type mismatch for ref={workspace_ref!r}",
            )
        if candidate.workspace_ref != workspace_ref:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"workspace ref mismatch for ref={workspace_ref!r}",
            )
        if not candidate.repo_root or not candidate.workdir:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"workspace boundary incomplete for ref={workspace_ref!r}",
            )
        # Authority identity is host-agnostic: repo_root/workdir are returned
        # verbatim so the bundle_hash stays stable across admission (host CLI)
        # and execution (docker worker). Translation to a concrete filesystem
        # path belongs to execution (sandbox config, adapter workdir input).
        return WorkspaceIdentity(
            workspace_ref=candidate.workspace_ref,
            repo_root=candidate.repo_root,
            workdir=candidate.workdir,
        )

    def resolve_runtime_profile(
        self,
        *,
        runtime_profile_ref: str,
    ) -> RuntimeProfile:
        candidate = self._select_one(
            reason_code="registry.profile_unknown",
            ref_name="runtime profile",
            ref_value=runtime_profile_ref,
            candidates=self._runtime_profile_records.get(runtime_profile_ref, ()),
        )
        if not isinstance(candidate, RuntimeProfileAuthorityRecord):
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"runtime profile record type mismatch for ref={runtime_profile_ref!r}",
            )
        if candidate.runtime_profile_ref != runtime_profile_ref:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"runtime profile ref mismatch for ref={runtime_profile_ref!r}",
            )
        if not candidate.model_profile_id or not candidate.provider_policy_id:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"runtime profile boundary incomplete for ref={runtime_profile_ref!r}",
            )
        sandbox_profile_ref = str(candidate.sandbox_profile_ref or "").strip()
        if not sandbox_profile_ref:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"runtime profile sandbox_profile_ref missing for ref={runtime_profile_ref!r}",
            )
        return RuntimeProfile(
            runtime_profile_ref=candidate.runtime_profile_ref,
            model_profile_id=candidate.model_profile_id,
            provider_policy_id=candidate.provider_policy_id,
            sandbox_profile_ref=sandbox_profile_ref,
        )

    def resolve_context_bundle(
        self,
        *,
        workflow_id: str,
        run_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        bundle_version: int,
        source_decision_refs: Sequence[str] = (),
    ) -> ContextBundle:
        if not workflow_id or not run_id:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                "workflow_id and run_id are required to build a context bundle",
            )
        if bundle_version < 1:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"unsupported context bundle version: {bundle_version}",
            )

        canonical_payload = {
            "bundle_version": bundle_version,
            "run_id": run_id,
            "runtime_profile": {
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                "sandbox_profile_ref": runtime_profile.sandbox_profile_ref,
            },
            "source_decision_refs": list(source_decision_refs),
            "workspace": {
                "repo_root": workspace.repo_root,
                "workdir": workspace.workdir,
                "workspace_ref": workspace.workspace_ref,
            },
            "workflow_id": workflow_id,
        }
        bundle_hash = canonical_digest_hex(
            canonical_payload,
            purpose="execution_boundary.authority_payload",
        )
        return ContextBundle(
            context_bundle_id=f"context:{run_id}",
            workflow_id=workflow_id,
            run_id=run_id,
            workspace_ref=workspace.workspace_ref,
            runtime_profile_ref=runtime_profile.runtime_profile_ref,
            model_profile_id=runtime_profile.model_profile_id,
            provider_policy_id=runtime_profile.provider_policy_id,
            sandbox_profile_ref=runtime_profile.sandbox_profile_ref,
            bundle_version=bundle_version,
            bundle_hash=bundle_hash,
            bundle_payload=canonical_payload,
            source_decision_refs=tuple(source_decision_refs),
            resolved_at=datetime.now(timezone.utc),
        )

    def build_unresolved_context(
        self,
        *,
        workflow_id: str,
        run_id: str,
        request_digest: str,
        workspace_ref: str,
        runtime_profile_ref: str,
        bundle_version: int,
        unresolved_reason_code: str,
        source_decision_refs: Sequence[str] = (),
    ) -> UnresolvedAuthorityContext:
        if bundle_version < 1:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                f"unsupported context bundle version: {bundle_version}",
            )
        if not unresolved_reason_code:
            raise RegistryBoundaryError(
                "registry.boundary_violation",
                "unresolved_reason_code is required to build reject-path authority context",
            )

        canonical_payload = {
            "authority_state": "unresolved",
            "bundle_version": bundle_version,
            "request_digest": request_digest,
            "run_id": run_id,
            "runtime_profile": {
                "runtime_profile_ref": runtime_profile_ref,
            },
            "source_decision_refs": list(source_decision_refs),
            "unresolved_reason_code": unresolved_reason_code,
            "workspace": {
                "workspace_ref": workspace_ref,
            },
            "workflow_id": workflow_id,
        }
        bundle_hash = canonical_digest_hex(
            canonical_payload,
            purpose="execution_boundary.authority_payload",
        )
        return UnresolvedAuthorityContext(
            context_bundle_id=f"context_unresolved:{run_id}",
            workflow_id=workflow_id,
            run_id=run_id,
            workspace_ref=workspace_ref,
            runtime_profile_ref=runtime_profile_ref,
            bundle_version=bundle_version,
            bundle_hash=bundle_hash,
            bundle_payload=canonical_payload,
            source_decision_refs=tuple(source_decision_refs),
            unresolved_reason_code=unresolved_reason_code,
            derived_at=datetime.now(timezone.utc),
        )


__all__ = [
    "AuthorityContext",
    "ContextBundle",
    "RegistryBoundaryError",
    "RegistryResolutionError",
    "RegistryResolver",
    "RuntimeProfile",
    "RuntimeProfileAuthorityRecord",
    "UnresolvedAuthorityContext",
    "WorkspaceAuthorityRecord",
    "WorkspaceIdentity",
]
