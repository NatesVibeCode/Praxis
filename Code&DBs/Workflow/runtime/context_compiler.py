"""Bounded context compiler.

The compiler requires canonical context bundle authority from a repository and
does not synthesize extra context from injected records.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from hashlib import sha256
import inspect
import json
import math
from types import MappingProxyType
from typing import Any

from registry.domain import ContextBundle, RuntimeProfile, WorkspaceIdentity
from registry.context_bundle_repository import (
    ContextBundleAnchorRecord,
    ContextBundleRepositoryError,
    ContextBundleSnapshot,
)
from registry.model_routing import ModelRouteDecision, ModelRouter, ModelRoutingError
from runtime.context_cache import CacheKey, get_context_cache
from runtime.compile_artifacts import CompileArtifactStore
class ContextCompilationError(RuntimeError):
    """Raised when a bounded context packet cannot be compiled safely."""

    def __init__(
        self,
        reason_code: str,
        details: str,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details
        self.metadata = dict(metadata or {})


@dataclass(frozen=True, slots=True)
class ContextAuthorityRecord:
    """Pre-admitted context payload behind one canonical context ref."""

    context_ref: str
    authority_kind: str
    content_hash: str
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CompiledContextEntry:
    """Immutable compiled context entry admitted into one packet."""

    context_ref: str
    authority_kind: str
    content_hash: str
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class BoundedContextPacket:
    """Immutable bounded packet used for downstream prompt injection."""

    context_packet_id: str
    workflow_id: str
    run_id: str
    workspace_ref: str
    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    route_decision_id: str
    selected_candidate_ref: str
    provider_ref: str
    provider_slug: str
    model_slug: str
    packet_version: int
    packet_hash: str
    entries: tuple[CompiledContextEntry, ...]
    source_decision_refs: tuple[str, ...]
    compiled_at: datetime
    packet_payload: Mapping[str, Any]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContextCompilationError(
            "context.invalid_authority",
            f"{field_name} must be a non-empty string",
            metadata={"field": field_name},
        )
    return value.strip()


def _normalize_unique_refs(
    refs: Sequence[str],
    *,
    field_name: str,
) -> tuple[str, ...]:
    normalized: list[str] = []
    for index, ref in enumerate(refs):
        normalized.append(_require_text(ref, field_name=f"{field_name}[{index}]"))
    return tuple(dict.fromkeys(normalized))


def _freeze_jsonish(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                str(key): _freeze_jsonish(item)
                for key, item in value.items()
            }
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_jsonish(item) for item in value)
    return value


def _normalize_json_value(
    value: object,
    *,
    context_ref: str,
    field_path: str,
) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContextCompilationError(
                "context.invalid_payload",
                f"context payload for ref={context_ref!r} contains a non-finite number",
                metadata={
                    "context_ref": context_ref,
                    "field_path": field_path,
                    "value_type": type(value).__name__,
                },
            )
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ContextCompilationError(
                    "context.invalid_payload",
                    (
                        f"context payload for ref={context_ref!r} must use string keys; "
                        f"got {type(key).__name__}"
                    ),
                    metadata={
                        "context_ref": context_ref,
                        "field_path": field_path,
                        "value_type": type(key).__name__,
                    },
                )
            normalized[key] = _normalize_json_value(
                item,
                context_ref=context_ref,
                field_path=f"{field_path}.{key}",
            )
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _normalize_json_value(
                item,
                context_ref=context_ref,
                field_path=f"{field_path}[{index}]",
            )
            for index, item in enumerate(value)
        ]
    raise ContextCompilationError(
        "context.invalid_payload",
        f"context payload for ref={context_ref!r} is not JSON-serializable",
        metadata={
            "context_ref": context_ref,
            "field_path": field_path,
            "value_type": type(value).__name__,
        },
    )


def _require_json_mapping(
    value: object,
    *,
    reason_code: str,
    details: str,
    metadata: Mapping[str, Any],
) -> Mapping[str, Any]:
    normalized_value = value
    if isinstance(normalized_value, str):
        try:
            normalized_value = json.loads(normalized_value)
        except json.JSONDecodeError as exc:
            raise ContextCompilationError(
                reason_code,
                details,
                metadata=metadata,
            ) from exc
    if not isinstance(normalized_value, Mapping):
        raise ContextCompilationError(
            reason_code,
            details,
            metadata=metadata,
        )
    return normalized_value


def _stable_hash_value(value: object) -> object:
    if is_dataclass(value):
        return {
            "__dataclass__": type(value).__qualname__,
            **{
                field.name: _stable_hash_value(getattr(value, field.name))
                for field in fields(value)
            },
        }
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            value = value.astimezone(timezone.utc)
        return {"__datetime__": value.isoformat()}
    if isinstance(value, Mapping):
        return {
            str(key): _stable_hash_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_stable_hash_value(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContextCompilationError(
                "context.invalid_payload",
                "cache definition contains a non-finite number",
                metadata={"value_type": type(value).__name__},
            )
        return value
    return {
        "__repr__": repr(value),
        "__type__": type(value).__qualname__,
    }


def _hash_definition(definition: object) -> str:
    payload_json = json.dumps(
        _stable_hash_value(definition),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256(payload_json.encode("utf-8")).hexdigest()


class ContextCompiler:
    """Compile immutable context packets from explicit authority refs only."""

    def __init__(
        self,
        *,
        context_records: Mapping[str, Sequence[ContextAuthorityRecord]] | None = None,
        context_bundle_repository: object | None = None,
        model_router: ModelRouter | None = None,
        artifact_store: CompileArtifactStore | None = None,
        packet_version: int = 1,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._context_bundle_repository = context_bundle_repository
        self._model_router = model_router
        self._artifact_store = artifact_store
        self._packet_version = packet_version
        self._clock = clock

    @staticmethod
    def _select_one(
        *,
        reason_code: str,
        ref_name: str,
        ref_value: str,
        candidates: Sequence[object],
    ) -> object:
        if not candidates:
            raise ContextCompilationError(
                reason_code,
                f"missing authoritative {ref_name} for ref={ref_value!r}",
                metadata={"ref_name": ref_name, "ref_value": ref_value},
            )
        if len(candidates) > 1:
            raise ContextCompilationError(
                "context.ambiguity",
                f"ambiguous authoritative {ref_name} for ref={ref_value!r}",
                metadata={"ref_name": ref_name, "ref_value": ref_value},
            )
        return candidates[0]

    @staticmethod
    def _bundle_id_for_run(*, run_id: str) -> str:
        return f"context:{run_id}"

    @staticmethod
    def _workspace_cache_path(workspace: WorkspaceIdentity) -> str:
        return "|".join(
            (
                workspace.workspace_ref,
                workspace.repo_root,
                workspace.workdir,
            )
        )

    @staticmethod
    def _runtime_profile_cache_name(runtime_profile: RuntimeProfile) -> str:
        return "|".join(
            (
                runtime_profile.runtime_profile_ref,
                runtime_profile.model_profile_id,
                runtime_profile.provider_policy_id,
            )
        )

    def _packet_cache_key(
        self,
        *,
        workflow_id: str,
        run_id: str,
        context_bundle_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        route_decision: ModelRouteDecision,
        normalized_context_refs: Sequence[str],
        normalized_source_decision_refs: Sequence[str],
    ) -> CacheKey:
        profile_name = self._runtime_profile_cache_name(runtime_profile)
        return CacheKey(
            definition_hash=_hash_definition(
                {
                    "context_bundle_id": context_bundle_id,
                    "context_refs": tuple(normalized_context_refs),
                    "packet_version": self._packet_version,
                    "route_decision": route_decision,
                    "run_id": run_id,
                    "source_decision_refs": tuple(normalized_source_decision_refs),
                    "workflow_id": workflow_id,
                }
            ),
            workspace_path=self._workspace_cache_path(workspace),
            profile_name=profile_name,
            token_budget=self._packet_version,
        )

    def _load_context_bundle_snapshot(
        self,
        *,
        context_bundle_id: str,
    ) -> ContextBundleSnapshot:
        repository = self._context_bundle_repository
        if repository is None:
            raise ContextCompilationError(
                "context.bundle_authority_missing",
                "canonical context bundle repository is required",
                metadata={"context_bundle_id": context_bundle_id},
            )
        load_context_bundle = getattr(repository, "load_context_bundle", None)
        if load_context_bundle is None:
            raise ContextCompilationError(
                "context.bundle_authority_missing",
                "canonical context bundle repository does not expose load_context_bundle()",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "repository_type": type(repository).__name__,
                },
            )
        try:
            snapshot = load_context_bundle(context_bundle_id=context_bundle_id)
        except ContextBundleRepositoryError as exc:
            if exc.reason_code == "context.bundle_unknown":
                raise ContextCompilationError(
                    "context.bundle_unknown",
                    "missing authoritative context bundle",
                    metadata={
                        "context_bundle_id": context_bundle_id,
                        **exc.details,
                    },
                ) from exc
            raise ContextCompilationError(
                "context.bundle_read_failed",
                "failed to load canonical context bundle facts",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "repository_reason_code": exc.reason_code,
                    **exc.details,
                },
            ) from exc
        if not isinstance(snapshot, ContextBundleSnapshot):
            raise ContextCompilationError(
                "context.invalid_authority",
                "context bundle repository returned an invalid snapshot",
                metadata={"context_bundle_id": context_bundle_id},
            )
        return snapshot

    async def _load_context_bundle_snapshot_async(
        self,
        *,
        context_bundle_id: str,
    ) -> ContextBundleSnapshot:
        repository = self._context_bundle_repository
        if repository is None:
            raise ContextCompilationError(
                "context.bundle_authority_missing",
                "canonical context bundle repository is required",
                metadata={"context_bundle_id": context_bundle_id},
            )
        load_context_bundle_async = getattr(repository, "load_context_bundle_async", None)
        if load_context_bundle_async is not None:
            try:
                snapshot = await load_context_bundle_async(
                    context_bundle_id=context_bundle_id,
                )
            except ContextBundleRepositoryError as exc:
                if exc.reason_code == "context.bundle_unknown":
                    raise ContextCompilationError(
                        "context.bundle_unknown",
                        "missing authoritative context bundle",
                        metadata={
                            "context_bundle_id": context_bundle_id,
                            **exc.details,
                        },
                    ) from exc
                raise ContextCompilationError(
                    "context.bundle_read_failed",
                    "failed to load canonical context bundle facts",
                    metadata={
                        "context_bundle_id": context_bundle_id,
                        "repository_reason_code": exc.reason_code,
                        **exc.details,
                    },
                ) from exc
        else:
            snapshot = self._load_context_bundle_snapshot(
                context_bundle_id=context_bundle_id,
            )
        if not isinstance(snapshot, ContextBundleSnapshot):
            raise ContextCompilationError(
                "context.invalid_authority",
                "context bundle repository returned an invalid snapshot",
                metadata={"context_bundle_id": context_bundle_id},
            )
        return snapshot

    @staticmethod
    def _build_context_bundle(
        *,
        workflow_id: str,
        run_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        bundle_version: int,
        source_decision_refs: Sequence[str],
        resolved_at: datetime,
    ) -> ContextBundle:
        canonical_payload = {
            "bundle_version": bundle_version,
            "run_id": run_id,
            "runtime_profile": {
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            },
            "source_decision_refs": list(source_decision_refs),
            "workspace": {
                "repo_root": workspace.repo_root,
                "workdir": workspace.workdir,
                "workspace_ref": workspace.workspace_ref,
            },
            "workflow_id": workflow_id,
        }
        payload_json = json.dumps(
            canonical_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        bundle_hash = sha256(payload_json.encode("utf-8")).hexdigest()
        return ContextBundle(
            context_bundle_id=ContextCompiler._bundle_id_for_run(run_id=run_id),
            workflow_id=workflow_id,
            run_id=run_id,
            workspace_ref=workspace.workspace_ref,
            runtime_profile_ref=runtime_profile.runtime_profile_ref,
            model_profile_id=runtime_profile.model_profile_id,
            provider_policy_id=runtime_profile.provider_policy_id,
            bundle_version=bundle_version,
            bundle_hash=bundle_hash,
            bundle_payload=canonical_payload,
            source_decision_refs=tuple(source_decision_refs),
            resolved_at=resolved_at,
        )

    @staticmethod
    def _anchors_from_context_records(
        context_records: Sequence[ContextAuthorityRecord],
    ) -> tuple[ContextBundleAnchorRecord, ...]:
        return tuple(
            ContextBundleAnchorRecord(
                anchor_ref=record.context_ref,
                anchor_kind=record.authority_kind,
                content_hash=record.content_hash,
                payload=record.payload,
                position_index=index,
            )
            for index, record in enumerate(context_records)
        )

    @staticmethod
    def _context_records_from_anchors(
        anchors: Sequence[ContextBundleAnchorRecord],
    ) -> tuple[ContextAuthorityRecord, ...]:
        return tuple(
            ContextAuthorityRecord(
                context_ref=anchor.anchor_ref,
                authority_kind=anchor.anchor_kind,
                content_hash=anchor.content_hash,
                payload=anchor.payload,
            )
            for anchor in sorted(anchors, key=lambda item: item.position_index)
        )

    @staticmethod
    def _normalize_bundle_context_refs(
        context_refs: Sequence[str],
    ) -> tuple[str, ...]:
        return _normalize_unique_refs(context_refs, field_name="context_refs")

    @staticmethod
    def _validate_loaded_bundle(
        *,
        bundle: ContextBundle,
        workflow_id: str,
        run_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        bundle_version: int,
        source_decision_refs: Sequence[str],
        context_bundle_id: str,
    ) -> None:
        if bundle.context_bundle_id != context_bundle_id:
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle id does not match the requested bundle id",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "loaded_bundle_id": bundle.context_bundle_id,
                },
            )
        if bundle.workflow_id != workflow_id or bundle.run_id != run_id:
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle does not match the requested run identity",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                },
            )
        if bundle.workspace_ref != workspace.workspace_ref or bundle.runtime_profile_ref != runtime_profile.runtime_profile_ref:
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle does not match the requested workspace/profile",
                metadata={"context_bundle_id": context_bundle_id},
            )
        if (
            bundle.model_profile_id != runtime_profile.model_profile_id
            or bundle.provider_policy_id != runtime_profile.provider_policy_id
        ):
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle does not match the requested runtime profile",
                metadata={"context_bundle_id": context_bundle_id},
            )
        if bundle.bundle_version != bundle_version:
            raise ContextCompilationError(
                "context.bundle_version_invalid",
                "loaded context bundle version does not match the compiler version",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "bundle_version": bundle.bundle_version,
                    "expected_version": bundle_version,
                },
            )
        if tuple(bundle.source_decision_refs) != tuple(source_decision_refs):
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle does not match the requested source decisions",
                metadata={"context_bundle_id": context_bundle_id},
            )
        expected_bundle = ContextCompiler._build_context_bundle(
            workflow_id=workflow_id,
            run_id=run_id,
            workspace=workspace,
            runtime_profile=runtime_profile,
            bundle_version=bundle_version,
            source_decision_refs=source_decision_refs,
            resolved_at=bundle.resolved_at,
        )
        if bundle.bundle_hash != expected_bundle.bundle_hash:
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle hash does not match the canonical bundle hash",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "bundle_hash": bundle.bundle_hash,
                    "expected_bundle_hash": expected_bundle.bundle_hash,
                },
            )
        if bundle.bundle_payload != expected_bundle.bundle_payload:
            raise ContextCompilationError(
                "context.bundle_mismatch",
                "loaded context bundle payload does not match the canonical payload",
                metadata={"context_bundle_id": context_bundle_id},
            )

    @staticmethod
    def _validate_loaded_context_refs(
        *,
        context_bundle_id: str,
        requested_context_refs: tuple[str, ...],
        loaded_context_records: Sequence[ContextAuthorityRecord],
    ) -> None:
        loaded_context_refs = tuple(record.context_ref for record in loaded_context_records)
        if requested_context_refs and requested_context_refs != loaded_context_refs:
            raise ContextCompilationError(
                "context.bundle_anchor_mismatch",
                "requested context refs do not match the stored bundle anchors",
                metadata={
                    "context_bundle_id": context_bundle_id,
                    "requested_context_refs": list(requested_context_refs),
                    "loaded_context_refs": list(loaded_context_refs),
                },
            )
        if not loaded_context_refs:
            raise ContextCompilationError(
                "context.bundle_anchor_missing",
                "stored context bundle does not contain any anchors",
                metadata={"context_bundle_id": context_bundle_id},
            )

    def _validate_route_decision(
        self,
        *,
        runtime_profile: RuntimeProfile,
        route_decision: ModelRouteDecision,
    ) -> None:
        route_allowed_candidates = tuple(route_decision.allowed_candidates)
        if not route_allowed_candidates:
            raise ContextCompilationError(
                "context.route_forged",
                "route decision does not carry any admitted candidates",
                metadata={"selected_candidate_ref": route_decision.selected_candidate_ref},
            )

        admitted_by_ref = {
            candidate.candidate_ref: candidate
            for candidate in route_allowed_candidates
        }
        admitted_candidate = admitted_by_ref.get(route_decision.selected_candidate_ref)
        if admitted_candidate is None:
            raise ContextCompilationError(
                "context.route_forged",
                "route decision selected_candidate_ref is not present in allowed_candidates",
                metadata={"selected_candidate_ref": route_decision.selected_candidate_ref},
            )
        if (
            route_decision.provider_ref,
            route_decision.provider_slug,
            route_decision.model_slug,
        ) != (
            admitted_candidate.provider_ref,
            admitted_candidate.provider_slug,
            admitted_candidate.model_slug,
        ):
            raise ContextCompilationError(
                "context.route_forged",
                "route decision provider/model slugs do not match the admitted candidate",
                metadata={"selected_candidate_ref": route_decision.selected_candidate_ref},
            )

        if self._model_router is None:
            return
        try:
            self._model_router.validate_route_decision(
                runtime_profile=runtime_profile,
                route_decision=route_decision,
            )
        except ModelRoutingError as exc:
            raise ContextCompilationError(
                "context.route_forged",
                "route decision does not match admitted routing authority",
                metadata={
                    "routing_reason_code": exc.reason_code,
                    **exc.metadata,
                },
            ) from exc

    def _build_packet(
        self,
        *,
        workflow_id: str,
        run_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        route_decision: ModelRouteDecision,
        context_bundle_id: str,
        normalized_context_refs: tuple[str, ...],
        normalized_source_decision_refs: tuple[str, ...],
        loaded_snapshot: ContextBundleSnapshot,
    ) -> BoundedContextPacket:
        self._validate_loaded_bundle(
            bundle=loaded_snapshot.bundle,
            workflow_id=workflow_id,
            run_id=run_id,
            workspace=workspace,
            runtime_profile=runtime_profile,
            bundle_version=self._packet_version,
            source_decision_refs=normalized_source_decision_refs,
            context_bundle_id=context_bundle_id,
        )
        loaded_context_records = self._context_records_from_anchors(
            loaded_snapshot.anchors,
        )
        self._validate_loaded_context_refs(
            context_bundle_id=context_bundle_id,
            requested_context_refs=normalized_context_refs,
            loaded_context_records=loaded_context_records,
        )
        context_source_records = loaded_context_records
        canonical_bundle = loaded_snapshot.bundle

        entries: list[CompiledContextEntry] = []
        serialized_entries: list[dict[str, Any]] = []
        for record in context_source_records:
            normalized_payload = _normalize_json_value(
                record.payload,
                context_ref=record.context_ref,
                field_path="payload",
            )
            if not isinstance(normalized_payload, Mapping):
                raise ContextCompilationError(
                    "context.invalid_payload",
                    f"context payload for ref={record.context_ref!r} must be a mapping",
                )
            frozen_payload = _freeze_jsonish(normalized_payload)
            if not isinstance(frozen_payload, Mapping):
                raise ContextCompilationError(
                    "context.invalid_payload",
                    f"context payload for ref={record.context_ref!r} must remain mapping-shaped",
                )
            entry = CompiledContextEntry(
                context_ref=record.context_ref,
                authority_kind=record.authority_kind,
                content_hash=record.content_hash,
                payload=frozen_payload,
            )
            entries.append(entry)
            serialized_entries.append(
                {
                    "context_ref": record.context_ref,
                    "authority_kind": record.authority_kind,
                    "content_hash": record.content_hash,
                    "payload": normalized_payload,
                }
            )

        packet_payload: dict[str, Any] = {
            "context": serialized_entries,
            "context_bundle": {
                "bundle_hash": canonical_bundle.bundle_hash,
                "bundle_version": canonical_bundle.bundle_version,
                "context_bundle_id": canonical_bundle.context_bundle_id,
                "resolved_at": canonical_bundle.resolved_at.isoformat(),
            },
            "model_route": {
                "allowed_candidate_refs": list(route_decision.allowed_candidate_refs),
                "balance_slot": route_decision.balance_slot,
                "decision_reason_code": route_decision.decision_reason_code,
                "model_slug": route_decision.model_slug,
                "provider_ref": route_decision.provider_ref,
                "provider_slug": route_decision.provider_slug,
                "route_decision_id": route_decision.route_decision_id,
                "selected_candidate_ref": route_decision.selected_candidate_ref,
            },
            "packet_version": self._packet_version,
            "run_id": run_id,
            "runtime_profile": {
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            },
            "source_decision_refs": list(normalized_source_decision_refs),
            "workflow_id": workflow_id,
            "workspace": {
                "workspace_ref": workspace.workspace_ref,
            },
        }
        try:
            payload_json = json.dumps(
                packet_payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ContextCompilationError(
                "context.invalid_payload",
                "compiled packet payload is not JSON-serializable",
            ) from exc
        packet_hash = sha256(payload_json.encode("utf-8")).hexdigest()
        packet_id = (
            f"context_packet:{run_id}:{route_decision.selected_candidate_ref}:"
            f"{packet_hash[:12]}"
        )
        frozen_packet_payload = _freeze_jsonish(packet_payload)
        if not isinstance(frozen_packet_payload, Mapping):
            raise ContextCompilationError(
                "context.invalid_payload",
                "compiled packet payload must remain mapping-shaped",
            )
        return BoundedContextPacket(
            context_packet_id=packet_id,
            workflow_id=workflow_id,
            run_id=run_id,
            workspace_ref=workspace.workspace_ref,
            runtime_profile_ref=runtime_profile.runtime_profile_ref,
            model_profile_id=runtime_profile.model_profile_id,
            provider_policy_id=runtime_profile.provider_policy_id,
            route_decision_id=route_decision.route_decision_id,
            selected_candidate_ref=route_decision.selected_candidate_ref,
            provider_ref=route_decision.provider_ref,
            provider_slug=route_decision.provider_slug,
            model_slug=route_decision.model_slug,
            packet_version=self._packet_version,
            packet_hash=packet_hash,
            entries=tuple(entries),
            source_decision_refs=normalized_source_decision_refs,
            compiled_at=self._clock(),
            packet_payload=frozen_packet_payload,
        )

    def compile_packet(
        self,
        *,
        workflow_id: str,
        run_id: str,
        workspace: WorkspaceIdentity,
        runtime_profile: RuntimeProfile,
        route_decision: ModelRouteDecision,
        context_refs: Sequence[str],
        source_decision_refs: Sequence[str] = (),
        context_bundle_id: str | None = None,
    ) -> BoundedContextPacket:
        workflow_id = _require_text(workflow_id, field_name="workflow_id")
        run_id = _require_text(run_id, field_name="run_id")
        context_bundle_id = _require_text(
            context_bundle_id or self._bundle_id_for_run(run_id=run_id),
            field_name="context_bundle_id",
        )
        if self._packet_version < 1:
            raise ContextCompilationError(
                "context.packet_version_invalid",
                f"unsupported context packet version: {self._packet_version}",
            )
        if route_decision.runtime_profile_ref != runtime_profile.runtime_profile_ref:
            raise ContextCompilationError(
                "context.route_mismatch",
                "route decision runtime profile does not match the admitted runtime profile",
                metadata={
                    "route_runtime_profile_ref": route_decision.runtime_profile_ref,
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                },
            )
        if route_decision.model_profile_id != runtime_profile.model_profile_id:
            raise ContextCompilationError(
                "context.route_mismatch",
                "route decision model profile does not match the admitted runtime profile",
            )
        if route_decision.provider_policy_id != runtime_profile.provider_policy_id:
            raise ContextCompilationError(
                "context.route_mismatch",
                "route decision provider policy does not match the admitted runtime profile",
            )
        self._validate_route_decision(
            runtime_profile=runtime_profile,
            route_decision=route_decision,
        )

        normalized_context_refs = _normalize_unique_refs(
            context_refs,
            field_name="context_refs",
        )
        normalized_source_decision_refs = _normalize_unique_refs(
            source_decision_refs,
            field_name="source_decision_refs",
        )
        cache_key = self._packet_cache_key(
            workflow_id=workflow_id,
            run_id=run_id,
            context_bundle_id=context_bundle_id,
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=route_decision,
            normalized_context_refs=normalized_context_refs,
            normalized_source_decision_refs=normalized_source_decision_refs,
        )
        cache = get_context_cache()
        cached_packet = cache.get(cache_key)
        if cached_packet is not None:
            return cached_packet

        loaded_snapshot = self._load_context_bundle_snapshot(
            context_bundle_id=context_bundle_id,
        )
        packet = self._build_packet(
            workflow_id=workflow_id,
            run_id=run_id,
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=route_decision,
            context_bundle_id=context_bundle_id,
            normalized_context_refs=normalized_context_refs,
            normalized_source_decision_refs=normalized_source_decision_refs,
            loaded_snapshot=loaded_snapshot,
        )
        cache.put(cache_key, packet)
        if self._artifact_store is not None:
            try:
                self._artifact_store.record_packet_lineage(
                    packet=dict(packet.packet_payload),
                    authority_refs=list(normalized_source_decision_refs),
                    decision_ref=f"decision.compile.packet.{packet.packet_hash[:16]}",
                    parent_artifact_ref=context_bundle_id,
                )
            except Exception as exc:
                raise ContextCompilationError(
                    "context.packet_artifact_persist_failed",
                    f"failed to persist packet lineage artifact: {exc}",
                ) from exc
        return packet



__all__ = [
    "BoundedContextPacket",
    "CompiledContextEntry",
    "ContextAuthorityRecord",
    "ContextCompilationError",
    "ContextCompiler",
    "_hash_definition",
]
