"""Pure portable-cartridge contract primitives.

This module validates the deployable cartridge contract only. It does not
load packages, resolve external systems, execute tasks, persist state, or
decide deployment policy outside the manifest surface it is handed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Literal

from runtime.crypto_authority import canonical_json, digest_bytes_hex


SUPPORTED_MANIFEST_VERSION = "1.0"

REQUIRED_TOP_LEVEL_FIELDS = (
    "manifest_version",
    "cartridge_id",
    "cartridge_version",
    "build_id",
    "created_at",
    "producer",
    "compatibility",
    "entrypoints",
    "object_truth",
    "assets",
    "bindings",
    "runtime",
    "compute",
    "verification",
    "audit",
    "signatures",
)

LIFECYCLE_ENTRYPOINTS = ("load", "execute", "verify", "retire")
OBJECT_TRUTH_CLASSES = ("primary", "optional", "derived")
RESOLUTION_ORDER = (
    "manifest_schema",
    "cartridge_integrity",
    "primary_truth",
    "required_bindings",
    "optional_truth",
    "derived_truth",
    "verifier_suite",
    "runtime_mount",
)

SUPPORTED_BINDING_KINDS = frozenset(
    {
        "object_reference",
        "secret_reference",
        "service_endpoint",
        "queue_topic",
        "model_handle",
        "policy_handle",
    }
)
SUPPORTED_BINDING_PHASES = frozenset(
    {
        "pre_load",
        "pre_execute",
        "pre_verify",
        "post_execute",
        "retire",
    }
)

SUPPORTED_TRUTH_FAILURE_POLICIES = frozenset(
    {
        "fail_closed",
        "warn_and_continue",
        "recompute_then_validate",
        "fallback_to_pinned",
    }
)

REQUIRED_VERIFIER_CATEGORIES = (
    "schema",
    "integrity",
    "compatibility",
    "dependency",
    "binding",
    "runtime_policy",
    "compute",
    "drift",
    "smoke",
)
VERIFIER_REASON_FAMILIES = {
    "schema": "SCHEMA_",
    "integrity": "INTEGRITY_",
    "compatibility": "COMPAT_",
    "dependency": "DEPENDENCY_",
    "binding": "BINDING_",
    "runtime_policy": "RUNTIME_",
    "compute": "COMPUTE_",
    "drift": "DRIFT_",
    "smoke": "SMOKE_",
}

DRIFT_HOOK_POINTS = (
    "build_time",
    "load_time",
    "execute_time",
    "post_run",
    "periodic_runtime",
)
DRIFT_DIMENSIONS = frozenset(
    {
        "manifest",
        "dependency",
        "binding",
        "policy",
        "compute",
        "runtime_capability",
        "output_lineage",
    }
)

RUNTIME_NETWORK_POLICIES = frozenset({"none", "restricted", "declared"})
RUNTIME_FILESYSTEM_POLICIES = frozenset(
    {"read-only", "read-mostly", "scratch-only", "declared-writes"}
)
_RUNTIME_FILESYSTEM_ALIASES = {
    "read_only": "read-only",
    "read_mostly": "read-mostly",
    "scratch_only": "scratch-only",
    "declared_writes": "declared-writes",
}
RUNTIME_SECRET_POLICIES = frozenset({"none", "injected-at-runtime", "external-reference"})

DEPLOYMENT_MODES = (
    "local_verification",
    "staged_deployment",
    "production_deployment",
    "offline_air_gapped",
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,126}[A-Za-z0-9]$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$", re.IGNORECASE)
_VERSION_PART_RE = re.compile(r"^(>=|<=|>|<|==)?\s*([0-9]+(?:\.[0-9]+)*)$")
_SHELL_PATH_TOKENS = (";", "|", "&&", "$(", "`")

Severity = Literal["error", "warning"]


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    severity: Severity
    category: str
    reason_code: str
    message: str
    path: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "category": self.category,
            "reason_code": self.reason_code,
            "message": self.message,
            "path": self.path,
        }
        if self.details:
            payload["details"] = dict(self.details)
        return payload


@dataclass(frozen=True, slots=True)
class ProducerInfo:
    name: str
    version: str

    def to_contract(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version}


@dataclass(frozen=True, slots=True)
class CompatibilityProfile:
    runtime_api: str
    os: tuple[str, ...]
    arch: tuple[str, ...]
    capabilities: tuple[str, ...] = ()

    def to_contract(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "runtime_api": self.runtime_api,
            "os": list(self.os),
            "arch": list(self.arch),
        }
        if self.capabilities:
            payload["capabilities"] = list(self.capabilities)
        return payload


@dataclass(frozen=True, slots=True)
class AssetRecord:
    path: str
    role: str
    media_type: str
    size_bytes: int
    digest: str
    executable: bool
    required: bool

    def to_contract(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "role": self.role,
            "media_type": self.media_type,
            "size_bytes": self.size_bytes,
            "digest": self.digest,
            "executable": self.executable,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class BindingRecord:
    binding_id: str
    kind: str
    required: bool
    resolution_phase: str
    source: str
    target: str
    contract_ref: str

    def to_contract(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "kind": self.kind,
            "required": self.required,
            "resolution_phase": self.resolution_phase,
            "source": self.source,
            "target": self.target,
            "contract_ref": self.contract_ref,
        }


@dataclass(frozen=True, slots=True)
class TruthParentRef:
    dependency_id: str | None
    digest: str

    def to_contract(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"digest": self.digest}
        if self.dependency_id:
            payload["dependency_id"] = self.dependency_id
        return payload


@dataclass(frozen=True, slots=True)
class ObjectTruthDependency:
    dependency_id: str
    dependency_class: str
    authority_source: str
    freshness_policy: Mapping[str, Any]
    failure_policy: str
    object_ref: str | None = None
    version: str | None = None
    digest: str | None = None
    required: bool = False
    parents: tuple[TruthParentRef, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def resolution_sort_key(self) -> tuple[int, str]:
        return (OBJECT_TRUTH_CLASSES.index(self.dependency_class), self.dependency_id)

    def to_contract(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dependency_id": self.dependency_id,
            "authority_source": self.authority_source,
            "freshness_policy": dict(self.freshness_policy),
            "failure_policy": self.failure_policy,
            "required": self.required,
        }
        if self.object_ref:
            payload["object_ref"] = self.object_ref
        if self.version:
            payload["version"] = self.version
        if self.digest:
            payload["digest"] = self.digest
        if self.parents:
            payload["parents"] = [parent.to_contract() for parent in self.parents]
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True, slots=True)
class RuntimeAssumptions:
    env: Mapping[str, Any]
    network: str
    filesystem: str
    secrets_policy: str
    privileges: str = "unprivileged"

    def to_contract(self) -> dict[str, Any]:
        payload = {
            "env": dict(self.env),
            "network": self.network,
            "filesystem": self.filesystem,
            "secrets_policy": self.secrets_policy,
        }
        if self.privileges != "unprivileged":
            payload["privileges"] = self.privileges
        return payload


@dataclass(frozen=True, slots=True)
class ComputeProfile:
    cpu: str
    memory_mb: int
    disk_mb: int
    accelerator: str | None
    expected_duration_s: int
    peak_concurrency: int = 1
    burst_tolerance: str = "none"

    def cpu_units(self) -> float:
        return _cpu_units(self.cpu)

    def sizing_class(self) -> str:
        if self.accelerator:
            return "accelerated"
        cpu = self.cpu_units()
        if cpu <= 1 and self.memory_mb <= 2048 and self.expected_duration_s <= 600:
            return "small"
        if cpu <= 2 and self.memory_mb <= 8192 and self.expected_duration_s <= 1800:
            return "medium"
        return "large"

    def to_contract(self) -> dict[str, Any]:
        return {
            "cpu": self.cpu,
            "memory_mb": self.memory_mb,
            "disk_mb": self.disk_mb,
            "accelerator": self.accelerator,
            "expected_duration_s": self.expected_duration_s,
            "peak_concurrency": self.peak_concurrency,
            "burst_tolerance": self.burst_tolerance,
        }


@dataclass(frozen=True, slots=True)
class VerifierCheck:
    check_id: str
    category: str
    required: bool = True
    contract_ref: str | None = None
    entrypoint: str | None = None
    reason_code_family: str | None = None

    def resolved_reason_code_family(self) -> str:
        return self.reason_code_family or VERIFIER_REASON_FAMILIES[self.category]

    def to_contract(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "check_id": self.check_id,
            "category": self.category,
            "required": self.required,
            "reason_code_family": self.resolved_reason_code_family(),
        }
        if self.contract_ref:
            payload["contract_ref"] = self.contract_ref
        if self.entrypoint:
            payload["entrypoint"] = self.entrypoint
        return payload


@dataclass(frozen=True, slots=True)
class VerifierSuite:
    suite_version: str
    required_checks: tuple[VerifierCheck, ...]

    def required_categories(self) -> tuple[str, ...]:
        return tuple(sorted({check.category for check in self.required_checks if check.required}))

    def to_contract(self) -> dict[str, Any]:
        return {
            "suite_version": self.suite_version,
            "required_checks": [
                check.to_contract() for check in sorted(self.required_checks, key=lambda item: item.check_id)
            ],
        }


@dataclass(frozen=True, slots=True)
class DriftHookRef:
    hook_id: str
    hook_point: str
    drift_dimensions: tuple[str, ...]
    evidence_contract_ref: str
    required: bool = True

    def to_contract(self) -> dict[str, Any]:
        return {
            "hook_id": self.hook_id,
            "hook_point": self.hook_point,
            "drift_dimensions": list(self.drift_dimensions),
            "evidence_contract_ref": self.evidence_contract_ref,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class AuditContract:
    content_digest: str
    dependency_digests: tuple[str, ...]
    drift_hooks: tuple[DriftHookRef, ...]

    def to_contract(self) -> dict[str, Any]:
        return {
            "content_digest": self.content_digest,
            "dependency_digests": list(self.dependency_digests),
            "drift_hooks": [
                hook.to_contract() for hook in sorted(self.drift_hooks, key=lambda item: item.hook_id)
            ],
        }


@dataclass(frozen=True, slots=True)
class SignatureRecord:
    signer: str
    signature: str
    algorithm: str
    key_id: str | None = None
    certificate_ref: str | None = None

    def identity(self) -> str:
        return self.key_id or self.signer

    def to_contract(self) -> dict[str, Any]:
        payload = {
            "signer": self.signer,
            "algorithm": self.algorithm,
            "signature": self.signature,
        }
        if self.key_id:
            payload["key_id"] = self.key_id
        if self.certificate_ref:
            payload["certificate_ref"] = self.certificate_ref
        return payload


@dataclass(frozen=True, slots=True)
class PortableCartridgeManifest:
    manifest_version: str
    cartridge_id: str
    cartridge_version: str
    build_id: str
    created_at: str
    producer: ProducerInfo
    compatibility: CompatibilityProfile
    entrypoints: Mapping[str, str]
    object_truth: Mapping[str, tuple[ObjectTruthDependency, ...]]
    assets: tuple[AssetRecord, ...]
    bindings: tuple[BindingRecord, ...]
    runtime: RuntimeAssumptions
    compute: ComputeProfile
    verification: VerifierSuite
    audit: AuditContract
    signatures: tuple[SignatureRecord, ...]

    def dependencies(self, dependency_class: str | None = None) -> tuple[ObjectTruthDependency, ...]:
        if dependency_class:
            return tuple(self.object_truth.get(dependency_class, ()))
        deps: list[ObjectTruthDependency] = []
        for class_name in OBJECT_TRUTH_CLASSES:
            deps.extend(self.object_truth.get(class_name, ()))
        return tuple(sorted(deps, key=lambda item: item.resolution_sort_key()))

    def to_contract(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "cartridge_id": self.cartridge_id,
            "cartridge_version": self.cartridge_version,
            "build_id": self.build_id,
            "created_at": self.created_at,
            "producer": self.producer.to_contract(),
            "compatibility": self.compatibility.to_contract(),
            "entrypoints": {key: self.entrypoints[key] for key in LIFECYCLE_ENTRYPOINTS},
            "object_truth": {
                class_name: [
                    dep.to_contract()
                    for dep in sorted(self.object_truth.get(class_name, ()), key=lambda item: item.dependency_id)
                ]
                for class_name in OBJECT_TRUTH_CLASSES
            },
            "assets": [asset.to_contract() for asset in sorted(self.assets, key=lambda item: item.path)],
            "bindings": [
                binding.to_contract() for binding in sorted(self.bindings, key=lambda item: item.binding_id)
            ],
            "runtime": self.runtime.to_contract(),
            "compute": self.compute.to_contract(),
            "verification": self.verification.to_contract(),
            "audit": self.audit.to_contract(),
            "signatures": [
                signature.to_contract()
                for signature in sorted(self.signatures, key=lambda item: item.identity())
            ],
        }

    def canonical_json(self) -> str:
        return canonical_json(self.to_contract())

    def canonical_digest(self) -> str:
        digest = digest_bytes_hex(
            self.canonical_json().encode("utf-8"),
            purpose="portable_cartridge.manifest_digest",
        )
        return f"sha256:{digest}"


@dataclass(frozen=True, slots=True)
class ManifestValidationReport:
    manifest: PortableCartridgeManifest | None
    findings: tuple[ValidationFinding, ...]
    canonical_digest: str | None = None

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": self.ok,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "canonical_digest": self.canonical_digest,
            "findings": [finding.to_dict() for finding in self.findings],
        }
        if self.manifest is not None:
            payload["resolution_order"] = [step.to_dict() for step in dependency_resolution_plan(self.manifest)]
        return payload


@dataclass(frozen=True, slots=True)
class ResolutionStep:
    order: int
    phase: str
    refs: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"order": self.order, "phase": self.phase, "refs": list(self.refs)}


@dataclass(frozen=True, slots=True)
class DigestValidationHook:
    hook_id: str
    target_kind: str
    target_ref: str
    expected_digest: str
    required: bool = True
    algorithm: str = "sha256"

    def to_dict(self) -> dict[str, Any]:
        return {
            "hook_id": self.hook_id,
            "target_kind": self.target_kind,
            "target_ref": self.target_ref,
            "expected_digest": self.expected_digest,
            "required": self.required,
            "algorithm": self.algorithm,
        }


@dataclass(frozen=True, slots=True)
class RuntimeCapabilityProfile:
    runtime_api: str
    os: str
    arch: str
    network: str
    filesystem: str
    secrets_policy: str
    max_cpu: str
    max_memory_mb: int
    max_disk_mb: int
    max_duration_s: int
    accelerators: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeploymentModeContract:
    mode: str
    require_signatures: bool
    require_drift_hooks: bool
    allow_mock_truth: bool
    require_authoritative_primary: bool
    offline: bool
    required_verifier_categories: tuple[str, ...] = REQUIRED_VERIFIER_CATEGORIES


def validate_portable_cartridge_manifest(payload: Mapping[str, Any]) -> ManifestValidationReport:
    findings: list[ValidationFinding] = []
    if not isinstance(payload, Mapping):
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_MANIFEST_NOT_OBJECT",
            "manifest must be a JSON object",
            "$",
            value_type=type(payload).__name__,
        )
        return ManifestValidationReport(manifest=None, findings=tuple(findings), canonical_digest=None)

    raw = dict(payload)
    for field_name in REQUIRED_TOP_LEVEL_FIELDS:
        if field_name not in raw:
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_REQUIRED_FIELD_MISSING",
                f"{field_name} is required",
                f"$.{field_name}",
            )

    manifest_version = _text(raw.get("manifest_version"), "$.manifest_version", findings)
    if manifest_version and manifest_version != SUPPORTED_MANIFEST_VERSION:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_MANIFEST_VERSION_UNSUPPORTED",
            f"unsupported manifest_version {manifest_version!r}",
            "$.manifest_version",
            supported=SUPPORTED_MANIFEST_VERSION,
        )
    cartridge_id = _identifier(raw.get("cartridge_id"), "$.cartridge_id", findings)
    cartridge_version = _text(raw.get("cartridge_version"), "$.cartridge_version", findings)
    build_id = _identifier(raw.get("build_id"), "$.build_id", findings)
    created_at = _rfc3339_utc(raw.get("created_at"), "$.created_at", findings)

    producer = _parse_producer(raw.get("producer"), findings)
    compatibility = _parse_compatibility(raw.get("compatibility"), findings)
    entrypoints = _parse_entrypoints(raw.get("entrypoints"), findings)
    object_truth = _parse_object_truth(raw.get("object_truth"), findings)
    assets = _parse_assets(raw.get("assets"), findings)
    bindings = _parse_bindings(raw.get("bindings"), findings)
    runtime = _parse_runtime(raw.get("runtime"), findings)
    compute = _parse_compute(raw.get("compute"), findings)
    verification = _parse_verification(raw.get("verification"), findings)
    audit = _parse_audit(raw.get("audit"), findings)
    signatures = _parse_signatures(raw.get("signatures"), findings)

    _validate_unique(
        [asset.path for asset in assets],
        "$.assets",
        "path",
        "SCHEMA_ASSET_PATH_DUPLICATE",
        findings,
    )
    _validate_unique(
        [binding.binding_id for binding in bindings],
        "$.bindings",
        "binding_id",
        "BINDING_ID_DUPLICATE",
        findings,
    )
    _validate_unique(
        [dep.dependency_id for dep in _all_dependencies(object_truth)],
        "$.object_truth",
        "dependency_id",
        "DEPENDENCY_ID_DUPLICATE",
        findings,
    )
    _validate_dependency_contracts(object_truth, findings)
    _validate_verifier_coverage(verification, findings)
    _validate_drift_hook_coverage(audit, findings)

    if findings and any(finding.severity == "error" for finding in findings):
        return ManifestValidationReport(manifest=None, findings=tuple(findings), canonical_digest=None)

    manifest = PortableCartridgeManifest(
        manifest_version=manifest_version or SUPPORTED_MANIFEST_VERSION,
        cartridge_id=cartridge_id or "",
        cartridge_version=cartridge_version or "",
        build_id=build_id or "",
        created_at=created_at or "",
        producer=producer,
        compatibility=compatibility,
        entrypoints=entrypoints,
        object_truth=object_truth,
        assets=assets,
        bindings=bindings,
        runtime=runtime,
        compute=compute,
        verification=verification,
        audit=audit,
        signatures=signatures,
    )
    return ManifestValidationReport(
        manifest=manifest,
        findings=tuple(findings),
        canonical_digest=manifest.canonical_digest(),
    )


def canonical_manifest_payload(manifest: PortableCartridgeManifest | Mapping[str, Any]) -> dict[str, Any]:
    parsed = manifest if isinstance(manifest, PortableCartridgeManifest) else _require_valid_manifest(manifest)
    return parsed.to_contract()


def canonical_manifest_digest(manifest: PortableCartridgeManifest | Mapping[str, Any]) -> str:
    parsed = manifest if isinstance(manifest, PortableCartridgeManifest) else _require_valid_manifest(manifest)
    return parsed.canonical_digest()


def dependency_resolution_plan(manifest: PortableCartridgeManifest) -> tuple[ResolutionStep, ...]:
    primary = tuple(dep.dependency_id for dep in manifest.dependencies("primary"))
    optional = tuple(dep.dependency_id for dep in manifest.dependencies("optional"))
    derived = tuple(dep.dependency_id for dep in manifest.dependencies("derived"))
    required_bindings = tuple(
        binding.binding_id for binding in sorted(manifest.bindings, key=lambda item: item.binding_id) if binding.required
    )
    verifier_refs = tuple(
        check.check_id
        for check in sorted(manifest.verification.required_checks, key=lambda item: item.check_id)
        if check.required
    )
    integrity_refs = [manifest.audit.content_digest]
    integrity_refs.extend(signature.identity() for signature in manifest.signatures)

    phase_refs = {
        "manifest_schema": (f"manifest_version:{manifest.manifest_version}",),
        "cartridge_integrity": tuple(integrity_refs),
        "primary_truth": primary,
        "required_bindings": required_bindings,
        "optional_truth": optional,
        "derived_truth": derived,
        "verifier_suite": verifier_refs,
        "runtime_mount": (manifest.cartridge_id,),
    }
    return tuple(
        ResolutionStep(order=index + 1, phase=phase, refs=phase_refs[phase])
        for index, phase in enumerate(RESOLUTION_ORDER)
    )


def digest_validation_hooks(manifest: PortableCartridgeManifest) -> tuple[DigestValidationHook, ...]:
    hooks: list[DigestValidationHook] = [
        DigestValidationHook(
            hook_id="digest.cartridge_content",
            target_kind="cartridge_content",
            target_ref=f"{manifest.cartridge_id}:{manifest.build_id}",
            expected_digest=manifest.audit.content_digest,
            required=True,
        )
    ]
    for asset in sorted(manifest.assets, key=lambda item: item.path):
        hooks.append(
            DigestValidationHook(
                hook_id=f"digest.asset.{_slug(asset.path)}",
                target_kind="asset",
                target_ref=asset.path,
                expected_digest=asset.digest,
                required=asset.required,
            )
        )
    for dep in manifest.dependencies():
        if dep.digest:
            hooks.append(
                DigestValidationHook(
                    hook_id=f"digest.object_truth.{_slug(dep.dependency_id)}",
                    target_kind="object_truth_dependency",
                    target_ref=dep.dependency_id,
                    expected_digest=dep.digest,
                    required=dep.required,
                )
            )
    return tuple(hooks)


def validate_digest_payloads(
    hooks: Sequence[DigestValidationHook],
    payloads: Mapping[str, bytes],
    *,
    require_all_payloads: bool = True,
) -> tuple[ValidationFinding, ...]:
    findings: list[ValidationFinding] = []
    for hook in hooks:
        payload = payloads.get(hook.target_ref)
        if payload is None:
            if hook.required and require_all_payloads:
                _finding(
                    findings,
                    "error",
                    "integrity",
                    "INTEGRITY_DIGEST_PAYLOAD_MISSING",
                    "required digest validation payload is missing",
                    f"$.digest_hooks.{hook.hook_id}",
                    target_ref=hook.target_ref,
                    target_kind=hook.target_kind,
                )
            continue
        if not isinstance(payload, bytes):
            _finding(
                findings,
                "error",
                "integrity",
                "INTEGRITY_DIGEST_PAYLOAD_NOT_BYTES",
                "digest validation payload must be bytes",
                f"$.digest_hooks.{hook.hook_id}",
                target_ref=hook.target_ref,
                value_type=type(payload).__name__,
            )
            continue
        observed = f"sha256:{digest_bytes_hex(payload, purpose='portable_cartridge.digest_payload')}"
        if observed != hook.expected_digest:
            _finding(
                findings,
                "error",
                "integrity",
                "INTEGRITY_DIGEST_MISMATCH",
                "digest validation failed",
                f"$.digest_hooks.{hook.hook_id}",
                target_ref=hook.target_ref,
                expected_digest=hook.expected_digest,
                observed_digest=observed,
            )
    return tuple(findings)


def validate_binding_values(
    manifest: PortableCartridgeManifest,
    binding_values: Mapping[str, Any],
) -> tuple[ValidationFinding, ...]:
    findings: list[ValidationFinding] = []
    if not isinstance(binding_values, Mapping):
        _finding(
            findings,
            "error",
            "binding",
            "BINDING_VALUES_NOT_OBJECT",
            "binding values must be a mapping keyed by binding_id",
            "$.bindings",
            value_type=type(binding_values).__name__,
        )
        return tuple(findings)
    declared = {binding.binding_id: binding for binding in manifest.bindings}
    for binding in declared.values():
        if binding.required and binding.binding_id not in binding_values:
            _finding(
                findings,
                "error",
                "binding",
                "BINDING_REQUIRED_MISSING",
                "required binding value is missing",
                f"$.bindings.{binding.binding_id}",
                binding_id=binding.binding_id,
                kind=binding.kind,
            )
    for binding_id in sorted(str(key) for key in binding_values):
        if binding_id not in declared:
            _finding(
                findings,
                "error",
                "binding",
                "BINDING_UNDECLARED",
                "runtime supplied an undeclared binding",
                f"$.binding_values.{binding_id}",
                binding_id=binding_id,
            )
    return tuple(findings)


def deployment_mode_contract(mode: str) -> DeploymentModeContract:
    normalized = str(mode or "").strip()
    if normalized == "local_verification":
        return DeploymentModeContract(
            mode=normalized,
            require_signatures=False,
            require_drift_hooks=False,
            allow_mock_truth=True,
            require_authoritative_primary=False,
            offline=False,
            required_verifier_categories=("schema", "integrity", "compatibility", "dependency", "binding", "smoke"),
        )
    if normalized == "staged_deployment":
        return DeploymentModeContract(
            mode=normalized,
            require_signatures=False,
            require_drift_hooks=True,
            allow_mock_truth=False,
            require_authoritative_primary=True,
            offline=False,
        )
    if normalized == "production_deployment":
        return DeploymentModeContract(
            mode=normalized,
            require_signatures=True,
            require_drift_hooks=True,
            allow_mock_truth=False,
            require_authoritative_primary=True,
            offline=False,
        )
    if normalized == "offline_air_gapped":
        return DeploymentModeContract(
            mode=normalized,
            require_signatures=True,
            require_drift_hooks=True,
            allow_mock_truth=False,
            require_authoritative_primary=True,
            offline=True,
        )
    raise ValueError(f"unsupported deployment mode: {mode!r}")


def validate_deployment_mode(
    manifest: PortableCartridgeManifest,
    mode: str,
) -> tuple[ValidationFinding, ...]:
    contract = deployment_mode_contract(mode)
    findings: list[ValidationFinding] = []
    if contract.require_signatures and not manifest.signatures:
        _finding(
            findings,
            "error",
            "integrity",
            "INTEGRITY_SIGNATURE_REQUIRED",
            f"{contract.mode} requires at least one producer signature",
            "$.signatures",
        )
    if contract.require_drift_hooks and not manifest.audit.drift_hooks:
        _finding(
            findings,
            "error",
            "drift",
            "DRIFT_HOOK_REQUIRED",
            f"{contract.mode} requires drift hooks",
            "$.audit.drift_hooks",
        )
    missing_categories = sorted(set(contract.required_verifier_categories) - set(manifest.verification.required_categories()))
    for category in missing_categories:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_VERIFIER_CATEGORY_MISSING",
            f"{contract.mode} requires verifier category {category}",
            "$.verification.required_checks",
            category=category,
        )
    if contract.require_authoritative_primary:
        for dep in manifest.dependencies("primary"):
            if dep.failure_policy not in {"fail_closed", "fallback_to_pinned"}:
                _finding(
                    findings,
                    "error",
                    "dependency",
                    "DEPENDENCY_PRIMARY_NOT_FAIL_CLOSED",
                    "primary truth dependency must fail closed or use an explicitly pinned fallback",
                    f"$.object_truth.primary.{dep.dependency_id}",
                    dependency_id=dep.dependency_id,
                    failure_policy=dep.failure_policy,
                )
    if contract.offline:
        if manifest.runtime.network not in {"none", "restricted"}:
            _finding(
                findings,
                "error",
                "runtime",
                "RUNTIME_OFFLINE_NETWORK_POLICY_INVALID",
                "offline deployment may not require declared or open network access",
                "$.runtime.network",
                network=manifest.runtime.network,
            )
        for binding in manifest.bindings:
            if binding.kind == "service_endpoint" and binding.required:
                _finding(
                    findings,
                    "error",
                    "runtime",
                    "RUNTIME_OFFLINE_SERVICE_ENDPOINT_REQUIRED",
                    "offline deployment cannot require a live service endpoint binding",
                    f"$.bindings.{binding.binding_id}",
                    binding_id=binding.binding_id,
                )
    return tuple(findings)


def validate_runtime_compatibility(
    manifest: PortableCartridgeManifest,
    profile: RuntimeCapabilityProfile,
) -> tuple[ValidationFinding, ...]:
    findings: list[ValidationFinding] = []
    if not _version_satisfies(profile.runtime_api, manifest.compatibility.runtime_api):
        _finding(
            findings,
            "error",
            "compatibility",
            "COMPAT_RUNTIME_API_UNSUPPORTED",
            "runtime API version does not satisfy manifest range",
            "$.compatibility.runtime_api",
            required=manifest.compatibility.runtime_api,
            observed=profile.runtime_api,
        )
    if profile.os not in manifest.compatibility.os:
        _finding(
            findings,
            "error",
            "compatibility",
            "COMPAT_OS_UNSUPPORTED",
            "runtime OS is not admitted by manifest",
            "$.compatibility.os",
            required=list(manifest.compatibility.os),
            observed=profile.os,
        )
    if profile.arch not in manifest.compatibility.arch:
        _finding(
            findings,
            "error",
            "compatibility",
            "COMPAT_ARCH_UNSUPPORTED",
            "runtime architecture is not admitted by manifest",
            "$.compatibility.arch",
            required=list(manifest.compatibility.arch),
            observed=profile.arch,
        )
    missing_capabilities = sorted(set(manifest.compatibility.capabilities) - set(profile.capabilities))
    if missing_capabilities:
        _finding(
            findings,
            "error",
            "compatibility",
            "COMPAT_CAPABILITY_MISSING",
            "runtime is missing required capability declarations",
            "$.compatibility.capabilities",
            missing=missing_capabilities,
        )
    if profile.network != manifest.runtime.network:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_NETWORK_POLICY_MISMATCH",
            "runtime network policy does not match manifest assumption",
            "$.runtime.network",
            required=manifest.runtime.network,
            observed=profile.network,
        )
    if _canonical_filesystem_policy(profile.filesystem) != manifest.runtime.filesystem:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_FILESYSTEM_POLICY_MISMATCH",
            "runtime filesystem policy does not match manifest assumption",
            "$.runtime.filesystem",
            required=manifest.runtime.filesystem,
            observed=profile.filesystem,
        )
    if profile.secrets_policy != manifest.runtime.secrets_policy:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_SECRETS_POLICY_MISMATCH",
            "runtime secrets policy does not match manifest assumption",
            "$.runtime.secrets_policy",
            required=manifest.runtime.secrets_policy,
            observed=profile.secrets_policy,
        )
    if _cpu_units(profile.max_cpu) < manifest.compute.cpu_units():
        _finding(
            findings,
            "error",
            "compute",
            "COMPUTE_CPU_UNDERPROVISIONED",
            "runtime CPU class is below manifest floor",
            "$.compute.cpu",
            required=manifest.compute.cpu,
            observed=profile.max_cpu,
        )
    if profile.max_memory_mb < manifest.compute.memory_mb:
        _finding(
            findings,
            "error",
            "compute",
            "COMPUTE_MEMORY_UNDERPROVISIONED",
            "runtime memory class is below manifest floor",
            "$.compute.memory_mb",
            required=manifest.compute.memory_mb,
            observed=profile.max_memory_mb,
        )
    if profile.max_disk_mb < manifest.compute.disk_mb:
        _finding(
            findings,
            "error",
            "compute",
            "COMPUTE_DISK_UNDERPROVISIONED",
            "runtime disk class is below manifest floor",
            "$.compute.disk_mb",
            required=manifest.compute.disk_mb,
            observed=profile.max_disk_mb,
        )
    if profile.max_duration_s < manifest.compute.expected_duration_s:
        _finding(
            findings,
            "error",
            "compute",
            "COMPUTE_DURATION_UNDERPROVISIONED",
            "runtime duration limit is below manifest expectation",
            "$.compute.expected_duration_s",
            required=manifest.compute.expected_duration_s,
            observed=profile.max_duration_s,
        )
    if manifest.compute.accelerator and manifest.compute.accelerator not in profile.accelerators:
        _finding(
            findings,
            "error",
            "compute",
            "COMPUTE_ACCELERATOR_UNAVAILABLE",
            "runtime does not expose the requested accelerator",
            "$.compute.accelerator",
            required=manifest.compute.accelerator,
            observed=list(profile.accelerators),
        )
    return tuple(findings)


def _parse_producer(value: Any, findings: list[ValidationFinding]) -> ProducerInfo:
    mapping = _mapping(value, "$.producer", findings)
    name = _text(mapping.get("name"), "$.producer.name", findings)
    version = _text(mapping.get("version"), "$.producer.version", findings)
    return ProducerInfo(name=name or "", version=version or "")


def _parse_compatibility(value: Any, findings: list[ValidationFinding]) -> CompatibilityProfile:
    mapping = _mapping(value, "$.compatibility", findings)
    runtime_api = _text(mapping.get("runtime_api"), "$.compatibility.runtime_api", findings)
    os_values = _text_tuple(mapping.get("os"), "$.compatibility.os", findings, require_non_empty=True)
    arch_values = _text_tuple(mapping.get("arch"), "$.compatibility.arch", findings, require_non_empty=True)
    capabilities = _text_tuple(
        mapping.get("capabilities", ()),
        "$.compatibility.capabilities",
        findings,
        require_non_empty=False,
    )
    return CompatibilityProfile(
        runtime_api=runtime_api or "",
        os=tuple(sorted(os_values)),
        arch=tuple(sorted(arch_values)),
        capabilities=tuple(sorted(capabilities)),
    )


def _parse_entrypoints(value: Any, findings: list[ValidationFinding]) -> Mapping[str, str]:
    mapping = _mapping(value, "$.entrypoints", findings)
    entrypoints: dict[str, str] = {}
    for key in LIFECYCLE_ENTRYPOINTS:
        raw = mapping.get(key)
        text = _text(raw, f"$.entrypoints.{key}", findings)
        if text and not _is_symbolic_ref(text):
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_ENTRYPOINT_NOT_SYMBOLIC",
                "entrypoints must be symbolic task contract identifiers, not environment paths",
                f"$.entrypoints.{key}",
                value=text,
            )
        entrypoints[key] = text or ""
    return entrypoints


def _parse_object_truth(value: Any, findings: list[ValidationFinding]) -> Mapping[str, tuple[ObjectTruthDependency, ...]]:
    mapping = _mapping(value, "$.object_truth", findings)
    parsed: dict[str, tuple[ObjectTruthDependency, ...]] = {}
    for class_name in OBJECT_TRUTH_CLASSES:
        raw_items = mapping.get(class_name)
        if raw_items is None:
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_OBJECT_TRUTH_CLASS_MISSING",
                f"object_truth.{class_name} must be declared",
                f"$.object_truth.{class_name}",
            )
            parsed[class_name] = ()
            continue
        items = _sequence(raw_items, f"$.object_truth.{class_name}", findings)
        deps: list[ObjectTruthDependency] = []
        for index, raw in enumerate(items):
            dep = _parse_dependency(raw, class_name, f"$.object_truth.{class_name}[{index}]", findings)
            if dep:
                deps.append(dep)
        parsed[class_name] = tuple(sorted(deps, key=lambda item: item.dependency_id))
    return parsed


def _parse_dependency(
    value: Any,
    dependency_class: str,
    path: str,
    findings: list[ValidationFinding],
) -> ObjectTruthDependency | None:
    mapping = _mapping(value, path, findings)
    dependency_id = _identifier(mapping.get("dependency_id"), f"{path}.dependency_id", findings)
    authority_source = _text(mapping.get("authority_source"), f"{path}.authority_source", findings)
    object_ref = _optional_text(mapping.get("object_ref"), f"{path}.object_ref", findings)
    version = _optional_text(mapping.get("version"), f"{path}.version", findings)
    digest = _optional_digest(mapping.get("digest"), f"{path}.digest", findings)
    freshness_policy = _mapping(mapping.get("freshness_policy"), f"{path}.freshness_policy", findings)
    failure_policy = _text(mapping.get("failure_policy"), f"{path}.failure_policy", findings)
    if failure_policy and failure_policy not in SUPPORTED_TRUTH_FAILURE_POLICIES:
        _finding(
            findings,
            "error",
            "dependency",
            "DEPENDENCY_FAILURE_POLICY_UNSUPPORTED",
            "truth dependency has unsupported failure_policy",
            f"{path}.failure_policy",
            failure_policy=failure_policy,
            supported=sorted(SUPPORTED_TRUTH_FAILURE_POLICIES),
        )
    if not version and not digest:
        _finding(
            findings,
            "error",
            "dependency",
            "DEPENDENCY_VERSION_OR_DIGEST_REQUIRED",
            "truth dependency must declare version or digest",
            path,
        )
    required = _optional_bool(mapping.get("required"), f"{path}.required", findings)
    if required is None:
        required = dependency_class == "primary"
    raw_parents = mapping.get("parents", ())
    parents = _parse_parents(raw_parents, f"{path}.parents", findings)
    metadata = _optional_mapping(mapping.get("metadata", {}), f"{path}.metadata", findings)
    if dependency_class == "derived" and not parents:
        _finding(
            findings,
            "error",
            "dependency",
            "DEPENDENCY_DERIVED_PARENT_REQUIRED",
            "derived truth objects must declare parent objects by digest",
            f"{path}.parents",
        )
    if dependency_id is None or authority_source is None or failure_policy is None:
        return None
    return ObjectTruthDependency(
        dependency_id=dependency_id,
        dependency_class=dependency_class,
        authority_source=authority_source,
        object_ref=object_ref,
        version=version,
        digest=digest,
        freshness_policy=freshness_policy,
        failure_policy=failure_policy,
        required=required,
        parents=parents,
        metadata=metadata,
    )


def _parse_parents(
    value: Any,
    path: str,
    findings: list[ValidationFinding],
) -> tuple[TruthParentRef, ...]:
    if value in (None, ()):
        return ()
    items = _sequence(value, path, findings)
    parents: list[TruthParentRef] = []
    for index, raw in enumerate(items):
        item_path = f"{path}[{index}]"
        mapping = _mapping(raw, item_path, findings)
        dependency_id = _optional_text(mapping.get("dependency_id"), f"{item_path}.dependency_id", findings)
        digest = _optional_digest(mapping.get("digest"), f"{item_path}.digest", findings)
        if digest is None:
            _finding(
                findings,
                "error",
                "dependency",
                "DEPENDENCY_DERIVED_PARENT_DIGEST_MISSING",
                "derived dependency parent refs must include a digest",
                item_path,
            )
            continue
        parents.append(TruthParentRef(dependency_id=dependency_id, digest=digest))
    return tuple(sorted(parents, key=lambda item: (item.dependency_id or "", item.digest)))


def _parse_assets(value: Any, findings: list[ValidationFinding]) -> tuple[AssetRecord, ...]:
    items = _sequence(value, "$.assets", findings)
    assets: list[AssetRecord] = []
    for index, raw in enumerate(items):
        path = f"$.assets[{index}]"
        mapping = _mapping(raw, path, findings)
        asset_path = _text(mapping.get("path"), f"{path}.path", findings)
        if asset_path and not _is_relative_path(asset_path):
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_ASSET_PATH_NOT_PORTABLE",
                "asset path must be relative and may not traverse directories",
                f"{path}.path",
                value=asset_path,
            )
        role = _text(mapping.get("role"), f"{path}.role", findings)
        media_type = _text(mapping.get("media_type"), f"{path}.media_type", findings)
        size_bytes = _positive_int(mapping.get("size_bytes"), f"{path}.size_bytes", findings)
        digest = _digest(mapping.get("digest"), f"{path}.digest", findings)
        executable = _bool(mapping.get("executable"), f"{path}.executable", findings)
        required = _bool(mapping.get("required"), f"{path}.required", findings)
        if all(item is not None for item in (asset_path, role, media_type, size_bytes, digest, executable, required)):
            assets.append(
                AssetRecord(
                    path=asset_path or "",
                    role=role or "",
                    media_type=media_type or "",
                    size_bytes=size_bytes or 0,
                    digest=digest or "",
                    executable=bool(executable),
                    required=bool(required),
                )
            )
    return tuple(sorted(assets, key=lambda item: item.path))


def _parse_bindings(value: Any, findings: list[ValidationFinding]) -> tuple[BindingRecord, ...]:
    items = _sequence(value, "$.bindings", findings)
    bindings: list[BindingRecord] = []
    for index, raw in enumerate(items):
        path = f"$.bindings[{index}]"
        mapping = _mapping(raw, path, findings)
        binding_id = _identifier(mapping.get("binding_id"), f"{path}.binding_id", findings)
        kind = _text(mapping.get("kind"), f"{path}.kind", findings)
        if kind and kind not in SUPPORTED_BINDING_KINDS:
            _finding(
                findings,
                "error",
                "binding",
                "BINDING_KIND_UNSUPPORTED",
                "binding kind is unsupported",
                f"{path}.kind",
                kind=kind,
                supported=sorted(SUPPORTED_BINDING_KINDS),
            )
        required = _bool(mapping.get("required"), f"{path}.required", findings)
        resolution_phase = _text(mapping.get("resolution_phase"), f"{path}.resolution_phase", findings)
        if resolution_phase and resolution_phase not in SUPPORTED_BINDING_PHASES:
            _finding(
                findings,
                "error",
                "binding",
                "BINDING_RESOLUTION_PHASE_UNSUPPORTED",
                "binding resolution phase is unsupported",
                f"{path}.resolution_phase",
                resolution_phase=resolution_phase,
                supported=sorted(SUPPORTED_BINDING_PHASES),
            )
        source = _text(mapping.get("source"), f"{path}.source", findings)
        target = _text(mapping.get("target"), f"{path}.target", findings)
        contract_ref = _text(mapping.get("contract_ref"), f"{path}.contract_ref", findings)
        if all(item is not None for item in (binding_id, kind, required, resolution_phase, source, target, contract_ref)):
            bindings.append(
                BindingRecord(
                    binding_id=binding_id or "",
                    kind=kind or "",
                    required=bool(required),
                    resolution_phase=resolution_phase or "",
                    source=source or "",
                    target=target or "",
                    contract_ref=contract_ref or "",
                )
            )
    return tuple(sorted(bindings, key=lambda item: item.binding_id))


def _parse_runtime(value: Any, findings: list[ValidationFinding]) -> RuntimeAssumptions:
    mapping = _mapping(value, "$.runtime", findings)
    env = _optional_mapping(mapping.get("env", {}), "$.runtime.env", findings)
    network = _text(mapping.get("network"), "$.runtime.network", findings)
    if network and network not in RUNTIME_NETWORK_POLICIES:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_NETWORK_POLICY_UNSUPPORTED",
            "runtime network policy is unsupported",
            "$.runtime.network",
            network=network,
            supported=sorted(RUNTIME_NETWORK_POLICIES),
        )
    filesystem = _text(mapping.get("filesystem"), "$.runtime.filesystem", findings)
    if filesystem:
        filesystem = _canonical_filesystem_policy(filesystem)
    if filesystem and filesystem not in RUNTIME_FILESYSTEM_POLICIES:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_FILESYSTEM_POLICY_UNSUPPORTED",
            "runtime filesystem policy is unsupported",
            "$.runtime.filesystem",
            filesystem=filesystem,
            supported=sorted(RUNTIME_FILESYSTEM_POLICIES),
        )
    secrets_policy = _text(mapping.get("secrets_policy"), "$.runtime.secrets_policy", findings)
    if secrets_policy and secrets_policy not in RUNTIME_SECRET_POLICIES:
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_SECRETS_POLICY_UNSUPPORTED",
            "runtime secrets policy is unsupported",
            "$.runtime.secrets_policy",
            secrets_policy=secrets_policy,
            supported=sorted(RUNTIME_SECRET_POLICIES),
        )
    privileges = _optional_text(mapping.get("privileges", "unprivileged"), "$.runtime.privileges", findings)
    if privileges == "privileged":
        _finding(
            findings,
            "error",
            "runtime",
            "RUNTIME_PRIVILEGED_UNSUPPORTED",
            "portable cartridges may not assume elevated privileges",
            "$.runtime.privileges",
        )
    return RuntimeAssumptions(
        env=env,
        network=network or "",
        filesystem=filesystem or "",
        secrets_policy=secrets_policy or "",
        privileges=privileges or "unprivileged",
    )


def _parse_compute(value: Any, findings: list[ValidationFinding]) -> ComputeProfile:
    mapping = _mapping(value, "$.compute", findings)
    cpu = _text(mapping.get("cpu"), "$.compute.cpu", findings)
    if cpu:
        try:
            if _cpu_units(cpu) <= 0:
                raise ValueError
        except ValueError:
            _finding(
                findings,
                "error",
                "compute",
                "COMPUTE_CPU_INVALID",
                "cpu must be a positive numeric string",
                "$.compute.cpu",
                value=cpu,
            )
    memory_mb = _positive_int(mapping.get("memory_mb"), "$.compute.memory_mb", findings)
    disk_mb = _positive_int(mapping.get("disk_mb"), "$.compute.disk_mb", findings)
    accelerator = mapping.get("accelerator")
    accelerator_text = None if accelerator is None else _text(accelerator, "$.compute.accelerator", findings)
    expected_duration_s = _positive_int(
        mapping.get("expected_duration_s"),
        "$.compute.expected_duration_s",
        findings,
    )
    peak_concurrency = _positive_int(mapping.get("peak_concurrency", 1), "$.compute.peak_concurrency", findings)
    burst_tolerance = _optional_text(mapping.get("burst_tolerance", "none"), "$.compute.burst_tolerance", findings)
    return ComputeProfile(
        cpu=cpu or "0",
        memory_mb=memory_mb or 0,
        disk_mb=disk_mb or 0,
        accelerator=accelerator_text,
        expected_duration_s=expected_duration_s or 0,
        peak_concurrency=peak_concurrency or 1,
        burst_tolerance=burst_tolerance or "none",
    )


def _parse_verification(value: Any, findings: list[ValidationFinding]) -> VerifierSuite:
    mapping = _mapping(value, "$.verification", findings)
    suite_version = _text(mapping.get("suite_version"), "$.verification.suite_version", findings)
    items = _sequence(mapping.get("required_checks"), "$.verification.required_checks", findings)
    checks: list[VerifierCheck] = []
    for index, raw in enumerate(items):
        path = f"$.verification.required_checks[{index}]"
        check_map = _mapping(raw, path, findings)
        check_id = _identifier(check_map.get("check_id"), f"{path}.check_id", findings)
        category = _text(check_map.get("category"), f"{path}.category", findings)
        if category and category not in REQUIRED_VERIFIER_CATEGORIES:
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_VERIFIER_CATEGORY_UNSUPPORTED",
                "verifier category is unsupported",
                f"{path}.category",
                category=category,
                supported=list(REQUIRED_VERIFIER_CATEGORIES),
            )
        required = _optional_bool(check_map.get("required"), f"{path}.required", findings)
        if required is None:
            required = True
        contract_ref = _optional_text(check_map.get("contract_ref"), f"{path}.contract_ref", findings)
        entrypoint = _optional_text(check_map.get("entrypoint"), f"{path}.entrypoint", findings)
        if entrypoint and not _is_symbolic_ref(entrypoint):
            _finding(
                findings,
                "error",
                "schema",
                "SCHEMA_VERIFIER_ENTRYPOINT_NOT_SYMBOLIC",
                "verifier entrypoint must be symbolic",
                f"{path}.entrypoint",
                entrypoint=entrypoint,
            )
        reason_family = _optional_text(
            check_map.get("reason_code_family"),
            f"{path}.reason_code_family",
            findings,
        )
        if category in VERIFIER_REASON_FAMILIES:
            expected_family = VERIFIER_REASON_FAMILIES[category]
            if reason_family and reason_family != expected_family:
                _finding(
                    findings,
                    "error",
                    "schema",
                    "SCHEMA_VERIFIER_REASON_FAMILY_INVALID",
                    "verifier reason_code_family must match its category",
                    f"{path}.reason_code_family",
                    expected=expected_family,
                    observed=reason_family,
                )
            reason_family = reason_family or expected_family
        if check_id and category in REQUIRED_VERIFIER_CATEGORIES:
            checks.append(
                VerifierCheck(
                    check_id=check_id,
                    category=category,
                    required=required,
                    contract_ref=contract_ref,
                    entrypoint=entrypoint,
                    reason_code_family=reason_family,
                )
            )
    _validate_unique(
        [check.check_id for check in checks],
        "$.verification.required_checks",
        "check_id",
        "SCHEMA_VERIFIER_CHECK_DUPLICATE",
        findings,
    )
    return VerifierSuite(suite_version=suite_version or "", required_checks=tuple(checks))


def _parse_audit(value: Any, findings: list[ValidationFinding]) -> AuditContract:
    mapping = _mapping(value, "$.audit", findings)
    content_digest = _digest(mapping.get("content_digest"), "$.audit.content_digest", findings)
    dependency_digests = tuple(
        digest
        for index, raw_digest in enumerate(
            _sequence(mapping.get("dependency_digests"), "$.audit.dependency_digests", findings)
        )
        if (digest := _digest(raw_digest, f"$.audit.dependency_digests[{index}]", findings))
    )
    hooks = _parse_drift_hooks(mapping.get("drift_hooks"), findings)
    return AuditContract(
        content_digest=content_digest or "",
        dependency_digests=tuple(sorted(dependency_digests)),
        drift_hooks=hooks,
    )


def _parse_drift_hooks(value: Any, findings: list[ValidationFinding]) -> tuple[DriftHookRef, ...]:
    items = _sequence(value, "$.audit.drift_hooks", findings)
    hooks: list[DriftHookRef] = []
    for index, raw in enumerate(items):
        path = f"$.audit.drift_hooks[{index}]"
        mapping = _mapping(raw, path, findings)
        hook_id = _identifier(mapping.get("hook_id"), f"{path}.hook_id", findings)
        hook_point = _text(mapping.get("hook_point"), f"{path}.hook_point", findings)
        if hook_point and hook_point not in DRIFT_HOOK_POINTS:
            _finding(
                findings,
                "error",
                "drift",
                "DRIFT_HOOK_POINT_UNSUPPORTED",
                "drift hook point is unsupported",
                f"{path}.hook_point",
                hook_point=hook_point,
                supported=list(DRIFT_HOOK_POINTS),
            )
        dimensions = _text_tuple(mapping.get("drift_dimensions"), f"{path}.drift_dimensions", findings, require_non_empty=True)
        unsupported = sorted(set(dimensions) - DRIFT_DIMENSIONS)
        if unsupported:
            _finding(
                findings,
                "error",
                "drift",
                "DRIFT_DIMENSION_UNSUPPORTED",
                "drift hook references unsupported dimensions",
                f"{path}.drift_dimensions",
                unsupported=unsupported,
                supported=sorted(DRIFT_DIMENSIONS),
            )
        evidence_ref = _text(mapping.get("evidence_contract_ref"), f"{path}.evidence_contract_ref", findings)
        required = _optional_bool(mapping.get("required"), f"{path}.required", findings)
        if required is None:
            required = True
        if hook_id and hook_point in DRIFT_HOOK_POINTS and evidence_ref:
            hooks.append(
                DriftHookRef(
                    hook_id=hook_id,
                    hook_point=hook_point,
                    drift_dimensions=tuple(sorted(dimensions)),
                    evidence_contract_ref=evidence_ref,
                    required=required,
                )
            )
    _validate_unique(
        [hook.hook_id for hook in hooks],
        "$.audit.drift_hooks",
        "hook_id",
        "DRIFT_HOOK_DUPLICATE",
        findings,
    )
    return tuple(sorted(hooks, key=lambda item: item.hook_id))


def _parse_signatures(value: Any, findings: list[ValidationFinding]) -> tuple[SignatureRecord, ...]:
    items = _sequence(value, "$.signatures", findings)
    signatures: list[SignatureRecord] = []
    for index, raw in enumerate(items):
        path = f"$.signatures[{index}]"
        mapping = _mapping(raw, path, findings)
        signer = _text(mapping.get("signer"), f"{path}.signer", findings)
        signature = _text(mapping.get("signature"), f"{path}.signature", findings)
        algorithm = _text(mapping.get("algorithm"), f"{path}.algorithm", findings)
        key_id = _optional_text(mapping.get("key_id"), f"{path}.key_id", findings)
        certificate_ref = _optional_text(mapping.get("certificate_ref"), f"{path}.certificate_ref", findings)
        if signer and signature and algorithm:
            signatures.append(
                SignatureRecord(
                    signer=signer,
                    signature=signature,
                    algorithm=algorithm,
                    key_id=key_id,
                    certificate_ref=certificate_ref,
                )
            )
    return tuple(sorted(signatures, key=lambda item: item.identity()))


def _validate_dependency_contracts(
    object_truth: Mapping[str, tuple[ObjectTruthDependency, ...]],
    findings: list[ValidationFinding],
) -> None:
    dependency_digests = {
        dep.digest
        for dep in _all_dependencies(object_truth)
        if dep.digest
    }
    dependency_ids = {dep.dependency_id for dep in _all_dependencies(object_truth)}
    for dep in object_truth.get("primary", ()):
        if not dep.required:
            _finding(
                findings,
                "error",
                "dependency",
                "DEPENDENCY_PRIMARY_MUST_BE_REQUIRED",
                "primary truth dependencies are required for correctness",
                f"$.object_truth.primary.{dep.dependency_id}",
                dependency_id=dep.dependency_id,
            )
        if dep.failure_policy not in {"fail_closed", "fallback_to_pinned"}:
            _finding(
                findings,
                "error",
                "dependency",
                "DEPENDENCY_PRIMARY_NOT_FAIL_CLOSED",
                "primary truth dependencies must fail closed or use fallback_to_pinned",
                f"$.object_truth.primary.{dep.dependency_id}",
                dependency_id=dep.dependency_id,
                failure_policy=dep.failure_policy,
            )
    for dep in object_truth.get("optional", ()):
        if dep.failure_policy == "fail_closed" and not dep.required:
            _finding(
                findings,
                "warning",
                "dependency",
                "DEPENDENCY_OPTIONAL_FAIL_CLOSED",
                "optional dependency is fail_closed without being required by policy",
                f"$.object_truth.optional.{dep.dependency_id}",
                dependency_id=dep.dependency_id,
            )
    for dep in object_truth.get("derived", ()):
        for parent in dep.parents:
            if parent.dependency_id and parent.dependency_id not in dependency_ids:
                _finding(
                    findings,
                    "error",
                    "dependency",
                    "DEPENDENCY_DERIVED_PARENT_UNKNOWN",
                    "derived dependency parent_id is not declared in object_truth",
                    f"$.object_truth.derived.{dep.dependency_id}.parents",
                    dependency_id=dep.dependency_id,
                    parent_dependency_id=parent.dependency_id,
                )
            if parent.digest not in dependency_digests:
                _finding(
                    findings,
                    "error",
                    "dependency",
                    "DEPENDENCY_DERIVED_PARENT_DIGEST_UNKNOWN",
                    "derived dependency parent digest is not declared by any dependency",
                    f"$.object_truth.derived.{dep.dependency_id}.parents",
                    dependency_id=dep.dependency_id,
                    parent_digest=parent.digest,
                )


def _validate_verifier_coverage(
    suite: VerifierSuite,
    findings: list[ValidationFinding],
) -> None:
    missing = sorted(set(REQUIRED_VERIFIER_CATEGORIES) - set(suite.required_categories()))
    for category in missing:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_VERIFIER_CATEGORY_MISSING",
            "verifier suite is missing a mandatory category",
            "$.verification.required_checks",
            category=category,
        )


def _validate_drift_hook_coverage(
    audit: AuditContract,
    findings: list[ValidationFinding],
) -> None:
    covered = {hook.hook_point for hook in audit.drift_hooks if hook.required}
    for hook_point in DRIFT_HOOK_POINTS:
        if hook_point not in covered:
            _finding(
                findings,
                "error",
                "drift",
                "DRIFT_HOOK_POINT_MISSING",
                "audit contract is missing a required drift hook point",
                "$.audit.drift_hooks",
                hook_point=hook_point,
            )


def _require_valid_manifest(payload: Mapping[str, Any]) -> PortableCartridgeManifest:
    report = validate_portable_cartridge_manifest(payload)
    if report.manifest is None:
        reason_codes = ", ".join(finding.reason_code for finding in report.findings[:5])
        raise ValueError(f"portable cartridge manifest is invalid: {reason_codes}")
    return report.manifest


def _all_dependencies(object_truth: Mapping[str, tuple[ObjectTruthDependency, ...]]) -> tuple[ObjectTruthDependency, ...]:
    deps: list[ObjectTruthDependency] = []
    for class_name in OBJECT_TRUTH_CLASSES:
        deps.extend(object_truth.get(class_name, ()))
    return tuple(deps)


def _finding(
    findings: list[ValidationFinding],
    severity: Severity,
    category: str,
    reason_code: str,
    message: str,
    path: str,
    **details: Any,
) -> None:
    findings.append(
        ValidationFinding(
            severity=severity,
            category=category,
            reason_code=reason_code,
            message=message,
            path=path,
            details={key: value for key, value in details.items() if value is not None},
        )
    )


def _mapping(value: Any, path: str, findings: list[ValidationFinding]) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    _finding(
        findings,
        "error",
        "schema",
        "SCHEMA_FIELD_NOT_OBJECT",
        "field must be an object",
        path,
        value_type=type(value).__name__,
    )
    return {}


def _optional_mapping(value: Any, path: str, findings: list[ValidationFinding]) -> Mapping[str, Any]:
    if value is None:
        return {}
    return _mapping(value, path, findings)


def _sequence(value: Any, path: str, findings: list[ValidationFinding]) -> tuple[Any, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    _finding(
        findings,
        "error",
        "schema",
        "SCHEMA_FIELD_NOT_LIST",
        "field must be a list",
        path,
        value_type=type(value).__name__,
    )
    return ()


def _text(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    _finding(
        findings,
        "error",
        "schema",
        "SCHEMA_TEXT_REQUIRED",
        "field must be a non-empty string",
        path,
        value_type=type(value).__name__,
    )
    return None


def _optional_text(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    if value is None:
        return None
    return _text(value, path, findings)


def _identifier(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    text = _text(value, path, findings)
    if text and not _IDENTIFIER_RE.match(text):
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_IDENTIFIER_INVALID",
            "identifier must start and end alphanumeric and contain only letters, numbers, dot, underscore, or hyphen",
            path,
            value=text,
        )
        return None
    return text


def _digest(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    text = _text(value, path, findings)
    if text is None:
        return None
    if not _DIGEST_RE.match(text):
        _finding(
            findings,
            "error",
            "integrity",
            "INTEGRITY_DIGEST_INVALID",
            "digest must be sha256:<64 lowercase hex characters>",
            path,
            value=text,
        )
        return None
    return text.lower()


def _optional_digest(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    if value is None:
        return None
    return _digest(value, path, findings)


def _bool(value: Any, path: str, findings: list[ValidationFinding]) -> bool | None:
    if isinstance(value, bool):
        return value
    _finding(
        findings,
        "error",
        "schema",
        "SCHEMA_BOOL_REQUIRED",
        "field must be a boolean",
        path,
        value_type=type(value).__name__,
    )
    return None


def _optional_bool(value: Any, path: str, findings: list[ValidationFinding]) -> bool | None:
    if value is None:
        return None
    return _bool(value, path, findings)


def _positive_int(value: Any, path: str, findings: list[ValidationFinding]) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_POSITIVE_INT_REQUIRED",
            "field must be a positive integer",
            path,
            value_type=type(value).__name__,
            value=value if isinstance(value, int) and not isinstance(value, bool) else None,
        )
        return None
    return value


def _text_tuple(
    value: Any,
    path: str,
    findings: list[ValidationFinding],
    *,
    require_non_empty: bool,
) -> tuple[str, ...]:
    items = _sequence(value, path, findings)
    texts: list[str] = []
    for index, item in enumerate(items):
        text = _text(item, f"{path}[{index}]", findings)
        if text:
            texts.append(text)
    if require_non_empty and not texts:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_LIST_EMPTY",
            "field must contain at least one item",
            path,
        )
    return tuple(texts)


def _rfc3339_utc(value: Any, path: str, findings: list[ValidationFinding]) -> str | None:
    text = _text(value, path, findings)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_RFC3339_UTC_INVALID",
            "created_at must be an RFC 3339 UTC timestamp",
            path,
            value=text,
        )
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        _finding(
            findings,
            "error",
            "schema",
            "SCHEMA_RFC3339_UTC_INVALID",
            "created_at must include UTC timezone",
            path,
            value=text,
        )
        return None
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_symbolic_ref(value: str) -> bool:
    return _is_relative_path(value) and not any(token in value for token in _SHELL_PATH_TOKENS)


def _is_relative_path(value: str) -> bool:
    text = value.strip()
    if not text or text.startswith("/") or "\\" in text or "://" in text:
        return False
    parts = [part for part in text.split("/") if part]
    return all(part not in {".", ".."} for part in parts)


def _validate_unique(
    values: Sequence[str],
    path: str,
    field_name: str,
    reason_code: str,
    findings: list[ValidationFinding],
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    for value in sorted(duplicates):
        _finding(
            findings,
            "error",
            "schema",
            reason_code,
            f"{field_name} values must be unique",
            path,
            value=value,
        )


def _cpu_units(value: str) -> float:
    return float(str(value).strip())


def _canonical_filesystem_policy(value: str) -> str:
    text = str(value or "").strip()
    return _RUNTIME_FILESYSTEM_ALIASES.get(text, text)


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _version_satisfies(observed: str, requirement: str) -> bool:
    try:
        observed_tuple = _version_tuple(observed.strip())
        for part in requirement.split():
            match = _VERSION_PART_RE.match(part)
            if not match:
                return False
            operator = match.group(1) or "=="
            required_tuple = _version_tuple(match.group(2))
            if operator == ">=" and not observed_tuple >= required_tuple:
                return False
            if operator == ">" and not observed_tuple > required_tuple:
                return False
            if operator == "<=" and not observed_tuple <= required_tuple:
                return False
            if operator == "<" and not observed_tuple < required_tuple:
                return False
            if operator == "==" and not observed_tuple == required_tuple:
                return False
    except (AttributeError, ValueError):
        return False
    return True


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return slug.strip("._-")[:80] or "target"


__all__ = [
    "DEPLOYMENT_MODES",
    "DRIFT_DIMENSIONS",
    "DRIFT_HOOK_POINTS",
    "LIFECYCLE_ENTRYPOINTS",
    "OBJECT_TRUTH_CLASSES",
    "REQUIRED_TOP_LEVEL_FIELDS",
    "REQUIRED_VERIFIER_CATEGORIES",
    "RESOLUTION_ORDER",
    "SUPPORTED_MANIFEST_VERSION",
    "AssetRecord",
    "AuditContract",
    "BindingRecord",
    "CompatibilityProfile",
    "ComputeProfile",
    "DeploymentModeContract",
    "DigestValidationHook",
    "DriftHookRef",
    "ManifestValidationReport",
    "ObjectTruthDependency",
    "PortableCartridgeManifest",
    "ProducerInfo",
    "ResolutionStep",
    "RuntimeAssumptions",
    "RuntimeCapabilityProfile",
    "SignatureRecord",
    "TruthParentRef",
    "ValidationFinding",
    "VerifierCheck",
    "VerifierSuite",
    "canonical_manifest_digest",
    "canonical_manifest_payload",
    "dependency_resolution_plan",
    "deployment_mode_contract",
    "digest_validation_hooks",
    "validate_binding_values",
    "validate_deployment_mode",
    "validate_digest_payloads",
    "validate_portable_cartridge_manifest",
    "validate_runtime_compatibility",
]
