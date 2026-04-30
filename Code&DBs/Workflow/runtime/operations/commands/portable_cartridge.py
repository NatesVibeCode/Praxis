"""CQRS command for portable cartridge deployment contract authority."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from runtime.cartridge import (
    RuntimeCapabilityProfile,
    canonical_manifest_payload,
    dependency_resolution_plan,
    deployment_mode_contract,
    digest_validation_hooks,
    validate_binding_values,
    validate_deployment_mode,
    validate_portable_cartridge_manifest,
    validate_runtime_compatibility,
)
from storage.postgres.portable_cartridge_repository import persist_portable_cartridge_record


DeploymentMode = Literal[
    "local_verification",
    "staged_deployment",
    "production_deployment",
    "offline_air_gapped",
]


class RuntimeCapabilityProfileInput(BaseModel):
    """Runtime capability profile used to validate one cartridge mount target."""

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
    accelerators: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)

    @field_validator(
        "runtime_api",
        "os",
        "arch",
        "network",
        "filesystem",
        "secrets_policy",
        "max_cpu",
        mode="before",
    )
    @classmethod
    def _normalize_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("runtime capability fields must be non-empty strings")
        return value.strip()

    def to_profile(self) -> RuntimeCapabilityProfile:
        return RuntimeCapabilityProfile(
            runtime_api=self.runtime_api,
            os=self.os,
            arch=self.arch,
            network=self.network,
            filesystem=self.filesystem,
            secrets_policy=self.secrets_policy,
            max_cpu=self.max_cpu,
            max_memory_mb=self.max_memory_mb,
            max_disk_mb=self.max_disk_mb,
            max_duration_s=self.max_duration_s,
            accelerators=tuple(sorted(self.accelerators)),
            capabilities=tuple(sorted(self.capabilities)),
        )


class RecordPortableCartridgeCommand(BaseModel):
    """Validate and persist a portable cartridge deployment contract."""

    manifest: dict[str, Any]
    deployment_mode: DeploymentMode = "staged_deployment"
    runtime_capability_profile: RuntimeCapabilityProfileInput | None = None
    binding_values: dict[str, Any] | None = None
    cartridge_record_id: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None
    require_ready: bool = False

    @field_validator("cartridge_record_id", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()


def handle_record_portable_cartridge(
    command: RecordPortableCartridgeCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    manifest_report = validate_portable_cartridge_manifest(command.manifest)
    if manifest_report.manifest is None:
        reason_codes = [finding.reason_code for finding in manifest_report.findings]
        raise ValueError(f"portable_cartridge.manifest_invalid:{','.join(reason_codes)}")

    manifest = manifest_report.manifest
    findings = list(manifest_report.findings)
    findings.extend(validate_deployment_mode(manifest, command.deployment_mode))
    if command.runtime_capability_profile is not None:
        findings.extend(
            validate_runtime_compatibility(
                manifest,
                command.runtime_capability_profile.to_profile(),
            )
        )
    if command.binding_values is not None:
        findings.extend(validate_binding_values(manifest, command.binding_values))

    validation_report = _validation_report(manifest_report.canonical_digest, findings, manifest)
    readiness_status = "ready" if validation_report["error_count"] == 0 else "blocked"
    if command.require_ready and readiness_status != "ready":
        reason_codes = ",".join(validation_report["reason_codes"])
        raise ValueError(f"portable_cartridge.not_ready:{reason_codes}")

    canonical_manifest = canonical_manifest_payload(manifest)
    deployment_contract = _deployment_contract(
        canonical_manifest=canonical_manifest,
        validation_report=validation_report,
        deployment_mode=command.deployment_mode,
        runtime_capability_profile=command.runtime_capability_profile,
    )
    cartridge_record_id = command.cartridge_record_id or _default_record_id(
        canonical_manifest,
        command.deployment_mode,
    )
    persisted = persist_portable_cartridge_record(
        conn,
        cartridge_record_id=cartridge_record_id,
        manifest=canonical_manifest,
        validation_report=validation_report,
        deployment_contract=deployment_contract,
        readiness_status=readiness_status,
        deployment_mode=command.deployment_mode,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "cartridge_record_id": cartridge_record_id,
        "cartridge_id": manifest.cartridge_id,
        "cartridge_version": manifest.cartridge_version,
        "build_id": manifest.build_id,
        "manifest_digest": validation_report["canonical_digest"],
        "deployment_mode": command.deployment_mode,
        "readiness_status": readiness_status,
        "error_count": validation_report["error_count"],
        "warning_count": validation_report["warning_count"],
        "object_truth_dependency_count": deployment_contract["object_truth_dependency_count"],
        "binding_count": deployment_contract["binding_count"],
        "verifier_check_count": deployment_contract["verifier_check_count"],
        "drift_hook_count": deployment_contract["drift_hook_count"],
        "runtime_sizing_class": deployment_contract["runtime_sizing_class"],
    }
    return {
        "ok": True,
        "operation": "authority.portable_cartridge.record",
        "cartridge_record_id": cartridge_record_id,
        "cartridge_id": manifest.cartridge_id,
        "cartridge_version": manifest.cartridge_version,
        "build_id": manifest.build_id,
        "manifest_digest": validation_report["canonical_digest"],
        "deployment_mode": command.deployment_mode,
        "readiness_status": readiness_status,
        "validation_report": validation_report,
        "deployment_contract": deployment_contract,
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _validation_report(canonical_digest: str | None, findings: list[Any], manifest: Any) -> dict[str, Any]:
    error_count = sum(1 for finding in findings if finding.severity == "error")
    warning_count = sum(1 for finding in findings if finding.severity == "warning")
    reason_codes = sorted({finding.reason_code for finding in findings})
    return {
        "ok": error_count == 0,
        "error_count": error_count,
        "warning_count": warning_count,
        "canonical_digest": canonical_digest,
        "reason_codes": reason_codes,
        "findings": [finding.to_dict() for finding in findings],
        "resolution_order": [step.to_dict() for step in dependency_resolution_plan(manifest)],
    }


def _deployment_contract(
    *,
    canonical_manifest: dict[str, Any],
    validation_report: dict[str, Any],
    deployment_mode: str,
    runtime_capability_profile: RuntimeCapabilityProfileInput | None,
) -> dict[str, Any]:
    contract = deployment_mode_contract(deployment_mode)
    object_truth_dependencies = _object_truth_dependencies(canonical_manifest)
    bindings = list(canonical_manifest.get("bindings") or [])
    verifier_checks = list((canonical_manifest.get("verification") or {}).get("required_checks") or [])
    drift_hooks = list((canonical_manifest.get("audit") or {}).get("drift_hooks") or [])
    compute = dict(canonical_manifest.get("compute") or {})
    runtime_sizing_class = _runtime_sizing_class(compute)
    mode_contract = asdict(contract)
    mode_contract["required_verifier_categories"] = list(contract.required_verifier_categories)
    return {
        "schema": "portable_cartridge.deployment_contract.v1",
        "cartridge_id": canonical_manifest["cartridge_id"],
        "cartridge_version": canonical_manifest["cartridge_version"],
        "build_id": canonical_manifest["build_id"],
        "manifest_version": canonical_manifest["manifest_version"],
        "manifest_digest": validation_report["canonical_digest"],
        "deployment_mode": deployment_mode,
        "mode_contract": mode_contract,
        "readiness_status": "ready" if validation_report["error_count"] == 0 else "blocked",
        "error_count": validation_report["error_count"],
        "warning_count": validation_report["warning_count"],
        "reason_codes": list(validation_report["reason_codes"]),
        "resolution_order": list(validation_report["resolution_order"]),
        "object_truth_dependency_count": len(object_truth_dependencies),
        "object_truth_dependencies": object_truth_dependencies,
        "asset_count": len(canonical_manifest.get("assets") or []),
        "assets": list(canonical_manifest.get("assets") or []),
        "binding_count": len(bindings),
        "required_bindings": [binding for binding in bindings if binding.get("required")],
        "verifier_check_count": len(verifier_checks),
        "verifier_checks": verifier_checks,
        "drift_hook_count": len(drift_hooks),
        "drift_hooks": drift_hooks,
        "digest_validation_hooks": _digest_hooks(canonical_manifest),
        "runtime_sizing_class": runtime_sizing_class,
        "compute": compute,
        "runtime_assumptions": dict(canonical_manifest.get("runtime") or {}),
        "runtime_capability_profile": (
            runtime_capability_profile.model_dump() if runtime_capability_profile is not None else None
        ),
    }


def _object_truth_dependencies(canonical_manifest: dict[str, Any]) -> list[dict[str, Any]]:
    object_truth = dict(canonical_manifest.get("object_truth") or {})
    dependencies: list[dict[str, Any]] = []
    for dependency_class in ("primary", "optional", "derived"):
        for item in object_truth.get(dependency_class) or []:
            dependencies.append({**dict(item), "dependency_class": dependency_class})
    return dependencies


def _runtime_sizing_class(compute: dict[str, Any]) -> str:
    if compute.get("accelerator"):
        return "accelerated"
    cpu = _cpu_units(str(compute.get("cpu") or "0"))
    memory_mb = int(compute.get("memory_mb") or 0)
    expected_duration_s = int(compute.get("expected_duration_s") or 0)
    if cpu <= 1 and memory_mb <= 2048 and expected_duration_s <= 600:
        return "small"
    if cpu <= 2 and memory_mb <= 8192 and expected_duration_s <= 1800:
        return "medium"
    return "large"


def _cpu_units(value: str) -> float:
    cleaned = value.strip().lower().removesuffix("vcpu").removesuffix("cpu").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _digest_hooks(canonical_manifest: dict[str, Any]) -> list[dict[str, Any]]:
    parsed = validate_portable_cartridge_manifest(canonical_manifest)
    if parsed.manifest is None:
        return []
    return [hook.to_dict() for hook in digest_validation_hooks(parsed.manifest)]


def _default_record_id(canonical_manifest: dict[str, Any], deployment_mode: str) -> str:
    return (
        "portable_cartridge_record."
        f"{canonical_manifest['cartridge_id']}."
        f"{canonical_manifest['build_id']}."
        f"{deployment_mode}"
    )


__all__ = [
    "RecordPortableCartridgeCommand",
    "RuntimeCapabilityProfileInput",
    "handle_record_portable_cartridge",
]
