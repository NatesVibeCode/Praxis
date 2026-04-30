from __future__ import annotations

from copy import deepcopy
import hashlib

from runtime.cartridge import (
    RESOLUTION_ORDER,
    RuntimeCapabilityProfile,
    canonical_manifest_digest,
    dependency_resolution_plan,
    digest_validation_hooks,
    validate_binding_values,
    validate_deployment_mode,
    validate_digest_payloads,
    validate_portable_cartridge_manifest,
    validate_runtime_compatibility,
)


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _verifier_checks() -> list[dict[str, object]]:
    categories = (
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
    return [
        {
            "check_id": f"check.{category}",
            "category": category,
            "required": True,
            "contract_ref": f"verifier.contract.{category}",
        }
        for category in categories
    ]


def _drift_hooks() -> list[dict[str, object]]:
    return [
        {
            "hook_id": "drift.build",
            "hook_point": "build_time",
            "drift_dimensions": ["manifest", "dependency"],
            "evidence_contract_ref": "audit.evidence.build",
            "required": True,
        },
        {
            "hook_id": "drift.load",
            "hook_point": "load_time",
            "drift_dimensions": ["binding", "runtime_capability"],
            "evidence_contract_ref": "audit.evidence.load",
            "required": True,
        },
        {
            "hook_id": "drift.execute",
            "hook_point": "execute_time",
            "drift_dimensions": ["dependency", "compute", "policy"],
            "evidence_contract_ref": "audit.evidence.execute",
            "required": True,
        },
        {
            "hook_id": "drift.post_run",
            "hook_point": "post_run",
            "drift_dimensions": ["output_lineage"],
            "evidence_contract_ref": "audit.evidence.post_run",
            "required": True,
        },
        {
            "hook_id": "drift.periodic",
            "hook_point": "periodic_runtime",
            "drift_dimensions": ["binding", "policy", "dependency"],
            "evidence_contract_ref": "audit.evidence.periodic",
            "required": True,
        },
    ]


def _valid_manifest_payload() -> dict[str, object]:
    primary_digest = _sha256(b"policy-snapshot")
    optional_digest = _sha256(b"model-card")
    derived_digest = _sha256(b"policy-index")
    return {
        "manifest_version": "1.0",
        "cartridge_id": "phase9-portable-cartridge",
        "cartridge_version": "2026.04.30",
        "build_id": "build_2026_04_30_0001",
        "created_at": "2026-04-30T00:00:00Z",
        "producer": {"name": "phase9-worker", "version": "1.0.0"},
        "compatibility": {
            "runtime_api": ">=1.0 <2.0",
            "os": ["linux"],
            "arch": ["amd64", "arm64"],
            "capabilities": ["object_truth_resolver"],
        },
        "entrypoints": {
            "load": "tasks/load",
            "execute": "tasks/execute",
            "verify": "tasks/verify",
            "retire": "tasks/retire",
        },
        "object_truth": {
            "primary": [
                {
                    "dependency_id": "policy.snapshot",
                    "object_ref": "object_truth.policy_snapshot",
                    "authority_source": "policy_registry",
                    "version": "2026.04.30",
                    "digest": primary_digest,
                    "freshness_policy": {"kind": "pinned", "max_age_s": 3600},
                    "failure_policy": "fail_closed",
                    "required": True,
                }
            ],
            "optional": [
                {
                    "dependency_id": "model.card",
                    "object_ref": "object_truth.model_card",
                    "authority_source": "model_registry",
                    "version": "2026.04.30",
                    "digest": optional_digest,
                    "freshness_policy": {"kind": "pinned"},
                    "failure_policy": "warn_and_continue",
                    "required": False,
                }
            ],
            "derived": [
                {
                    "dependency_id": "policy.index",
                    "object_ref": "object_truth.policy_index",
                    "authority_source": "derived_truth_builder",
                    "version": "2026.04.30",
                    "digest": derived_digest,
                    "freshness_policy": {"kind": "derived_from_parent"},
                    "failure_policy": "recompute_then_validate",
                    "required": True,
                    "parents": [
                        {
                            "dependency_id": "policy.snapshot",
                            "digest": primary_digest,
                        }
                    ],
                }
            ],
        },
        "assets": [
            {
                "path": "assets/workflow.json",
                "role": "workflow_definition",
                "media_type": "application/json",
                "size_bytes": len(b"asset-bytes"),
                "digest": _sha256(b"asset-bytes"),
                "executable": False,
                "required": True,
            }
        ],
        "bindings": [
            {
                "binding_id": "secret.crm_token",
                "kind": "secret_reference",
                "required": True,
                "resolution_phase": "pre_execute",
                "source": "secret_manager",
                "target": "runtime.env.CRM_TOKEN",
                "contract_ref": "binding.contract.secret.crm_token",
            },
            {
                "binding_id": "object.store",
                "kind": "object_reference",
                "required": True,
                "resolution_phase": "pre_load",
                "source": "deployment_controller",
                "target": "object_store.cartridge",
                "contract_ref": "binding.contract.object_store",
            },
        ],
        "runtime": {
            "env": {"CRM_TOKEN": {"source": "binding:secret.crm_token"}},
            "network": "restricted",
            "filesystem": "read-mostly",
            "secrets_policy": "injected-at-runtime",
        },
        "compute": {
            "cpu": "2",
            "memory_mb": 4096,
            "disk_mb": 2048,
            "accelerator": None,
            "expected_duration_s": 300,
            "peak_concurrency": 1,
            "burst_tolerance": "none",
        },
        "verification": {
            "suite_version": "1.0",
            "required_checks": _verifier_checks(),
        },
        "audit": {
            "content_digest": _sha256(b"package-bytes"),
            "dependency_digests": [primary_digest, optional_digest, derived_digest],
            "drift_hooks": _drift_hooks(),
        },
        "signatures": [],
    }


def _reason_codes(findings) -> set[str]:
    return {finding.reason_code for finding in findings}


def test_valid_manifest_has_canonical_digest_and_resolution_order() -> None:
    report = validate_portable_cartridge_manifest(_valid_manifest_payload())

    assert report.ok, [finding.to_dict() for finding in report.findings]
    assert report.manifest is not None
    assert report.canonical_digest == canonical_manifest_digest(report.manifest)
    assert report.manifest.compute.sizing_class() == "medium"

    plan = dependency_resolution_plan(report.manifest)
    assert tuple(step.phase for step in plan) == RESOLUTION_ORDER
    assert plan[2].refs == ("policy.snapshot",)
    assert plan[3].refs == ("object.store", "secret.crm_token")
    assert plan[4].refs == ("model.card",)
    assert plan[5].refs == ("policy.index",)


def test_canonical_digest_is_stable_across_declaration_order() -> None:
    payload = _valid_manifest_payload()
    shuffled = deepcopy(payload)
    shuffled["bindings"] = list(reversed(shuffled["bindings"]))
    shuffled["assets"] = list(reversed(shuffled["assets"]))
    shuffled["verification"]["required_checks"] = list(reversed(shuffled["verification"]["required_checks"]))
    shuffled["audit"]["drift_hooks"] = list(reversed(shuffled["audit"]["drift_hooks"]))

    first = validate_portable_cartridge_manifest(payload)
    second = validate_portable_cartridge_manifest(shuffled)

    assert first.ok
    assert second.ok
    assert first.canonical_digest == second.canonical_digest


def test_manifest_validation_reports_missing_required_fields() -> None:
    payload = _valid_manifest_payload()
    del payload["audit"]

    report = validate_portable_cartridge_manifest(payload)

    assert not report.ok
    assert report.manifest is None
    assert "SCHEMA_REQUIRED_FIELD_MISSING" in _reason_codes(report.findings)


def test_derived_truth_dependencies_must_reference_parent_digest() -> None:
    payload = _valid_manifest_payload()
    del payload["object_truth"]["derived"][0]["parents"][0]["digest"]

    report = validate_portable_cartridge_manifest(payload)

    assert not report.ok
    assert "DEPENDENCY_DERIVED_PARENT_DIGEST_MISSING" in _reason_codes(report.findings)


def test_digest_validation_hook_rejects_asset_mismatch() -> None:
    report = validate_portable_cartridge_manifest(_valid_manifest_payload())
    assert report.manifest is not None
    asset_hooks = [hook for hook in digest_validation_hooks(report.manifest) if hook.target_kind == "asset"]

    findings = validate_digest_payloads(asset_hooks, {"assets/workflow.json": b"wrong-bytes"})

    assert "INTEGRITY_DIGEST_MISMATCH" in _reason_codes(findings)


def test_binding_values_reject_missing_required_and_undeclared_bindings() -> None:
    report = validate_portable_cartridge_manifest(_valid_manifest_payload())
    assert report.manifest is not None

    missing = validate_binding_values(report.manifest, {})
    undeclared = validate_binding_values(
        report.manifest,
        {
            "secret.crm_token": "secret-ref",
            "object.store": "object-store-ref",
            "extra.binding": "nope",
        },
    )

    assert "BINDING_REQUIRED_MISSING" in _reason_codes(missing)
    assert "BINDING_UNDECLARED" in _reason_codes(undeclared)


def test_runtime_compatibility_and_deployment_mode_are_separate_gates() -> None:
    report = validate_portable_cartridge_manifest(_valid_manifest_payload())
    assert report.manifest is not None

    compatible = RuntimeCapabilityProfile(
        runtime_api="1.1",
        os="linux",
        arch="amd64",
        network="restricted",
        filesystem="read_mostly",
        secrets_policy="injected-at-runtime",
        max_cpu="2",
        max_memory_mb=4096,
        max_disk_mb=2048,
        max_duration_s=300,
        capabilities=("object_truth_resolver",),
    )
    underpowered = RuntimeCapabilityProfile(
        runtime_api="1.1",
        os="linux",
        arch="amd64",
        network="restricted",
        filesystem="read-mostly",
        secrets_policy="injected-at-runtime",
        max_cpu="1",
        max_memory_mb=1024,
        max_disk_mb=2048,
        max_duration_s=300,
        capabilities=("object_truth_resolver",),
    )
    wrong_runtime_policy = RuntimeCapabilityProfile(
        runtime_api="1.1",
        os="linux",
        arch="amd64",
        network="declared",
        filesystem="read-mostly",
        secrets_policy="injected-at-runtime",
        max_cpu="2",
        max_memory_mb=4096,
        max_disk_mb=2048,
        max_duration_s=300,
        capabilities=("object_truth_resolver",),
    )

    assert validate_runtime_compatibility(report.manifest, compatible) == ()
    assert "COMPUTE_CPU_UNDERPROVISIONED" in _reason_codes(
        validate_runtime_compatibility(report.manifest, underpowered)
    )
    assert "RUNTIME_NETWORK_POLICY_MISMATCH" in _reason_codes(
        validate_runtime_compatibility(report.manifest, wrong_runtime_policy)
    )
    assert "INTEGRITY_SIGNATURE_REQUIRED" in _reason_codes(
        validate_deployment_mode(report.manifest, "production_deployment")
    )
