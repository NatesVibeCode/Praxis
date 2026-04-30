from __future__ import annotations

from types import SimpleNamespace
import hashlib

import pytest

from runtime.operations.commands import portable_cartridge as commands
from runtime.operations.queries import portable_cartridge as queries


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _verifier_checks() -> list[dict[str, object]]:
    return [
        {
            "check_id": f"check.{category}",
            "category": category,
            "required": True,
            "contract_ref": f"verifier.contract.{category}",
        }
        for category in (
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
    ]


def _drift_hooks() -> list[dict[str, object]]:
    return [
        {
            "hook_id": f"drift.{hook_point}",
            "hook_point": hook_point,
            "drift_dimensions": dimensions,
            "evidence_contract_ref": f"audit.evidence.{hook_point}",
            "required": True,
        }
        for hook_point, dimensions in (
            ("build_time", ["manifest", "dependency"]),
            ("load_time", ["binding", "runtime_capability"]),
            ("execute_time", ["dependency", "compute", "policy"]),
            ("post_run", ["output_lineage"]),
            ("periodic_runtime", ["binding", "policy", "dependency"]),
        )
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
            "arch": ["amd64"],
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
                    "parents": [{"dependency_id": "policy.snapshot", "digest": primary_digest}],
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
        "verification": {"suite_version": "1.0", "required_checks": _verifier_checks()},
        "audit": {
            "content_digest": _sha256(b"package-bytes"),
            "dependency_digests": [primary_digest, optional_digest, derived_digest],
            "drift_hooks": _drift_hooks(),
        },
        "signatures": [],
    }


def _runtime_profile() -> dict[str, object]:
    return {
        "runtime_api": "1.1",
        "os": "linux",
        "arch": "amd64",
        "network": "restricted",
        "filesystem": "read-mostly",
        "secrets_policy": "injected-at-runtime",
        "max_cpu": "2",
        "max_memory_mb": 4096,
        "max_disk_mb": 2048,
        "max_duration_s": 300,
        "capabilities": ["object_truth_resolver"],
    }


def test_record_portable_cartridge_persists_ready_contract(monkeypatch) -> None:
    persist_calls: list[dict[str, object]] = []

    def _persist(conn, **kwargs):
        persist_calls.append(kwargs)
        return {
            "cartridge_record_id": kwargs["cartridge_record_id"],
            "readiness_status": kwargs["readiness_status"],
        }

    monkeypatch.setattr(commands, "persist_portable_cartridge_record", _persist)

    result = commands.handle_record_portable_cartridge(
        commands.RecordPortableCartridgeCommand(
            manifest=_valid_manifest_payload(),
            deployment_mode="staged_deployment",
            runtime_capability_profile=_runtime_profile(),
            binding_values={
                "secret.crm_token": "secret-ref",
                "object.store": "object-store-ref",
            },
            observed_by_ref="operator:nate",
            source_ref="phase_09_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "authority.portable_cartridge.record"
    assert result["readiness_status"] == "ready"
    assert result["deployment_contract"]["object_truth_dependency_count"] == 3
    assert result["deployment_contract"]["binding_count"] == 2
    assert result["event_payload"]["verifier_check_count"] == 9
    assert persist_calls[0]["deployment_mode"] == "staged_deployment"
    assert persist_calls[0]["readiness_status"] == "ready"


def test_record_portable_cartridge_can_block_non_ready_production_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        commands,
        "persist_portable_cartridge_record",
        lambda conn, **kwargs: {"cartridge_record_id": kwargs["cartridge_record_id"]},
    )

    result = commands.handle_record_portable_cartridge(
        commands.RecordPortableCartridgeCommand(
            manifest=_valid_manifest_payload(),
            deployment_mode="production_deployment",
        ),
        _subsystems(),
    )

    assert result["readiness_status"] == "blocked"
    assert "INTEGRITY_SIGNATURE_REQUIRED" in result["validation_report"]["reason_codes"]

    with pytest.raises(ValueError, match="portable_cartridge.not_ready"):
        commands.handle_record_portable_cartridge(
            commands.RecordPortableCartridgeCommand(
                manifest=_valid_manifest_payload(),
                deployment_mode="production_deployment",
                require_ready=True,
            ),
            _subsystems(),
        )


def test_read_portable_cartridge_lists_and_describes_records(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_portable_cartridge_records",
        lambda conn, **kwargs: [{"cartridge_record_id": "portable_cartridge_record.demo", **kwargs}],
    )
    monkeypatch.setattr(
        queries,
        "load_portable_cartridge_record",
        lambda conn, cartridge_record_id, **kwargs: {
            "cartridge_record_id": cartridge_record_id,
            "bindings": [{}] if kwargs["include_bindings"] else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_portable_cartridge_dependencies",
        lambda conn, **kwargs: [{"dependency_class": kwargs["dependency_class"]}],
    )
    monkeypatch.setattr(
        queries,
        "list_portable_cartridge_bindings",
        lambda conn, **kwargs: [{"kind": kwargs["binding_kind"]}],
    )
    monkeypatch.setattr(
        queries,
        "list_portable_cartridge_verifiers",
        lambda conn, **kwargs: [{"category": kwargs["verifier_category"]}],
    )
    monkeypatch.setattr(
        queries,
        "list_portable_cartridge_drift_hooks",
        lambda conn, **kwargs: [{"hook_point": kwargs["hook_point"]}],
    )

    listed = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="list_records",
            readiness_status="ready",
        ),
        _subsystems(),
    )
    described = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="describe_record",
            cartridge_record_id="portable_cartridge_record.demo",
            include_bindings=True,
        ),
        _subsystems(),
    )
    dependencies = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="list_dependencies",
            dependency_class="primary",
        ),
        _subsystems(),
    )
    bindings = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="list_bindings",
            binding_kind="secret_reference",
        ),
        _subsystems(),
    )
    verifiers = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="list_verifiers",
            verifier_category="schema",
        ),
        _subsystems(),
    )
    drift_hooks = queries.handle_read_portable_cartridge(
        queries.ReadPortableCartridgeQuery(
            action="list_drift_hooks",
            hook_point="execute_time",
        ),
        _subsystems(),
    )

    assert listed["count"] == 1
    assert described["record"]["bindings"] == [{}]
    assert dependencies["items"][0]["dependency_class"] == "primary"
    assert bindings["items"][0]["kind"] == "secret_reference"
    assert verifiers["items"][0]["category"] == "schema"
    assert drift_hooks["items"][0]["hook_point"] == "execute_time"
