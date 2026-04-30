from __future__ import annotations

from storage.postgres import portable_cartridge_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO portable_cartridge_records" in sql:
            return {
                "cartridge_record_id": args[0],
                "cartridge_id": args[1],
                "cartridge_version": args[2],
                "build_id": args[3],
                "manifest_digest": args[5],
                "deployment_mode": args[6],
                "readiness_status": args[7],
                "object_truth_dependency_count": args[10],
                "binding_count": args[12],
                "verifier_check_count": args[14],
                "drift_hook_count": args[15],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _manifest() -> dict[str, object]:
    return {
        "manifest_version": "1.0",
        "cartridge_id": "phase9-portable-cartridge",
        "cartridge_version": "2026.04.30",
        "build_id": "build_2026_04_30_0001",
        "object_truth": {
            "primary": [
                {
                    "dependency_id": "policy.snapshot",
                    "object_ref": "object_truth.policy_snapshot",
                    "authority_source": "policy_registry",
                    "version": "2026.04.30",
                    "digest": "sha256:" + "a" * 64,
                    "freshness_policy": {"kind": "pinned"},
                    "failure_policy": "fail_closed",
                    "required": True,
                }
            ],
            "optional": [],
            "derived": [],
        },
        "assets": [
            {
                "path": "assets/workflow.json",
                "role": "workflow_definition",
                "media_type": "application/json",
                "size_bytes": 10,
                "digest": "sha256:" + "b" * 64,
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
            }
        ],
        "verification": {
            "suite_version": "1.0",
            "required_checks": [
                {
                    "check_id": "check.schema",
                    "category": "schema",
                    "required": True,
                    "contract_ref": "verifier.contract.schema",
                    "reason_code_family": "SCHEMA",
                }
            ],
        },
        "audit": {
            "content_digest": "sha256:" + "c" * 64,
            "dependency_digests": ["sha256:" + "a" * 64],
            "drift_hooks": [
                {
                    "hook_id": "drift.load",
                    "hook_point": "load_time",
                    "drift_dimensions": ["binding"],
                    "evidence_contract_ref": "audit.evidence.load",
                    "required": True,
                }
            ],
        },
    }


def test_persist_portable_cartridge_record_writes_parent_and_facets() -> None:
    conn = _RecordingConn()

    persisted = repo.persist_portable_cartridge_record(
        conn,
        cartridge_record_id="portable_cartridge_record.demo",
        manifest=_manifest(),
        validation_report={
            "ok": True,
            "error_count": 0,
            "warning_count": 0,
            "canonical_digest": "sha256:" + "d" * 64,
            "findings": [],
            "reason_codes": [],
        },
        deployment_contract={
            "schema": "portable_cartridge.deployment_contract.v1",
            "runtime_sizing_class": "medium",
        },
        readiness_status="ready",
        deployment_mode="staged_deployment",
        observed_by_ref="operator:nate",
        source_ref="phase_09_test",
    )

    assert "INSERT INTO portable_cartridge_records" in conn.fetchrow_calls[0][0]
    assert persisted["cartridge_record_id"] == "portable_cartridge_record.demo"
    assert persisted["readiness_status"] == "ready"
    assert any("DELETE FROM portable_cartridge_drift_hooks" in call[0] for call in conn.execute_calls)
    assert any("INSERT INTO portable_cartridge_object_truth_dependencies" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO portable_cartridge_assets" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO portable_cartridge_binding_contracts" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO portable_cartridge_verifier_checks" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO portable_cartridge_drift_hooks" in call[0] for call in conn.batch_calls)


def test_portable_cartridge_repository_lists_records_with_filters() -> None:
    conn = _RecordingConn()

    records = repo.list_portable_cartridge_records(
        conn,
        cartridge_id="phase9-portable-cartridge",
        readiness_status="ready",
        deployment_mode="staged_deployment",
    )
    dependencies = repo.list_portable_cartridge_dependencies(
        conn,
        dependency_class="primary",
        required=True,
    )
    bindings = repo.list_portable_cartridge_bindings(
        conn,
        binding_kind="secret_reference",
        required=True,
    )

    assert records == []
    assert dependencies == []
    assert bindings == []
    assert "FROM portable_cartridge_records" in conn.fetch_calls[0][0]
    assert "FROM portable_cartridge_object_truth_dependencies" in conn.fetch_calls[1][0]
    assert "FROM portable_cartridge_binding_contracts" in conn.fetch_calls[2][0]
