from __future__ import annotations

from pathlib import Path

import runtime.operations.queries.operator_next as operator_next_mod
from runtime.operations.queries.operator_next import OperatorNextQuery, handle_operator_next
from surfaces.mcp.catalog import get_tool_catalog


class _FakeConn:
    def __init__(
        self,
        *,
        verify_refs: set[str] | None = None,
    ) -> None:
        self.verify_refs = verify_refs or set()

    def execute(self, *_args, **_kwargs):
        query = str(_args[0] if _args else "")
        requested = set(_args[1] if len(_args) > 1 and isinstance(_args[1], list) else [])
        if "FROM verify_refs" in query:
            return [
                {"verify_ref": ref}
                for ref in sorted(self.verify_refs & requested)
            ]
        return []


class _FakeSubsystems:
    _repo_root = Path(__file__).resolve().parents[2]

    def __init__(self, *, conn: _FakeConn | None = None) -> None:
        self._conn = conn or _FakeConn()

    def get_pg_conn(self):
        return self._conn


def test_praxis_next_catalog_surface_is_progressive_read_only():
    catalog = get_tool_catalog()

    definition = catalog["praxis_next"]

    assert definition.cli_surface == "operator"
    assert definition.cli_tier == "stable"
    assert definition.selector_enum == (
        "next",
        "launch_gate",
        "failure_triage",
        "manifest_audit",
        "toolsmith",
        "unlock_frontier",
    )
    assert definition.risk_levels == ("read",)


def test_next_blocks_fleet_without_proof_run():
    query = OperatorNextQuery(
        action="next",
        intent="fire workflow fleet safely",
        fleet_size=12,
    )

    result = handle_operator_next(query, _FakeSubsystems())

    assert result["ok"] is True
    assert result["verdict"] == "inspect_then_act"
    assert result["blocked_actions"][0]["reason"] == "one_proof_before_fleet"
    assert result["recommended_actions"][0]["action"] == "launch_gate"


def test_manifest_audit_fails_closed_on_scope_and_verifier_gaps():
    query = OperatorNextQuery(
        action="manifest_audit",
        manifest={
            "jobs": [
                {
                    "label": "Execute packet",
                    "result_kind": "code_change",
                    "primary_paths": ["artifacts/workflow/packet/EXECUTION.md"],
                    "verify_refs": ["verifier.packet.execution_file_exists"],
                    "allowed_tools": ["praxis_search"],
                }
            ]
        },
    )

    result = handle_operator_next(query, _FakeSubsystems())

    codes = {finding["code"] for finding in result["findings"]}
    assert result["verdict"] == "block"
    assert "manifest.write_scope_missing" in codes
    assert "manifest.unknown_verifiers" in codes
    assert "manifest.orient_not_admitted" in codes


def test_manifest_audit_accepts_canonical_verify_refs_authority():
    query = OperatorNextQuery(
        action="manifest_audit",
        manifest={
            "jobs": [
                {
                    "label": "Execute packet",
                    "result_kind": "code_change",
                    "primary_paths": ["artifacts/workflow/packet/EXECUTION.md"],
                    "write_scope": ["artifacts/workflow/packet/EXECUTION.md"],
                    "verify_refs": ["verify.generated.execute_packet"],
                    "allowed_tools": ["praxis_orient", "praxis_search"],
                }
            ]
        },
    )

    result = handle_operator_next(
        query,
        _FakeSubsystems(conn=_FakeConn(verify_refs={"verify.generated.execute_packet"})),
    )

    codes = {finding["code"] for finding in result["findings"]}
    assert "manifest.unknown_verifiers" not in codes


def test_manifest_audit_detects_execution_manifest_drift_and_scratch_scope():
    query = OperatorNextQuery(
        action="manifest_audit",
        manifest={
            "jobs": [
                {
                    "label": "Plan packet",
                    "result_kind": "artifact_bundle",
                    "primary_paths": ["PLAN.md"],
                    "write_scope": ["artifacts/workflow/packet/PLAN.md"],
                    "verify_refs": ["verifier.plan.file_exists"],
                    "allowed_tools": ["praxis_orient", "praxis_search"],
                    "execution_manifest": {
                        "access_policy": {
                            "write_scope": ["scratch/workflow_abc"],
                        },
                        "tool_allowlist": ["praxis_orient"],
                        "verify_refs": ["verifier.other"],
                    },
                }
            ]
        },
    )

    result = handle_operator_next(query, _FakeSubsystems())

    codes = {finding["code"] for finding in result["findings"]}
    assert "manifest.write_scope_drift" in codes
    assert "manifest.scratch_scope_for_declared_artifact" in codes
    assert "manifest.tool_allowlist_drift" in codes
    assert "manifest.verify_ref_drift" in codes


def test_unlock_frontier_returns_math_model_and_frontier_shape():
    query = OperatorNextQuery(
        action="unlock_frontier",
        facts=["queue:healthy", "providers:observable"],
        limit=3,
    )

    result = handle_operator_next(query, _FakeSubsystems())

    assert result["verdict"] == "frontier_computed"
    assert result["mathematical_model"]["graph"] == "typed action hypergraph"
    assert "legal_actions" in result
    assert "best_repairs" in result
    assert result["tool_legality"]["ok"] is True
    assert "legal_actions" in result["tool_legality"]


def test_launch_gate_ignores_capacity_for_operator_disabled_provider(monkeypatch) -> None:
    monkeypatch.setattr(
        operator_next_mod,
        "_context_snapshot",
        lambda *_args, **_kwargs: {
            "queue": {"queue_depth_status": "ok"},
            "provider_slots": [
                {
                    "provider_slug": "anthropic",
                    "max_concurrent": 4,
                    "active_slots": 4.0,
                    "provider_disabled": True,
                }
            ],
            "host_resources": [],
            "catalog": {},
            "run": None,
            "proof_run": {
                "run_id": "run_canary",
                "status": "succeeded",
                "jobs": [],
                "failed_jobs": [],
            },
            "operation_catalog": [],
        },
    )

    result = handle_operator_next(
        OperatorNextQuery(
            action="launch_gate",
            proof_run_id="run_canary",
            fleet_size=2,
        ),
        _FakeSubsystems(),
    )

    provider_check = next(
        check for check in result["checks"] if check["name"] == "provider_capacity"
    )
    assert result["verdict"] == "allow"
    assert provider_check["status"] == "pass"
