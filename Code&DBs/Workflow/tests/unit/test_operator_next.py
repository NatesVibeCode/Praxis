from __future__ import annotations

from pathlib import Path

from runtime.operations.queries.operator_next import OperatorNextQuery, handle_operator_next
from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.tools import operator as operator_tools


class _FakeConn:
    def execute(self, *_args, **_kwargs):
        return []


class _FakeSubsystems:
    _repo_root = Path(__file__).resolve().parents[2]

    def get_pg_conn(self):
        return _FakeConn()


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


def test_praxis_next_actions_is_legacy_alias_for_progressive_front_door():
    catalog = get_tool_catalog()

    definition = catalog["praxis_next_actions"]

    assert definition.kind == "alias"
    assert definition.cli_replacement == "praxis_next"
    assert definition.risk_levels == ("read",)


def test_praxis_legal_tools_is_legacy_alias_for_progressive_front_door():
    catalog = get_tool_catalog()

    definition = catalog["praxis_legal_tools"]

    assert definition.kind == "alias"
    assert definition.cli_replacement == "praxis_next"
    assert definition.risk_levels == ("read",)


def test_praxis_legal_tools_delegates_to_progressive_unlock_frontier(monkeypatch):
    calls = []

    def fake_execute_catalog_tool(*, operation_name, payload):
        calls.append((operation_name, payload))
        return {
            "tool_legality": {
                "legal_action_count": 1,
                "blocked_action_count": 0,
                "legal_actions": [{"tool_name": "praxis_run"}],
                "blocked_actions": [],
                "typed_gaps": [],
                "repair_actions": [],
                "state": {"run_id": "workflow_123"},
                "authority_sources": ["operation_catalog_registry"],
            }
        }

    monkeypatch.setattr(operator_tools, "_execute_catalog_tool", fake_execute_catalog_tool)

    result = operator_tools.tool_praxis_legal_tools(
        {
            "intent": "prove whether this run actually fired",
            "run_id": "workflow_123",
            "limit": 3,
        }
    )

    assert calls == [
        (
            "operator.next",
            {
                "action": "unlock_frontier",
                "detail": "standard",
                "intent": "prove whether this run actually fired",
                "run_id": "workflow_123",
                "state": {},
                "allowed_tools": None,
                "include_blocked": True,
                "include_mutating": False,
                "limit": 3,
            },
        )
    ]
    assert result["deprecated_alias"]["replacement"] == "praxis_next"
    assert result["legal_action_count"] == 1
    assert result["legal_actions"] == [{"tool_name": "praxis_run"}]


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
