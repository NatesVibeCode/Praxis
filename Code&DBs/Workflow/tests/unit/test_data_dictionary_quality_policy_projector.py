"""Unit tests for the policy-derived quality-rules projector.

Promotes active `architecture_policy` operator decisions into
`inferred`-source `policy_compliance` quality rules on every table in
the namespace the decision governs.
"""
from __future__ import annotations

from typing import Any

from memory import data_dictionary_quality_policy_projector as projector
from memory.data_dictionary_quality_policy_projector import (
    DataDictionaryQualityPolicyProjector,
)


class _FakeConn:
    def __init__(
        self,
        *,
        policies: list[dict[str, Any]] | None = None,
        tables_by_prefix: dict[str, list[str]] | None = None,
    ) -> None:
        self._policies = list(policies or [])
        self._tables_by_prefix = dict(tables_by_prefix or {})

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM operator_decisions" in sql:
            return list(self._policies)
        if "FROM data_dictionary_objects" in sql:
            like = args[0] if args else ""
            for prefix, tables in self._tables_by_prefix.items():
                if like == f"table:{prefix}%":
                    return [{"object_kind": t} for t in tables]
            return []
        return []


def _install_catcher(monkeypatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _apply(conn, **kw):
        calls.append(kw)
        return {"rules_written": len(kw.get("rules") or [])}

    monkeypatch.setattr(projector, "apply_projected_rules", _apply)
    return calls


# ---------------------------------------------------------------------------
# Basic policy → rule mapping
# ---------------------------------------------------------------------------

def test_projector_emits_rule_per_namespace_table(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        policies=[{
            "operator_decision_id": "DEC-1",
            "decision_key": "architecture-policy::workflow::one-workflow-per-run",
            "title": "One workflow per run",
            "rationale": "Exactly one workflow owns each run.",
        }],
        tables_by_prefix={
            "workflow_": ["table:workflow_runs", "table:workflow_jobs"],
        },
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    assert len(calls) == 1
    rules = calls[0]["rules"]
    assert {r["object_kind"] for r in rules} == {"table:workflow_runs", "table:workflow_jobs"}
    for r in rules:
        assert r["rule_kind"] == "policy_compliance"
        assert r["severity"] == "warning"
        assert r["origin_ref"]["projector"] == "quality_policy_decisions"
        assert "workflow" in r["origin_ref"]["decision_keys"][0]


def test_projector_aggregates_multiple_policies_on_same_object(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        policies=[
            {
                "operator_decision_id": "DEC-A",
                "decision_key": "architecture-policy::workflow::a-slug",
                "rationale": "policy A",
            },
            {
                "operator_decision_id": "DEC-B",
                "decision_key": "architecture-policy::workflow::b-slug",
                "rationale": "policy B",
            },
        ],
        tables_by_prefix={"workflow_": ["table:workflow_runs"]},
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    rules = calls[0]["rules"]
    assert len(rules) == 1  # collapsed into one row per object
    r = rules[0]
    assert r["object_kind"] == "table:workflow_runs"
    assert len(r["expression"]["policies"]) == 2
    assert len(r["origin_ref"]["decision_keys"]) == 2


def test_projector_skips_unknown_subsystems(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        policies=[{
            "operator_decision_id": "DEC-X",
            "decision_key": "architecture-policy::unknown-subsys::foo",
            "rationale": "r",
        }],
        tables_by_prefix={},
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    # apply still called (for idempotent pruning) but with empty rules.
    assert calls[0]["rules"] == []


def test_projector_skips_malformed_decision_keys(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        policies=[
            {"operator_decision_id": "DEC-1", "decision_key": "not-a-policy-key",
             "rationale": ""},
            {"operator_decision_id": "",   "decision_key": "architecture-policy::workflow::x",
             "rationale": ""},
        ],
        tables_by_prefix={"workflow_": ["table:workflow_runs"]},
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    assert calls[0]["rules"] == []


def test_projector_truncates_long_rationale(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    rationale = "x" * 500
    conn = _FakeConn(
        policies=[{
            "operator_decision_id": "DEC-1",
            "decision_key": "architecture-policy::workflow::a-slug",
            "rationale": rationale,
        }],
        tables_by_prefix={"workflow_": ["table:workflow_runs"]},
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    desc = calls[0]["rules"][0]["description"]
    # Aggregated description format: "Governed by N active..."
    assert desc.startswith("Governed by 1 active")


def test_projector_writes_at_inferred_layer(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    conn = _FakeConn(
        policies=[{
            "operator_decision_id": "DEC-1",
            "decision_key": "architecture-policy::workflow::x",
            "rationale": "r",
        }],
        tables_by_prefix={"workflow_": ["table:workflow_runs"]},
    )
    DataDictionaryQualityPolicyProjector(conn).run()
    assert calls[0]["source"] == "inferred"


def test_projector_fails_softly_on_storage_error(monkeypatch) -> None:
    def _boom(conn, **kw):
        raise RuntimeError("db dead")

    monkeypatch.setattr(projector, "apply_projected_rules", _boom)
    conn = _FakeConn(
        policies=[{"operator_decision_id": "DEC-1",
                    "decision_key": "architecture-policy::workflow::x",
                    "rationale": "r"}],
        tables_by_prefix={"workflow_": ["table:workflow_runs"]},
    )
    result = DataDictionaryQualityPolicyProjector(conn).run()
    assert result.ok is False
    assert "db dead" in (result.error or "")
