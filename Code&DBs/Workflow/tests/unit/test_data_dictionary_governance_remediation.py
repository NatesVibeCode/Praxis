"""Unit tests for the governance remediation planner.

Each `suggest_remediation` call must return a two-path plan:

    {
        "violation":  <serialized violation>,
        "immediate":  [<action>, ...],   # fix this one right now
        "permanent":  [<action>, ...],   # prevent the class from recurring
    }

Every action is a dict with `kind`, `summary`, `confidence`,
`autorun_ok`, and optionally `command` / `explain`.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.data_dictionary_governance import GovernanceViolation
from runtime import data_dictionary_governance_remediation as rem
from runtime.data_dictionary_governance_remediation import (
    RemediationAction,
    inline_immediate_summary,
    suggest_all_remediations,
    suggest_remediation,
)


# ---------------------------------------------------------------------------
# RemediationAction payload shape
# ---------------------------------------------------------------------------

def test_remediation_action_to_payload_shape() -> None:
    a = RemediationAction(
        kind="mcp_tool_call",
        summary="do the thing",
        command="praxis workflow tools call x",
        confidence=0.77,
        autorun_ok=False,
        explain="context",
    )
    p = a.to_payload()
    assert p["kind"] == "mcp_tool_call"
    assert p["summary"] == "do the thing"
    assert p["autorun_ok"] is False
    assert p["confidence"] == 0.77
    assert p["command"] == "praxis workflow tools call x"
    assert p["explain"] == "context"


def test_remediation_action_omits_empty_optionals() -> None:
    a = RemediationAction(kind="k", summary="s")
    p = a.to_payload()
    assert "command" not in p
    assert "explain" not in p


# ---------------------------------------------------------------------------
# pii_without_owner / sensitive_without_owner — immediate
# ---------------------------------------------------------------------------

def test_owner_immediate_prefers_lineage_nearest_owner(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: "team-data")
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    plan = suggest_remediation(object(), v)
    assert plan["immediate"][0]["kind"] == "mcp_tool_call"
    assert "team-data" in plan["immediate"][0]["summary"]
    assert "team-data" in plan["immediate"][0]["command"]
    assert plan["immediate"][0]["autorun_ok"] is False  # owner setting is a decision


def test_owner_immediate_suggests_namespace_default_when_available(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: None)
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: "bug_authority")
    v = GovernanceViolation(
        policy="sensitive_without_owner", object_kind="table:bug_evidence_links",
    )
    plan = suggest_remediation(object(), v)
    assert len(plan["immediate"]) == 1
    a = plan["immediate"][0]
    assert "bug_authority" in a["command"]
    assert a["confidence"] <= 0.8


def test_owner_immediate_includes_both_lineage_and_namespace_when_different(
    monkeypatch,
) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: "alice")
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: "bug_authority")
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:bugs")
    plan = suggest_remediation(object(), v)
    assert len(plan["immediate"]) == 2
    owners_in_commands = "".join(a["command"] for a in plan["immediate"])
    assert "alice" in owners_in_commands
    assert "bug_authority" in owners_in_commands
    # Higher-confidence (lineage) suggestion first.
    assert plan["immediate"][0]["confidence"] > plan["immediate"][1]["confidence"]


def test_owner_immediate_fallback_when_nothing_inferred(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: None)
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="object_type:x")
    plan = suggest_remediation(object(), v)
    assert len(plan["immediate"]) == 1
    a = plan["immediate"][0]
    assert a["confidence"] <= 0.4  # unsure; needs human judgment
    assert "<team-or-person>" in a["command"]


# ---------------------------------------------------------------------------
# pii_without_owner / sensitive_without_owner — permanent
# ---------------------------------------------------------------------------

def test_owner_permanent_always_files_architecture_policy(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    plan = suggest_remediation(object(), v)
    kinds = [a["kind"] for a in plan["permanent"]]
    assert "operator_decision" in kinds
    # The decision_key should be deterministic and policy-keyed.
    decision = next(a for a in plan["permanent"] if a["kind"] == "operator_decision")
    assert "pii-requires-owner" in decision["command"]


def test_owner_permanent_suggests_projector_extension_for_uncovered_namespace(
    monkeypatch,
) -> None:
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:xyz_thing")
    plan = suggest_remediation(object(), v)
    assert any(a["kind"] == "code_change" for a in plan["permanent"])


def test_owner_permanent_skips_projector_extension_when_already_mapped(
    monkeypatch,
) -> None:
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: "bug_authority")
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:bugs")
    plan = suggest_remediation(object(), v)
    # Namespace is already mapped → no redundant code-change suggestion.
    assert not any(a["kind"] == "code_change" for a in plan["permanent"])


def test_owner_permanent_always_offers_quality_rule_backstop(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: "x")
    v = GovernanceViolation(policy="sensitive_without_owner", object_kind="table:x")
    plan = suggest_remediation(object(), v)
    rule_action = next(a for a in plan["permanent"] if a["kind"] == "quality_rule")
    assert "owner_present" in rule_action["command"]


# ---------------------------------------------------------------------------
# error_rule_failing — immediate + permanent
# ---------------------------------------------------------------------------

def test_rule_immediate_has_reevaluate_first_and_autorun_ok() -> None:
    v = GovernanceViolation(
        policy="error_rule_failing",
        object_kind="table:bugs",
        rule_kind="not_null",
    )
    plan = suggest_remediation(object(), v)
    assert len(plan["immediate"]) == 3
    reeval = plan["immediate"][0]
    assert "evaluate" in reeval["command"]
    assert reeval["autorun_ok"] is True   # pure observation; safe
    # The disable + downgrade suggestions must NOT be autorun_ok.
    for a in plan["immediate"][1:]:
        assert a["autorun_ok"] is False


def test_rule_immediate_re_evaluate_has_highest_confidence() -> None:
    v = GovernanceViolation(
        policy="error_rule_failing", object_kind="table:x", rule_kind="unique",
    )
    plan = suggest_remediation(object(), v)
    # The re-run suggestion should be ranked highest.
    assert plan["immediate"][0]["confidence"] > plan["immediate"][-1]["confidence"]


def test_rule_permanent_includes_code_change_and_policy() -> None:
    v = GovernanceViolation(
        policy="error_rule_failing", object_kind="table:x", rule_kind="range",
    )
    plan = suggest_remediation(object(), v)
    kinds = {a["kind"] for a in plan["permanent"]}
    assert {"code_change", "operator_decision"}.issubset(kinds)


# ---------------------------------------------------------------------------
# Unknown policy
# ---------------------------------------------------------------------------

def test_unknown_policy_returns_empty_plan() -> None:
    v = GovernanceViolation(policy="not_a_real_policy", object_kind="x")
    plan = suggest_remediation(object(), v)
    assert plan["immediate"] == []
    assert plan["permanent"] == []
    assert plan["violation"]["policy"] == "not_a_real_policy"


# ---------------------------------------------------------------------------
# suggest_all_remediations
# ---------------------------------------------------------------------------

def test_suggest_all_remediations_attaches_plan_per_violation(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: "alice")
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    monkeypatch.setattr(
        rem, "scan_violations",
        lambda conn: [
            GovernanceViolation(policy="pii_without_owner", object_kind="table:a"),
            GovernanceViolation(
                policy="error_rule_failing",
                object_kind="table:b",
                rule_kind="not_null",
            ),
        ],
    )
    result = suggest_all_remediations(object())
    assert result["total_violations"] == 2
    assert len(result["plans"]) == 2
    assert result["plans"][0]["violation"]["object_kind"] == "table:a"
    assert result["plans"][1]["violation"]["object_kind"] == "table:b"


# ---------------------------------------------------------------------------
# inline_immediate_summary (used by bug-filing path)
# ---------------------------------------------------------------------------

def test_inline_immediate_summary_renders_top_suggestions(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_nearest_upstream_owner", lambda conn, k: "alice")
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:users")
    text = inline_immediate_summary(object(), v)
    assert "Immediate remediation" in text
    assert "alice" in text
    assert "$ praxis workflow tools call" in text


def test_inline_immediate_summary_is_empty_for_unknown_policy() -> None:
    v = GovernanceViolation(policy="unknown", object_kind="x")
    assert inline_immediate_summary(object(), v) == ""


# ---------------------------------------------------------------------------
# Namespace parsing
# ---------------------------------------------------------------------------

def test_parse_namespace_extracts_prefix() -> None:
    assert rem._parse_namespace("table:workflow_runs") == "workflow"
    assert rem._parse_namespace("table:data_dictionary_entries") == "data"
    assert rem._parse_namespace("object_type:contact") is None
    assert rem._parse_namespace("table:foo") == "foo"


# ---------------------------------------------------------------------------
# praxis_discover enrichment
# ---------------------------------------------------------------------------

def test_discover_enriches_rule_permanent_explain(monkeypatch) -> None:
    def _fake_discover(query: str, limit: int) -> list[dict[str, Any]]:
        assert "table:bugs" in query or "bugs" in query
        return [
            {"name": "write_bug", "kind": "function",
             "path": "runtime/bug_tracker.py", "similarity": 0.81},
            {"name": "upsert_bug", "kind": "function",
             "path": "storage/bugs.py", "similarity": 0.77},
        ]

    v = GovernanceViolation(
        policy="error_rule_failing", object_kind="table:bugs", rule_kind="not_null",
    )
    plan = suggest_remediation(object(), v, discover=_fake_discover)
    upstream_fix = next(
        a for a in plan["permanent"] if a["kind"] == "code_change"
    )
    assert "Candidate code paths" in upstream_fix["explain"]
    assert "runtime/bug_tracker.py" in upstream_fix["explain"]
    assert "similarity 0.81" in upstream_fix["explain"]


def test_discover_enrichment_absent_when_no_callable() -> None:
    v = GovernanceViolation(
        policy="error_rule_failing", object_kind="table:bugs", rule_kind="not_null",
    )
    plan = suggest_remediation(object(), v)
    upstream_fix = next(
        a for a in plan["permanent"] if a["kind"] == "code_change"
    )
    assert "Candidate code paths" not in upstream_fix["explain"]


def test_discover_errors_are_swallowed_safely(monkeypatch) -> None:
    def _bad_discover(query: str, limit: int) -> list[dict[str, Any]]:
        raise RuntimeError("indexer dead")

    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)
    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:xyz_thing")
    plan = suggest_remediation(object(), v, discover=_bad_discover)
    # Code-change backstop should still be present — discover failure shouldn't
    # omit the remediation, just the enrichment.
    code_changes = [a for a in plan["permanent"] if a["kind"] == "code_change"]
    assert code_changes
    assert "Candidate code paths" not in code_changes[0]["explain"]


def test_discover_enriches_owner_permanent_code_change(monkeypatch) -> None:
    monkeypatch.setattr(rem, "_namespace_owner_suggestion", lambda k: None)

    def _fake_discover(query: str, limit: int) -> list[dict[str, Any]]:
        return [
            {"name": "xyz_writer", "kind": "function",
             "path": "memory/xyz_projector.py", "similarity": 0.66},
        ]

    v = GovernanceViolation(policy="pii_without_owner", object_kind="table:xyz_thing")
    plan = suggest_remediation(object(), v, discover=_fake_discover)
    code_change = next(
        a for a in plan["permanent"] if a["kind"] == "code_change"
    )
    assert "memory/xyz_projector.py" in code_change["explain"]
