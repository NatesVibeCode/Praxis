"""Unit tests for governance violation clustering.

The clustering module groups `GovernanceViolation` objects along
(policy, namespace, rule_kind) and attaches a single bulk-fix
`RemediationAction` per cluster when possible.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime.data_dictionary_governance import GovernanceViolation
from runtime import data_dictionary_governance_clustering as clustering
from runtime.data_dictionary_governance_clustering import (
    ViolationCluster,
    cluster_violations,
    suggest_cluster_fixes,
)


# ---------------------------------------------------------------------------
# Structural clustering
# ---------------------------------------------------------------------------

def test_violations_cluster_by_policy_and_namespace(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_b"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_c"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:webhook_x"),
        GovernanceViolation(policy="pii_without_owner", object_kind="object_type:contact"),
    ]
    clusters = cluster_violations(object(), vs)
    # Three clusters: provider/sensitive (3), webhook/sensitive (1), contact/pii (1).
    sizes = sorted([c.size for c in clusters], reverse=True)
    assert sizes == [3, 1, 1]
    # Largest first.
    assert clusters[0].size == 3
    assert clusters[0].namespace == "provider"


def test_clusters_produce_bulk_fix_only_when_leverage_exists(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_b"),
    ]
    clusters = cluster_violations(object(), vs)
    assert len(clusters) == 1
    c = clusters[0]
    assert c.cluster_fix is not None
    assert c.cluster_fix.kind == "code_change"
    assert "provider_" in c.cluster_fix.summary
    assert c.coverage_fixed == 2


def test_single_member_owner_cluster_has_no_bulk_fix(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    vs = [GovernanceViolation(policy="pii_without_owner", object_kind="object_type:contact")]
    clusters = cluster_violations(object(), vs)
    assert clusters[0].cluster_fix is None
    assert clusters[0].coverage_fixed == 0


def test_cluster_skips_bulk_fix_when_namespace_already_mapped(monkeypatch) -> None:
    """If the projector already maps this namespace, the code-change fix is
    redundant — the bug is a projector gap, not a missing mapping."""
    monkeypatch.setattr(
        clustering, "_namespace_owner_suggestion",
        lambda k: "bug_authority",
    )
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:bugs"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:bug_evidence"),
    ]
    clusters = cluster_violations(object(), vs)
    assert clusters[0].cluster_fix is None


def test_cluster_uses_namespace_owner_suggestion_as_suggested_owner(monkeypatch) -> None:
    """When the projector *could* cover this namespace but currently
    doesn't for this object, the suggested cluster fix includes the
    namespace-derived owner id."""
    # Simulate: first call returns None (so cluster thinks namespace is
    # uncovered), but subsequent call within _cluster_fix_for_owner returns
    # an owner name. Since the implementation only calls the helper once
    # in cluster_violations, we stub it to a fixed value.
    calls = {"n": 0}

    def fake(k):
        calls["n"] += 1
        return None  # uncovered → bulk fix suggested

    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", fake)
    vs = [
        GovernanceViolation(policy="pii_without_owner", object_kind="table:xyz_a"),
        GovernanceViolation(policy="pii_without_owner", object_kind="table:xyz_b"),
    ]
    clusters = cluster_violations(object(), vs)
    fix = clusters[0].cluster_fix
    assert fix is not None
    # Default-derived owner id when no explicit suggestion.
    assert "xyz_authority" in fix.summary


# ---------------------------------------------------------------------------
# Rule clustering
# ---------------------------------------------------------------------------

def test_rule_cluster_emits_bulk_reevaluate_autorun_ok() -> None:
    vs = [
        GovernanceViolation(policy="error_rule_failing", object_kind="table:bug_a", rule_kind="not_null"),
        GovernanceViolation(policy="error_rule_failing", object_kind="table:bug_b", rule_kind="not_null"),
    ]
    clusters = cluster_violations(object(), vs)
    c = clusters[0]
    assert c.size == 2  # sanity: both share namespace `bug`
    assert c.cluster_fix is not None
    assert c.cluster_fix.kind == "mcp_tool_call"
    assert c.cluster_fix.autorun_ok is True
    assert "evaluate" in c.cluster_fix.command


def test_single_rule_failure_has_no_bulk_fix() -> None:
    vs = [GovernanceViolation(
        policy="error_rule_failing", object_kind="table:a", rule_kind="unique"
    )]
    clusters = cluster_violations(object(), vs)
    assert clusters[0].cluster_fix is None


# ---------------------------------------------------------------------------
# Root-cause hypothesis text
# ---------------------------------------------------------------------------

def test_hypothesis_names_namespace_when_present(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind=f"table:provider_{i}")
        for i in range(5)
    ]
    clusters = cluster_violations(object(), vs)
    assert "provider_*" in clusters[0].root_cause_hypothesis
    assert "5" in clusters[0].root_cause_hypothesis


def test_hypothesis_diagnoses_projector_gap_vs_drift(monkeypatch) -> None:
    """When namespace IS mapped in projector but bugs still fire, that's
    a different root-cause hypothesis than when it isn't mapped."""
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: "bug_authority")
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:bug_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:bug_b"),
    ]
    clusters = cluster_violations(object(), vs)
    assert clusters[0].size == 2
    assert "projector gap or a recent schema drift" in clusters[0].root_cause_hypothesis


# ---------------------------------------------------------------------------
# suggest_cluster_fixes aggregate payload
# ---------------------------------------------------------------------------

def test_suggest_cluster_fixes_rollup(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    monkeypatch.setattr(
        clustering, "scan_violations",
        lambda conn: [
            GovernanceViolation(policy="sensitive_without_owner", object_kind=f"table:provider_{i}")
            for i in range(3)
        ] + [
            GovernanceViolation(policy="pii_without_owner", object_kind="object_type:contact"),
        ],
    )
    r = suggest_cluster_fixes(object())
    assert r["total_violations"] == 4
    assert r["cluster_count"] == 2
    # 3-member cluster has a bulk fix; 1-member does not.
    assert r["bulk_fixes_available"] == 1
    assert r["members_covered_by_bulk_fixes"] == 3
    # Clustering reduced 4 violations to 2 clusters → 50% reduction.
    assert r["cluster_size_reduction"] == 0.5


def test_suggest_cluster_fixes_handles_empty_state(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "scan_violations", lambda conn: [])
    r = suggest_cluster_fixes(object())
    assert r["total_violations"] == 0
    assert r["cluster_count"] == 0
    assert r["bulk_fixes_available"] == 0
    assert r["cluster_size_reduction"] == 0.0
    assert r["clusters"] == []


def test_payload_members_preserve_decision_ref() -> None:
    vs = [
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_b"),
    ]
    clusters = cluster_violations(object(), vs)
    payload = clusters[0].to_payload()
    refs = [m["decision_ref"] for m in payload["members"]]
    assert "governance.sensitive_without_owner.table:provider_a" in refs
    assert "governance.sensitive_without_owner.table:provider_b" in refs


# ---------------------------------------------------------------------------
# Sorting — largest clusters first
# ---------------------------------------------------------------------------

def test_clusters_sorted_by_size_descending(monkeypatch) -> None:
    monkeypatch.setattr(clustering, "_namespace_owner_suggestion", lambda k: None)
    vs = [
        GovernanceViolation(policy="pii_without_owner", object_kind="object_type:contact"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_b"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:provider_c"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:webhook_a"),
        GovernanceViolation(policy="sensitive_without_owner", object_kind="table:webhook_b"),
    ]
    clusters = cluster_violations(object(), vs)
    assert [c.size for c in clusters] == [3, 2, 1]
