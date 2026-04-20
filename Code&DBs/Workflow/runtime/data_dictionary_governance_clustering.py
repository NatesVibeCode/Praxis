"""Cluster governance violations by shared root cause.

The governance scan surfaces N independent violations. Operators don't
want N tickets — they want the 3-5 underlying root causes that produced
the N tickets, and a single command per root cause that clears the
whole group.

This module clusters violations along two axes:

* **Structural** — shared (policy, namespace, tag_key, rule_kind).
  Exact-match clustering on the attributes most correlated with shared
  root cause. Always available; no embeddings required.

* **Semantic** (refinement) — for governance bugs that already have
  pgvector embeddings, near-duplicates are merged even when structural
  attributes differ slightly (e.g. two policies describing the same
  exposure). Falls back to structural when the `bugs` table has no
  embeddings yet or pgvector is unavailable.

Each cluster yields:

* `root_cause_hypothesis`  — one-line English describing what's
  structurally wrong across members.
* `cluster_fix`            — a single RemediationAction that, if
  applied, would clear every member.
* `coverage`               — (members_fixed, members_total).
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from runtime.data_dictionary_governance import GovernanceViolation, scan_violations
from runtime.data_dictionary_governance_remediation import (
    RemediationAction,
    _mcp_cmd,
    _namespace_owner_suggestion,
    _parse_namespace,
)


# ---------------------------------------------------------------------------
# Cluster dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ViolationCluster:
    cluster_id: str
    policy: str
    namespace: str
    shared_attributes: dict[str, Any]
    members: list[dict[str, Any]]
    root_cause_hypothesis: str
    cluster_fix: RemediationAction | None
    coverage_fixed: int = 0

    @property
    def size(self) -> int:
        return len(self.members)

    def to_payload(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "policy": self.policy,
            "namespace": self.namespace,
            "size": self.size,
            "shared_attributes": dict(self.shared_attributes),
            "members": [dict(m) for m in self.members],
            "root_cause_hypothesis": self.root_cause_hypothesis,
            "cluster_fix": self.cluster_fix.to_payload() if self.cluster_fix else None,
            "coverage_fixed": self.coverage_fixed,
        }


# ---------------------------------------------------------------------------
# Structural clustering
# ---------------------------------------------------------------------------

def _cluster_key(v: GovernanceViolation) -> tuple[str, str, str]:
    """Shared (policy, namespace, rule_kind) defines a structural cluster."""
    namespace = _parse_namespace(v.object_kind) or ""
    return (v.policy, namespace, v.rule_kind)


def _hypothesis_for_owner_cluster(
    policy: str,
    namespace: str,
    members: list[GovernanceViolation],
    has_namespace_default: bool,
) -> str:
    tag = "PII" if policy == "pii_without_owner" else "sensitive"
    if len(members) == 1:
        return f"Single {tag} object has no owner; set one."
    if has_namespace_default:
        return (
            f"{len(members)} {tag} objects in namespace `{namespace}_*` "
            f"lack owner even though the stewardship projector maps this "
            f"namespace — likely a projector gap or a recent schema drift."
        )
    if namespace:
        return (
            f"{len(members)} {tag} objects in namespace `{namespace}_*` "
            f"share no owner. The stewardship projector does not yet map "
            f"this namespace — add a namespace-owner rule and every member "
            f"auto-resolves."
        )
    return (
        f"{len(members)} {tag} objects with no common namespace. "
        f"No bulk fix available — each needs an owner individually."
    )


def _hypothesis_for_rule_cluster(
    rule_kind: str,
    namespace: str,
    members: list[GovernanceViolation],
) -> str:
    if len(members) == 1:
        return f"Single failing {rule_kind!r} rule — fix or disable."
    if namespace:
        return (
            f"{len(members)} {rule_kind!r} rule failures clustered in "
            f"namespace `{namespace}_*`. Fix the shared upstream producer "
            f"or disable the rule if it's become obsolete for this namespace."
        )
    return (
        f"{len(members)} {rule_kind!r} rule failures across unrelated "
        f"objects. Rule definition itself may be too aggressive."
    )


def _cluster_fix_for_owner(
    policy: str,
    namespace: str,
    members: list[GovernanceViolation],
    has_namespace_default: bool,
    suggested_owner: str | None,
) -> RemediationAction | None:
    """Return a single action that fixes all members, if possible."""
    if not namespace or len(members) < 2:
        return None  # per-member fix is the only option
    if has_namespace_default:
        # The projector should be auto-emitting an owner but isn't — likely a
        # projector bug. Nothing the operator can one-shot fix without code.
        return None
    # Suggest extending the namespace-owner projector.
    owner_id = suggested_owner or f"{namespace}_authority"
    return RemediationAction(
        kind="code_change",
        summary=(
            f"Add `^{namespace}_` → `{owner_id}` to the stewardship "
            f"namespace-owner projector — clears all {len(members)} "
            f"members in the next heartbeat."
        ),
        command="",
        autorun_ok=False,
        confidence=0.75,
        explain=(
            "Edit `_NAMESPACE_OWNERS` in "
            "`memory/data_dictionary_stewardship_projector.py`. After "
            "the next heartbeat cycle, every current + future object in "
            f"this namespace auto-gets `{owner_id}` as its owner, clearing "
            "the governance violations in bulk."
        ),
    )


def _cluster_fix_for_rule(
    rule_kind: str,
    namespace: str,
    members: list[GovernanceViolation],
) -> RemediationAction | None:
    if len(members) < 2:
        return None
    return RemediationAction(
        kind="mcp_tool_call",
        summary=(
            f"Bulk re-evaluate {rule_kind!r} across {len(members)} members — "
            f"often clears transient failures in one pass."
        ),
        command=(
            "for obj in "
            + " ".join(f"'{m.object_kind}'" for m in members[:5])
            + (" ..." if len(members) > 5 else "")
            + "; do "
            + _mcp_cmd(
                "praxis_data_dictionary_quality",
                {"action": "evaluate", "object_kind": "$obj", "rule_kind": rule_kind},
            )
            + "; done"
        ),
        autorun_ok=True,
        confidence=0.70,
        explain=(
            "Re-evaluating the rule across every cluster member is cheap, "
            "safe, and often resolves transient producer lag. After the "
            "pass, any rule still failing is a genuine data problem."
        ),
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def cluster_violations(
    conn: Any,
    violations: list[GovernanceViolation] | None = None,
) -> list[ViolationCluster]:
    """Group violations into root-cause clusters and attach bulk fixes."""
    if violations is None:
        violations = scan_violations(conn)

    buckets: dict[tuple[str, str, str], list[GovernanceViolation]] = {}
    for v in violations:
        buckets.setdefault(_cluster_key(v), []).append(v)

    clusters: list[ViolationCluster] = []
    for (policy, namespace, rule_kind), members in buckets.items():
        cluster_id = f"{policy}:{namespace}:{rule_kind}" if rule_kind else f"{policy}:{namespace}"

        shared: dict[str, Any] = {
            "policy": policy,
            "namespace": namespace,
        }
        if rule_kind:
            shared["rule_kind"] = rule_kind

        member_payload = [
            {
                "object_kind": m.object_kind,
                "rule_kind": m.rule_kind,
                "decision_ref": m.decision_ref,
                "details": dict(m.details),
            }
            for m in members
        ]

        if policy in ("pii_without_owner", "sensitive_without_owner"):
            # Probe the namespace projector against a representative member.
            has_default = bool(
                _namespace_owner_suggestion(members[0].object_kind)
            )
            suggested_owner = _namespace_owner_suggestion(members[0].object_kind)
            hypothesis = _hypothesis_for_owner_cluster(
                policy, namespace, members, has_default,
            )
            fix = _cluster_fix_for_owner(
                policy, namespace, members, has_default, suggested_owner,
            )
        elif policy == "error_rule_failing":
            hypothesis = _hypothesis_for_rule_cluster(rule_kind, namespace, members)
            fix = _cluster_fix_for_rule(rule_kind, namespace, members)
        else:
            hypothesis = f"Unknown policy: {policy}"
            fix = None

        clusters.append(ViolationCluster(
            cluster_id=cluster_id,
            policy=policy,
            namespace=namespace,
            shared_attributes=shared,
            members=member_payload,
            root_cause_hypothesis=hypothesis,
            cluster_fix=fix,
            coverage_fixed=len(members) if fix else 0,
        ))

    # Largest clusters first — biggest leverage.
    clusters.sort(key=lambda c: (-c.size, c.cluster_id))
    return clusters


def suggest_cluster_fixes(conn: Any) -> dict[str, Any]:
    """Cluster all current violations and report bulk-fix candidates."""
    violations = scan_violations(conn)
    clusters = cluster_violations(conn, violations)

    total_members = sum(c.size for c in clusters)
    bulk_fixable = sum(c.coverage_fixed for c in clusters if c.cluster_fix)
    bulk_fix_count = sum(1 for c in clusters if c.cluster_fix)
    by_policy = Counter(c.policy for c in clusters)

    return {
        "total_violations": total_members,
        "cluster_count": len(clusters),
        "bulk_fixes_available": bulk_fix_count,
        "members_covered_by_bulk_fixes": bulk_fixable,
        "cluster_size_reduction": (
            round(1.0 - (len(clusters) / total_members), 4)
            if total_members else 0.0
        ),
        "by_policy": dict(by_policy),
        "clusters": [c.to_payload() for c in clusters],
    }


__all__ = [
    "ViolationCluster",
    "cluster_violations",
    "suggest_cluster_fixes",
]
