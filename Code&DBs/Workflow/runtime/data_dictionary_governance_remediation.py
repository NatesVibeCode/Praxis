"""Per-violation remediation plans for data-dictionary governance.

Given a `GovernanceViolation`, produce two ordered ranked plans:

* **immediate** — concrete, executable commands that fix this specific
  violation right now (one-click if the operator confirms).
* **permanent** — structural backstops that prevent the violation class
  from recurring (code changes, architecture-policy decisions, new
  projector rules, new quality rules).

Every command is a dict with:

    kind        tool identifier — one of {mcp_tool_call, operator_decision,
                projector_rule, quality_rule, heartbeat_config, code_change}
    summary     single-line explanation shown to the operator
    command     shell-ready `praxis workflow tools call ...` string when
                possible, or a JSON-serializable instruction otherwise
    autorun_ok  whether it is safe to run without further human review
    confidence  0..1 when an argument was inferred (e.g., suggested owner)
    explain     optional multi-line detail for the operator

No mutation happens in this module. The operator / Claude invokes the
returned commands through the existing MCP surface.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from runtime.data_dictionary_governance import (
    GovernanceViolation,
    _nearest_upstream_owner,
    scan_violations,
)


DiscoverFn = Callable[[str, int], list[dict[str, Any]]]
"""Signature: (query, limit) -> list of {name, kind, path, similarity, ...}.

Callers (HTTP handler, MCP tool) wrap `_subs.get_module_indexer().search()`
into this shape. Runtime stays decoupled from the MCP subsystems module.
"""


# ---------------------------------------------------------------------------
# Remediation action shape
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RemediationAction:
    kind: str
    summary: str
    command: str = ""
    autorun_ok: bool = False
    confidence: float = 1.0
    explain: str = ""

    def to_payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "kind": self.kind,
            "summary": self.summary,
            "autorun_ok": self.autorun_ok,
            "confidence": round(float(self.confidence), 4),
        }
        if self.command:
            out["command"] = self.command
        if self.explain:
            out["explain"] = self.explain
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_namespace(object_kind: str) -> str | None:
    """Return the best-effort namespace prefix of a table:* object_kind.

    e.g. `table:workflow_runs` → `workflow`, `table:bug_evidence_links` → `bug`
    """
    if not object_kind.startswith("table:"):
        return None
    name = object_kind[len("table:"):]
    head = name.split("_", 1)[0]
    return head or None


def _mcp_cmd(tool: str, args: dict[str, Any]) -> str:
    """Build a `praxis workflow tools call ...` invocation."""
    return (
        f"praxis workflow tools call {tool} "
        f"--input-json '{json.dumps(args, separators=(',', ':'))}'"
    )


def _namespace_owner_suggestion(object_kind: str) -> str | None:
    """Try to derive a namespace-scoped owner from the projector's own map.

    Imported lazily so this module doesn't hard-depend on the memory
    projector package at import time.
    """
    try:
        from memory.data_dictionary_stewardship_projector import _namespace_owner
    except Exception:
        return None
    ns = _parse_namespace(object_kind)
    if not ns:
        return None
    # The projector expects a table name, not a table: prefix.
    if object_kind.startswith("table:"):
        return _namespace_owner(object_kind[len("table:"):])
    return _namespace_owner(ns)


# ---------------------------------------------------------------------------
# praxis_discover enrichment
# ---------------------------------------------------------------------------

def _discover_write_paths(
    discover: DiscoverFn | None,
    object_kind: str,
    *,
    limit: int = 3,
) -> list[str]:
    """Ask praxis_discover for the code paths most likely to write `object_kind`.

    Returns rendered "path (similarity)" strings for inclusion in the
    remediation explain text. Safe-by-default: any error (no indexer,
    no embeddings, nothing found) returns [] rather than raising.
    """
    if discover is None or not object_kind:
        return []
    # Extract the bare table name so the query matches natural-language
    # docstrings better than the `table:` URI form.
    target = object_kind.split(":", 1)[-1]
    queries = [
        f"code that writes to {target}",
        f"{target} producer / upsert / insert",
    ]
    seen: dict[str, float] = {}
    for q in queries:
        try:
            rows = discover(q, limit) or []
        except Exception:
            continue
        for r in rows:
            path = str(r.get("path") or "").strip()
            sim = float(r.get("similarity") or 0.0)
            if not path:
                continue
            if path not in seen or sim > seen[path]:
                seen[path] = sim
    top = sorted(seen.items(), key=lambda p: -p[1])[:limit]
    return [f"{p} (similarity {s:.2f})" for p, s in top]


def _discover_explain_suffix(paths: list[str]) -> str:
    if not paths:
        return ""
    return (
        "\n\nCandidate code paths (from praxis_discover):\n"
        + "\n".join(f"  • {p}" for p in paths)
    )


# ---------------------------------------------------------------------------
# Owner-missing remediation (covers pii_without_owner + sensitive_without_owner)
# ---------------------------------------------------------------------------

def _owner_immediate(
    conn: Any,
    violation: GovernanceViolation,
) -> list[RemediationAction]:
    out: list[RemediationAction] = []

    # Suggestion A: the nearest upstream/downstream owner found in the lineage
    # neighborhood (includes FK-reference walks for linkable-PII).
    nearest = _nearest_upstream_owner(conn, violation.object_kind)
    if nearest:
        out.append(RemediationAction(
            kind="mcp_tool_call",
            summary=(
                f"Set {nearest!r} as owner of {violation.object_kind} "
                f"(inherited from lineage neighborhood)"
            ),
            command=_mcp_cmd(
                "praxis_data_dictionary_stewardship",
                {
                    "action": "set",
                    "object_kind": violation.object_kind,
                    "steward_kind": "owner",
                    "steward_id": nearest,
                    "steward_type": "team",
                },
            ),
            autorun_ok=False,  # owner assignment is a governance decision
            confidence=0.85,
            explain=(
                "The nearest owner in the lineage neighborhood is the most "
                "likely correct assignee. Confirm the steward_type before "
                "running — 'team' is the default; use 'person' / 'agent' / "
                "'service' / 'role' if more accurate."
            ),
        ))

    # Suggestion B: the namespace-default owner from the stewardship
    # projector. Only suggest if different from the lineage-derived one.
    ns_owner = _namespace_owner_suggestion(violation.object_kind)
    if ns_owner and ns_owner != nearest:
        out.append(RemediationAction(
            kind="mcp_tool_call",
            summary=(
                f"Set {ns_owner!r} as owner of {violation.object_kind} "
                f"(namespace-default from projector)"
            ),
            command=_mcp_cmd(
                "praxis_data_dictionary_stewardship",
                {
                    "action": "set",
                    "object_kind": violation.object_kind,
                    "steward_kind": "owner",
                    "steward_id": ns_owner,
                    "steward_type": "service",
                },
            ),
            autorun_ok=False,
            confidence=0.70,
            explain=(
                "The stewardship namespace-owner projector already maps "
                f"this namespace prefix to {ns_owner!r}. If that's the "
                "right owner, running this command promotes the projector's "
                "suggestion into an operator-layer record."
            ),
        ))

    # Suggestion C: generic manual fallback when nothing can be inferred.
    if not out:
        out.append(RemediationAction(
            kind="mcp_tool_call",
            summary=(
                f"Decide who owns {violation.object_kind} and set them as "
                f"owner"
            ),
            command=_mcp_cmd(
                "praxis_data_dictionary_stewardship",
                {
                    "action": "set",
                    "object_kind": violation.object_kind,
                    "steward_kind": "owner",
                    "steward_id": "<team-or-person>",
                    "steward_type": "team",
                },
            ),
            autorun_ok=False,
            confidence=0.30,
            explain=(
                "No upstream owner and no namespace-default matched. "
                "Replace <team-or-person> with the responsible party. "
                "If this object is no longer in use, consider dropping "
                "the underlying table instead."
            ),
        ))
    return out


def _owner_permanent(
    conn: Any,
    violation: GovernanceViolation,
    discover: DiscoverFn | None = None,
) -> list[RemediationAction]:
    ns = _parse_namespace(violation.object_kind) or ""
    discover_paths = _discover_write_paths(discover, violation.object_kind)
    out: list[RemediationAction] = []

    # Backstop A: file an architecture-policy decision that makes the
    # invariant explicit and visible during every orient.
    tag = (
        "pii-requires-owner" if violation.policy == "pii_without_owner"
        else "sensitive-requires-owner"
    )
    out.append(RemediationAction(
        kind="operator_decision",
        summary=(
            "File an architecture-policy decision: "
            f"'{violation.policy.replace('_',' ')}' is a standing prohibition"
        ),
        command=_mcp_cmd(
            "praxis_operator_architecture_policy",
            {
                "action": "add",
                "decision_key": f"architecture-policy::governance::{tag}",
                "rationale": (
                    f"Governance invariant: every object carrying the "
                    f"{'pii' if violation.policy == 'pii_without_owner' else 'sensitive'} "
                    "tag must have at least one owner steward. Governance "
                    "heartbeat files a bug against violations."
                ),
            },
        ),
        autorun_ok=False,
        confidence=0.95,
        explain=(
            "Once recorded, every orient shows the standing order. "
            "Governance bugs cite this decision via decision_ref."
        ),
    ))

    # Backstop B: extend the namespace-owner projector to cover this
    # namespace, so future tables auto-get the owner.
    if ns and not _namespace_owner_suggestion(violation.object_kind):
        explain = (
            "Edit `_NAMESPACE_OWNERS` in "
            "`memory/data_dictionary_stewardship_projector.py` to "
            f"map `^{ns}_` to the appropriate service owner. After "
            "the next heartbeat, every future table in this "
            "namespace auto-gets an owner."
        )
        explain += _discover_explain_suffix(discover_paths)
        out.append(RemediationAction(
            kind="code_change",
            summary=(
                f"Add namespace prefix '{ns}_' to the stewardship "
                "namespace-owner projector"
            ),
            command="",
            autorun_ok=False,
            confidence=0.60,
            explain=explain,
        ))

    # Backstop C: add a quality rule that makes owner-presence a
    # first-class, evaluable check at the data-dictionary level.
    out.append(RemediationAction(
        kind="quality_rule",
        summary=(
            "Add a presence-of-owner quality rule so failures surface "
            "in the quality axis alongside the governance scan"
        ),
        command=_mcp_cmd(
            "praxis_data_dictionary_quality",
            {
                "action": "set",
                "object_kind": violation.object_kind,
                "rule_kind": "owner_present",
                "severity": "error",
                "expression": {
                    "check": "stewardship_effective",
                    "steward_kind": "owner",
                    "count_gte": 1,
                },
                "description": (
                    "This object must have at least one `owner` steward."
                ),
            },
        ),
        autorun_ok=False,
        confidence=0.65,
        explain=(
            "With this rule in place, the failure shows up on both "
            "axes (quality + governance), and the quality-runs history "
            "gives a time series of the gap."
        ),
    ))
    return out


# ---------------------------------------------------------------------------
# Failing-rule remediation
# ---------------------------------------------------------------------------

def _owner_immediate_wrapped(
    conn: Any,
    violation: GovernanceViolation,
    discover: DiscoverFn | None = None,
) -> list[RemediationAction]:
    """Thin wrapper so `_POLICY_IMMEDIATE` dispatch carries the discover kwarg."""
    return _owner_immediate(conn, violation)


def _rule_immediate(
    conn: Any,
    violation: GovernanceViolation,
) -> list[RemediationAction]:
    out: list[RemediationAction] = []
    rule = violation.rule_kind
    obj = violation.object_kind

    # A: re-evaluate the rule — if the last fail was transient, the
    # violation clears without further work.
    out.append(RemediationAction(
        kind="mcp_tool_call",
        summary=(
            f"Re-run the {rule!r} rule on {obj} to check whether the "
            "latest failure is transient"
        ),
        command=_mcp_cmd(
            "praxis_data_dictionary_quality",
            {"action": "evaluate", "object_kind": obj, "rule_kind": rule},
        ),
        autorun_ok=True,  # pure observation; safe to auto-run
        confidence=0.95,
        explain=(
            "A pass on the re-run closes the governance violation on the "
            "next heartbeat cycle automatically."
        ),
    ))

    # B: disable the rule if it's known-deprecated (operator judgment call).
    out.append(RemediationAction(
        kind="mcp_tool_call",
        summary=(
            f"If the rule is no longer applicable, disable {rule!r} on "
            f"{obj}"
        ),
        command=_mcp_cmd(
            "praxis_data_dictionary_quality",
            {
                "action": "set",
                "object_kind": obj,
                "rule_kind": rule,
                "enabled": False,
                "description": "Disabled via governance remediation.",
            },
        ),
        autorun_ok=False,
        confidence=0.55,
        explain=(
            "Use this only when the rule has become obsolete. Prefer "
            "fixing the underlying data when the rule still encodes a "
            "true invariant."
        ),
    ))

    # C: downgrade severity from error → warn if the rule is informational.
    out.append(RemediationAction(
        kind="mcp_tool_call",
        summary=(
            f"Downgrade severity of {rule!r} on {obj} from error → warn "
            "so it no longer trips governance"
        ),
        command=_mcp_cmd(
            "praxis_data_dictionary_quality",
            {
                "action": "set",
                "object_kind": obj,
                "rule_kind": rule,
                "severity": "warn",
            },
        ),
        autorun_ok=False,
        confidence=0.45,
        explain=(
            "Governance only promotes error-severity rule failures to "
            "bugs. Warn-severity failures still get recorded by the "
            "quality heartbeat but don't escalate."
        ),
    ))

    return out


def _rule_permanent(
    conn: Any,
    violation: GovernanceViolation,
    discover: DiscoverFn | None = None,
) -> list[RemediationAction]:
    rule = violation.rule_kind
    obj = violation.object_kind
    discover_paths = _discover_write_paths(discover, obj)

    upstream_explain = (
        f"Use `praxis workflow discover 'write path for {obj}'` "
        "to find the code that produces this data, then repair "
        "it. Long-term this is the only fix that actually reduces "
        "governance noise."
    )
    upstream_explain += _discover_explain_suffix(discover_paths)
    return [
        RemediationAction(
            kind="code_change",
            summary=(
                "Fix the upstream producer so the rule passes on every "
                "run"
            ),
            command="",
            autorun_ok=False,
            confidence=0.75,
            explain=upstream_explain,
        ),
        RemediationAction(
            kind="heartbeat_config",
            summary=(
                f"Attach an auto-heal heartbeat module for the {rule!r} "
                "rule class"
            ),
            command="",
            autorun_ok=False,
            confidence=0.45,
            explain=(
                "Some rule failures have deterministic repairs (null "
                "backfills, range clamps, reference regeneration). "
                "Wrap the repair as a HeartbeatModule so the next "
                "cycle both detects and heals."
            ),
        ),
        RemediationAction(
            kind="operator_decision",
            summary=(
                "File an architecture-policy decision documenting the "
                "rule's intent so future maintainers know whether "
                "disabling it is safe"
            ),
            command=_mcp_cmd(
                "praxis_operator_architecture_policy",
                {
                    "action": "add",
                    "decision_key": (
                        f"architecture-policy::data-quality::{rule}-rationale"
                    ),
                    "rationale": (
                        f"Why {rule!r} on {obj} matters, who it protects, "
                        "and under what conditions disabling it is "
                        "appropriate."
                    ),
                },
            ),
            autorun_ok=False,
            confidence=0.70,
        ),
    ]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_POLICY_IMMEDIATE = {
    "pii_without_owner": _owner_immediate_wrapped,
    "sensitive_without_owner": _owner_immediate_wrapped,
    "error_rule_failing": lambda conn, v, discover=None: _rule_immediate(conn, v),
}

_POLICY_PERMANENT = {
    "pii_without_owner": _owner_permanent,
    "sensitive_without_owner": _owner_permanent,
    "error_rule_failing": _rule_permanent,
}


def suggest_remediation(
    conn: Any,
    violation: GovernanceViolation,
    *,
    discover: DiscoverFn | None = None,
) -> dict[str, Any]:
    """Return `{violation, immediate, permanent}` for a single violation.

    When `discover` is supplied (a callable with the `DiscoverFn` shape),
    permanent remediations are enriched with the top write-path matches
    from the codebase behavioral index.
    """
    imm_fn = _POLICY_IMMEDIATE.get(violation.policy)
    perm_fn = _POLICY_PERMANENT.get(violation.policy)
    immediate = (
        [a.to_payload() for a in imm_fn(conn, violation, discover=discover)]
        if imm_fn else []
    )
    permanent = (
        [a.to_payload() for a in perm_fn(conn, violation, discover=discover)]
        if perm_fn else []
    )
    return {
        "violation": violation.to_payload(),
        "immediate": immediate,
        "permanent": permanent,
    }


def suggest_all_remediations(
    conn: Any,
    *,
    discover: DiscoverFn | None = None,
) -> dict[str, Any]:
    """Scan for violations and attach remediation plans to each."""
    violations = scan_violations(conn)
    plans = [suggest_remediation(conn, v, discover=discover) for v in violations]
    return {
        "total_violations": len(violations),
        "plans": plans,
    }


def inline_immediate_summary(
    conn: Any,
    violation: GovernanceViolation,
    max_lines: int = 3,
) -> str:
    """Render a compact, human-readable immediate-remediation summary.

    Used by the bug-filing path to embed the top suggestion directly in
    the bug description so operators see the fix without having to
    re-query the governance tool.
    """
    imm_fn = _POLICY_IMMEDIATE.get(violation.policy)
    if not imm_fn:
        return ""
    actions = imm_fn(conn, violation)
    if not actions:
        return ""
    lines = ["Immediate remediation (highest-confidence first):"]
    for action in actions[:max_lines]:
        lines.append(f"  • {action.summary}")
        if action.command:
            lines.append(f"      $ {action.command}")
    return "\n".join(lines)


__all__ = [
    "RemediationAction",
    "inline_immediate_summary",
    "suggest_all_remediations",
    "suggest_remediation",
]
