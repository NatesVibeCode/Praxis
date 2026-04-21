"""Generic audit + resolution primitive.

The pattern we've built 3+ times (governance, drift, wiring) abstracts
cleanly into three types and a registry:

    Finding           — an immutable observation: "something is wrong here"
    ResolutionPattern — a named, deterministic recipe that fixes one
                        shape of Finding
    PlannedAction     — a concrete application of a Pattern to a specific
                        Finding, carrying enough data to execute

The whole point of this module is that **executing a PlannedAction does
not require reasoning**. The structured shape lets either deterministic
code or a smaller/cheaper model apply the fix. The judgment step
("which Pattern fits this Finding?") is the only part that may need an
LLM, and even then, rule-based mapping often suffices.

Registration shape:

    register_audit(AuditContract(
        audit_kind="wiring",
        finding_kind="hardcoded_localhost",
        scanner=audit_hard_paths,                   # Callable[[conn], list[Finding]]
        default_pattern="use_env_var_for_host",
    ))

    register_pattern(ResolutionPattern(
        name="use_env_var_for_host",
        applies_to={"hardcoded_localhost"},
        planner=_plan_host_env_var,                 # Callable[[Finding], PlannedAction]
        executor=_execute_regex_replace,            # Callable[[conn, PlannedAction], Result]
        deterministic=True,
    ))

No DB tables required — this module just composes existing audit runtimes.
Persistence, if needed, is the caller's responsibility.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from runtime.primitive_contracts import bug_open_status_values


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Finding:
    """A single observation from one audit."""

    audit_kind: str           # wiring | governance | drift | ...
    finding_kind: str         # hardcoded_localhost | unreferenced_decision | ...
    subject: str              # file:line | decision_key | object_kind
    evidence: str             # short human-readable snippet
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        """Stable dedup key across scans."""
        return f"{self.audit_kind}.{self.finding_kind}.{self.subject}"


@dataclass(frozen=True)
class PlannedAction:
    """A concrete fix for a specific Finding.

    `action_kind` is drawn from a small enum of deterministic operations:

      * regex_replace      — sed-style edit on a file:line
      * sql_update         — parameterized SQL UPDATE (e.g., tombstone decision)
      * audit_rule_exclude — add the finding's subject to an exclusion set
      * mark_table_retired — explicit tombstone on a table via operator tag
      * operator_review    — cannot be resolved deterministically; file for human

    `args` carries everything the executor needs. No open-ended prose —
    the structured shape is what lets a small model OR pure code apply it.
    """

    pattern_name: str
    action_kind: str
    subject: str              # same subject as the Finding
    args: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    confidence: float = 1.0
    autorun_ok: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern_name,
            "action": self.action_kind,
            "subject": self.subject,
            "args": dict(self.args),
            "description": self.description,
            "confidence": round(float(self.confidence), 4),
            "autorun_ok": self.autorun_ok,
        }


@dataclass(frozen=True)
class AuditContract:
    """Register an audit source into the primitive."""

    audit_kind: str
    finding_kind: str
    scanner: Callable[..., Iterable[Finding]]
    default_pattern: str | None = None
    needs_conn: bool = True

    def scan(self, conn: Any) -> list[Finding]:
        if self.needs_conn:
            out = list(self.scanner(conn) or [])
        else:
            out = list(self.scanner() or [])
        # Make sure every yielded Finding carries our (audit, finding_kind).
        normalized: list[Finding] = []
        for f in out:
            if isinstance(f, Finding):
                normalized.append(f)
            elif isinstance(f, dict):
                normalized.append(Finding(
                    audit_kind=self.audit_kind,
                    finding_kind=self.finding_kind,
                    subject=str(f.get("subject") or ""),
                    evidence=str(f.get("evidence") or ""),
                    details=dict(f.get("details") or {}),
                ))
            else:
                # Assume it's a WiringFinding / GovernanceViolation style
                # dataclass — adapt the common shape.
                normalized.append(_adapt_existing(f, self.audit_kind, self.finding_kind))
        return normalized


#  Cost tier lattice — ordered cheapest → most expensive.
#
#  Aligned with `provider_model_candidates.route_tier` ∈ {low, medium,
#  high} so the scheduler can resolve a pattern's cost_tier to an actual
#  provider/model candidate via the routing authority — no vendor name
#  hardcoded anywhere in this module. The two endpoints of the lattice
#  are `deterministic` (no model at all) and `human` (can't be
#  autonomously satisfied).
#
#  Never write `haiku`/`sonnet`/`opus`/`gpt-...` etc. in pattern
#  metadata — the provider registry decides which concrete model
#  serves each route_tier.
_COST_TIER_ORDER = ("deterministic", "low", "medium", "high", "human")


def tier_rank(tier: str) -> int:
    try:
        return _COST_TIER_ORDER.index(tier)
    except ValueError:
        return len(_COST_TIER_ORDER)  # unknown = most-expensive


def tier_at_most(candidate: str, ceiling: str) -> bool:
    """True if `candidate` is no more expensive than `ceiling`."""
    return tier_rank(candidate) <= tier_rank(ceiling)


@dataclass(frozen=True)
class ResolutionPattern:
    """A named recipe that turns a Finding into a PlannedAction.

    Attributes that drive on-rails execution:

      * deterministic  — planner + executor are pure code; no model call
      * cost_tier      — cheapest route_tier that can *invoke* this
                          pattern correctly. Values aligned with
                          provider_model_candidates.route_tier:
                          'deterministic' (no model needed), 'low',
                          'medium', 'high', or 'human' (requires
                          judgment no model can supply). Scheduler
                          prefers cheaper tiers and resolves the
                          concrete model via the routing authority.
      * preconditions  — informal schema describing what a Finding must
                          carry for this pattern to apply. Kept loose
                          for now (just field names); formalize later if
                          type-driven tool selection becomes load-bearing.
      * postconditions — what the system state looks like after apply.
                          "finding_resolved" | "audit_scope_tightened" |
                          "record_tombstoned" | "tag_set".
    """

    name: str
    applies_to: frozenset[str]
    planner: Callable[[Finding], PlannedAction | None]
    executor: Callable[[Any, PlannedAction], dict[str, Any]] | None = None
    deterministic: bool = True
    cost_tier: str = "deterministic"     # deterministic | low | medium | high | human
    preconditions: tuple[str, ...] = ()
    postconditions: tuple[str, ...] = ()

    def plan(self, finding: Finding) -> PlannedAction | None:
        if finding.finding_kind not in self.applies_to:
            return None
        return self.planner(finding)


def _adapt_existing(obj: Any, audit_kind: str, finding_kind: str) -> Finding:
    """Best-effort adapter for legacy finding dataclasses (WiringFinding etc.).

    Looks for .subject / .evidence / .details attributes. Unknown shapes
    fall back to stringification.
    """
    return Finding(
        audit_kind=audit_kind,
        finding_kind=getattr(obj, "kind", finding_kind) or finding_kind,
        subject=str(getattr(obj, "subject", "") or ""),
        evidence=str(getattr(obj, "evidence", "") or ""),
        details=dict(getattr(obj, "details", {}) or {}),
    )


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

_AUDIT_REGISTRY: dict[tuple[str, str], AuditContract] = {}
_PATTERN_REGISTRY: dict[str, ResolutionPattern] = {}


def register_audit(contract: AuditContract) -> None:
    key = (contract.audit_kind, contract.finding_kind)
    _AUDIT_REGISTRY[key] = contract


def register_pattern(pattern: ResolutionPattern) -> None:
    _PATTERN_REGISTRY[pattern.name] = pattern


def registered_audits() -> list[AuditContract]:
    return list(_AUDIT_REGISTRY.values())


def registered_patterns() -> list[ResolutionPattern]:
    return list(_PATTERN_REGISTRY.values())


# ---------------------------------------------------------------------------
# Planner — turn findings into planned actions
# ---------------------------------------------------------------------------

def _pick_pattern(finding: Finding) -> ResolutionPattern | None:
    """Pick the registered pattern that matches this finding's kind.

    If multiple patterns apply, the first registered one wins. For
    ambiguous cases a smarter policy (or LLM) would go here, but the
    whole point of the primitive is to keep this step rule-based.
    """
    for p in _PATTERN_REGISTRY.values():
        if finding.finding_kind in p.applies_to:
            return p
    return None


def plan_resolution(finding: Finding) -> PlannedAction | None:
    pat = _pick_pattern(finding)
    if pat is None:
        return PlannedAction(
            pattern_name="operator_review",
            action_kind="operator_review",
            subject=finding.subject,
            description=(
                f"No deterministic pattern matches {finding.finding_kind!r}; "
                "needs human triage."
            ),
            confidence=0.0,
            autorun_ok=False,
        )
    return pat.plan(finding)


def plan_all(conn: Any, *, max_tier: str | None = None) -> dict[str, Any]:
    """Run every registered audit + plan resolutions for each finding.

    When `max_tier` is set (e.g. 'deterministic', 'low'), patterns
    whose cost_tier exceeds the ceiling are marked `gated_by_tier=True`
    in the payload — so the caller sees what would be skipped. Apply
    still refuses to execute tier-exceeding patterns via the same check.

    Returns a structured payload grouped by pattern so the operator can
    scan the plan at a glance.
    """
    all_findings: list[Finding] = []
    for contract in _AUDIT_REGISTRY.values():
        try:
            all_findings.extend(contract.scan(conn))
        except Exception:  # noqa: BLE001
            # Audit failures are non-fatal — the operator sees the gap
            # on the next heartbeat's snapshot count rather than via
            # an exception bubbling up.
            continue

    plans: list[dict[str, Any]] = []
    by_pattern: dict[str, int] = {}
    by_cost_tier: dict[str, int] = {}
    deterministic_count = 0
    gated_count = 0
    ceiling = max_tier or "human"    # default: all tiers allowed

    for f in all_findings:
        action = plan_resolution(f)
        if action is None:
            continue
        pat = _PATTERN_REGISTRY.get(action.pattern_name)
        pattern_tier = pat.cost_tier if pat else "human"
        gated = not tier_at_most(pattern_tier, ceiling)
        if gated:
            gated_count += 1

        entry = {
            "finding": {
                "audit_kind": f.audit_kind,
                "finding_kind": f.finding_kind,
                "subject": f.subject,
                "evidence": f.evidence,
                "details": dict(f.details),
            },
            "action": action.to_payload(),
            "cost_tier": pattern_tier,
            "gated_by_tier": gated,
        }
        plans.append(entry)
        by_pattern[action.pattern_name] = by_pattern.get(action.pattern_name, 0) + 1
        by_cost_tier[pattern_tier] = by_cost_tier.get(pattern_tier, 0) + 1
        if pat and pat.deterministic and action.action_kind != "operator_review":
            deterministic_count += 1

    return {
        "total_findings": len(all_findings),
        "total_plans": len(plans),
        "deterministic": deterministic_count,
        "needs_review": len(plans) - deterministic_count,
        "max_tier_ceiling": ceiling,
        "gated_by_tier": gated_count,
        "by_pattern": by_pattern,
        "by_cost_tier": by_cost_tier,
        "plans": plans,
    }


# ---------------------------------------------------------------------------
# Execution (dry-run by default; applies only when apply=True)
# ---------------------------------------------------------------------------

def execute_action(
    conn: Any,
    action: PlannedAction,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Run a single planned action. Pattern-owned executor handles details.

    Dry-run returns `{would_do: <descriptor>}` without side effects. Apply
    mode returns `{applied: True, result: ...}`. Patterns without an
    executor always no-op with a `{skipped: "no executor"}` payload.
    """
    pat = _PATTERN_REGISTRY.get(action.pattern_name)
    if pat is None or pat.executor is None:
        return {
            "pattern": action.pattern_name,
            "subject": action.subject,
            "skipped": "no executor registered",
        }
    if not apply:
        return {
            "pattern": action.pattern_name,
            "subject": action.subject,
            "would_do": action.to_payload(),
        }
    try:
        result = pat.executor(conn, action)
        return {
            "pattern": action.pattern_name,
            "subject": action.subject,
            "applied": True,
            "result": result,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "pattern": action.pattern_name,
            "subject": action.subject,
            "applied": False,
            "error": str(exc),
        }


def apply_autorunnable(
    conn: Any,
    *,
    only_patterns: set[str] | None = None,
    max_per_pattern: int = 200,
    max_tier: str = "deterministic",
) -> dict[str, Any]:
    """Apply every action whose pattern is deterministic + autorun_ok.

    This is the 'on-rails' path: findings whose resolution requires zero
    human judgment get resolved automatically. Every applied action is
    logged in the returned payload so the caller (heartbeat / HTTP /
    MCP) can record a receipt.

    Patterns are gated on three criteria to count as autorunnable:

      1. pattern.deterministic is True
      2. action.autorun_ok is True (planner-authored per finding)
      3. pattern.name is in `only_patterns` when that filter is set

    Anything else falls through to the needs-review pile and is
    returned unchanged.
    """
    plan = plan_all(conn, max_tier=max_tier)
    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    counts_per_pattern: dict[str, int] = {}

    for entry in plan["plans"]:
        action_payload = entry["action"]
        pattern_name = action_payload["pattern"]
        action_kind = action_payload["action"]
        if action_kind == "operator_review":
            skipped.append({
                **entry,
                "reason": "requires operator review",
            })
            continue
        pat = _PATTERN_REGISTRY.get(pattern_name)
        if pat is None:
            skipped.append({**entry, "reason": "unknown pattern"})
            continue
        if not tier_at_most(pat.cost_tier, max_tier):
            skipped.append({
                **entry,
                "reason": f"pattern tier {pat.cost_tier} > ceiling {max_tier}",
            })
            continue
        if not pat.deterministic:
            skipped.append({**entry, "reason": "non-deterministic pattern"})
            continue
        if not action_payload.get("autorun_ok"):
            skipped.append({**entry, "reason": "action not marked autorun_ok"})
            continue
        if only_patterns is not None and pattern_name not in only_patterns:
            skipped.append({**entry, "reason": "pattern not in autorun allowlist"})
            continue
        if counts_per_pattern.get(pattern_name, 0) >= max_per_pattern:
            skipped.append({**entry, "reason": f"pattern cap {max_per_pattern} reached"})
            continue

        action = PlannedAction(
            pattern_name=pattern_name,
            action_kind=action_kind,
            subject=action_payload["subject"],
            args=dict(action_payload.get("args") or {}),
            description=action_payload.get("description", ""),
            confidence=float(action_payload.get("confidence", 1.0)),
            autorun_ok=bool(action_payload.get("autorun_ok")),
        )
        result = execute_action(conn, action, apply=True)
        counts_per_pattern[pattern_name] = counts_per_pattern.get(pattern_name, 0) + 1
        if result.get("applied"):
            applied.append({"finding": entry["finding"], "result": result})
        elif result.get("error"):
            errors.append({"finding": entry["finding"], "error": result.get("error")})
        else:
            skipped.append({**entry, "reason": result.get("skipped", "no-op")})

    return {
        "applied_count": len(applied),
        "skipped_count": len(skipped),
        "error_count":   len(errors),
        "by_pattern":    counts_per_pattern,
        "applied":       applied[:20],          # truncate for payload size
        "applied_total": len(applied),
        "skipped":       skipped[:20],
        "errors":        errors,
    }


def derive_playbook() -> dict[str, Any]:
    """Build the usage playbook from the registries.

    Hand-written prose goes stale the moment a new pattern is added.
    Deriving the playbook from registered audits + patterns means the
    guidance the MCP tool hands to callers is always in sync with
    actual capabilities — no drift, no documentation debt.
    """
    # Group patterns by cost_tier so the caller can pick a ceiling
    # without reading every entry.
    by_tier: dict[str, list[dict[str, Any]]] = {t: [] for t in _COST_TIER_ORDER}
    for p in _PATTERN_REGISTRY.values():
        by_tier.setdefault(p.cost_tier, []).append({
            "name": p.name,
            "applies_to": sorted(p.applies_to),
            "deterministic": p.deterministic,
            "has_executor": p.executor is not None,
            "postconditions": list(p.postconditions),
        })

    # Which finding_kinds have SOME pattern, which don't (= operator_review fall-through)
    covered_kinds: set[str] = set()
    for p in _PATTERN_REGISTRY.values():
        covered_kinds.update(p.applies_to)
    uncovered_kinds = sorted({
        c.finding_kind for c in _AUDIT_REGISTRY.values()
    } - covered_kinds)

    return {
        "purpose": (
            "Platform-native audit remediation. The registries — not "
            "this document — are the source of truth; everything below "
            "is generated from them. Ask `registered` at any time."
        ),
        "actions": [
            {
                "name": "registered",
                "shape": "read-only",
                "returns": "list of audits + patterns + metadata",
            },
            {
                "name": "plan",
                "shape": "read-only",
                "args": {"max_tier": "|".join(_COST_TIER_ORDER)},
                "returns": "every Finding + proposed PlannedAction, gated by max_tier",
            },
            {
                "name": "apply",
                "shape": "mutating",
                "args": {
                    "max_tier": "|".join(_COST_TIER_ORDER) + " (default: none)",
                    "only_patterns": "optional allowlist",
                    "max_per_pattern": "int cap per batch",
                },
                "returns": (
                    "applied / skipped / errors. Only patterns where "
                    "pattern.deterministic AND action.autorun_ok AND "
                    "pattern.cost_tier <= max_tier execute."
                ),
            },
        ],
        "cost_tier_lattice": list(_COST_TIER_ORDER),
        "patterns_by_tier": by_tier,
        "uncovered_finding_kinds": uncovered_kinds,
        "execution_guarantees": [
            "patterns with cost_tier='deterministic' need zero model calls",
            "patterns with deterministic=False are never autorun",
            "apply with max_tier='deterministic' is safe to schedule unattended",
            "apply with max_tier='human' still only runs patterns whose "
            "action.autorun_ok is True — human judgment about whether to "
            "run the pattern at all is separate from tier selection",
            "model tier names (low/medium/high) are aligned with "
            "provider_model_candidates.route_tier — concrete model "
            "resolution goes through the provider routing authority",
        ],
    }


# ---------------------------------------------------------------------------
# TaskContract — declarative goal + verifier + autonomous escalation
# ---------------------------------------------------------------------------
#
# Design point (set by the operator): "the human will probably never look
# at the findings." This changes the dead-end of the pipeline. Instead of
# parking hard cases in a review queue nobody reads, contracts escalate
# unresolved state into the governance bug authority — which IS visible
# via scorecard, heartbeat status, and bug tools.
#
# A Verifier is a small structured predicate the runtime can evaluate
# without a model:
#
#   {"kind": "no_findings_of_kind",
#    "args": {"audit_kind": "wiring", "finding_kind": "code_orphan_view"}}
#
# Verifier kinds registered here:
#   - no_findings_of_kind    — plan_all returns no findings of (kind)
#   - sql_count_zero         — the given SELECT returns zero rows
#   - scorecard_metric_gte   — scorecard metric >= threshold

_VERIFIER_REGISTRY: dict[str, Callable[[Any, dict[str, Any]], tuple[bool, dict[str, Any]]]] = {}


def register_verifier(
    kind: str,
    fn: Callable[[Any, dict[str, Any]], tuple[bool, dict[str, Any]]],
) -> None:
    _VERIFIER_REGISTRY[kind] = fn


def _verify_no_findings_of_kind(
    conn: Any, args: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    audit_kind = str(args.get("audit_kind") or "")
    finding_kind = str(args.get("finding_kind") or "")
    plan = plan_all(conn)
    matching = [
        p for p in plan.get("plans", [])
        if p["finding"]["audit_kind"] == audit_kind
        and p["finding"]["finding_kind"] == finding_kind
    ]
    return (len(matching) == 0, {
        "matching": len(matching),
        "audit_kind": audit_kind,
        "finding_kind": finding_kind,
    })


def _verify_sql_count_zero(
    conn: Any, args: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    query = str(args.get("query") or "").strip()
    if not query:
        return False, {"error": "empty query"}
    try:
        rows = conn.execute(query)
        count = len(rows or [])
        return (count == 0, {"count": count, "query": query[:120]})
    except Exception as exc:  # noqa: BLE001
        return False, {"error": str(exc), "query": query[:120]}


def _verify_scorecard_metric_gte(
    conn: Any, args: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    metric = str(args.get("metric") or "")
    threshold = float(args.get("threshold") or 0.0)
    try:
        from runtime.data_dictionary_governance import compute_scorecard

        card = compute_scorecard(conn)
    except Exception as exc:  # noqa: BLE001
        return False, {"error": f"scorecard unavailable: {exc}"}

    # Metric can live at top level (compliance_score, grade) or inside
    # the `metrics` sub-dict (owned_pct, wiring_pct, etc.).
    value: Any = card.get(metric)
    if value is None:
        value = (card.get("metrics") or {}).get(metric)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False, {"error": f"metric {metric!r} not numeric", "value": value}
    return (numeric >= threshold, {
        "metric": metric, "threshold": threshold, "value": numeric,
    })


register_verifier("no_findings_of_kind", _verify_no_findings_of_kind)
register_verifier("sql_count_zero", _verify_sql_count_zero)
register_verifier("scorecard_metric_gte", _verify_scorecard_metric_gte)


@dataclass(frozen=True)
class Verifier:
    kind: str
    args: dict[str, Any] = field(default_factory=dict)

    def evaluate(self, conn: Any) -> tuple[bool, dict[str, Any]]:
        fn = _VERIFIER_REGISTRY.get(self.kind)
        if fn is None:
            return False, {"error": f"unknown verifier kind: {self.kind}"}
        return fn(conn, self.args)


@dataclass(frozen=True)
class TaskContract:
    """Declarative spec: 'this state must hold; verify this way.'

    The runtime proves completion; the contract does not prescribe the
    sequence of steps, just the destination. Patterns + tier ceilings
    determine HOW the runtime gets there.
    """

    name: str
    goal: str
    verify: Verifier
    max_tier: str = "deterministic"                     # tier ceiling for apply
    allowed_patterns: frozenset[str] | None = None      # narrow, or None for all
    max_iterations: int = 3
    # Default escalation: file a governance bug so the unresolved state
    # surfaces on the scorecard. Set to None to silently fail.
    escalate_as_bug: bool = True


_CONTRACT_REGISTRY: dict[str, TaskContract] = {}


def register_contract(contract: TaskContract) -> None:
    _CONTRACT_REGISTRY[contract.name] = contract


def registered_contracts() -> list[TaskContract]:
    return list(_CONTRACT_REGISTRY.values())


def _escalate_contract_to_bug(
    conn: Any,
    contract: TaskContract,
    verdict: dict[str, Any],
    applied_count: int,
    iterations: int,
) -> str | None:
    """Turn unresolved contract state into a visible governance bug.

    Uses the existing bug_tracker surface. decision_ref is stable on
    contract name so subsequent cycles dedupe against the open bug.
    """
    try:
        from runtime.bug_tracker import BugCategory, BugSeverity, BugTracker

        tracker = BugTracker(conn)
        # Dedup against an open bug on this contract.
        open_rows = conn.execute(
            "SELECT bug_id FROM bugs "
            "WHERE decision_ref = $1 AND status = ANY($2) "
            "ORDER BY opened_at DESC LIMIT 1",
            f"contract.{contract.name}",
            list(bug_open_status_values()),
        )
        if open_rows:
            return str(open_rows[0]["bug_id"])

        bug, _ = tracker.file_bug(
            title=f"Task contract {contract.name} unresolved after {iterations} iter(s)",
            severity=BugSeverity.P2,
            category=BugCategory.ARCHITECTURE,
            description=(
                f"goal:          {contract.goal}\n"
                f"verify.kind:   {contract.verify.kind}\n"
                f"verify.args:   {dict(contract.verify.args)}\n"
                f"max_tier:      {contract.max_tier}\n"
                f"iterations:    {iterations}\n"
                f"applied:       {applied_count}\n"
                f"last_verdict:  {verdict}\n\n"
                "Filed automatically by the audit primitive when the "
                "contract's allowed tier could not satisfy the goal. "
                "Raise the tier, extend allowed_patterns, or accept the "
                "stalled state."
            ),
            filed_by="audit_primitive",
            source_kind="contract_escalation",
            decision_ref=f"contract.{contract.name}",
            tags=("audit_contract", contract.name),
        )
        return bug.bug_id
    except Exception:
        return None


def execute_contract(conn: Any, contract: TaskContract) -> dict[str, Any]:
    """Run the contract's verify→plan→apply→re-verify loop to completion.

    Returns a structured summary: whether the contract is satisfied
    after the run, how many iterations it took, what was applied, and
    whether the unresolved state was escalated to a bug.
    """
    applied_total = 0
    iterations = 0
    last_verdict: dict[str, Any] = {}
    satisfied = False

    # Short-circuit if already satisfied.
    ok, verdict = contract.verify.evaluate(conn)
    last_verdict = dict(verdict)
    if ok:
        return {
            "contract": contract.name,
            "satisfied_already": True,
            "satisfied": True,
            "iterations": 0,
            "applied_total": 0,
            "last_verdict": last_verdict,
        }

    for i in range(contract.max_iterations):
        iterations = i + 1
        result = apply_autorunnable(
            conn,
            only_patterns=(
                set(contract.allowed_patterns) if contract.allowed_patterns
                else None
            ),
            max_tier=contract.max_tier,
        )
        applied_total += int(result.get("applied_count") or 0)

        ok, verdict = contract.verify.evaluate(conn)
        last_verdict = dict(verdict)
        if ok:
            satisfied = True
            break
        # If no actions applied AND not satisfied, further iterations won't
        # help — exit early.
        if int(result.get("applied_count") or 0) == 0:
            break

    escalated_bug_id: str | None = None
    if not satisfied and contract.escalate_as_bug:
        escalated_bug_id = _escalate_contract_to_bug(
            conn, contract, last_verdict, applied_total, iterations,
        )

    return {
        "contract": contract.name,
        "satisfied_already": False,
        "satisfied": satisfied,
        "iterations": iterations,
        "applied_total": applied_total,
        "last_verdict": last_verdict,
        "escalated_bug_id": escalated_bug_id,
    }


def execute_all_contracts(conn: Any) -> dict[str, Any]:
    results = [execute_contract(conn, c) for c in _CONTRACT_REGISTRY.values()]
    return {
        "count": len(results),
        "satisfied": sum(1 for r in results if r.get("satisfied")),
        "escalated": sum(1 for r in results if r.get("escalated_bug_id")),
        "results": results,
    }


__all__ = [
    "AuditContract",
    "Finding",
    "PlannedAction",
    "ResolutionPattern",
    "TaskContract",
    "Verifier",
    "apply_autorunnable",
    "derive_playbook",
    "execute_action",
    "execute_all_contracts",
    "execute_contract",
    "plan_all",
    "plan_resolution",
    "register_audit",
    "register_contract",
    "register_pattern",
    "register_verifier",
    "registered_audits",
    "registered_contracts",
    "registered_patterns",
    "tier_at_most",
    "tier_rank",
]
