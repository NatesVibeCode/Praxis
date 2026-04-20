"""Register the wiring audits + their resolution patterns into the primitive.

This file is imported (once, eagerly) to populate the audit_primitive
registries. Each registration is pure — no side effects beyond the
module-level registry mutation.

Resolution patterns registered here:

  * use_env_var_for_host     — replace hardcoded `localhost` / `127.0.0.1`
                                with `os.environ.get(VAR, default)`
  * use_env_var_for_port     — replace `:5432` etc. with env-var lookup
  * use_path_relative        — replace `/Users/...` paths with
                                Path(__file__)-relative construction
  * tombstone_legacy_decision— mark aged `legacy_fallback` decisions
                                as superseded
  * mark_table_retired       — explicit operator tag for `_legacy` /
                                `_ready` tables nothing references
  * audit_rule_exclude       — add view-shaped object_kinds to the
                                orphan scan's exclusion set
  * operator_review          — fall-through for anything unhandled
"""
from __future__ import annotations

from typing import Any

from runtime.audit_primitive import (
    AuditContract,
    Finding,
    PlannedAction,
    ResolutionPattern,
    TaskContract,
    Verifier,
    register_audit,
    register_contract,
    register_pattern,
)
from runtime.data_dictionary_wiring_audit import (
    audit_code_orphan_tables,
    audit_hard_paths,
    audit_unreferenced_decisions,
)


# ---------------------------------------------------------------------------
# Scanner adapters — turn existing audit outputs into Findings
# ---------------------------------------------------------------------------

def _scan_hard_path_localhost(conn: Any) -> list[Finding]:
    out: list[Finding] = []
    for f in audit_hard_paths():
        if f.kind != "hardcoded_localhost":
            continue
        out.append(Finding(
            audit_kind="wiring",
            finding_kind="hardcoded_localhost",
            subject=f.subject,
            evidence=f.evidence,
            details=dict(f.details),
        ))
    return out


def _scan_hard_path_port(conn: Any) -> list[Finding]:
    out: list[Finding] = []
    for f in audit_hard_paths():
        if f.kind != "hardcoded_port":
            continue
        out.append(Finding(
            audit_kind="wiring",
            finding_kind="hardcoded_port",
            subject=f.subject,
            evidence=f.evidence,
            details=dict(f.details),
        ))
    return out


def _scan_hard_path_user(conn: Any) -> list[Finding]:
    out: list[Finding] = []
    for f in audit_hard_paths():
        if f.kind != "absolute_user_path":
            continue
        out.append(Finding(
            audit_kind="wiring",
            finding_kind="absolute_user_path",
            subject=f.subject,
            evidence=f.evidence,
            details=dict(f.details),
        ))
    return out


def _scan_decisions(conn: Any) -> list[Finding]:
    out: list[Finding] = []
    for f in audit_unreferenced_decisions(conn):
        decision_kind = str(f.details.get("decision_kind") or "unknown")
        # Split into sub-finding-kinds so the pattern matcher can fan
        # out: legacy_fallback gets auto-tombstoned, architecture_policy
        # gets triage, everything else goes to operator review.
        if decision_kind == "legacy_fallback":
            fk = "unreferenced_decision_legacy"
        elif decision_kind == "architecture_policy":
            fk = "unreferenced_decision_policy"
        else:
            fk = "unreferenced_decision_other"
        out.append(Finding(
            audit_kind="wiring",
            finding_kind=fk,
            subject=f.subject,
            evidence=f.evidence,
            details=dict(f.details),
        ))
    return out


def _scan_orphans(conn: Any) -> list[Finding]:
    out: list[Finding] = []
    for f in audit_code_orphan_tables(conn):
        object_kind = f.subject
        name = object_kind[len("table:"):] if object_kind.startswith("table:") else ""
        # Views and legacy-named tables fan out to specific sub-kinds.
        if name.startswith("v_"):
            fk = "code_orphan_view"
        elif name.endswith(("_legacy", "_ready")):
            fk = "code_orphan_legacy_named"
        else:
            fk = "code_orphan_other"
        out.append(Finding(
            audit_kind="wiring",
            finding_kind=fk,
            subject=object_kind,
            evidence=f.evidence,
            details=dict(f.details),
        ))
    return out


# ---------------------------------------------------------------------------
# Resolution pattern planners (pure functions: Finding -> PlannedAction)
# ---------------------------------------------------------------------------

# Map of common host-referencing call sites to their env-var names.
_HOST_ENV_HINTS = {
    "postgres": "WORKFLOW_DB_HOST",
    "db":       "WORKFLOW_DB_HOST",
    "sql":      "WORKFLOW_DB_HOST",
    "pg":       "WORKFLOW_DB_HOST",
    "redis":    "PRAXIS_REDIS_HOST",
    "api":      "PRAXIS_API_HOST",
    "server":   "PRAXIS_API_HOST",
    "cli":      "PRAXIS_API_HOST",
}


def _guess_host_env_var(subject: str, evidence: str) -> str:
    lowered = f"{subject} {evidence}".lower()
    for hint, var in _HOST_ENV_HINTS.items():
        if hint in lowered:
            return var
    return "PRAXIS_DEFAULT_HOST"


def _plan_host_env_var(finding: Finding) -> PlannedAction | None:
    env_var = _guess_host_env_var(finding.subject, finding.evidence)
    match_str = str(finding.details.get("match") or "localhost")
    return PlannedAction(
        pattern_name="use_env_var_for_host",
        action_kind="regex_replace",
        subject=finding.subject,
        args={
            "file_line": finding.subject,
            "search": match_str,
            "replace": f'os.environ.get("{env_var}", "{match_str}")',
            "env_var": env_var,
        },
        description=(
            f"Replace hardcoded {match_str!r} with "
            f"os.environ.get({env_var!r}, {match_str!r}) at {finding.subject}"
        ),
        confidence=0.7,  # env-var name is a guess; confirmation recommended
        autorun_ok=False,
    )


def _plan_port_env_var(finding: Finding) -> PlannedAction | None:
    port = str(finding.details.get("port") or "5432")
    # Env var name derived from well-known ports.
    var_map = {
        "5432": "WORKFLOW_DB_PORT",
        "6379": "PRAXIS_REDIS_PORT",
        "8420": "PRAXIS_API_PORT",
        "8000": "PRAXIS_API_PORT",
        "9000": "PRAXIS_METRICS_PORT",
        "3000": "PRAXIS_UI_PORT",
        "5000": "PRAXIS_SIDECAR_PORT",
    }
    env_var = var_map.get(port, f"PRAXIS_PORT_{port}")
    return PlannedAction(
        pattern_name="use_env_var_for_port",
        action_kind="regex_replace",
        subject=finding.subject,
        args={
            "file_line": finding.subject,
            "search": f":{port}",
            "replace": f':{{int(os.environ.get("{env_var}", "{port}"))}}',
            "env_var": env_var,
            "port": port,
        },
        description=(
            f"Replace hardcoded :{port} with env-var {env_var!r} "
            f"at {finding.subject}"
        ),
        confidence=0.9,
        autorun_ok=False,
    )


def _plan_path_relative(finding: Finding) -> PlannedAction | None:
    return PlannedAction(
        pattern_name="use_path_relative",
        action_kind="regex_replace",
        subject=finding.subject,
        args={
            "file_line": finding.subject,
            "search": str(finding.details.get("match") or ""),
            "replacement_hint": "Path(__file__).resolve().parents[N] / 'subpath'",
        },
        description=(
            f"Replace absolute path at {finding.subject} with a "
            "Path(__file__)-relative construction. Depth N depends on "
            "file location — reviewer should verify."
        ),
        confidence=0.6,
        autorun_ok=False,
    )


def _plan_tombstone_decision(finding: Finding) -> PlannedAction | None:
    return PlannedAction(
        pattern_name="tombstone_legacy_decision",
        action_kind="sql_update",
        subject=finding.subject,
        args={
            "table": "operator_decisions",
            "update": {"decision_status": "superseded"},
            "where":  {"decision_key": finding.subject},
            "rationale": (
                "legacy_fallback decision is >90d old and nothing cites "
                "it; auto-tombstone."
            ),
        },
        description=(
            f"Mark decision {finding.subject[:60]} as superseded "
            "(legacy_fallback with no citations)."
        ),
        confidence=0.95,
        autorun_ok=True,  # safe: only transitions status, doesn't delete
    )


def _plan_policy_auto_bind(finding: Finding) -> PlannedAction | None:
    # Architecture policies get queued for review — they genuinely need
    # human judgment to decide "is this still enforced?". Tagging them
    # in the plan makes the shape explicit; the resolver that picks up
    # operator_review can batch these for one focused triage pass.
    return PlannedAction(
        pattern_name="auto_bind_or_review",
        action_kind="operator_review",
        subject=finding.subject,
        args={
            "decision_key": finding.subject,
            "review_question": (
                "Is this policy still enforced? If yes, add a "
                "semantic_assertion with bound_decision_id. If no, "
                "transition status to 'superseded' with rationale."
            ),
        },
        description=(
            f"architecture_policy {finding.subject[:50]} — human "
            "judgment needed to close the loop."
        ),
        confidence=0.3,
        autorun_ok=False,
    )


def _plan_mark_retired(finding: Finding) -> PlannedAction | None:
    return PlannedAction(
        pattern_name="mark_table_retired",
        action_kind="set_operator_tag",
        subject=finding.subject,
        args={
            "object_kind": finding.subject,
            "tag_key": "lifecycle",
            "tag_value": "retired",
            "rationale": (
                "Table name ends in _legacy/_ready and no code "
                "references it; flagging as retired."
            ),
        },
        description=(
            f"Tag {finding.subject} as lifecycle=retired "
            "(no code refs + legacy naming)."
        ),
        confidence=0.85,
        autorun_ok=False,  # operator should confirm deletion intent
    )


def _plan_exclude_view(finding: Finding) -> PlannedAction | None:
    return PlannedAction(
        pattern_name="audit_rule_exclude",
        action_kind="audit_rule_exclude",
        subject=finding.subject,
        args={
            "audit_kind": "wiring",
            "finding_kind": "code_orphan_view",
            "rationale": (
                "Views are accessed via SQL, not Python imports; "
                "code-orphan check is a false positive for views."
            ),
        },
        description=(
            f"Exclude {finding.subject} from future code-orphan scans "
            "(views don't need Python import references)."
        ),
        confidence=1.0,
        autorun_ok=True,  # just an audit-config tweak
    )


# ---------------------------------------------------------------------------
# Executors — the deterministic code that actually applies an action
# ---------------------------------------------------------------------------

def _execute_audit_rule_exclude(
    conn: Any, action: PlannedAction,
) -> dict[str, Any]:
    """Insert a row into `audit_exclusions` so future scans skip this subject."""
    args = action.args
    row = conn.fetchrow(
        """
        INSERT INTO audit_exclusions
            (audit_kind, finding_kind, subject, rationale, created_by)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (audit_kind, finding_kind, subject) DO UPDATE
            SET rationale = EXCLUDED.rationale
        RETURNING exclusion_id::text, created_at
        """,
        str(args.get("audit_kind") or "wiring"),
        str(args.get("finding_kind") or "code_orphan_view"),
        action.subject,
        str(args.get("rationale") or ""),
        "audit_primitive_autorunner",
    )
    return {
        "exclusion_id": row["exclusion_id"] if row else None,
        "audit_kind":   args.get("audit_kind"),
        "finding_kind": args.get("finding_kind"),
        "subject":      action.subject,
    }


def _execute_tombstone_legacy_decision(
    conn: Any, action: PlannedAction,
) -> dict[str, Any]:
    """Mark a legacy_fallback decision as 'superseded'."""
    decision_key = action.subject
    prior = conn.fetchrow(
        "SELECT decision_status FROM operator_decisions WHERE decision_key = $1",
        decision_key,
    )
    if prior is None:
        return {"skipped": "decision no longer exists", "decision_key": decision_key}
    prior_status = str(prior["decision_status"])
    if prior_status == "superseded":
        return {"skipped": "already superseded", "decision_key": decision_key}

    conn.execute(
        """
        UPDATE operator_decisions
           SET decision_status = 'superseded',
               updated_at = now(),
               effective_to = COALESCE(effective_to, now())
         WHERE decision_key = $1
        """,
        decision_key,
    )
    return {
        "decision_key": decision_key,
        "prior_status": prior_status,
        "new_status":   "superseded",
    }


# ---------------------------------------------------------------------------
# Registration — single import-time side effect
# ---------------------------------------------------------------------------

def register_all() -> None:
    # Audits
    register_audit(AuditContract(
        audit_kind="wiring", finding_kind="hardcoded_localhost",
        scanner=_scan_hard_path_localhost, default_pattern="use_env_var_for_host",
    ))
    register_audit(AuditContract(
        audit_kind="wiring", finding_kind="hardcoded_port",
        scanner=_scan_hard_path_port, default_pattern="use_env_var_for_port",
    ))
    register_audit(AuditContract(
        audit_kind="wiring", finding_kind="absolute_user_path",
        scanner=_scan_hard_path_user, default_pattern="use_path_relative",
    ))
    register_audit(AuditContract(
        audit_kind="wiring", finding_kind="unreferenced_decision",
        scanner=_scan_decisions,
    ))
    register_audit(AuditContract(
        audit_kind="wiring", finding_kind="code_orphan_table",
        scanner=_scan_orphans,
    ))

    # Patterns — cost_tier says "what's the cheapest route_tier that
    # can correctly decide to invoke this pattern?". Values align with
    # provider_model_candidates.route_tier:
    #   deterministic = no model
    #   low           = fast / cheap general model
    #   medium        = standard general model
    #   high          = capability-heavy reasoning model
    #   human         = can't be autonomously resolved
    #
    # Never hardcode vendor model names (haiku / sonnet / opus / gpt-*)
    # here — the provider routing authority resolves route_tier to a
    # concrete model slug at invocation time.

    # Code-editing patterns still need human review (`autorun_ok=False`),
    # but a `low`-tier model could correctly IDENTIFY that the pattern
    # applies and pre-fill the action args. The human is only doing the
    # final yes/no.
    register_pattern(ResolutionPattern(
        name="use_env_var_for_host",
        applies_to=frozenset({"hardcoded_localhost"}),
        planner=_plan_host_env_var,
        deterministic=True,
        cost_tier="low",
        postconditions=("code_edit_proposed",),
    ))
    register_pattern(ResolutionPattern(
        name="use_env_var_for_port",
        applies_to=frozenset({"hardcoded_port"}),
        planner=_plan_port_env_var,
        deterministic=True,
        cost_tier="low",
        postconditions=("code_edit_proposed",),
    ))
    register_pattern(ResolutionPattern(
        name="use_path_relative",
        applies_to=frozenset({"absolute_user_path"}),
        planner=_plan_path_relative,
        deterministic=True,
        cost_tier="medium",   # path depth requires file-location analysis
        postconditions=("code_edit_proposed",),
    ))
    register_pattern(ResolutionPattern(
        name="tombstone_legacy_decision",
        applies_to=frozenset({"unreferenced_decision_legacy"}),
        planner=_plan_tombstone_decision,
        executor=_execute_tombstone_legacy_decision,
        deterministic=True,
        cost_tier="deterministic",
        postconditions=("record_tombstoned", "finding_resolved"),
    ))
    register_pattern(ResolutionPattern(
        name="auto_bind_or_review",
        applies_to=frozenset({"unreferenced_decision_policy"}),
        planner=_plan_policy_auto_bind,
        deterministic=False,
        cost_tier="medium",   # requires reading decision text + code
        postconditions=("review_queued",),
    ))
    register_pattern(ResolutionPattern(
        name="mark_table_retired",
        applies_to=frozenset({"code_orphan_legacy_named"}),
        planner=_plan_mark_retired,
        deterministic=True,
        cost_tier="low",   # low-tier model can confirm naming heuristic
        postconditions=("tag_set",),
    ))
    register_pattern(ResolutionPattern(
        name="audit_rule_exclude",
        applies_to=frozenset({"code_orphan_view"}),
        planner=_plan_exclude_view,
        executor=_execute_audit_rule_exclude,
        deterministic=True,
        cost_tier="deterministic",   # views detectable by regex (v_*)
        postconditions=("audit_scope_tightened", "finding_resolved"),
    ))

    # --- Default TaskContracts -------------------------------------------
    #
    # These run every heartbeat cycle. Each contract is a declaration:
    # "this state must hold; verify this way; escalate to a bug if not."
    # The operator never manually triages; scorecard + bugs surface the
    # outcomes.

    register_contract(TaskContract(
        name="clear_view_orphan_findings",
        goal="No SQL-view code-orphan findings remain (views are accessed via SQL, not Python)",
        verify=Verifier(
            kind="no_findings_of_kind",
            args={"audit_kind": "wiring", "finding_kind": "code_orphan_view"},
        ),
        max_tier="deterministic",
        allowed_patterns=frozenset({"audit_rule_exclude"}),
        max_iterations=2,
    ))

    register_contract(TaskContract(
        name="tombstone_aged_legacy_fallback_decisions",
        goal="No legacy_fallback decisions >90d old without citations remain",
        verify=Verifier(
            kind="sql_count_zero",
            args={"query": (
                "SELECT 1 FROM operator_decisions d "
                "WHERE d.decision_kind = 'legacy_fallback' "
                "  AND d.decision_status IN ('recorded','admitted','decided') "
                "  AND d.decided_at < now() - interval '90 days' "
                "  AND NOT EXISTS ("
                "    SELECT 1 FROM bugs b WHERE b.decision_ref = d.decision_key"
                "  ) "
                "  AND NOT EXISTS ("
                "    SELECT 1 FROM semantic_assertions a "
                "    WHERE a.bound_decision_id = d.operator_decision_id"
                "      AND a.assertion_status = 'active'"
                "  ) "
                "LIMIT 1"
            )},
        ),
        max_tier="deterministic",
        allowed_patterns=frozenset({"tombstone_legacy_decision"}),
        max_iterations=2,
    ))

    register_contract(TaskContract(
        name="maintain_wiring_health",
        goal="Governance scorecard wiring_pct >= 0.5 (VPS-migration readiness floor)",
        verify=Verifier(
            kind="scorecard_metric_gte",
            args={"metric": "wiring_pct", "threshold": 0.5},
        ),
        max_tier="deterministic",       # no model-assisted patterns yet
        allowed_patterns=None, # any pattern allowed within tier
        max_iterations=1,
    ))


__all__ = ["register_all"]
