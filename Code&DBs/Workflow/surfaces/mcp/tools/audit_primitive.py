"""Tool: praxis_audit_primitive — generic scan/plan/resolve surface.

Exposes the audit primitive to jobs, workflows, and the MCP catalog.
A job can orchestrate audit remediation end-to-end by calling actions
in order:

    1. `playbook`   — read the structured usage playbook (small-model
                       friendly; explains which action to call when)
    2. `registered` — list what audits + resolution patterns exist
    3. `plan`       — run every audit, get PlannedAction per finding
    4. `apply`      — execute only patterns whose pattern.deterministic
                       is True AND action.autorun_ok is True, optionally
                       narrowed by `only_patterns`
"""
from __future__ import annotations

from typing import Any

from runtime.audit_primitive import (
    apply_autorunnable,
    derive_playbook,
    execute_all_contracts,
    execute_contract,
    plan_all,
    registered_audits,
    registered_contracts,
    registered_patterns,
)

# Eager registration — populate registries at import time so the tool
# catalog / live MCP server see the audits + patterns.
from runtime.audit_primitive_wiring import register_all as _register_wiring

_register_wiring()

from ..subsystems import _subs


def _conn() -> Any:
    return _subs.get_pg_conn()


# ---------------------------------------------------------------------------
# Playbook — the explanation every job should read first
# ---------------------------------------------------------------------------

_PLAYBOOK = {
    "purpose": (
        "Automated remediation of platform audit findings (wiring, "
        "governance, drift). Designed so a small / cheap model — or "
        "even deterministic code — can execute cleanly without "
        "judgment calls, by choosing from a small enum of PATTERNS."
    ),
    "sequence": [
        {
            "step": 1,
            "action": "registered",
            "why": (
                "Discover what audits + resolution patterns this "
                "installation has. Different environments may register "
                "different ones."
            ),
        },
        {
            "step": 2,
            "action": "plan",
            "why": (
                "Run every registered audit; every finding gets mapped "
                "to a PlannedAction. The response includes "
                "`deterministic` and `needs_review` counts."
            ),
        },
        {
            "step": 3,
            "action": "apply",
            "why": (
                "Execute only auto-safe patterns (deterministic + "
                "autorun_ok). Pass `only_patterns: [...]` to narrow. "
                "Always dry-run-view first by inspecting the plan."
            ),
            "safety": (
                "Apply NEVER touches code files. It only performs "
                "reversible DB-level actions (audit_rule_exclude, "
                "sql_update tombstones, set_operator_tag). Code-edit "
                "patterns like `use_env_var_for_host` are marked "
                "autorun_ok=False and get skipped here."
            ),
        },
    ],
    "companion_tools": [
        {
            "tool": "praxis_data_dictionary_wiring_audit",
            "purpose": (
                "Direct access to individual audits (hard_paths, "
                "decisions, orphans, trend). Use this when you need "
                "raw findings without the plan/apply envelope."
            ),
        },
        {
            "tool": "praxis_data_dictionary_governance",
            "purpose": (
                "Governance scorecard + enforce loop. The scorecard "
                "absorbs wiring counts so you can watch audit "
                "resolution in a single number."
            ),
        },
        {
            "tool": "praxis_data_dictionary_stewardship",
            "purpose": (
                "Tag-editing surface used by `set_operator_tag` "
                "actions. Small model uses this after an auto-apply "
                "leaves a `lifecycle=retired` tag behind."
            ),
        },
    ],
    "tier_guidance": {
        "tier_1_always_safe": [
            "audit_rule_exclude — only tweaks scanner config, "
            "zero behavior change",
        ],
        "tier_2_reversible_db_write": [
            "tombstone_legacy_decision — SQL UPDATE to status='superseded'; "
            "reversible with a matching UPDATE back. INSPECT a sample row "
            "before auto-applying any batch > 10.",
            "mark_table_retired — adds a lifecycle=retired operator tag; "
            "doesn't change behavior, easy to clear.",
        ],
        "tier_3_never_auto_apply": [
            "use_env_var_for_host / use_env_var_for_port / use_path_relative "
            "— edit production code. Propose a diff, require human "
            "review, never auto-apply.",
            "auto_bind_or_review — architecture policies need judgment "
            "about current enforcement state.",
            "operator_review — by definition, requires a human.",
        ],
    },
    "failure_modes_to_watch_for": [
        "Audit premise mismatch: the scanner matches the finding shape "
        "but the semantic doesn't fit (e.g., `legacy_fallback` "
        "decisions don't expect `semantic_assertion` bindings — don't "
        "flag them as unreferenced). If a batch > 20 records surfaces, "
        "sample 3-5 before applying.",
        "Env-var name guessing: patterns with confidence < 0.8 should "
        "never auto-apply. `use_env_var_for_host` is 0.7 — always "
        "flagged as needs-review.",
        "Heuristic table naming: `_legacy` is unambiguous; `_ready` "
        "could mean 'prepared' or 'retired'. Don't auto-retire "
        "`_ready` tables.",
    ],
}


# ---------------------------------------------------------------------------
# Tool entrypoint
# ---------------------------------------------------------------------------

def tool_praxis_audit_primitive(params: dict[str, Any]) -> dict[str, Any]:
    """Generic scan/plan/resolve surface for platform audits."""
    action = str(params.get("action") or "playbook").lower().strip()
    max_tier = str(params.get("max_tier") or "").strip().lower() or None
    try:
        if action == "playbook":
            # Prefer the generated playbook (always in sync with registry);
            # fall back to the hand-written one only if registry is empty.
            derived = derive_playbook()
            if derived.get("patterns_by_tier") and any(
                derived["patterns_by_tier"].values()
            ):
                return {"action": "playbook", "playbook": derived}
            return {"action": "playbook", "playbook": _PLAYBOOK}

        if action == "registered":
            audits = [
                {
                    "audit_kind": c.audit_kind,
                    "finding_kind": c.finding_kind,
                    "default_pattern": c.default_pattern,
                }
                for c in registered_audits()
            ]
            patterns = [
                {
                    "name": p.name,
                    "applies_to": sorted(p.applies_to),
                    "deterministic": p.deterministic,
                    "has_executor": p.executor is not None,
                }
                for p in registered_patterns()
            ]
            return {
                "action": "registered",
                "audit_count": len(audits),
                "pattern_count": len(patterns),
                "audits": audits,
                "patterns": patterns,
            }

        if action == "plan":
            return {"action": "plan", **plan_all(_conn(), max_tier=max_tier)}

        if action == "apply":
            raw = params.get("only_patterns")
            only = None
            if isinstance(raw, (list, tuple)):
                only = {str(s) for s in raw if str(s).strip()}
            elif isinstance(raw, str) and raw.strip():
                only = {s.strip() for s in raw.split(",") if s.strip()}
            max_per = int(params.get("max_per_pattern", 200))
            # Default tier ceiling is the cheapest: 'none'. Callers who
            # want model-assisted patterns to auto-apply must explicitly
            # raise it — on-rails by default.
            return {
                "action": "apply",
                **apply_autorunnable(
                    _conn(),
                    only_patterns=only,
                    max_per_pattern=max_per,
                    max_tier=max_tier or "none",
                ),
            }

        if action == "contracts":
            rows = []
            for c in registered_contracts():
                rows.append({
                    "name": c.name,
                    "goal": c.goal,
                    "verify": {"kind": c.verify.kind, "args": dict(c.verify.args)},
                    "max_tier": c.max_tier,
                    "allowed_patterns": sorted(c.allowed_patterns) if c.allowed_patterns else None,
                    "max_iterations": c.max_iterations,
                    "escalate_as_bug": c.escalate_as_bug,
                })
            return {"action": "contracts", "count": len(rows), "contracts": rows}

        if action == "execute_contract":
            name = str(params.get("name") or "").strip()
            if not name:
                return {"error": "name is required", "status_code": 400}
            for c in registered_contracts():
                if c.name == name:
                    return {"action": "execute_contract", **execute_contract(_conn(), c)}
            return {"error": f"unknown contract: {name}", "status_code": 404}

        if action == "execute_all_contracts":
            return {
                "action": "execute_all_contracts",
                **execute_all_contracts(_conn()),
            }

        return {
            "error": f"unknown action: {action}",
            "hint": "call action='playbook' for the list of actions",
            "status_code": 400,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_audit_primitive": (
        tool_praxis_audit_primitive,
        {
            "description": (
                "Generic scan/plan/resolve surface for platform audits "
                "(wiring, governance, drift). Call action='playbook' "
                "first to read the structured usage guide; then "
                "'registered' to discover audits/patterns, 'plan' to "
                "see findings + proposed actions, 'apply' to execute "
                "auto-safe patterns. Code-editing patterns are gated "
                "behind autorun_ok=False and never fire from 'apply'."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "playbook", "registered", "plan", "apply",
                            "contracts", "execute_contract",
                            "execute_all_contracts",
                        ],
                        "default": "playbook",
                    },
                    "name": {
                        "type": "string",
                        "description": "Contract name (for execute_contract).",
                    },
                    "max_tier": {
                        "type": "string",
                        "enum": [
                            "deterministic", "low", "medium", "high", "human",
                        ],
                        "description": (
                            "Cost-tier ceiling (aligned with "
                            "provider_model_candidates.route_tier). "
                            "'deterministic' = pure-code patterns only "
                            "(zero model calls). 'low' = cheap/fast "
                            "model class. 'medium' = standard model. "
                            "'high' = capability-heavy model. 'human' = "
                            "everything visible. Default for apply is "
                            "'deterministic' (safe); default for plan "
                            "is 'human' (see everything, mark gated)."
                        ),
                    },
                    "only_patterns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Limit apply to specific pattern names. "
                            "Omit to run every auto-safe pattern."
                        ),
                    },
                    "max_per_pattern": {
                        "type": "integer",
                        "default": 200,
                        "description": (
                            "Per-batch cap so a misconfigured audit "
                            "can't nuke hundreds of rows before review."
                        ),
                    },
                },
            },
            "cli": {
                "surface": "general",
                "tier": "advanced",
                "recommended_alias": None,
                "examples": [
                    {
                        "description": "Read the playbook before doing anything.",
                        "input": {"action": "playbook"},
                    },
                    {
                        "description": "Show every audit + resolution pattern registered.",
                        "input": {"action": "registered"},
                    },
                    {
                        "description": "Dry-run: see findings + proposed actions.",
                        "input": {"action": "plan"},
                    },
                    {
                        "description": "Auto-apply ONLY the view-exclusion pattern.",
                        "input": {
                            "action": "apply",
                            "only_patterns": ["audit_rule_exclude"],
                        },
                    },
                    {
                        "description": "Auto-apply every safe pattern with a cap.",
                        "input": {"action": "apply", "max_per_pattern": 50},
                    },
                ],
                "when_to_use": (
                    "An audit-remediation job; a scheduled cleanup "
                    "heartbeat; operator wants to know 'what can be "
                    "fixed right now with zero risk?'. Always start "
                    "with `playbook` + `plan` before any `apply`."
                ),
                "when_not_to_use": (
                    "Don't use for one-off fact-finding on a specific "
                    "finding — that's what the individual audit tools "
                    "(praxis_data_dictionary_wiring_audit, etc.) are "
                    "for. Don't use for code-edit fixes — the primitive "
                    "doesn't touch source files."
                ),
            },
        },
    ),
}
