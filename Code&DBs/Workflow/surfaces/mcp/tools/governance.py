"""Tools: praxis_governance, praxis_heal."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs


def tool_praxis_governance(params: dict) -> dict:
    """Governance: scan prompts for secrets, check scope."""
    action = params.get("action", "scan_prompt")
    gov = _subs.get_governance_filter()

    if action == "scan_prompt":
        text = params.get("text", "")
        if not text:
            return {"error": "text is required for scan_prompt"}
        result = gov.scan_prompt(text)
        if result.passed:
            return {"passed": True, "findings_count": 0}
        return {
            "passed": False,
            "blocked_reason": result.blocked_reason,
            "findings": [
                {
                    "pattern": f.pattern_name,
                    "line": f.line_number,
                    "severity": f.severity,
                    "redacted": f.redacted_match,
                }
                for f in result.findings
            ],
        }

    if action == "scan_scope":
        write_paths = params.get("write_paths", [])
        allowed_paths = params.get("allowed_paths") or None
        result = gov.scan_scope(write_paths, allowed_paths)
        if result.passed:
            return {"passed": True}
        return {
            "passed": False,
            "blocked_reason": result.blocked_reason,
            "out_of_scope": list(result.out_of_scope_paths),
        }

    return {"error": f"Unknown governance action: {action}"}


def tool_praxis_heal(params: dict) -> dict:
    """Diagnose a failure and get recovery recommendation."""
    job_label = params.get("job_label", "")
    failure_code = params.get("failure_code", "")
    stderr = params.get("stderr", "")
    if not job_label:
        return {"error": "job_label is required"}
    if not failure_code and not stderr:
        return {"error": "failure_code or stderr is required"}

    healer = _subs.get_self_healer()
    resolved_failure_code = healer.resolve_failure_code(failure_code, stderr)
    rec = healer.diagnose(job_label, failure_code, stderr)
    return {
        "action": rec.action.value,
        "reason": rec.reason,
        "confidence": round(rec.confidence, 3),
        "context_patches": list(rec.context_patches),
        "diagnostics_run": rec.diagnostics_run,
        "resolved_failure_code": resolved_failure_code,
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_governance": (
        tool_praxis_governance,
        {
            "description": (
                "Safety checks before dispatching work. Scan prompts for leaked secrets (API keys, "
                "tokens, passwords) or verify that a set of file paths falls within allowed scope.\n\n"
                "USE WHEN: you're about to submit a workflow prompt and want to verify it doesn't contain "
                "secrets, or you want to check if write paths are within allowed boundaries.\n\n"
                "EXAMPLES:\n"
                "  Scan for secrets:  praxis_governance(action='scan_prompt', text='Use key sk-abc123...')\n"
                "  Check scope:       praxis_governance(action='scan_scope', "
                "write_paths=['runtime/workflow.py'], allowed_paths=['runtime/'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'scan_prompt' or 'scan_scope'.",
                        "enum": ["scan_prompt", "scan_scope"],
                    },
                    "text": {"type": "string", "description": "Prompt text to scan (for scan_prompt)."},
                    "write_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Paths to check (for scan_scope).",
                    },
                    "allowed_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Allowed scope (for scan_scope).",
                    },
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_heal": (
        tool_praxis_heal,
        {
            "description": (
                "Diagnose why a workflow job failed and get a recommended recovery action: retry "
                "(transient error), escalate (needs human attention), skip (non-critical), or halt "
                "(stop the pipeline).\n\n"
                "USE WHEN: a workflow job failed and you need to decide what to do next. Pass the "
                "job_label plus failure_code from the receipt, stderr from the runner, or both.\n\n"
                "EXAMPLE: praxis_heal(job_label='build_api', failure_code='EXEC_TIMEOUT', "
                "stderr='Process killed after 120s')\n"
                "EXAMPLE: praxis_heal(job_label='build_api', "
                "stderr='failure_code must be a non-empty string')\n\n"
                "DO NOT USE: for retrying a job (use praxis_workflow action='retry' after getting the recommendation)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_label": {"type": "string", "description": "The failed job label."},
                    "failure_code": {
                        "type": "string",
                        "description": "Failure code from the receipt, if available.",
                    },
                    "stderr": {
                        "type": "string",
                        "description": (
                            "Stderr output from the failed job; used to infer a stable failure code "
                            "when the failure envelope is broken."
                        ),
                    },
                },
                "required": ["job_label"],
            },
        },
    ),
}
