"""Progressive operator next-action surface.

``praxis_next`` is intentionally one front door with action modes rather than
six new leaf tools. The handler is a thin MCP wrapper over the gateway-backed
``operator.next`` query operation, so receipts and catalog binding remain the
authority path.
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems

from ..subsystems import _subs

_OPERATION_NAME = "operator.next"


def tool_praxis_next(params: dict, _progress_emitter=None) -> dict[str, Any]:
    payload = dict(params or {})
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Computing Praxis next action: {payload.get('action') or 'next'}",
        )
    result = execute_operation_from_subsystems(
        _subs,
        operation_name=_OPERATION_NAME,
        payload=payload,
        requested_mode="query",
    )
    if _progress_emitter:
        status = "ok" if isinstance(result, dict) and result.get("ok", True) else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - praxis_next {status}",
        )
    if isinstance(result, dict):
        result.setdefault("_meta", {})["dispatch_path"] = "gateway"
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_next": (
        tool_praxis_next,
        {
            "kind": "analytics",
            "description": (
                "Progressive-disclosure operator front door for deciding what to do next. "
                "Composes existing Praxis authority instead of exposing the raw tool pile: "
                "catalog metadata, manifests, workflow run state, queue state, provider "
                "slots, host-resource leases, verifier refs, and retry/launch doctrine.\n\n"
                "ACTIONS:\n"
                "  next            - return the smallest legal composite action menu\n"
                "  launch_gate     - decide whether launch/retry/fleet execution is allowed\n"
                "  failure_triage  - group run failures and state retry eligibility\n"
                "  manifest_audit  - check manifest scope/tool/verifier/artifact alignment\n"
                "  toolsmith       - dedupe and shape a proposed new tool\n"
                "  unlock_frontier - math-shaped typed action frontier / blocker analysis\n\n"
                "USE WHEN: the operator/model faces too many possible tools, wants a "
                "canary/fleet safety check, needs retry discipline, or wants to grow the "
                "tool surface without adding catalog sprawl."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "next",
                            "launch_gate",
                            "failure_triage",
                            "manifest_audit",
                            "toolsmith",
                            "unlock_frontier",
                        ],
                        "default": "next",
                        "description": "Progressive action mode.",
                    },
                    "detail": {
                        "type": "string",
                        "enum": ["brief", "standard", "evidence", "repair"],
                        "default": "brief",
                        "description": "How much detail to return.",
                    },
                    "intent": {
                        "type": "string",
                        "description": "Plain-English operator intent used to narrow actions or dedupe tools.",
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Workflow run to inspect for launch gating or failure triage.",
                    },
                    "proof_run_id": {
                        "type": "string",
                        "description": "Canary/proof run used before fleet launch or retry.",
                    },
                    "spec_path": {
                        "type": "string",
                        "description": "Workflow spec path to audit or launch-gate.",
                    },
                    "manifest_path": {
                        "type": "string",
                        "description": "Manifest JSON path to audit.",
                    },
                    "manifest": {
                        "type": "object",
                        "description": "Inline manifest object to audit.",
                    },
                    "tool_name": {
                        "type": "string",
                        "description": "Proposed or existing tool name for toolsmith dedupe.",
                    },
                    "state": {
                        "type": "object",
                        "description": "Typed state already known by the caller; keys can satisfy tool required inputs.",
                        "default": {},
                    },
                    "allowed_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional allowlist for unlock_frontier legal-tool analysis.",
                    },
                    "include_blocked": {
                        "type": "boolean",
                        "default": True,
                        "description": "Include blocked legal-tool candidates and repair actions.",
                    },
                    "include_mutating": {
                        "type": "boolean",
                        "default": False,
                        "description": "Allow write/launch/session tools in legal-tool analysis.",
                    },
                    "facts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Known typed facts for unlock_frontier.",
                    },
                    "fleet_size": {
                        "type": "integer",
                        "default": 1,
                        "description": "Number of jobs/runs being considered for broad launch.",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 8,
                        "description": "Maximum recommendations or rows to return.",
                    },
                },
                "x-action-requirements": {
                    "failure_triage": {"required": ["run_id"]},
                    "manifest_audit": {"anyOf": [["manifest"], ["manifest_path"], ["spec_path"]]},
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "stable",
                "when_to_use": (
                    "Ask what the next legal operator move is, gate launches/retries, "
                    "triage failures, audit manifests, dedupe tool ideas, or compute "
                    "the unlock frontier."
                ),
                "when_not_to_use": (
                    "Do not use it to mutate workflow state or launch work directly; "
                    "it is a read-only decision surface."
                ),
                "risks": {
                    "default": "read",
                    "actions": {
                        "next": "read",
                        "launch_gate": "read",
                        "failure_triage": "read",
                        "manifest_audit": "read",
                        "toolsmith": "read",
                        "unlock_frontier": "read",
                    },
                },
                "examples": [
                    {
                        "title": "Get the next operator move",
                        "input": {
                            "action": "next",
                            "intent": "fire workflow fleet safely",
                            "fleet_size": 12,
                        },
                    },
                    {
                        "title": "Gate a fleet launch from a canary",
                        "input": {
                            "action": "launch_gate",
                            "proof_run_id": "workflow_abc123",
                            "fleet_size": 20,
                            "detail": "repair",
                        },
                    },
                    {
                        "title": "Compute the unlock frontier",
                        "input": {
                            "action": "unlock_frontier",
                            "facts": ["queue:healthy", "providers:observable"],
                            "detail": "standard",
                        },
                    },
                ],
            },
        },
    )
}
