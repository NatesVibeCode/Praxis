"""Tools: praxis_setup."""

from __future__ import annotations

from typing import Any

from runtime.setup_wizard import setup_payload
from ..subsystems import REPO_ROOT


def tool_praxis_setup(params: dict, _progress_emitter=None) -> dict:
    """Inspect or plan runtime-target setup through the shared setup authority."""
    del _progress_emitter
    action = str(params.get("action") or "doctor").strip().lower()
    if action not in {"doctor", "plan", "apply"}:
        return {
            "ok": False,
            "error_code": "setup.invalid_action",
            "message": "action must be one of: doctor, plan, apply",
        }
    approved = bool(params.get("yes") or params.get("approved") or params.get("apply"))
    return setup_payload(action, repo_root=REPO_ROOT, apply=approved, authority_surface="mcp")


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_setup": (
        tool_praxis_setup,
        {
            "description": (
                "Runtime-target setup authority for Praxis. Reports the active runtime_target_ref, "
                "substrate kind, API authority, DB authority, native_instance contract, workspace "
                "authority, provider-family thin sandbox image contract, and the "
                "empty_thin_sandbox_default pass/fail. USE WHEN: moving Praxis between machines, "
                "adopting an existing runtime, repointing the package at a DB, or checking that the "
                "CLI, MCP, and API are bound to the same repo-local instance. Operations belong to "
                "API/MCP; CLI and website are clients. SSH is build/deploy transport only."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["doctor", "plan", "apply"],
                        "default": "doctor",
                    },
                    "yes": {
                        "type": "boolean",
                        "default": False,
                        "description": "Explicit approval for apply-mode gates.",
                    },
                },
            },
            "cli": {
                "surface": "setup",
                "tier": "core",
                "recommended_alias": None,
                "when_to_use": (
                    "Inspect or plan runtime-target setup through the same authority as "
                    "`praxis setup doctor|plan|apply`, including the native_instance contract."
                ),
                "when_not_to_use": "Do not use as a workflow launch/status tool.",
                "risks": {
                    "default": "read",
                    "actions": {
                        "doctor": "read",
                        "plan": "read",
                        "apply": "write",
                    },
                },
                "examples": [
                    {
                        "label": "Setup doctor",
                        "input": {"action": "doctor"},
                    },
                    {
                        "label": "Setup plan",
                        "input": {"action": "plan"},
                    },
                ],
            },
        },
    ),
}
