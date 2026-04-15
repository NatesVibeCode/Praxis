"""Tools: praxis_diagnose."""
from __future__ import annotations

from typing import Any

from runtime.workflow_diagnose import diagnose_run


def tool_praxis_diagnose(params: dict) -> dict:
    """Diagnose one workflow run by id and return receipt + provider health context."""
    run_id = str(params.get("run_id") or "").strip()
    if not run_id:
        return {"error": "run_id is required"}
    return diagnose_run(run_id)


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_diagnose": (
        tool_praxis_diagnose,
        {
            "description": (
                "Diagnose one workflow run by id. Combines the receipt, failure classification, "
                "and provider health into a single operator-facing report.\n\n"
                "USE WHEN: you have a specific run_id and want to know why it failed or what its "
                "current health context looks like.\n\n"
                "EXAMPLE: praxis_diagnose(run_id='run_abc123')\n\n"
                "DO NOT USE: for general health checks (use praxis_health) or broad receipt search "
                "(use praxis_receipts)."
            ),
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "recommended_alias": "diagnose",
                "when_to_use": (
                    "Inspect one workflow run by id to combine receipt details, failure class, and "
                    "provider health into a single diagnosis."
                ),
                "when_not_to_use": "Do not use it for broad health checks or general receipt search.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Diagnose a specific run",
                        "input": {"run_id": "run_abc123"},
                    }
                ],
            },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Workflow run id or a unique suffix recognized by workflow_diagnose.",
                    },
                },
                "required": ["run_id"],
            },
        },
    ),
}

