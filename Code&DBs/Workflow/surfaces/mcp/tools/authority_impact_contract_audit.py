"""MCP tool for authority impact contract drift audit."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_audit_authority_impact_contract(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.impact_contract_audit.scan",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_audit_authority_impact_contract": (
        tool_praxis_audit_authority_impact_contract,
        {
            "kind": "search",
            "operation_names": ["authority.impact_contract_audit.scan"],
            "description": (
                "Audit a list of paths for impact-contract coverage. Each path is "
                "classified as not_authority_bearing, covered (a candidate exists "
                "with this path in intended_files), or uncovered (authority-bearing "
                "but no backing candidate). Closes the gap left by the candidate-"
                "path enforcement chain: catches direct commits, scripted edits, "
                "and hot-fixes that bypass the gated pipeline."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of repository-relative paths to audit. Typically "
                            "the output of `git diff --name-only` over an audit window."
                        ),
                    },
                },
                "required": ["paths"],
            },
        },
    ),
}
