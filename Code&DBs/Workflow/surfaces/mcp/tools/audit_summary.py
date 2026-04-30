"""MCP tool wrapping ``audit.summary`` for one-call audit aggregates.

Thin gateway wrapper. The operation itself lives in
``runtime.operations.queries.audit`` and was registered via migration
351. This module only adds the MCP-tier binding so agents can call it
directly instead of going through ``execute_operation_from_subsystems``.
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_audit_summary(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="audit.summary",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_audit_summary": (
        tool_praxis_audit_summary,
        {
            "kind": "analytics",
            "operation_names": ["audit.summary"],
            "description": (
                "Aggregate audit lens over the gateway dispatch ledger and "
                "policy-enforcement ledger. Returns trailing-window totals "
                "(receipts, completed, replayed, failed, untagged_transport) "
                "plus per-transport / per-execution-status / per-operation-kind "
                "buckets, top-10 operations with failure counts, and a "
                "compliance breakdown (admits, rejects, top tables, top "
                "policies). Backed by authority_operation_receipts + "
                "authority_compliance_receipts."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "since_hours": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 720,
                        "default": 24,
                        "description": (
                            "Trailing window in hours. Defaults to 24, capped "
                            "at 720 (30 days)."
                        ),
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
}
