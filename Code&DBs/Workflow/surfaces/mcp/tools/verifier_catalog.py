"""MCP tools wrapping the verifier.catalog.list and verifier.runs.list
CQRS queries so agents can discover registered verifier authority refs and
inspect run history without going through the bug-resolve flow.

Thin gateway wrappers. The CQRS query operations live in
``runtime.operations.queries.verifier_catalog``; ``verifier.catalog.list``
was registered via migration 369, ``verifier.runs.list`` via the
register_operation forge path. This module only adds the MCP-tier
bindings so agents and operators can:

- list registered verifiers (target_kind, healers, enabled state) before
  picking one for a review gate, and
- inspect past verification_runs (status, target, duration, attempted_at)
  to confirm a fix actually verified — no need to walk through the
  bug-resolve flow.

Why this matters: until these wrappers landed, the verifier subsystem was
internally complete (``verifier_authority.py``, six built-in verifiers,
three healers, full verification_runs / healing_runs ledger) but only
addressable from ``praxis_bugs action=resolve``. Workflow-packet authors
and operators had no first-class read surface.
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_verifier_catalog(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="verifier.catalog.list",
        payload=payload,
    )


def tool_praxis_verifier_runs_list(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="verifier.runs.list",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_verifier_catalog": (
        tool_praxis_verifier_catalog,
        {
            "kind": "search",
            "operation_names": ["verifier.catalog.list"],
            "description": (
                "List registered verifier authority refs from "
                "verifier_registry. Returns each verifier's verifier_ref, "
                "kind (platform / receipt / run / path), enabled state, "
                "and any bound suggested-healer refs. Use this before "
                "picking a verifier for a bug-resolve, code-change "
                "preflight, or workflow-packet review gate so the chosen "
                "ref actually exists and is enabled. Backed by the "
                "verifier.catalog.list CQRS query (registered via "
                "migration 369)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "default": True,
                        "description": (
                            "Filter to enabled verifiers only. Pass false "
                            "to include disabled rows; omit (or pass true) "
                            "to see only the verifiers that the runtime "
                            "will actually execute."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 100,
                        "description": (
                            "Maximum verifiers to return. Defaults to 100, "
                            "capped at 500 (the registry should never grow "
                            "beyond a few dozen rows in practice)."
                        ),
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
    "praxis_verifier_runs_list": (
        tool_praxis_verifier_runs_list,
        {
            "kind": "search",
            "operation_names": ["verifier.runs.list"],
            "description": (
                "List past verification_runs newest-first, optionally "
                "filtered by verifier_ref, target_kind (platform / "
                "receipt / run / path), target_ref, status (passed / "
                "failed / error), and an ISO trailing-window. Use this "
                "to confirm a verifier actually ran on a target — for "
                "example, to verify a fix's evidence chain before "
                "calling a bug FIXED, or to inspect failure rates of a "
                "specific verifier ref. Returns full run rows including "
                "inputs, outputs, suggested_healer_ref, decision_ref, "
                "and duration_ms. Backed by the verifier.runs.list CQRS "
                "query."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "verifier_ref": {
                        "type": "string",
                        "description": (
                            "Filter to runs of a specific verifier_ref. "
                            "Omit to see runs across all verifiers."
                        ),
                    },
                    "target_kind": {
                        "type": "string",
                        "enum": ["platform", "receipt", "run", "path"],
                        "description": (
                            "Filter to runs against one target_kind."
                        ),
                    },
                    "target_ref": {
                        "type": "string",
                        "description": (
                            "Filter to runs against one target_ref "
                            "(e.g. an absolute file path for path-kind "
                            "verifiers, a receipt_id for receipt-kind)."
                        ),
                    },
                    "status": {
                        "type": "string",
                        "enum": ["passed", "failed", "error"],
                        "description": (
                            "Filter by run outcome. 'passed' means "
                            "the verifier signed off, 'failed' means "
                            "it ran but rejected, 'error' means the "
                            "executor itself crashed."
                        ),
                    },
                    "since_iso": {
                        "type": "string",
                        "description": (
                            "ISO-8601 timestamp; only runs at or after "
                            "this attempted_at are returned. Use for "
                            "trailing-window reads "
                            "(e.g. '2026-05-01T00:00:00Z')."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 100,
                        "description": (
                            "Maximum runs to return. Defaults to 100, "
                            "capped at 500."
                        ),
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
}
