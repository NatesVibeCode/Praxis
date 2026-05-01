"""MCP tools for healer authority — catalog list, runs list, run.

Companion to surfaces/mcp/tools/verifier_catalog.py. Healers are the
repair side of the verifier subsystem: a healer is bound to one or more
verifiers and runs to attempt a fix when the verifier fails. Three
built-ins ship today (schema_bootstrap, receipt_provenance_backfill,
proof_backfill), each guarded auto_mode.

This module gives operators and workflow packets first-class addressable
read+write surfaces over the healer registry without reaching into
runtime.verifier_authority directly:

- praxis_healer_catalog (read) — list registered healers + bound verifiers
- praxis_healer_runs_list (read) — query past healing_runs
- praxis_healer_run (write) — manually trigger a heal

CQRS ops behind these tools live at:
- runtime.operations.queries.healer_catalog (catalog + runs list)
- runtime.operations.commands.healer_run (run command)
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_healer_catalog(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="healer.catalog.list",
        payload=payload,
    )


def tool_praxis_healer_runs_list(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="healer.runs.list",
        payload=payload,
    )


def tool_praxis_healer_run(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="healer.run",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_healer_catalog": (
        tool_praxis_healer_catalog,
        {
            "kind": "search",
            "operation_names": ["healer.catalog.list"],
            "description": (
                "List registered healer authority refs from healer_registry. "
                "Returns each healer's healer_ref, executor_kind, action_ref, "
                "auto_mode (manual / assisted / automatic), safety_mode "
                "(guarded / unsafe), enabled state, and the bound "
                "verifier_refs (which verifiers this healer can repair "
                "after a failure). Use this before picking a healer for "
                "praxis_healer_run, or to inspect the repair surface."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "default": True,
                        "description": (
                            "Filter to enabled healers only. Pass false to "
                            "include disabled rows."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 100,
                        "description": "Maximum healers to return.",
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
    "praxis_healer_runs_list": (
        tool_praxis_healer_runs_list,
        {
            "kind": "search",
            "operation_names": ["healer.runs.list"],
            "description": (
                "List past healing_runs newest-first, optionally filtered "
                "by healer_ref, verifier_ref (which verifier triggered the "
                "heal), target_kind / target_ref, status (succeeded / "
                "failed / skipped / error), and trailing-window. Returns "
                "full run rows including action+post-verification outputs."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "healer_ref": {
                        "type": "string",
                        "description": "Filter to runs of a specific healer_ref.",
                    },
                    "verifier_ref": {
                        "type": "string",
                        "description": "Filter to heals triggered by a specific verifier_ref.",
                    },
                    "target_kind": {
                        "type": "string",
                        "enum": ["platform", "receipt", "run", "path"],
                        "description": "Filter by target kind.",
                    },
                    "target_ref": {
                        "type": "string",
                        "description": "Filter by exact target_ref (path / receipt_id / run_id).",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["succeeded", "failed", "skipped", "error"],
                        "description": (
                            "Run outcome. 'succeeded' = healer action succeeded AND "
                            "post-verification passed. 'failed' = either step "
                            "didn't pass. 'skipped' = healer action returned skipped. "
                            "'error' = exception during the run."
                        ),
                    },
                    "since_iso": {
                        "type": "string",
                        "description": "ISO-8601 timestamp; only runs at or after this attempted_at are returned.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 100,
                        "description": "Maximum runs to return.",
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
    "praxis_healer_run": (
        tool_praxis_healer_run,
        {
            "kind": "write",
            "operation_names": ["healer.run"],
            "description": (
                "Run a registered healer to attempt repair after a "
                "verifier failure. verifier_ref is required (every heal "
                "is invoked in the context of a verifier whose result it "
                "tries to fix). healer_ref is OPTIONAL — when omitted, "
                "the runtime auto-resolves from the verifier's bound "
                "healers and errors if zero or multiple are bound. The "
                "runtime reruns the bound verifier as post-verification, "
                "so 'status: succeeded' means BOTH the healer action "
                "returned succeeded AND the post-verification passed."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["verifier_ref"],
                "properties": {
                    "verifier_ref": {
                        "type": "string",
                        "description": "Verifier whose failure this heal is trying to repair.",
                    },
                    "healer_ref": {
                        "type": "string",
                        "description": (
                            "Specific healer to run. Optional — when "
                            "omitted, runtime resolves from verifier "
                            "bindings (errors if 0 or >1 bound)."
                        ),
                    },
                    "target_kind": {
                        "type": "string",
                        "enum": ["platform", "receipt", "run", "path"],
                        "default": "platform",
                        "description": "Target kind. Must match what the underlying verifier accepts.",
                    },
                    "target_ref": {
                        "type": "string",
                        "default": "",
                        "description": "Target reference (defaults to the verifier_ref's normalized fallback).",
                    },
                    "inputs": {
                        "type": "object",
                        "description": "Per-call input overrides merged onto verifier+healer defaults.",
                    },
                    "record_run": {
                        "type": "boolean",
                        "default": True,
                        "description": "Write the healing_runs row. False = dry-run.",
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
}
