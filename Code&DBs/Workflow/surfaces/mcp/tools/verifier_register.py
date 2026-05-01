"""MCP tools: praxis_verifier_register + praxis_healer_register.

Forge-path tools that close the gap noted in the strategic memo: adding
a new verifier or healer today requires a SQL migration. With these
tools, an operator or agent can call praxis_verifier_register /
praxis_healer_register and the runtime upserts the registry row through
a receipt-backed gateway dispatch — same dogfooding pattern that
integration_register (migration 400) uses for new MCP integrations.

Backed by:
- runtime.operations.commands.verifier_register (CQRS command, migration 414)
- runtime.operations.commands.healer_register   (CQRS command, migration 415)
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_verifier_register(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="verifier.register",
        payload=payload,
    )


def tool_praxis_healer_register(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="healer.register",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_verifier_register": (
        tool_praxis_verifier_register,
        {
            "kind": "write",
            "operation_names": ["verifier.register"],
            "description": (
                "Register (or update) a verifier authority ref without "
                "authoring a SQL migration. Upserts a verifier_registry "
                "row idempotently and optionally creates "
                "verifier_healer_bindings for any healer_refs you pass "
                "via bind_healer_refs. Per the registry CHECK constraint, "
                "exactly one of builtin_ref OR verification_ref must be "
                "set, matching verifier_kind. Use this instead of "
                "writing a migration when adding a new verifier."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["verifier_ref", "display_name", "verifier_kind", "decision_ref"],
                "properties": {
                    "verifier_ref": {
                        "type": "string",
                        "description": "Stable id, e.g. 'verifier.platform.foo'.",
                    },
                    "display_name": {
                        "type": "string",
                        "description": "Human-readable name for catalog reads.",
                    },
                    "description": {
                        "type": "string",
                        "default": "",
                        "description": "What this verifier checks.",
                    },
                    "verifier_kind": {
                        "type": "string",
                        "enum": ["builtin", "verification_ref"],
                        "description": "Builtin handler vs. verification_registry-backed verifier.",
                    },
                    "builtin_ref": {
                        "type": "string",
                        "description": (
                            "Required when verifier_kind='builtin' "
                            "(e.g. 'verify_schema_authority'). Must NOT "
                            "be set for kind='verification_ref'."
                        ),
                    },
                    "verification_ref": {
                        "type": "string",
                        "description": (
                            "Required when verifier_kind='verification_ref' "
                            "(FK to verification_registry). Must NOT be set "
                            "for kind='builtin'."
                        ),
                    },
                    "default_inputs": {
                        "type": "object",
                        "description": "Default inputs merged onto every run of this verifier.",
                    },
                    "enabled": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether the runtime should execute this verifier.",
                    },
                    "decision_ref": {
                        "type": "string",
                        "description": "Operator decision ref anchoring why this verifier exists.",
                    },
                    "bind_healer_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional list of existing healer_refs to bind via "
                            "verifier_healer_bindings. Each must already exist "
                            "in healer_registry."
                        ),
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
    "praxis_healer_register": (
        tool_praxis_healer_register,
        {
            "kind": "write",
            "operation_names": ["healer.register"],
            "description": (
                "Register (or update) a healer authority ref without "
                "authoring a SQL migration. Upserts a healer_registry "
                "row idempotently. action_ref names a built-in handler "
                "from runtime.verifier_builtins.run_builtin_healer; "
                "executor_kind is fixed at 'builtin' today (registry "
                "CHECK constraint). auto_mode controls when the runtime "
                "auto-fires (manual / assisted / automatic) and "
                "safety_mode is the replay-safety classifier (guarded / "
                "unsafe). Both default to safest values."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["healer_ref", "display_name", "action_ref", "decision_ref"],
                "properties": {
                    "healer_ref": {
                        "type": "string",
                        "description": "Stable id, e.g. 'healer.platform.foo'.",
                    },
                    "display_name": {
                        "type": "string",
                        "description": "Human-readable name for catalog reads.",
                    },
                    "description": {
                        "type": "string",
                        "default": "",
                        "description": "What this healer repairs.",
                    },
                    "executor_kind": {
                        "type": "string",
                        "enum": ["builtin"],
                        "default": "builtin",
                        "description": "Executor backend. Only 'builtin' supported today.",
                    },
                    "action_ref": {
                        "type": "string",
                        "description": (
                            "Built-in handler ref from "
                            "runtime.verifier_builtins (e.g. "
                            "'heal_schema_bootstrap')."
                        ),
                    },
                    "auto_mode": {
                        "type": "string",
                        "enum": ["manual", "assisted", "automatic"],
                        "default": "manual",
                        "description": "When the runtime auto-fires this healer.",
                    },
                    "safety_mode": {
                        "type": "string",
                        "enum": ["guarded", "unsafe"],
                        "default": "guarded",
                        "description": "Risk classifier for replay safety.",
                    },
                    "enabled": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether the runtime should execute this healer.",
                    },
                    "decision_ref": {
                        "type": "string",
                        "description": "Operator decision ref anchoring why this healer exists.",
                    },
                },
                "additionalProperties": False,
            },
        },
    ),
}
