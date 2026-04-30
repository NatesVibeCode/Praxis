"""MCP tool for compose-time canonical authority resolution."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_resolve_compose_authority_binding(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.compose_binding.resolve",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_resolve_compose_authority_binding": (
        tool_praxis_resolve_compose_authority_binding,
        {
            "kind": "search",
            "operation_names": ["authority.compose_binding.resolve"],
            "description": (
                "Resolve the compose-time canonical authority binding for a set of target "
                "authority units. Returns the canonical write scope (units the worker may "
                "edit), the read-only predecessor obligation pack (units the worker must "
                "read but not extend), and explicit blocked-compat units. The active "
                "prevention behind the impact contract — when a packet is composed against "
                "this binding, duplicate authority becomes invisible to the worker."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "targets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "unit_kind": {
                                    "type": "string",
                                    "description": (
                                        "One of: operation_ref, authority_object_ref, "
                                        "data_dictionary_object_kind, http_route, mcp_tool, "
                                        "cli_alias, migration_ref, database_object, handler_ref, "
                                        "verifier_ref, event_type, provider_route_ref, source_path."
                                    ),
                                },
                                "unit_ref": {"type": "string"},
                            },
                            "required": ["unit_kind", "unit_ref"],
                        },
                    },
                },
                "required": ["targets"],
            },
        },
    ),
}
