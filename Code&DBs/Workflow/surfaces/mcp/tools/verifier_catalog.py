"""MCP tool wrapping ``verifier.catalog.list`` so agents can discover
registered verifier authority refs without going through the bug-resolve
flow.

Thin gateway wrapper. The CQRS query operation lives in
``runtime.operations.queries.verifier_catalog`` and was registered via
migration 369. This module only adds the MCP-tier binding so agents and
operators can list verifiers (target_kind, suggested healers, enabled
state) directly.

Why this matters: until now the verifier subsystem was internally complete
(``verifier_authority.py``, six built-in verifiers, three healers, full
verification_runs / healing_runs ledger) but only addressable from
``praxis_bugs action=resolve``. Plan-author packets and operators had no
way to ask "what verifiers exist for path-kind targets?" — this wrapper
closes that gap.
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
}
