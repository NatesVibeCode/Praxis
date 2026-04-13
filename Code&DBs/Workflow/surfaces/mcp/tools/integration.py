"""Tools: praxis_integration."""
from __future__ import annotations

import json
from typing import Any

from storage.postgres import get_workflow_pool
from storage.postgres.connection import SyncPostgresConnection


def tool_praxis_integration(params: dict) -> dict:
    """Call, list, or describe registered integrations."""
    action = params.get("action", "list")
    conn = SyncPostgresConnection(get_workflow_pool())

    if action == "call":
        return _call_integration(params, conn)
    if action == "list":
        return _list(conn)
    if action == "describe":
        return _describe(params, conn)
    if action == "test_credentials":
        return _test_credentials(params, conn)
    if action == "health":
        return _health(conn)

    return {"error": f"Unknown action: {action}. Use 'call', 'list', 'describe', 'test_credentials', or 'health'."}


# ── Thin dispatchers calling registry functions ──────────────────────


def _call_integration(params: dict, conn: SyncPostgresConnection) -> dict:
    integration_id = (params.get("integration_id") or "").strip()
    integration_action = (params.get("integration_action") or "").strip()
    if not integration_id:
        return {"error": "integration_id is required"}
    if not integration_action:
        return {"error": "integration_action is required"}

    args = params.get("args") or {}
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except (json.JSONDecodeError, TypeError):
            args = {}

    from runtime.integrations import execute_integration
    return execute_integration(integration_id, integration_action, args, conn)


def _list(conn: SyncPostgresConnection) -> dict:
    from runtime.integrations.integration_registry import list_integrations
    integrations = list_integrations(conn)
    return {"integrations": integrations, "count": len(integrations)}


def _describe(params: dict, conn: SyncPostgresConnection) -> dict:
    integration_id = (params.get("integration_id") or "").strip()
    if not integration_id:
        return {"error": "integration_id is required"}

    from runtime.integrations.integration_registry import describe_integration
    result = describe_integration(conn, integration_id)
    if result is None:
        return {"error": f"Integration '{integration_id}' not found"}
    return result


def _health(conn: SyncPostgresConnection) -> dict:
    from runtime.integrations.integration_registry import health_overview
    return health_overview(conn)


def _test_credentials(params: dict, conn: SyncPostgresConnection) -> dict:
    """Validate credentials exist and aren't expired without making an API call."""
    integration_id = (params.get("integration_id") or "").strip()
    if not integration_id:
        return {"error": "integration_id is required"}

    from runtime.integrations.integration_registry import load_authority, parse_jsonb

    definition = load_authority(conn, integration_id)
    if definition is None:
        return {"error": f"Integration '{integration_id}' not found"}

    auth_shape = parse_jsonb(definition.get("auth_shape"))
    auth_kind = str(auth_shape.get("kind", "")).strip().lower()

    if auth_kind in ("none", "anonymous", ""):
        return {"integration_id": integration_id, "credential_status": "valid", "detail": "No authentication required"}

    from runtime.integration_manifest import resolve_token
    try:
        token = resolve_token(auth_shape, conn, integration_id)
    except Exception as exc:
        return {"integration_id": integration_id, "credential_status": "error", "detail": f"Resolution raised: {exc}"}

    if token is None:
        env_var = auth_shape.get("env_var", "")
        credential_ref = auth_shape.get("credential_ref", "")
        hints = []
        if env_var:
            hints.append(f"env var '{env_var}' not set")
        if credential_ref:
            hints.append(f"credential_ref '{credential_ref}' not found")
        return {
            "integration_id": integration_id,
            "credential_status": "missing",
            "detail": "; ".join(hints) if hints else f"No credential found for auth kind '{auth_kind}'",
        }

    # Check OAuth expiry
    if auth_kind == "oauth2":
        try:
            expiry_rows = conn.execute(
                """SELECT expires_at FROM credential_tokens
                     WHERE integration_id = $1 AND token_kind = 'access'
                     ORDER BY updated_at DESC LIMIT 1""",
                integration_id,
            )
            if expiry_rows and expiry_rows[0].get("expires_at"):
                from datetime import datetime, timezone
                expires_at = expiry_rows[0]["expires_at"]
                if hasattr(expires_at, "tzinfo") and expires_at < datetime.now(timezone.utc):
                    return {
                        "integration_id": integration_id,
                        "credential_status": "expired",
                        "detail": f"OAuth token expired at {expires_at.isoformat()}",
                    }
        except Exception:
            pass

    return {"integration_id": integration_id, "credential_status": "valid", "detail": f"Credential resolved via {auth_kind}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_integration": (
        tool_praxis_integration,
        {
            "description": (
                "Call, list, or describe registered integrations (API connectors, webhooks, "
                "and other external services).\n\n"
                "USE WHEN: you need to call an external API, send a webhook, list available "
                "integrations, or check what actions a connector supports.\n\n"
                "EXAMPLES:\n"
                "  List available:     praxis_integration(action='list')\n"
                "  Describe one:       praxis_integration(action='describe', integration_id='stripe')\n"
                "  Call an action:     praxis_integration(action='call', integration_id='stripe', "
                "integration_action='list_payments', args={'limit': 10})\n\n"
                "Integrations are registered via praxis_connector (build + register) or TOML manifests. "
                "Use action='list' to discover what's available before calling.\n\n"
                "DO NOT USE: for workflow dispatch (use praxis_workflow), or for building new "
                "connectors (use praxis_connector)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["call", "list", "describe", "test_credentials", "health"],
                        "default": "list",
                        "description": (
                            "Operation: 'call' (execute an integration action), "
                            "'list' (show available integrations), "
                            "'describe' (get details about one integration), "
                            "'test_credentials' (validate credentials without calling), "
                            "'health' (all-integrations health overview)."
                        ),
                    },
                    "integration_id": {
                        "type": "string",
                        "description": (
                            "Integration identifier (e.g. 'stripe', 'slack'). "
                            "Required for 'call', 'describe', and 'test_credentials'."
                        ),
                    },
                    "integration_action": {
                        "type": "string",
                        "description": (
                            "Action to execute (e.g. 'list_payments', 'send_message'). "
                            "Required for 'call'. Use 'describe' to see available actions."
                        ),
                    },
                    "args": {
                        "type": "object",
                        "description": (
                            "Arguments to pass to the integration action. "
                            "Structure depends on the specific action."
                        ),
                    },
                },
            },
        },
    ),
}
