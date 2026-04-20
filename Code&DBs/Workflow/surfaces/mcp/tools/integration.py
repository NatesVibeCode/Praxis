"""Tools: praxis_integration."""
from __future__ import annotations

import json
from typing import Any

from ..subsystems import workflow_database_env
from storage.postgres import get_workflow_pool
from storage.postgres.connection import SyncPostgresConnection


def _integration_conn() -> SyncPostgresConnection:
    return SyncPostgresConnection(get_workflow_pool(env=workflow_database_env()))


def tool_praxis_integration(params: dict) -> dict:
    """Call, list, describe, create, or manage registered integrations."""
    action = params.get("action", "list")
    conn = _integration_conn()

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
    if action == "create":
        return _create(params, conn)
    if action == "set_secret":
        return _set_secret(params, conn)
    if action == "reload":
        return _reload(conn)

    return {"error": f"Unknown action: {action}. Use 'call', 'list', 'describe', 'test_credentials', 'health', 'create', 'set_secret', or 'reload'."}


# ── Thin wrappers calling registry functions ─────────────────────────


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


def _create(params: dict, conn: SyncPostgresConnection) -> dict:
    """Create a DB-native integration row. Same validation surface as POST /api/integrations."""
    from surfaces.api.handlers.integrations_admin import (
        _VALID_ID_RE,
        _normalize_auth,
        _normalize_capabilities,
    )
    from runtime.integrations.integration_registry import upsert_integration

    integration_id = str(params.get("integration_id", "")).strip()
    if not _VALID_ID_RE.match(integration_id):
        return {"error": "integration_id must be 2-128 chars of [a-zA-Z0-9._-], start/end alphanumeric"}

    name = str(params.get("name", "")).strip()
    if not name:
        return {"error": "name is required"}

    caps, cap_err = _normalize_capabilities(params.get("capabilities"))
    if cap_err:
        return {"error": cap_err}

    auth_shape, auth_err = _normalize_auth(params.get("auth"))
    if auth_err:
        return {"error": auth_err}

    upsert_integration(
        conn,
        integration_id=integration_id,
        name=name,
        description=str(params.get("description", "")),
        provider=str(params.get("provider", "http")).strip() or "http",
        capabilities=caps,
        auth_status="connected" if auth_shape.get("kind") == "none" else "pending",
        manifest_source=str(params.get("manifest_source", "mcp")).strip() or "mcp",
        connector_slug=None,
        auth_shape=auth_shape,
    )

    return {
        "integration_id": integration_id,
        "status": "created",
        "capabilities": [c["action"] for c in caps],
        "auth_shape": auth_shape,
    }


def _set_secret(params: dict, conn: SyncPostgresConnection) -> dict:
    """Store a secret in the macOS Keychain under service=praxis."""
    integration_id = str(params.get("integration_id", "")).strip()
    value = str(params.get("value", ""))
    if not integration_id:
        return {"error": "integration_id is required"}
    if not value:
        return {"error": "value is required"}

    from runtime.integrations.integration_registry import load_authority, parse_jsonb
    definition = load_authority(conn, integration_id)
    if definition is None:
        return {"error": f"integration {integration_id!r} not found"}

    auth_shape = parse_jsonb(definition.get("auth_shape"))
    env_var = str(auth_shape.get("env_var", "")).strip()
    if not env_var:
        return {"error": "integration has no auth.env_var — nothing to store"}

    from adapters.keychain import keychain_set
    ok = keychain_set(env_var, value)
    if not ok:
        return {
            "error": "keychain_set unavailable (non-Darwin or security CLI failed)",
            "hint": f"set env var {env_var} via .env or environment variable as fallback",
        }

    try:
        conn.execute(
            "UPDATE integration_registry SET auth_status = 'connected', updated_at = now() WHERE id = $1",
            integration_id,
        )
    except Exception:
        pass

    return {"integration_id": integration_id, "env_var": env_var, "stored": True}


def _reload(conn: SyncPostgresConnection) -> dict:
    from registry.integration_registry_sync import sync_integration_registry
    n = sync_integration_registry(conn)
    return {"synced": n}


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
                "DO NOT USE: for workflow launch (use praxis_workflow), or for building new "
                "connectors (use praxis_connector)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "call",
                            "list",
                            "describe",
                            "test_credentials",
                            "health",
                            "create",
                            "set_secret",
                            "reload",
                        ],
                        "default": "list",
                        "description": (
                            "Operation: 'call' (execute an integration action), "
                            "'list' (show available integrations), "
                            "'describe' (get details about one integration), "
                            "'test_credentials' (validate credentials without calling), "
                            "'health' (all-integrations health overview), "
                            "'create' (register a new DB-native integration from a JSON spec), "
                            "'set_secret' (store the integration's env_var secret in the macOS Keychain), "
                            "'reload' (re-run sync_integration_registry to refresh static + manifest rows)."
                        ),
                    },
                    "name": {
                        "type": "string",
                        "description": "Display name. Required for 'create'.",
                    },
                    "description": {
                        "type": "string",
                        "description": "Integration description. Optional for 'create'.",
                    },
                    "provider": {
                        "type": "string",
                        "description": "Provider label (e.g. 'http', 'stripe'). Optional for 'create', defaults to 'http'.",
                    },
                    "capabilities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {"type": "string"},
                                "description": {"type": "string"},
                                "method": {"type": "string"},
                                "path": {"type": "string"},
                                "body_template": {"type": "object"},
                                "response_extract": {"type": "string"},
                            },
                            "required": ["action"],
                        },
                        "description": "List of capabilities. Required for 'create'.",
                    },
                    "auth": {
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string", "enum": ["none", "env_var", "api_key", "oauth2"]},
                            "env_var": {"type": "string"},
                            "credential_ref": {"type": "string"},
                            "scopes": {"type": "array", "items": {"type": "string"}},
                            "token_url": {"type": "string"},
                            "authorize_url": {"type": "string"},
                        },
                        "description": "Auth shape. Required for 'create' unless kind is 'none'.",
                    },
                    "value": {
                        "type": "string",
                        "description": "Secret value. Required for 'set_secret'.",
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
