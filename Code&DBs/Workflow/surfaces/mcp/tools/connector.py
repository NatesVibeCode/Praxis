"""Tools: praxis_connector."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from ..subsystems import REPO_ROOT
from ..subsystems import workflow_database_env


_TEMPLATE_PATH = REPO_ROOT / "config" / "cascade" / "specs" / "W_integration_builder_template.queue.json"
_CONNECTORS_DIR = REPO_ROOT / "artifacts" / "connectors"
_SPECS_DIR = REPO_ROOT / "artifacts" / "workflow" / "integration_builder"


def _slugify(name: str) -> str:
    """Turn 'HubSpot CRM' into 'hubspot_crm'."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip()).strip("_").lower()
    return slug


def _default_auth_docs_url(app_name: str) -> str:
    return f"https://www.google.com/search?q={quote_plus(app_name + ' API documentation')}"


def _default_secret_env_var(app_slug: str) -> str:
    return f"{app_slug.upper().replace('-', '_').replace('.', '_')}_API_TOKEN"


def _stamp_spec(
    app_name: str,
    app_slug: str,
    *,
    auth_docs_url: str = "",
    secret_env_var: str = "",
) -> tuple[str, dict]:
    """Read the template, replace placeholders, write the stamped spec. Return (abs_path, spec)."""
    template_text = _TEMPLATE_PATH.read_text(encoding="utf-8")
    stamped_text = (
        template_text
        .replace("<<INTEGRATION_NAME>>", app_name)
        .replace("<<INTEGRATION_SLUG>>", app_slug)
        .replace("<<AUTH_DOCS_URL>>", auth_docs_url or _default_auth_docs_url(app_name))
        .replace("<<SECRET_ENV_VAR>>", secret_env_var or _default_secret_env_var(app_slug))
    )
    spec = json.loads(stamped_text)

    # Write launch spec under the workflow artifact authority, not retired config/specs.
    os.makedirs(_SPECS_DIR, exist_ok=True)
    launch_path = _SPECS_DIR / f"connector_{app_slug}.queue.json"
    launch_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")

    return str(launch_path), spec


def _launch_workflow(spec_path: str) -> dict[str, Any]:
    """Submit the spec through the workflow runner. Returns run payload or error."""
    from surfaces.mcp.tools.workflow import tool_praxis_workflow
    return tool_praxis_workflow({"action": "run", "spec_path": spec_path, "wait": False})


def _connector_conn():
    from storage.postgres import get_workflow_pool
    from storage.postgres.connection import SyncPostgresConnection

    return SyncPostgresConnection(get_workflow_pool(env=workflow_database_env()))


def _connector_status(app_slug: str) -> dict[str, Any]:
    """Check which artifacts exist for a connector."""
    d = _CONNECTORS_DIR / app_slug
    if not d.is_dir():
        return {"app": app_slug, "exists": False}

    artifacts = {}
    for name in ("client.py", "models.py", "__init__.py", "spec.queue.json"):
        artifacts[name] = (d / name).exists()

    # Phase is based on code files only — research lives in the service bus.
    if artifacts.get("client.py"):
        phase = "built"
    else:
        phase = "pending"

    return {"app": app_slug, "exists": True, "phase": phase, "artifacts": artifacts}


def _register_connector(params: dict) -> dict:
    """Register a built connector into integration_registry so it's callable in workflows."""
    app_slug = (params.get("app_slug") or "").strip()
    if not app_slug:
        return {"error": "app_slug is required for action='register'"}

    status = _connector_status(app_slug)
    if not status.get("exists"):
        return {"error": f"No connector found for '{app_slug}'", "exists": False}
    if status.get("phase") != "built":
        return {"error": f"Connector '{app_slug}' is not fully built yet (phase: {status.get('phase')})"}

    display_name = params.get("app_name") or app_slug.replace("_", " ").title()

    from runtime.integrations.connector_registrar import register_built_connector

    conn = _connector_conn()
    return register_built_connector(app_slug, display_name, conn)


def _verify_connector(params: dict) -> dict:
    """Run capability verification against a live API."""
    app_slug = (params.get("app_slug") or "").strip()
    if not app_slug:
        return {"error": "app_slug is required for action='verify'"}

    from runtime.integrations.connector_verifier import verify_connector

    conn = _connector_conn()
    actions_filter = params.get("actions")
    if isinstance(actions_filter, str):
        actions_filter = [a.strip() for a in actions_filter.split(",") if a.strip()]
    return verify_connector(app_slug, conn, actions=actions_filter)


def tool_praxis_connector(params: dict) -> dict:
    """One front door for API connector builds: stamp, launch, and query."""
    action = params.get("action", "build")

    # ── verify: run capability verification against a live API ────────
    if action == "verify":
        return _verify_connector(params)

    # ── register: promote a built connector to a callable integration ─
    if action == "register":
        return _register_connector(params)

    # ── build: stamp spec + launch workflow in one call ──────────────
    if action == "build":
        app_name = (params.get("app_name") or "").strip()
        if not app_name:
            return {"error": "app_name is required for action='build'"}
        app_slug = params.get("app_slug") or _slugify(app_name)
        auth_docs_url = str(params.get("auth_docs_url") or "").strip()
        secret_env_var = str(params.get("secret_env_var") or "").strip()

        if not _TEMPLATE_PATH.exists():
            return {"error": f"Template not found at {_TEMPLATE_PATH}"}

        spec_path, spec = _stamp_spec(
            app_name,
            app_slug,
            auth_docs_url=auth_docs_url,
            secret_env_var=secret_env_var,
        )
        run_result = _launch_workflow(spec_path)

        return {
            "action": "build",
            "app_name": app_name,
            "app_slug": app_slug,
            "output_dir": f"artifacts/integration_builder_{app_slug}",
            "workflow_spec_path": spec_path,
            "jobs": [j.get("label") for j in spec.get("jobs", [])],
            "workflow": run_result,
        }

    # ── list: show all connectors and their build phase ─────────────
    if action == "list":
        if not _CONNECTORS_DIR.is_dir():
            return {"connectors": [], "count": 0}
        connectors = []
        for child in sorted(_CONNECTORS_DIR.iterdir()):
            if child.is_dir() and not child.name.startswith("."):
                connectors.append(_connector_status(child.name))
        return {"connectors": connectors, "count": len(connectors)}

    # ── get: read code artifacts for a specific connector ───────────
    if action == "get":
        app_slug = (params.get("app_slug") or "").strip()
        if not app_slug:
            return {"error": "app_slug is required for action='get'"}
        status = _connector_status(app_slug)
        if not status["exists"]:
            return {"error": f"No connector found for '{app_slug}'", "exists": False}

        d = _CONNECTORS_DIR / app_slug
        result: dict[str, Any] = {**status}
        for name in ("client.py", "models.py", "__init__.py"):
            p = d / name
            if p.exists():
                text = p.read_text(encoding="utf-8")
                if len(text) > 8000:
                    text = text[:8000] + "\n\n... (truncated)"
                result[name.replace(".", "_")] = text
        return result

    return {"error": f"Unknown action: {action}. Use 'build', 'list', 'get', 'register', or 'verify'."}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_connector": (
        tool_praxis_connector,
        {
            "description": (
                "Build API connectors for third-party applications. One call stamps a workflow spec "
                "and launches a 4-job pipeline (discover API → map objects → build client → review).\n\n"
                "USE WHEN: you want to create a Python API connector for a third-party service, "
                "check on existing connectors, read generated code, or register a built connector "
                "so it's callable from workflow jobs and webhook triggers.\n\n"
                "EXAMPLES:\n"
                "  Build a connector:  praxis_connector(action='build', app_name='Slack')\n"
                "  List connectors:    praxis_connector(action='list')\n"
                "  Read generated code: praxis_connector(action='get', app_slug='slack')\n"
                "  Register for use:   praxis_connector(action='register', app_slug='slack')\n"
                "  Verify capabilities: praxis_connector(action='verify', app_slug='slack')\n\n"
                "OUTPUT: Code lands in artifacts/connectors/<slug>/ (client.py, models.py, __init__.py).\n"
                "Research and review outputs flow through the service bus, not files.\n\n"
                "After build completes, use action='register' to make the connector callable from "
                "workflow jobs via integration_id/integration_action and from webhook triggers.\n\n"
                "DO NOT USE: for general research (use praxis_research), or for managing existing integrations."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["build", "list", "get", "register", "verify"],
                        "default": "build",
                        "description": (
                            "Operation: 'build' (stamp spec + launch workflow), "
                            "'list' (show all connectors and build phase), "
                            "'get' (read a connector's generated code), "
                            "'register' (promote built connector to callable integration), "
                            "'verify' (run capability verification against live API)."
                        ),
                    },
                    "app_name": {
                        "type": "string",
                        "description": (
                            "Display name of the application (e.g. 'HubSpot', 'Stripe', 'Jira'). "
                            "Required for 'build'."
                        ),
                    },
                    "app_slug": {
                        "type": "string",
                        "description": (
                            "Lowercase identifier (e.g. 'hubspot', 'stripe'). Auto-derived from "
                            "app_name if not provided. Required for 'get'."
                        ),
                    },
                    "auth_docs_url": {
                        "type": "string",
                        "description": "Public API documentation URL used by action='build'. Defaults to a web-search URL for the app.",
                    },
                    "secret_env_var": {
                        "type": "string",
                        "description": "Secret env var/keychain service name the generated manifest should use. Defaults to <APP_SLUG>_API_TOKEN.",
                    },
                },
                "x-action-requirements": {
                    "build": {"required": ["app_name"]},
                    "get": {"required": ["app_slug"]},
                    "register": {"required": ["app_slug"]},
                    "verify": {"required": ["app_slug"]},
                },
            },
        },
    ),
}
