"""Register a built connector into integration_registry so it becomes callable.

Reads artifacts/connectors/{slug}/client.py, introspects the client class to
derive capabilities, then upserts both connector_registry and integration_registry.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from pathlib import Path
from typing import Any

from runtime.workspace_paths import repo_root as workspace_repo_root

logger = logging.getLogger(__name__)

_CONNECTORS_DIR = workspace_repo_root() / "artifacts" / "connectors"


# ── Client introspection (shared with connector_executor) ────────────


def find_client_class(mod: Any) -> type | None:
    """Find the client class in a connector module.

    Convention: single class whose name ends in 'Client'.
    Fallback: first class defined in the module.
    """
    classes = [
        obj for name, obj in inspect.getmembers(mod, inspect.isclass)
        if obj.__module__ == mod.__name__
    ]
    for cls in classes:
        if cls.__name__.endswith("Client"):
            return cls
    return classes[0] if classes else None


# ── Registration ─────────────────────────────────────────────────────


def register_built_connector(
    slug: str,
    display_name: str,
    pg: Any,
) -> dict[str, Any]:
    """Register a built connector into both registries."""
    connector_dir = _CONNECTORS_DIR / slug
    client_path = connector_dir / "client.py"

    if not client_path.exists():
        return {"error": f"No client.py found at {connector_dir}"}

    module_path = f"artifacts.connectors.{slug}.client"

    capabilities = _introspect_capabilities(module_path)
    if not capabilities:
        return {"error": f"No callable methods found in {module_path}"}

    auth_shape = _infer_auth_shape(module_path, slug)

    # Upsert schema registry (api_schemas + api_endpoints)
    schema_id = None
    try:
        from runtime.integrations.connector_registry import upsert_connector_schema
        schema_id = upsert_connector_schema(pg, slug, display_name, capabilities, auth_shape)
    except Exception as exc:
        logger.warning("Failed to write schema registry for %s: %s", slug, exc)

    # Upsert connector_registry
    try:
        from runtime.integrations.connector_registry import register_connector
        register_connector(pg, slug, display_name, module_path=module_path, schema_id=schema_id)
    except Exception as exc:
        return {"error": f"connector_registry upsert failed: {exc}"}

    # Upsert integration_registry
    try:
        from runtime.integrations.integration_registry import upsert_integration
        upsert_integration(
            pg,
            integration_id=slug,
            name=display_name,
            description=f"Generated connector for {display_name}",
            provider="connector",
            capabilities=capabilities,
            auth_status="pending",
            manifest_source="connector",
            connector_slug=slug,
            auth_shape=auth_shape,
        )
    except Exception as exc:
        return {"error": f"integration_registry upsert failed: {exc}"}

    # Generate default verification spec from introspected capabilities
    verification_spec = _generate_default_verification_spec(capabilities)
    try:
        import json as _json
        pg.execute(
            "UPDATE connector_registry SET verification_spec = $2::jsonb WHERE slug = $1",
            slug, _json.dumps(verification_spec),
        )
    except Exception as exc:
        logger.warning("Failed to write verification_spec for %s: %s", slug, exc)

    logger.info("Registered connector %s with %d capabilities", slug, len(capabilities))

    return {
        "registered": True,
        "slug": slug,
        "capabilities": capabilities,
        "auth_shape": auth_shape,
        "verification_spec": verification_spec,
    }


def sync_built_connectors(pg: Any) -> int:
    """Register every built connector artifact that is present on disk.

    This keeps ``connector_registry`` and the derived ``integration_registry``
    projections aligned with built connector artifacts without requiring a
    manual registration call for each one.
    """
    if pg is None or not _CONNECTORS_DIR.is_dir():
        return 0

    registered = 0
    for connector_dir in sorted(_CONNECTORS_DIR.iterdir()):
        if not connector_dir.is_dir() or connector_dir.name.startswith("."):
            continue
        client_path = connector_dir / "client.py"
        if not client_path.exists():
            continue

        slug = connector_dir.name
        display_name = slug.replace("_", " ").title()
        try:
            result = register_built_connector(slug, display_name, pg)
        except Exception as exc:
            logger.warning("connector auto-registration failed for %s: %s", slug, exc)
            continue

        if result.get("registered"):
            registered += 1

    return registered


# ── Introspection helpers ────────────────────────────────────────────


def _introspect_capabilities(module_path: str) -> list[dict[str, str]]:
    """Import the connector module and extract public methods as capabilities."""
    try:
        mod = importlib.import_module(module_path)
    except Exception as exc:
        logger.warning("Failed to import %s for introspection: %s", module_path, exc)
        return []

    client_class = find_client_class(mod)
    if client_class is None:
        return []

    capabilities: list[dict[str, str]] = []
    for method_name, method in inspect.getmembers(client_class, predicate=inspect.isfunction):
        if method_name.startswith("_"):
            continue
        doc = (inspect.getdoc(method) or "").split("\n")[0].strip()
        capabilities.append({
            "action": method_name,
            "description": doc or f"Call {method_name}",
        })

    return capabilities


def _infer_auth_shape(module_path: str, slug: str) -> dict[str, Any]:
    """Infer auth requirements from the client constructor signature."""
    try:
        mod = importlib.import_module(module_path)
    except Exception as exc:
        return {
            "kind": "unknown",
            "required": None,
            "reason": "connector_import_failed",
            "detail": str(exc),
        }

    client_class = find_client_class(mod)
    if client_class is None:
        return {
            "kind": "unknown",
            "required": None,
            "reason": "client_class_not_found",
        }

    sig = inspect.signature(client_class.__init__)
    params = set(sig.parameters.keys()) - {"self"}

    for param_name in ("api_key", "token", "auth_token", "access_token"):
        if param_name in params:
            return {
                "kind": "unknown",
                "required": True,
                "parameter": param_name,
                "reason": "constructor_auth_param_without_declared_secret",
            }

    return {"kind": "none"}


_MUTATING_PREFIXES = ("create_", "update_", "delete_", "remove_", "send_", "post_", "put_", "patch_")


def _generate_default_verification_spec(capabilities: list[dict]) -> list[dict]:
    """Generate a default verification spec from introspected capabilities.

    Read-only actions are active by default. Mutating actions default to skip=true
    to avoid creating test data against live APIs.
    """
    spec = []
    for cap in capabilities:
        action = cap.get("action", "")
        if not action:
            continue
        is_mutating = any(action.startswith(p) for p in _MUTATING_PREFIXES)
        spec.append({
            "action": action,
            "args": {},
            "expect": {"status": "succeeded"},
            "description": cap.get("description") or f"Verify {action}",
            "skip": is_mutating,
        })
    return spec
