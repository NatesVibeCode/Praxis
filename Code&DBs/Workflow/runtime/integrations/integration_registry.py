"""Postgres-backed integration registry — identity, capabilities, and health queries.

All reads and writes to the ``integration_registry`` table go through this
module.  Executors, MCP surfaces, and registrars import from here instead
of writing inline SQL.
"""

from __future__ import annotations

import json
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


# ── JSON helpers ─────────────────────────────────────────────────────


def parse_jsonb(value: Any) -> Any:
    """Parse a JSONB value that may arrive as a string, dict, or list."""
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def normalize_capabilities(raw_capabilities: object) -> list[dict[str, Any]]:
    """Normalize capabilities into a list of ``{action: str, ...}`` dicts."""
    if isinstance(raw_capabilities, str):
        try:
            raw_capabilities = json.loads(raw_capabilities)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(raw_capabilities, list):
        return []

    capabilities: list[dict[str, Any]] = []
    for item in raw_capabilities:
        if isinstance(item, str):
            action = item.strip()
            if action:
                capabilities.append({"action": action})
            continue
        if isinstance(item, dict) and isinstance(item.get("action"), str) and item.get("action", "").strip():
            capabilities.append(dict(item))
    return capabilities


def extract_actions(capabilities: Any) -> list[dict[str, str]]:
    """Extract ``[{action, description}]`` from a capabilities list."""
    caps = parse_jsonb(capabilities) if not isinstance(capabilities, list) else capabilities
    if not isinstance(caps, list):
        return []
    actions: list[dict[str, str]] = []
    for cap in caps:
        if isinstance(cap, dict) and cap.get("action"):
            actions.append({"action": cap["action"], "description": cap.get("description", "")})
        elif isinstance(cap, str):
            actions.append({"action": cap, "description": ""})
    return actions


def find_capability(definition: dict[str, Any], action: str) -> dict[str, Any] | None:
    """Find a single capability entry by action name."""
    capabilities = definition.get("capabilities") or []
    if isinstance(capabilities, str):
        try:
            capabilities = json.loads(capabilities)
        except (json.JSONDecodeError, TypeError):
            return None
    for cap in capabilities:
        if isinstance(cap, dict) and cap.get("action") == action:
            return cap
    return None


# ── Authority lookup ─────────────────────────────────────────────────


def load_authority(conn: "SyncPostgresConnection", integration_id: str) -> dict[str, Any] | None:
    """Load a single integration definition with normalized capabilities."""
    rows = conn.execute(
        """SELECT id, name, description, provider, capabilities, auth_status, icon,
                  mcp_server_id, manifest_source, auth_shape, endpoint_templates,
                  catalog_dispatch, connector_slug
             FROM integration_registry
            WHERE id = $1
            LIMIT 1""",
        integration_id,
    )
    if not rows:
        return None
    row = dict(rows[0])
    row["capabilities"] = normalize_capabilities(row.get("capabilities"))
    return row


# ── List / describe / health ─────────────────────────────────────────


def list_integrations(conn: "SyncPostgresConnection") -> list[dict[str, Any]]:
    """List all connected/pending integrations with connector health."""
    rows = conn.execute(
        """SELECT ir.id, ir.name, ir.description, ir.provider, ir.capabilities,
                  ir.auth_status, ir.manifest_source, ir.connector_slug,
                  cr.health_status, cr.error_rate
             FROM integration_registry ir
             LEFT JOIN connector_registry cr ON cr.slug = ir.connector_slug
            WHERE ir.auth_status IN ('connected', 'pending')
            ORDER BY ir.id""",
    )
    integrations: list[dict[str, Any]] = []
    for row in rows or []:
        integrations.append({
            "id": row["id"],
            "name": row.get("name", row["id"]),
            "description": row.get("description", ""),
            "provider": row.get("provider", ""),
            "auth_status": row.get("auth_status", ""),
            "source": row.get("manifest_source", ""),
            "health_status": row.get("health_status"),
            "error_rate": row.get("error_rate"),
            "actions": extract_actions(row.get("capabilities")),
        })
    return integrations


def describe_integration(conn: "SyncPostgresConnection", integration_id: str) -> dict[str, Any] | None:
    """Full integration details including connector health."""
    rows = conn.execute(
        """SELECT ir.id, ir.name, ir.description, ir.provider, ir.capabilities,
                  ir.auth_status, ir.manifest_source, ir.connector_slug,
                  ir.auth_shape, ir.icon,
                  cr.health_status, cr.error_rate, cr.total_calls, cr.total_errors,
                  cr.last_health_check, cr.last_call_at, cr.last_success_at, cr.last_error_at
             FROM integration_registry ir
             LEFT JOIN connector_registry cr ON cr.slug = ir.connector_slug
            WHERE ir.id = $1
            LIMIT 1""",
        integration_id,
    )
    if not rows:
        return None

    row = rows[0]
    caps = parse_jsonb(row.get("capabilities"))
    auth_shape = parse_jsonb(row.get("auth_shape"))

    result: dict[str, Any] = {
        "id": row["id"],
        "name": row.get("name", row["id"]),
        "description": row.get("description", ""),
        "provider": row.get("provider", ""),
        "auth_status": row.get("auth_status", ""),
        "source": row.get("manifest_source", ""),
        "connector_slug": row.get("connector_slug"),
        "auth_kind": auth_shape.get("kind", ""),
        "capabilities": caps if isinstance(caps, list) else [],
    }

    if row.get("health_status") is not None:
        result["health"] = {
            "status": row["health_status"],
            "error_rate": row.get("error_rate"),
            "total_calls": row.get("total_calls", 0),
            "total_errors": row.get("total_errors", 0),
            "last_call": _isoformat(row.get("last_call_at")),
            "last_success": _isoformat(row.get("last_success_at")),
            "last_error": _isoformat(row.get("last_error_at")),
        }

    return result


def health_overview(conn: "SyncPostgresConnection") -> dict[str, Any]:
    """All-integrations health grouped by status."""
    rows = conn.execute(
        """SELECT ir.id, ir.name, ir.auth_status,
                  cr.health_status, cr.error_rate, cr.total_calls, cr.total_errors,
                  cr.last_call_at
             FROM integration_registry ir
             LEFT JOIN connector_registry cr ON cr.slug = ir.connector_slug
            WHERE ir.auth_status IN ('connected', 'pending')
            ORDER BY ir.id""",
    )

    healthy, degraded, unhealthy, unknown = [], [], [], []
    for row in rows or []:
        entry = {
            "id": row["id"],
            "name": row.get("name", row["id"]),
            "health_status": row.get("health_status"),
            "error_rate": row.get("error_rate"),
            "total_calls": row.get("total_calls", 0),
            "last_call": _isoformat(row.get("last_call_at")),
        }
        error_rate = row.get("error_rate")
        health = row.get("health_status")

        if health is None and error_rate is None:
            unknown.append(entry)
        elif error_rate is not None and error_rate > 0.5:
            unhealthy.append(entry)
        elif error_rate is not None and error_rate > 0.1:
            degraded.append(entry)
        elif health == "degraded":
            degraded.append(entry)
        else:
            healthy.append(entry)

    return {
        "summary": {
            "healthy": len(healthy),
            "degraded": len(degraded),
            "unhealthy": len(unhealthy),
            "unknown": len(unknown),
        },
        "integrations": healthy + degraded + unhealthy + unknown,
    }


# ── Writes ───────────────────────────────────────────────────────────


def upsert_integration(
    conn: "SyncPostgresConnection",
    *,
    integration_id: str,
    name: str,
    description: str,
    provider: str,
    capabilities: list[dict[str, Any]],
    auth_status: str = "pending",
    manifest_source: str = "connector",
    connector_slug: str | None = None,
    auth_shape: dict[str, Any] | None = None,
) -> None:
    """Insert or update an integration_registry row."""
    conn.execute(
        """INSERT INTO integration_registry
               (id, name, description, provider, capabilities, auth_status,
                manifest_source, connector_slug, auth_shape)
           VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::jsonb)
           ON CONFLICT (id) DO UPDATE SET
               name = EXCLUDED.name,
               capabilities = EXCLUDED.capabilities::jsonb,
               manifest_source = EXCLUDED.manifest_source,
               connector_slug = EXCLUDED.connector_slug,
               auth_shape = EXCLUDED.auth_shape::jsonb,
               updated_at = now()""",
        integration_id,
        name,
        description,
        provider,
        json.dumps(capabilities),
        auth_status,
        manifest_source,
        connector_slug,
        json.dumps(auth_shape or {}),
    )


# ── Manifest conversion ─────────────────────────────────────────────


def manifest_to_registry_row(manifest: Any) -> dict[str, Any]:
    """Convert an IntegrationManifest dataclass to an integration_registry upsert row."""
    caps = []
    for cap in manifest.capabilities:
        entry: dict[str, Any] = {"action": cap.action, "description": cap.description}
        entry["method"] = cap.method
        if cap.path:
            entry["path"] = cap.path
        if cap.body_template:
            entry["body_template"] = cap.body_template
        if cap.response_extract:
            entry["response_extract"] = cap.response_extract
        caps.append(entry)

    return {
        "id": manifest.id,
        "name": manifest.name,
        "description": manifest.description,
        "provider": manifest.provider,
        "capabilities": caps,
        "auth_status": "connected",
        "icon": manifest.icon,
        "mcp_server_id": None,
        "manifest_source": "manifest",
        "auth_shape": {
            "kind": manifest.auth_shape.kind,
            "credential_ref": manifest.auth_shape.credential_ref,
            "env_var": manifest.auth_shape.env_var,
            "scopes": list(manifest.auth_shape.scopes),
            "token_url": manifest.auth_shape.token_url,
            "authorize_url": manifest.auth_shape.authorize_url,
        },
        "endpoint_templates": {
            cap.action: cap.path for cap in manifest.capabilities if cap.path
        },
    }


# ── Private helpers ──────────────────────────────────────────────────


def _isoformat(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)
