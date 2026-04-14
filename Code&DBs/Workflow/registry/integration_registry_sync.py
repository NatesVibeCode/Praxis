"""Integration registry synchronization helpers.

Static integrations and MCP tool metadata both project into
``integration_registry`` through this one sync path.
"""

from __future__ import annotations

import logging
from typing import Any

import json

from surfaces.mcp.catalog import projected_mcp_integrations

logger = logging.getLogger(__name__)


_STATIC_INTEGRATIONS: list[dict[str, Any]] = [
    {
        "id": "praxis-dispatch",
        "name": "Praxis Dispatch",
        "description": "Submit workflow jobs, inspect status, and search receipts.",
        "provider": "praxis",
        "capabilities": [
            {
                "action": "dispatch_job",
                "description": "Submit a workflow job to the Praxis Engine runtime",
            },
            {
                "action": "check_status",
                "description": "Inspect the status of an existing workflow run.",
            },
            {
                "action": "search_receipts",
                "description": "Search historical workflow runs and receipts.",
            },
        ],
        "auth_status": "connected",
        "icon": "bolt",
        "mcp_server_id": None,
    },
    {
        "id": "notifications",
        "name": "Notifications",
        "description": "Send notification messages through the platform notification channel.",
        "provider": "praxis",
        "capabilities": [
            {"action": "send", "description": "Send a notification message."},
        ],
        "auth_status": "connected",
        "icon": "bell",
        "mcp_server_id": None,
    },
    {
        "id": "webhook",
        "name": "Webhook",
        "description": "Post structured payloads to external HTTP endpoints.",
        "provider": "http",
        "capabilities": [
            {"action": "post", "description": "POST a payload to an external HTTP endpoint."},
        ],
        "auth_status": "connected",
        "icon": "webhook",
        "mcp_server_id": None,
    },
    {
        "id": "workflow",
        "name": "Workflow",
        "description": "Invoke registered workflows from the runtime control plane.",
        "provider": "praxis",
        "capabilities": [
            {"action": "invoke", "description": "Invoke a registered workflow by workflow id."},
        ],
        "auth_status": "connected",
        "icon": "workflow",
        "mcp_server_id": None,
    },
]


def sync_integration_registry(conn: Any) -> int:
    """Best-effort upsert of static integrations and MCP tool rows."""
    if conn is None:
        return 0

    try:
        columns = _integration_registry_columns(conn)
    except Exception as exc:
        logger.warning("integration registry source load failed: %s", exc)
        return 0

    required = {"id", "name", "description", "provider", "capabilities", "auth_status"}
    if not required.issubset(columns):
        return 0

    insert_columns = ["id", "name", "description", "provider", "capabilities", "auth_status"]
    placeholders = ["$1", "$2", "$3", "$4", "$5::jsonb", "$6"]
    update_assignments = [
        "name = EXCLUDED.name",
        "description = EXCLUDED.description",
        "provider = EXCLUDED.provider",
        "capabilities = EXCLUDED.capabilities",
        "auth_status = EXCLUDED.auth_status",
    ]

    if "icon" in columns:
        insert_columns.append("icon")
        placeholders.append(f"${len(placeholders) + 1}")
        update_assignments.append("icon = EXCLUDED.icon")
    if "mcp_server_id" in columns:
        insert_columns.append("mcp_server_id")
        placeholders.append(f"${len(placeholders) + 1}")
        update_assignments.append("mcp_server_id = EXCLUDED.mcp_server_id")
    if "manifest_source" in columns:
        insert_columns.append("manifest_source")
        placeholders.append(f"${len(placeholders) + 1}")
        update_assignments.append("manifest_source = EXCLUDED.manifest_source")
    if "auth_shape" in columns:
        insert_columns.append("auth_shape")
        placeholders.append(f"${len(placeholders) + 1}::jsonb")
        update_assignments.append("auth_shape = EXCLUDED.auth_shape")
    if "endpoint_templates" in columns:
        insert_columns.append("endpoint_templates")
        placeholders.append(f"${len(placeholders) + 1}::jsonb")
        update_assignments.append("endpoint_templates = EXCLUDED.endpoint_templates")

    sql = f"""
        INSERT INTO integration_registry (
            {", ".join(insert_columns)}
        )
        VALUES ({", ".join(placeholders)})
        ON CONFLICT (id) DO UPDATE SET
            {", ".join(update_assignments)}
    """

    rows = []
    for integration in _all_integration_rows():
        row = [
            integration["id"],
            integration["name"],
            integration["description"],
            integration["provider"],
            json.dumps(integration["capabilities"]),
            integration["auth_status"],
        ]
        if "icon" in columns:
            row.append(integration["icon"])
        if "mcp_server_id" in columns:
            row.append(integration.get("mcp_server_id"))
        if "manifest_source" in columns:
            row.append(integration.get("manifest_source", "static"))
        if "auth_shape" in columns:
            row.append(json.dumps(integration.get("auth_shape", {})))
        if "endpoint_templates" in columns:
            row.append(json.dumps(integration.get("endpoint_templates", {})))
        rows.append(tuple(row))

    try:
        conn.execute_many(sql, rows)
    except Exception as exc:
        logger.warning("integration registry sync failed: %s", exc)
        return 0

    return len(rows)


def _all_integration_rows() -> list[dict[str, Any]]:
    rows = list(_STATIC_INTEGRATIONS)
    try:
        from runtime.integration_manifest import load_manifests, manifest_to_registry_row

        for manifest in load_manifests():
            rows.append(manifest_to_registry_row(manifest))
    except Exception as exc:
        logger.warning("manifest loading failed: %s", exc)
    try:
        rows.extend(projected_mcp_integrations())
    except Exception as exc:
        logger.warning("mcp tool catalog projection failed: %s", exc)
    return rows


def _integration_registry_columns(conn: Any) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_name = 'integration_registry'
        """
    )
    return {
        str(row.get("column_name"))
        for row in (rows or [])
        if row.get("column_name")
    }


__all__ = ["sync_integration_registry"]
