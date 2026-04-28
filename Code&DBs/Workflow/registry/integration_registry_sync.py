"""Integration registry synchronization helpers.

Static integrations and MCP tool metadata both project into
``integration_registry`` through this one sync path.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import json

import runtime.integration_manifest as integration_manifest
from runtime.integrations.platform import projected_platform_integrations
from surfaces.mcp.catalog import projected_mcp_integrations

logger = logging.getLogger(__name__)


def sync_integration_registry(conn: Any) -> int:
    """Authoritative upsert of static integrations and MCP tool rows."""
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
    if "catalog_dispatch" in columns:
        insert_columns.append("catalog_dispatch")
        placeholders.append(f"${len(placeholders) + 1}")
        update_assignments.append("catalog_dispatch = EXCLUDED.catalog_dispatch")

    sql = f"""
        INSERT INTO integration_registry (
            {", ".join(insert_columns)}
        )
        VALUES ({", ".join(placeholders)})
        ON CONFLICT (id) DO UPDATE SET
            {", ".join(update_assignments)}
    """

    rows, manifest_errors = _all_integration_rows()
    if manifest_errors:
        raise RuntimeError(
            "integration registry sync aborted due to malformed manifest(s): "
            + "; ".join(manifest_errors)
        )

    batch_rows = []
    for integration in rows:
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
        if "catalog_dispatch" in columns:
            row.append(bool(integration.get("catalog_dispatch", False)))
        batch_rows.append(tuple(row))

    try:
        conn.execute_many(sql, batch_rows)
    except Exception as exc:
        logger.warning("integration registry sync failed: %s", exc)
        return 0

    return len(batch_rows)


def _all_integration_rows() -> tuple[list[dict[str, Any]], list[str]]:
    rows = list(projected_platform_integrations())
    manifest_errors: list[str] = []
    try:
        manifest_dir = Path(getattr(integration_manifest, "_MANIFEST_DIR"))
        report = integration_manifest.load_manifest_report(manifest_dir)
        rows.extend(
            integration_manifest.manifest_to_registry_row(manifest)
            for manifest in report.manifests
        )
        manifest_errors.extend(report.errors)
    except Exception as exc:
        manifest_errors.append(f"manifest directory load failed: {type(exc).__name__}: {exc}")
    try:
        rows.extend(projected_mcp_integrations())
    except Exception as exc:
        logger.warning("mcp tool catalog projection failed: %s", exc)
    return rows, manifest_errors


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
