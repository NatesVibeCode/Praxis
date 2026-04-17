"""Reference catalog synchronization helpers."""

from __future__ import annotations

import json
import logging
from typing import Any

from runtime.object_schema import list_compiled_object_types

logger = logging.getLogger(__name__)


def sync_reference_catalog(
    conn: Any,
    *,
    integrations: list[dict[str, Any]] | None = None,
    object_types: list[dict[str, Any]] | None = None,
) -> int:
    """Best-effort upsert of catalog entries from live runtime sources."""
    if conn is None:
        return 0

    try:
        integrations = integrations if integrations is not None else _load_integrations(conn)
        object_types = object_types if object_types is not None else _load_object_types(conn)
        agent_routes = _load_agent_routes(conn)
    except Exception as exc:
        logger.warning("reference catalog source load failed: %s", exc)
        return 0

    columns = _reference_catalog_columns(conn)
    if not columns:
        return 0

    rows = [
        *list(_integration_rows(integrations)),
        *list(_object_rows(object_types)),
        *list(_agent_rows(agent_routes)),
    ]
    if not rows:
        return 0

    try:
        schema_column = "schema_def" if "schema_def" in columns else "schema" if "schema" in columns else None
        has_examples = "examples" in columns
        has_updated_at = "updated_at" in columns
        insert_columns = [
            "slug",
            "ref_type",
            "display_name",
            "description",
            "resolved_table",
            "resolved_id",
        ]
        placeholders = ["$1", "$2", "$3", "$4", "$5", "$6"]
        update_assignments = [
            "ref_type = EXCLUDED.ref_type",
            "display_name = EXCLUDED.display_name",
            "description = EXCLUDED.description",
            "resolved_table = EXCLUDED.resolved_table",
            "resolved_id = EXCLUDED.resolved_id",
        ]
        if schema_column:
            insert_columns.append(schema_column)
            placeholders.append(f"${len(placeholders) + 1}::jsonb")
            update_assignments.append(f"{schema_column} = EXCLUDED.{schema_column}")
        if has_examples:
            insert_columns.append("examples")
            placeholders.append(f"${len(placeholders) + 1}::text[]")
            update_assignments.append("examples = EXCLUDED.examples")
        if has_updated_at:
            update_assignments.append("updated_at = NOW()")

        sql = f"""
            INSERT INTO reference_catalog (
                {", ".join(insert_columns)}
            )
            VALUES ({", ".join(placeholders)})
            ON CONFLICT (slug) DO UPDATE SET
                {", ".join(update_assignments)}
        """
        normalized_rows = [_row_for_schema(row, bool(schema_column), has_examples) for row in rows]
        conn.execute_many(
            sql,
            normalized_rows,
        )
    except Exception as exc:
        logger.warning("reference catalog sync failed: %s", exc)
        return 0

    return len(rows)


def _reference_catalog_columns(conn: Any) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_name = 'reference_catalog'
        """
    )
    return {_as_text(row.get("column_name")) for row in (rows or []) if _as_text(row.get("column_name"))}


def _load_integrations(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, provider, capabilities, auth_status, description
          FROM integration_registry
         ORDER BY name
        """
    )
    integrations: list[dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        raw_capabilities = item.get("capabilities")
        if isinstance(raw_capabilities, str):
            try:
                raw_capabilities = json.loads(raw_capabilities)
            except (json.JSONDecodeError, TypeError):
                raw_capabilities = []
        capabilities: list[dict[str, Any]] = []
        for capability in raw_capabilities or []:
            if isinstance(capability, str):
                action = _slugify(capability)
                if action:
                    capabilities.append({"action": action})
                continue
            if isinstance(capability, dict):
                action = _slugify(capability.get("action"))
                if not action:
                    continue
                capabilities.append(
                    {
                        "action": action,
                        "description": _as_text(capability.get("description")),
                        "inputs": capability.get("inputs") if isinstance(capability.get("inputs"), list) else [],
                        "requiredArgs": capability.get("requiredArgs")
                        if isinstance(capability.get("requiredArgs"), list)
                        else [],
                    }
                )
        integrations.append(
            {
                "id": _slugify(item.get("id")),
                "name": _as_text(item.get("name")),
                "provider": _as_text(item.get("provider")),
                "auth_status": _as_text(item.get("auth_status")),
                "description": _as_text(item.get("description")),
                "capabilities": capabilities,
            }
        )
    return integrations


def _load_object_types(conn: Any) -> list[dict[str, Any]]:
    return list_compiled_object_types(conn, limit=1000)


def _load_agent_routes(conn: Any) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT task_type
          FROM task_type_routing
         WHERE permitted = TRUE
         ORDER BY task_type
        """
    )
    return [
        f"auto/{task_type.split('/', 1)[1] if task_type.startswith('auto/') else task_type}"
        for row in (rows or [])
        if (task_type := _slugify(row.get("task_type")))
    ]


def _integration_rows(integrations: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for integration in integrations:
        integration_id = _slugify(integration.get("id"))
        if not integration_id:
            continue
        for capability in integration.get("capabilities") or []:
            action = _slugify(capability.get("action"))
            if not action:
                continue
            slug = f"@{integration_id}/{action}"
            rows.append(
                (
                    slug,
                    "integration",
                    f"{integration.get('name') or integration_id}: {action}",
                    _as_text(capability.get("description")) or _as_text(integration.get("description")),
                    "integration_registry",
                    integration_id,
                    json.dumps(
                        {
                            "integration_id": integration_id,
                            "provider": integration.get("provider"),
                            "action": action,
                            "auth_status": integration.get("auth_status"),
                            "inputs": capability.get("inputs") or [],
                            "required_args": capability.get("requiredArgs") or [],
                        }
                    ),
                    [slug],
                )
            )
    return rows


def _object_rows(object_types: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for object_type in object_types:
        type_id = _slugify(object_type.get("type_id"))
        if not type_id:
            continue
        fields = object_type.get("fields") or []
        base_slug = f"#{type_id}"
        rows.append(
            (
                base_slug,
                "object",
                _as_text(object_type.get("name")) or type_id,
                _as_text(object_type.get("description")),
                "object_types",
                type_id,
                json.dumps({"type_id": type_id, "fields": fields}),
                [base_slug],
            )
        )
        for field in fields:
            field_name = _slugify(field.get("name"))
            if not field_name:
                continue
            slug = f"{base_slug}/{field_name}"
            rows.append(
                (
                    slug,
                    "object",
                    f"{_as_text(object_type.get('name')) or type_id}: {field.get('label') or field_name}",
                    _as_text(field.get("description")) or _as_text(object_type.get("description")),
                    "object_types",
                    type_id,
                    json.dumps(
                        {
                            "type_id": type_id,
                            "field_name": field_name,
                            "field_label": field.get("label"),
                            "field_type": field.get("type"),
                        }
                    ),
                    [slug, base_slug],
                )
            )
    return rows


def _agent_rows(agent_routes: list[str]) -> list[tuple[Any, ...]]:
    return [
        (
            route,
            "agent",
            route,
            f"TaskTypeRouter route for {route}",
            "task_type_routing",
            route.removeprefix("auto/"),
            json.dumps({"route": route}),
            [route],
        )
        for route in agent_routes
    ]


def _row_for_schema(
    row: tuple[Any, ...],
    include_schema: bool,
    include_examples: bool,
) -> tuple[Any, ...]:
    base = list(row[:6])
    if include_schema:
        base.append(row[6])
    if include_examples:
        base.append(row[7])
    return tuple(base)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _slugify(value: Any) -> str:
    return _as_text(value).lower().replace(" ", "-")
